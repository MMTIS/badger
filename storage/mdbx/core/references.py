import logging

from mdbx import MDBXCursorOp, MDBXDBFlags

from domain.netex.indexes.inverse_class import collect_classes_index
from utils.aux_logging import log_all
from domain.netex.services.model_typing import Tid
from domain.netex.model import EntityStructure, NameOfClass
from domain.netex.services.recursive_attributes import only_reference_objects, only_embedding, embedding_obj_iter
from domain.utils import get_object_name
from storage.interface import Serializer

from storage.mdbx.core.implementation import (
    MdbxStorage,
    DB_UNRESOLVED,
    DB_REFERENCE_OUTWARD,
    DB_ID_IDX,
    DB_EMBEDDED_ID_IDX,
    DB_UNRESOLVED_FLAGS,
    DB_REFERENCE_OUTWARD_FLAGS,
    DB_ID_IDX_FLAGS,
    DB_EMBEDDED_ID_IDX_FLAGS,
)
from mdbx.mdbx import TXN
from typing import Optional, Generator


def resolve_embeddings_iterable(
    storage: MdbxStorage, txn: TXN, clazz: type[EntityStructure], interesting_classes: Optional[set[Tid]] = None, ignore: Optional[set[Tid]] = None
) -> Generator[tuple[bytes, type[EntityStructure], type[EntityStructure]], None, None]:
    """
    In resolve_embeddings we are creating a lookup from an existing instance to the location an embedded object remains.
    Hence, it is not 'you can find this embedded object there' but 'this object has a relationship with that object'.
    When exporting or processing data for a specific profile, we may be interested in the exact locations of these
    references. An example could be a DayType, which may be part of a ServiceCalendar.

    If for every lookup we must deserialise the entire database this is unfeasible. The problem, taking the not naive
    approaches:

    1. When we would store the identifiers of all possible objects embedded within this specific object,
    this potentially causes a huge table. With O(1) access via ids, with the chance we would never ever
    require such individual access ever. The further downside is that upon insert we must maintain such
    table, hence for every write such thing must be checked.

    2. When we would consider embedded object "not a good fit" we could deembed them, regardless of the NeTEx-schema
    allowing it to be a first class object. We could rewrite the original object to take a reference (if possible).
    If the reference is possible we could cleanly store the deembedded object, and manipulate it. If not, we must
    update the new location, and the initial embedding.

    3. When we would do our computation using collect_classes_index and get the reverse index of all potential
    objects and would iterate over all potential "parent" candidates, which are already limited to
    "just iterate over all objects" would still cause the effect that for every individual query we would
    deserialise a parent-type. Hence, if we would be looking for Quays and later Entrances, we would be serialising
    StopPlaces twice. We could overcome this by registering which object types we are interested in, do the full
    scan (limited to the collect_classes_index via the classes of interest) and then either create a lookup or
    a direct materialised view. The latter assumes we are not changing the embedded object, but only query it once.
    """

    for key, obj in storage.iter_objects(txn, clazz):
        for candidate in embedding_obj_iter(storage.serializer, obj, interesting_classes, ignore):
            yield key, obj, candidate


def resolve(storage: MdbxStorage) -> None:
    log_all(logging.INFO, "[resolve] resolving references")
    if storage.readonly:
        raise

    # TODO: get exclusively from KeyCodec
    separator = bytes([10])

    seen_count = 0

    with storage.env.rw_transaction() as txn:
        db_unresolved = txn.open_map(DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
        db_id_idx = txn.open_map(DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
        db_reference_forward = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)

        log_all(logging.INFO, f"[unresolved references] {db_unresolved.get_stat(txn).ms_entries}")

        unresolved_cursor = txn.cursor(db=db_unresolved)
        for it in unresolved_cursor.iter_dupsort_rows():
            references_to_fix: list[tuple] = []

            for idx, value in it:
                seen_count += 1
                if seen_count % 1_000_000 == 0:
                    log_all(logging.INFO, f"[resolve] {seen_count} references processed...")
                parts: list[str] | None = None
                prefix: str | None = None
                resolved_idx = db_id_idx.get(txn, value)  # This will be the id + version + class check
                class_change = False
                version_change = False
                if not resolved_idx:
                    cursor = txn.cursor(db=db_id_idx)

                    parts = storage.serializer.split_key(value)
                    class_part = separator + parts[-1]

                    # Alternative 1, id + version exists, class does not match
                    parts.pop()
                    prefix = separator.join(parts) + separator
                    for check_key, check_idx in cursor.iter(prefix):
                        if check_key.startswith(prefix):
                            class_change = check_idx
                            resolved_idx = check_idx
                        break

                    if not resolved_idx:
                        # Alternative 2, id exists
                        parts.pop()

                        prefix = separator.join(parts) + separator
                        for check_key, check_idx in cursor.iter(prefix):
                            if check_key.startswith(prefix) and check_key.endswith(class_part):
                                class_change = False
                                version_change = check_idx
                                resolved_idx = check_idx
                                break

                            elif check_key.startswith(prefix):
                                class_change = check_idx
                                version_change = check_idx
                                resolved_idx = check_idx
                                # Continue to find a better match

                            else:
                                break

                if resolved_idx:
                    if version_change or class_change:
                        references_to_fix.append((resolved_idx, value, version_change, class_change))

                    # f = storage.load_object_by_full_key(txn, idx)
                    # t = storage.load_object_by_full_key(txn, resolved_idx)

                    # print(f"{f.id} {f.__class__} -> {t.id} {t.__class__}")

                    db_reference_forward.put(txn, idx, resolved_idx)
                    unresolved_cursor.delete(MDBXCursorOp.MDBX_PREV)

                # else:
                #    print("unresolved", value, idx)

            # In this situation the original reference was incomplete
            if len(references_to_fix) > 0:
                referencing_class_idx, referencing_key = Serializer.full_key_to_clazz_idx(idx)
                referencing_class = storage.idx_class[referencing_class_idx]
                referencing_obj: Tid = storage.load_object(txn, referencing_class, referencing_key)

                for resolved_idx, value, version_change, class_change in references_to_fix:
                    referenced_class_idx, referenced_key = Serializer.full_key_to_clazz_idx(resolved_idx)

                    for reference in only_reference_objects(storage.serializer, referencing_obj):
                        if isinstance(reference.name_of_ref_class, str):
                            # TODO: I think we want to write to the console that a NameOfRefClass has been specified that does not match the natural scope of the Reference.
                            # print(reference.name_of_ref_class, reference)
                            if reference.name_of_ref_class not in storage.serializer.name_object:
                                reference.name_of_ref_class = 'DataManagedObject'
                            name_of_ref_class = reference.name_of_ref_class

                        elif reference.name_of_ref_class.value not in storage.serializer.name_object:
                            # TODO: Add a warning.
                            reference.name_of_ref_class = 'DataManagedObject'

                        else:
                            name_of_ref_class = reference.name_of_ref_class.value

                        cmp_value = storage.serializer.encode_key(
                            reference.ref, getattr(reference, "version", "any"), storage.serializer.name_object[name_of_ref_class]
                        )
                        if value == cmp_value:
                            if class_change:
                                referenced_class = storage.idx_class[referenced_class_idx]
                                reference.name_of_ref_class = NameOfClass(
                                    get_object_name(referenced_class)
                                )  # I am very afraid how this might be handled in terms of comparisons later.
                            if version_change:
                                referenced_clazz = storage.idx_class[referenced_class_idx]
                                referenced_obj: Tid = storage.load_object(txn, referenced_clazz, referenced_key)
                                reference.version = referenced_obj.version

                # TODO: buffer this write to ~10000 objects of the same type?
                db = txn.open_map(referencing_class_idx, flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
                db.put(txn, referencing_key, storage.serializer.marshall(referencing_obj, referencing_obj.__class__))

        log_all(logging.INFO, f"[unresolved references] {db_unresolved.get_stat(txn).ms_entries}")
        txn.commit()


def resolve_embeddings_index(storage: MdbxStorage):
    log_all(logging.INFO, "[resolve] resolving embeddings")

    missing_classes = set([])
    unresolved_pairs: dict[bytes, set[bytes]] = {}
    references_to_fix: list[tuple] = []

    # TODO: fix with keycodec
    separator = bytes([10])

    if storage.readonly:
        raise

    with storage.env.rw_transaction() as txn:
        db_unresolved = txn.open_map(DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
        db_id_idx = txn.create_map(DB_EMBEDDED_ID_IDX, flags=DB_EMBEDDED_ID_IDX_FLAGS)
        db_reference_forward = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)

        unresolved_cursor = txn.cursor(db=db_unresolved)
        for idx, value in unresolved_cursor.iter():
            # print(idx, value)
            parts = storage.serializer.split_key(value)
            unresolved_pairs.setdefault(value, set()).add(idx)
            missing_classes.add(storage.idx_class[parts[-1]])

        log_all(logging.INFO, f"[unresolved references] {db_unresolved.get_stat(txn).ms_entries}")

        used_classes_in_database = set(storage.db_names(txn).values())
        index = collect_classes_index(used_classes_in_database, scope_classes=missing_classes)
        clazzes: set[type] = set().union(*index.values())

        for clazz in clazzes:
            this_class_idx = storage.class_idx[clazz]
            db = txn.open_map(this_class_idx, flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
            with txn.cursor(db) as cur:
                for idx, value in cur.iter():
                    full_key = idx + this_class_idx.ljust(4, b'\x00')
                    obj: Tid = storage.serializer.unmarshall(value, clazz)

                    # TODO: None should be replaced with the set of potential sub classes, hence the superset of missing_classes
                    for candidate, _obj in only_embedding(storage.serializer, obj, None):  # we zouden hier direct het path binnen het object ook kunnen opslaan
                        db_id_idx.put(txn, candidate, full_key)

        # TODO: If this works, deduplicate the code with the regular one
        unresolved_cursor = txn.cursor(db=db_unresolved)
        for it in unresolved_cursor.iter_dupsort_rows():
            references_to_fix: list[tuple] = []

            for idx, value in it:
                parts: list[str] | None = None
                prefix: str | None = None
                resolved_idx = db_id_idx.get(txn, value)  # This will be the id + version + class check
                class_change = False
                version_change = False
                if not resolved_idx:
                    cursor = txn.cursor(db=db_id_idx)

                    parts = storage.serializer.split_key(value)
                    class_part = separator + parts[-1]

                    # Alternative 1, id + version exists, class does not match
                    parts.pop()
                    prefix = separator.join(parts) + separator
                    for check_key, check_idx in cursor.iter(prefix):
                        if check_key.startswith(prefix):
                            class_change = check_idx
                            resolved_idx = check_idx
                        break

                    if not resolved_idx:
                        # Alternative 2, id exists
                        parts.pop()

                        prefix = separator.join(parts) + separator
                        for check_key, check_idx in cursor.iter(prefix):
                            if check_key.startswith(prefix) and check_key.endswith(class_part):
                                class_change = False
                                version_change = check_idx
                                resolved_idx = check_idx
                                break

                            elif check_key.startswith(prefix):
                                class_change = check_idx
                                version_change = check_idx
                                resolved_idx = check_idx
                                # Continue to find a better match

                            else:
                                break

                if resolved_idx:
                    if version_change or class_change:
                        references_to_fix.append((resolved_idx, value, version_change, class_change))

                    # f = storage.load_object_by_full_key(txn, idx)
                    # t = storage.load_object_by_full_key(txn, resolved_idx)

                    # print(f"{f.id} {f.__class__} -> {t.id} {t.__class__}")

                    db_reference_forward.put(txn, idx, resolved_idx)
                    unresolved_cursor.delete(MDBXCursorOp.MDBX_PREV)

                # else:
                #    print("unresolved", value, idx)

            # In this situation the original reference was incomplete
            if len(references_to_fix) > 0:
                referencing_class_idx, referencing_key = Serializer.full_key_to_clazz_idx(idx)
                referencing_class = storage.idx_class[referencing_class_idx]
                referencing_obj: Tid = storage.load_object(txn, referencing_class, referencing_key)

                for resolved_idx, value, version_change, class_change in references_to_fix:
                    referenced_class_idx, referenced_key = Serializer.full_key_to_clazz_idx(resolved_idx)

                    for reference in only_reference_objects(storage.serializer, referencing_obj):
                        if isinstance(reference.name_of_ref_class, str):
                            # TODO: I think we want to write to the console that a NameOfRefClass has been specified that does not match the natural scope of the Reference.
                            # print(reference.name_of_ref_class, reference)
                            if reference.name_of_ref_class not in storage.serializer.name_object:
                                reference.name_of_ref_class = 'DataManagedObject'
                            name_of_ref_class = reference.name_of_ref_class

                        elif reference.name_of_ref_class.value not in storage.serializer.name_object:
                            # TODO: Add a warning.
                            reference.name_of_ref_class = 'DataManagedObject'

                        else:
                            name_of_ref_class = reference.name_of_ref_class.value

                        cmp_value = storage.serializer.encode_key(
                            reference.ref, getattr(reference, "version", "any"), storage.serializer.name_object[name_of_ref_class]
                        )
                        if value == cmp_value:
                            if class_change:
                                referenced_class = storage.idx_class[referenced_class_idx]
                                reference.name_of_ref_class = NameOfClass(
                                    get_object_name(referenced_class)
                                )  # I am very afraid how this might be handled in terms of comparisons later.
                            if version_change:
                                referenced_clazz = storage.idx_class[referenced_class_idx]
                                referenced_obj: Tid = storage.load_object(txn, referenced_clazz, referenced_key)
                                reference.version = referenced_obj.version

                # TODO: buffer this write to ~10000 objects of the same type?
                db = txn.open_map(referencing_class_idx, flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
                db.put(txn, referencing_key, storage.serializer.marshall(referencing_obj, referencing_obj.__class__))

        log_all(logging.INFO, f"[unresolved references] {db_unresolved.get_stat(txn).ms_entries}")

        db_id_idx.drop(txn, delete=True)
        txn.commit()
