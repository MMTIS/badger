from mdbx import MDBXCursorOp

from domain.netex.indexes.inverse_class import collect_classes_index
from domain.netex.services.model_typing import Tid
from domain.netex.model import EntityStructure
from domain.netex.services.recursive_attributes import only_reference_objects, only_embedding, embedding_obj_iter
from domain.utils import get_object_name
from storage.mdbx.core.implementation import MdbxStorage, DB_UNRESOLVED, DB_REFERENCE_OUTWARD, DB_ID_IDX
from storage.mdbx.serialization.byteserializer import ByteSerializer
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


def resolve_embeddings(storage: MdbxStorage):
    missing_classes = set([])
    unresolved_pairs: dict[bytes, set[bytes]] = {}

    with storage.env.rw_transaction() as txn:
        db_unresolved = txn.open_map(DB_UNRESOLVED)
        db_reference_outward = txn.open_map(DB_REFERENCE_OUTWARD)

        unresolved_cursor = txn.cursor(db=db_unresolved)
        for idx, value in unresolved_cursor.iter():
            parts = storage.serializer.split_key(value)
            unresolved_pairs.setdefault(value, set()).add(idx)
            missing_classes.add(storage.idx_class[parts[-1]])

        used_classes_in_database = set(storage.db_names(txn).values())
        index = collect_classes_index(used_classes_in_database, scope_classes=missing_classes)
        clazzes: set[type] = set().union(*index.values())

        class_count: dict[type, int] = {}
        for clazz in clazzes:
            db = txn.open_map(storage.class_idx[clazz])
            class_count[clazz] = db.get_stat(txn).ms_entries

        for clazz, count in sorted(class_count.items(), key=lambda item: item[1]):
            db = txn.open_map(storage.class_idx[clazz])
            with txn.cursor(db) as cur:
                for idx, value in cur.iter():
                    obj: Tid = storage.serializer.unmarshall(value, clazz)
                    for candidate in only_embedding(storage.serializer, obj, missing_classes):
                        if candidate in unresolved_pairs:
                            full_key = ((int.from_bytes(storage.class_idx[clazz], 'little') << 32) | int.from_bytes(idx, 'little')).to_bytes(8, 'little')
                            for resolved_index in unresolved_pairs[candidate]:
                                # Bij deze twee schrijfacties ontstaat build/lib/mdb.c:2156: Assertion 'rc == 0' failed in mdb_page_dirty()
                                db_reference_outward.put(txn, resolved_index, full_key)
                                db_unresolved.delete(txn, resolved_index, candidate)
                            del unresolved_pairs[candidate]
        txn.commit()


def resolve(storage: MdbxStorage) -> None:
    if storage.readonly:
        raise

    separator = bytes([ByteSerializer.SEPARATOR])

    with storage.env.rw_transaction() as txn:
        db_unresolved = txn.open_map(DB_UNRESOLVED)
        db_id_idx = txn.open_map(DB_ID_IDX)
        db_reference_forward = txn.open_map(DB_REFERENCE_OUTWARD)

        unresolved_cursor = txn.cursor(db=db_unresolved)
        for idx, value in unresolved_cursor.iter():
            parts: list[str] | None = None
            prefix: str | None = None
            resolved_idx = db_id_idx.get(txn, value)  # This will be the id + version + class check
            class_change = False
            version_change = False
            if not resolved_idx:
                cursor = txn.cursor(db=db_id_idx)

                parts = storage.serializer.split_key(value)

                # Alternative 1, id + version exists, class does not match
                parts.pop()
                prefix = separator.join(parts)
                for check_key, check_idx in cursor.iter(prefix):
                    if check_key.startswith(prefix):
                        class_change = check_idx
                        resolved_idx = check_idx
                    break

                if not resolved_idx:
                    # Alternative 2, id exists
                    # TODO: we might be able to also do a variant where we do check the class
                    parts.pop()

                    prefix = separator.join(parts)
                    for check_key, check_idx in cursor.iter(prefix):
                        if check_key.startswith(prefix):
                            version_change = check_idx
                            resolved_idx = check_idx
                        break

            if resolved_idx:
                if version_change or class_change:
                    # In this situation the original reference was incomplete
                    referenced_class_idx, referenced_key = storage.serializer.full_key_to_idx(resolved_idx)
                    referencing_class_idx, referencing_key = storage.serializer.full_key_to_idx(idx)
                    referencing_class = storage.idx_class[referencing_class_idx]
                    referencing_obj: Tid = storage.load_object(txn, referencing_class, referencing_key)

                    for reference in only_reference_objects(referencing_obj):
                        if reference.name_of_ref_class not in storage.serializer.name_object:
                            # TODO: Add a warning.
                            continue

                        cmp_value = storage.serializer.encode_key(
                            reference.ref, getattr(reference, "version", "any"), storage.serializer.name_object[reference.name_of_ref_class], True
                        )
                        if value == cmp_value:
                            if class_change:
                                referenced_class = storage.idx_class[referenced_class_idx]
                                reference.name_of_ref_class = get_object_name(referenced_class)
                            if version_change:
                                referenced_clazz = storage.idx_class[referenced_class_idx]
                                referenced_obj: Tid = storage.load_object(txn, referenced_clazz, referenced_key)
                                reference.version = referenced_obj.version

                    # TODO: buffer this write to ~10000 objects of the same type?
                    db = txn.open_map(referencing_class_idx)
                    db.put(txn, referencing_key, storage.serializer.marshall(referencing_obj, referencing_obj.__class__))

                # f = storage.load_object_by_full_key(txn, idx)
                # t = storage.load_object_by_full_key(txn, resolved_idx)

                # print(f"{f.id} {f.__class__} -> {t.id} {t.__class__}")

                db_reference_forward.put(txn, idx, resolved_idx)
                unresolved_cursor.delete(MDBXCursorOp.MDBX_PREV)

            # else:
            #    print("unresolved", value, idx)

        txn.commit()
