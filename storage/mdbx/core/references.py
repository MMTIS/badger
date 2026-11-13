import logging

from mdbx import MDBXDBFlags

from domain.netex.indexes.inverse_class import collect_classes_index, classes_and_subclasses, create_variant_classes_idx
from storage.objectserializer.cloudpickle import serializer
from storage.objectserializer.codecs.path import decode_path
from utils.aux_logging import log_all
from domain.netex.services.model_typing import Tid
from domain.netex.model import EntityStructure, EntityInVersionStructure, NameOfClass
from domain.netex.services.recursive_attributes import only_reference_objects, only_embedding, embedding_obj_iter
from domain.utils import get_object_name
from storage.keycodec.relation import RelationKeyCodec
from domain.netex.services.recursive_attributes import resolve_path

from storage.mdbx.core.implementation import MdbxStorage
from storage.mdbx.core.const import (
    DB_UNRESOLVED,
    DB_REFERENCE_OUTWARD,
    DB_ID_IDX,
    DB_EMBEDDED_ID_IDX,
    DB_UNRESOLVED_FLAGS,
    DB_REFERENCE_OUTWARD_FLAGS,
    DB_ID_IDX_FLAGS,
    DB_EMBEDDED_ID_IDX_FLAGS,
)
from mdbx.mdbx import TXN, DBI
from collections.abc import Generator, Sequence
from typing import Optional, Any, cast
import re

REF_SUFFIX = re.compile(r"(?:RefStructure|Ref)$")

def references_fix_by_path(storage: MdbxStorage, txn: TXN, referencing_obj: EntityStructure, references_to_fix: list[tuple[bytes, bytes | None, Sequence[int], bytes | None, bytes | None]]) -> None:
    for resolved_idx, key, path, version_change, class_change in references_to_fix:
        referenced_clazz_idx, referenced_key = RelationKeyCodec.full_key_to_clazz_idx(resolved_idx)
        reference = resolve_path(referencing_obj, path)
        if class_change:
            referenced_class = storage.idx_clazz[referenced_clazz_idx]
            reference.name_of_ref_class = NameOfClass(
                get_object_name(referenced_class)
            )  # I am very afraid how this might be handled in terms of comparisons later.
        if version_change:
            referenced_clazz = storage.idx_clazz[referenced_clazz_idx]
            referenced_obj: EntityInVersionStructure = cast(
                EntityInVersionStructure,
                storage.load_object(
                    txn, referenced_clazz, referenced_key
                ),
            )
            reference.version = referenced_obj.version

def references_fix_by_original_iteration(storage: MdbxStorage, txn: TXN, referencing_obj: EntityStructure, references_to_fix: list[tuple[bytes, bytes | None, Sequence[int], bool, bool]]):
    for resolved_idx, value, path, version_change, class_change in references_to_fix:
        referenced_clazz_idx, referenced_key = RelationKeyCodec.full_key_to_clazz_idx(resolved_idx)
        for reference in only_reference_objects(referencing_obj):
            if isinstance(reference.name_of_ref_class, str):
                # TODO: I think we want to write to the console that a NameOfRefClass has been specified that does not match the natural scope of the Reference.
                # print(reference.name_of_ref_class, reference)
                if reference.name_of_ref_class not in storage.serializer.name_object:
                    reference.name_of_ref_class = NameOfClass.DATA_MANAGED_OBJECT
                name_of_ref_class = reference.name_of_ref_class

            elif not reference.name_of_ref_class or (reference.name_of_ref_class.value not in storage.serializer.name_object):
                # TODO: Add a warning.
                reference.name_of_ref_class = NameOfClass.DATA_MANAGED_OBJECT

            else:
                name_of_ref_class = reference.name_of_ref_class.value

            cmp_value = storage.serializer.encode_key(
                reference.ref, getattr(reference, "version", "any"), storage.serializer.name_object[name_of_ref_class]
            )
            if value == cmp_value:
                if class_change:
                    referenced_class = storage.idx_clazz[referenced_clazz_idx]
                    reference.name_of_ref_class = NameOfClass(
                        get_object_name(referenced_class)
                    )  # I am very afraid how this might be handled in terms of comparisons later.
                if version_change:
                    referenced_clazz = storage.idx_clazz[referenced_clazz_idx]
                    referenced_obj: EntityInVersionStructure = cast(
                        EntityInVersionStructure, storage.load_object(txn, referenced_clazz, referenced_key)
                    )
                    reference.version = referenced_obj.version

def resolve_embeddings_iterable(
    storage: MdbxStorage, txn: TXN, clazz: type[EntityStructure], interesting_classes: Optional[set[type[Tid]]] = None, ignore: Optional[set[type[Tid]]] = None
) -> Generator[tuple[bytes, EntityStructure, tuple[bytes, Any, tuple[int, ...]]], None, None]:
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

def resolve_ids(storage: MdbxStorage, db_unresolved: DBI, db_id_idx: DBI, txn: TXN):
    get_variant_classes_idx = create_variant_classes_idx(
        storage.serializer.clazz_idx
    )

    seen_count = 0

    db_reference_forward = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)

    unresolved_cursor = txn.cursor(db=db_unresolved)
    for it in unresolved_cursor.iter_dupsort_rows():
        references_to_fix: list[tuple[bytes, bytes | None, Sequence[int], bytes | None, bytes | None]] = []

        for idx, key_path in it:
            assert idx is not None
            assert key_path is not None
            seen_count += 1
            if seen_count % 1_000_000 == 0:
                log_all(logging.INFO, f"[resolve] {seen_count} references processed...")
            parts: list[bytes]
            prefix: bytes
            resolved_idx: bytes | None = None
            class_change: bytes | None = None
            version_change: bytes | None = None

            out_id, out_version, out_clazz, out_path, out_ref_clazz = storage.serializer.decode_ref_path(key_path)
            key = storage.serializer.encode_key_clazz(out_id, out_version, out_clazz)

            # Let us first know, do we already have candidate
            resolved_idx = db_id_idx.get(txn, key)  # This will be the id + version + class check
            if not resolved_idx:
                # 1. Good: The version is incorrect or not specified, but original class matches (highest rank)
                # 2. Good: The version is correct, the original class does not match, but its variants do
                # 3. Wrong: The class is functionally incorrect, for example a QuayRef references a ScheduledStopPoint
                # 4. Wrong: Nothing is found

                clazz_name = REF_SUFFIX.sub("", out_ref_clazz.__name__) # Wat werkt hier niet: FromJourney, ToJourney, terwijl daar wel een JourneyRefStructure achter zit
                abstract_clazz = storage.serializer.class_by_name(clazz_name)
                if abstract_clazz is None:
                    print(clazz_name, out_id, out_version, out_clazz, out_path, out_ref_clazz)
                assert abstract_clazz is not None

                variant_classes = classes_and_subclasses((abstract_clazz,))
                variant_classes_idx = get_variant_classes_idx(variant_classes)

                out_clazz_idx = storage.serializer.clazz_idx.get(out_clazz, None)

                prefix = storage.serializer.encode_key_clazz(out_id, None, None)
                cursor = txn.cursor(db=db_id_idx)
                wrong_classes: set[bytes] = set()
                for check_key, check_idx in cursor.iter(prefix):
                    assert check_key is not None
                    assert check_idx is not None

                    if not check_key.startswith(prefix):
                        break

                    # The best situation would be the match prefix \0 any \0 any of the right classes in variant_classes
                    # We report an error if nothing would match, but we did find a prefix for the wrong class
                    this_id, this_version, this_clazz_idx = storage.serializer.split_key(check_key)

                    if out_clazz and this_clazz_idx == out_clazz:
                        # Best case, thus break
                        class_change = check_idx
                        version_change = check_idx
                        resolved_idx = check_idx
                        break

                    elif this_clazz_idx in variant_classes_idx:
                        class_change = check_idx
                        version_change = check_idx
                        resolved_idx = check_idx

                    else:
                        wrong_classes.add(this_clazz_idx)

            if resolved_idx:
                if version_change or class_change:
                    references_to_fix.append((resolved_idx, key, out_path, version_change, class_change))

                # f = storage.load_object_by_full_key(txn, idx)
                # t = storage.load_object_by_full_key(txn, resolved_idx)

                # print(f"{f.id} {f.__class__} -> {t.id} {t.__class__}")

                db_reference_forward.put(txn, idx, resolved_idx)
                # db_unresolved.delete(txn, idx, key) OR:
                it.cur.delete()  # The reason here, the it, has a duplicated cursor

            # else:
            #    print("unresolved", key, idx)

        # In this situation the original reference was incomplete
        if len(references_to_fix) > 0:
            referencing_clazz_idx, referencing_idx = RelationKeyCodec.full_key_to_clazz_idx(idx)
            referencing_clazz = storage.idx_clazz[referencing_clazz_idx]
            referencing_obj: EntityStructure = storage.load_object(txn, referencing_clazz, referencing_idx)

            references_fix_by_path(storage, txn, referencing_obj, references_to_fix)
            # references_fix_by_original_iteration(storage, txn, referencing_obj, references_to_fix)

            # TODO: buffer this write to ~10000 objects of the same type?
            db = txn.open_map(referencing_clazz_idx, flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
            db.put(txn, referencing_idx, storage.serializer.marshall(referencing_obj, referencing_obj.__class__))


def resolve(storage: MdbxStorage) -> None:
    log_all(logging.INFO, "[resolve] resolving references")
    if storage.readonly:
        raise

    with storage.env.rw_transaction() as txn:
        db_unresolved = txn.open_map(DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
        log_all(logging.INFO, f"[unresolved references] {db_unresolved.get_stat(txn).ms_entries}")

        if db_unresolved.get_stat(txn).ms_entries == 0:
            return

        db_id_idx = txn.open_map(DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
        resolve_ids(storage, db_unresolved, db_id_idx, txn)

        log_all(logging.INFO, f"[unresolved references] {db_unresolved.get_stat(txn).ms_entries}")
        txn.commit()


"""
For readibility we split the creation of the embedded index
"""
def create_embedded_id_index(storage: MdbxStorage, db_unresolved: DBI, db_id_idx: DBI, txn: TXN):
    missing_classes = set([])
    unresolved_pairs: dict[bytes, set[bytes]] = {}

    unresolved_cursor = txn.cursor(db=db_unresolved)
    for full_key, key_path in unresolved_cursor.iter():
        assert full_key is not None
        assert key_path is not None

        # TODO: this is a temporary implementation to show existing code still works
        out_id, out_version, out_clazz, out_path, out_ref_clazz = storage.serializer.decode_ref_path(key_path)
        key = storage.serializer.encode_key_clazz(out_id, out_version, out_ref_clazz)

        clazz_name = REF_SUFFIX.sub("",
                                    out_ref_clazz.__name__)  # Wat werkt hier niet: FromJourney, ToJourney, terwijl daar wel een JourneyRefStructure achter zit
        abstract_clazz = storage.serializer.class_by_name(clazz_name)
        if abstract_clazz is None:
            print(clazz_name, out_id, out_version, out_clazz, out_path, out_ref_clazz)
        assert abstract_clazz is not None

        variant_classes = classes_and_subclasses((abstract_clazz,))

        unresolved_pairs.setdefault(key, set()).add(full_key)
        missing_classes.update(variant_classes)

    used_classes_in_database = set(storage.db_names(txn).values())
    index = collect_classes_index(used_classes_in_database, scope_classes=missing_classes)
    clazzes: set[type] = set().union(*index.values())

    for clazz in clazzes:
        this_clazz_idx = storage.clazz_idx[clazz]
        db = txn.open_map(this_clazz_idx, flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
        with txn.cursor(db) as cur:
            for idx, value in cur.iter():
                assert idx is not None
                assert value is not None
                full_key = idx + this_clazz_idx.ljust(4, b'\x00')
                obj: EntityStructure = storage.serializer.unmarshall(value, clazz)

                # TODO: None should be replaced with the set of potential sub classes, hence the superset of missing_classes
                for candidate, _obj in only_embedding(storage.serializer,
                                                      obj):  # we zouden hier direct het path binnen het object ook kunnen opslaan
                    db_id_idx.put(txn, candidate, full_key)

def resolve_embeddings_index(storage: MdbxStorage) -> None:
    log_all(logging.INFO, "[resolve] resolving embeddings")

    if storage.readonly:
        raise

    with storage.env.rw_transaction() as txn:
        db_unresolved = txn.open_map(DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
        log_all(logging.INFO, f"[unresolved references] {db_unresolved.get_stat(txn).ms_entries}")

        if db_unresolved.get_stat(txn).ms_entries == 0:
            return

        db_id_idx = txn.create_map(DB_EMBEDDED_ID_IDX, flags=DB_EMBEDDED_ID_IDX_FLAGS)

        create_embedded_id_index(storage, db_unresolved, db_id_idx, txn)
        resolve_ids(storage, db_unresolved, db_id_idx, txn)

        log_all(logging.INFO, f"[unresolved references] {db_unresolved.get_stat(txn).ms_entries}")

        db_id_idx.drop(txn, delete=True)
        txn.commit()
