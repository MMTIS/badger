from mdbx import MDBXCursorOp

from domain.netex.indexes.inverse_class import collect_classes_index
from domain.netex.services.model_typing import Tid
from domain.netex.services.recursive_attributes import only_reference_objects, only_embedding
from domain.utils import get_object_name
from storage.mdbx.core.implementation import MdbxStorage, DB_UNRESOLVED, DB_REFERENCE_OUTWARD, DB_ID_IDX
from storage.mdbx.serialization.byteserializer import ByteSerializer


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
            resolved_idx = db_id_idx.get(txn, value)  # This will be the id + version + class check
            class_change = False
            version_change = False
            if not resolved_idx:
                cursor = txn.cursor(db=db_id_idx)

                parts = storage.serializer.split_key(value)

                # Alternative 1, id + version exists, class does not match
                parts.pop()
                prefix = separator.join(parts)
                for it in cursor.iter_dupsort_rows(prefix):
                    for _, resolved_idx in it:
                        class_change = resolved_idx
                        break
                    break

                if not resolved_idx:
                    # Alternative 2, id exists
                    # TODO: we might be able to also do a variant where we do check the class
                    parts.pop()

                    prefix = separator.join(parts)
                    for it in cursor.iter_dupsort_rows(prefix):
                        for _, resolved_idx in it:
                            version_change = resolved_idx
                            break
                        break

            if resolved_idx:
                if version_change or class_change:
                    # In this situation the original reference was incomplete
                    referenced_class_idx, referenced_key = storage.serializer.full_key_to_idx(resolved_idx)
                    referencing_class_idx, referencing_key = storage.serializer.full_key_to_idx(idx)
                    referencing_class = storage.idx_class[referencing_class_idx]
                    referencing_obj: Tid = storage.load_object(txn, referencing_class, referencing_key)

                    for reference in only_reference_objects(referencing_obj):
                        cmp_value = storage.serializer.encode_key(
                            reference.ref, getattr(reference, "version", "any"), storage.serializer.name_object[reference.name_of_ref_class], True
                        )
                        if value == cmp_value:
                            if class_change:
                                referenced_class = storage.idx_class[referenced_class_idx]
                                reference.name_of_ref_class = get_object_name(referenced_class)
                            if version_change:
                                referenced_clazz = storage.idx_class[referenced_class_idx]
                                referenced_obj: Tid = storage.load_object(txn, referenced_clazz, referenced_key, txn)
                                reference.version = referenced_obj.version

                    # TODO: buffer this write to ~10000 objects of the same type?
                    db = txn.open_map(referencing_class_idx)
                    db.put(txn, referencing_key, storage.serializer.marshall(referencing_obj, referencing_obj.__class__))

                db_reference_forward.put(txn, idx, resolved_idx)
                unresolved_cursor.delete(MDBXCursorOp.MDBX_PREV)

            # else:
            #    print("unresolved", value, idx)

        txn.commit()
