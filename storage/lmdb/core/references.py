from domain.netex.indexes.inverse_class import collect_classes_index
from domain.netex.services.model_typing import Tid
from domain.netex.services.recursive_attributes import only_reference_objects, only_embedding
from domain.utils import get_object_name
from storage.lmdb.core.implementation import LmdbStorage, DB_UNRESOLVED, DB_REFERENCE_OUTWARD, DB_REFERENCE_INWARD, DB_ID_IDX
from storage.lmdb.serialization.byteserializer import ByteSerializer


def resolve_embeddings(storage: LmdbStorage):
    missing_classes = set([])
    unresolved_pairs: dict[bytes, set[bytes]] = {}
    now_resolved: list[tuple[bytes, bytes]] = []

    with storage.env.begin(write=True) as txn:
        db_reference_forward = storage.env.open_db(DB_REFERENCE_OUTWARD, txn=txn, create=False)
        db_reference_inward = storage.env.open_db(DB_REFERENCE_INWARD, txn=txn, create=False)
        db_unresolved = storage.env.open_db(DB_UNRESOLVED, txn=txn, create=False)
        unresolved_cursor = txn.cursor(db=db_unresolved)
        for idx, value in unresolved_cursor:
            parts = storage.serializer.split_key(value)
            unresolved_pairs.setdefault(value, set()).add(idx)
            missing_classes.add(storage.idx_class[parts[-1]])

        used_classes_in_database = set(storage.db_names().values())
        index = collect_classes_index(used_classes_in_database, scope_classes=missing_classes)
        clazzes: set[type] = set().union(*index.values())

        class_count: dict[type, int] = {}
        for clazz in clazzes:
            db = storage.env.open_db(storage.class_idx[clazz], txn=txn, create=False)
            stat = txn.stat(db)
            entries = stat["entries"]
            class_count[clazz] = entries

        for clazz, count in sorted(class_count.items(), key=lambda item: item[1]):
            db = storage.env.open_db(storage.class_idx[clazz], txn=txn, create=False)
            for idx, value in txn.cursor(db=db):
                obj = storage.serializer.unmarshall(value, clazz)
                for candidate in only_embedding(storage.serializer, obj, missing_classes):
                    if candidate in unresolved_pairs:
                        full_key = ((int.from_bytes(storage.class_idx[clazz], 'little') << 32) | int.from_bytes(idx, 'little')).to_bytes(8, 'little')
                        for resolved_index in unresolved_pairs[candidate]:
                            txn.put(resolved_index, full_key, db=db_reference_forward)
                            txn.put(full_key, resolved_index, db=db_reference_inward)
                            now_resolved.append((resolved_index, candidate))
                        del unresolved_pairs[candidate]

    # Workaround for very strang LMDB results
    with storage.env.begin(write=True) as txn:
        db_unresolved = storage.env.open_db(DB_UNRESOLVED, txn=txn, create=False)
        for idx, value in now_resolved:
            txn.delete(idx, value, db=db_unresolved)

def resolve(storage: LmdbStorage) -> None:
    if storage.readonly:
        raise

    separator = bytes([ByteSerializer.SEPARATOR])

    with storage.env.begin(write=True) as txn:
        db_unresolved = storage.env.open_db(DB_UNRESOLVED, txn=txn, create=False)
        db_id_idx = storage.env.open_db(DB_ID_IDX, txn=txn, create=False)
        db_reference_forward = storage.env.open_db(DB_REFERENCE_OUTWARD, txn=txn, create=False)
        db_reference_inward = storage.env.open_db(DB_REFERENCE_INWARD, txn=txn, create=False)

        now_resolved: list[tuple[bytes, bytes]] = []

        unresolved_cursor = txn.cursor(db=db_unresolved)
        has_item = unresolved_cursor.first()
        while has_item:
            idx = bytes(unresolved_cursor.key())
            value = bytes(unresolved_cursor.value())
            resolved_idx = txn.get(value, db=db_id_idx)  # This will be the id + version + class check
            class_change = False
            version_change = False
            if not resolved_idx:
                cursor = txn.cursor(db=db_id_idx)

                parts = storage.serializer.split_key(value)

                # Alternative 1, id + version exists, class does not match
                parts.pop()
                prefix = separator.join(parts)
                if cursor.set_range(prefix):  # This will be the id check
                    while cursor.key().startswith(prefix):
                        resolved_idx = cursor.value()
                        # class_idx, resolved_obj_key = storage.serializer.full_key_to_idx(resolved_idx)
                        class_change = resolved_idx
                        break

                if not resolved_idx:
                    # Alternative 2, id exists
                    # TODO: we might be able to also do a variant where we do check the class
                    parts.pop()

                    prefix = separator.join(parts)
                    if cursor.set_range(prefix):  # This will be the id + version check
                        while cursor.key().startswith(prefix):
                            resolved_idx = cursor.value()
                            version_change = resolved_idx
                            break

            if resolved_idx:
                if version_change or class_change:
                    # In this situation the original reference was incomplete
                    referenced_class_idx, referenced_key = storage.serializer.full_key_to_idx(version_change or class_change)
                    referencing_class_idx, referencing_key = storage.serializer.full_key_to_idx(idx)
                    referencing_class = storage.idx_class[referencing_class_idx]
                    referencing_obj: Tid = storage.load_object(referencing_class, referencing_key)

                    for reference in only_reference_objects(referencing_obj):
                        cmp_value = storage.serializer.encode_key(reference.ref, getattr(reference, "version", "any"), storage.serializer.name_object[reference.name_of_ref_class], True)
                        if value == cmp_value:
                            if class_change:
                                referenced_class = storage.idx_class[referenced_class_idx]
                                reference.name_of_ref_class = get_object_name(referenced_class)
                            if version_change:
                                referenced_clazz = storage.idx_class[referenced_class_idx]
                                referenced_obj: Tid = storage.load_object(referenced_clazz, referenced_key)
                                reference.version = referenced_obj.version

                    # TODO: buffer this write to ~10000 objects of the same type?
                    db = storage.env.open_db(storage.class_idx[referencing_obj.__class__], txn=txn)
                    txn.put(referenced_key, storage.serializer.marshall(referencing_obj, referencing_obj.__class__), db=db)

                txn.put(idx, resolved_idx, db=db_reference_forward)
                txn.put(resolved_idx, idx, db=db_reference_inward)

                # Because cursor.delete() does very funky things.
                now_resolved.append((idx, value))

            # else:
            #    print("unresolved", value, idx)

            has_item = unresolved_cursor.next()

    # Workaround for very strang LMDB results
    with storage.env.begin(write=True) as txn:
        db_unresolved = storage.env.open_db(DB_UNRESOLVED, txn=txn, create=False)
        for idx, value in now_resolved:
            txn.delete(idx, value, db=db_unresolved)