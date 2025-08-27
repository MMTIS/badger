from storage.lmdb.core.implementation import LmdbStorage, DB_UNRESOLVED, DB_REFERENCE_OUTWARD, DB_REFERENCE_INWARD, DB_ID_IDX
from storage.lmdb.serialization.byteserializer import ByteSerializer


def resolve(storage: LmdbStorage) -> None:
    if storage.readonly:
        raise

    separator = bytes([ByteSerializer.SEPARATOR])

    with storage.env.begin(write=True) as txn:
        db_unresolved = storage.env.open_db(DB_UNRESOLVED, txn=txn, create=False)
        db_id_idx = storage.env.open_db(DB_ID_IDX, txn=txn, create=False)
        db_reference_forward = storage.env.open_db(DB_REFERENCE_OUTWARD, txn=txn, create=False)
        db_reference_inward = storage.env.open_db(DB_REFERENCE_INWARD, txn=txn, create=False)

        unresolved_cursor = txn.cursor(db=db_unresolved)
        has_item = unresolved_cursor.first()
        while has_item:
            value = unresolved_cursor.value()
            resolved_idx = txn.get(value, db=db_id_idx)  # This will be the id + version + class check
            if not resolved_idx:
                parts = storage.serializer.split_key(value)
                parts[-1] = b''
                prefix = separator.join(parts)
                cursor = txn.cursor(db=db_id_idx)
                if cursor.set_range(prefix):  # This will be the id + version check
                    while cursor.key().startswith(prefix):
                        resolved_idx = cursor.value()
                        break

                if not resolved_idx:
                    parts.pop()
                    parts[-1] = b''
                    prefix = separator.join(parts)
                    if cursor.set_range(prefix):  # This will be the id check
                        while cursor.key().startswith(prefix):
                            resolved_idx = cursor.value()
                            break

            if resolved_idx:
                idx = unresolved_cursor.key()
                txn.put(idx, resolved_idx, db=db_reference_forward)
                txn.put(resolved_idx, idx, db=db_reference_inward)

                # Because cursor.delete() does funky things.
                txn.delete(idx, value, db=db_unresolved)

            else:
                print("unresolved", value, unresolved_cursor.key())

            has_item = unresolved_cursor.next()
