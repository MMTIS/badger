from pathlib import Path

from domain.netex.services.utils import get_boring_classes
from storage.lmdb.core.implementation import LmdbStorage, DB_ID_IDX, DB_UNRESOLVED

from storage.lmdb.serialization.byteserializer import ByteSerializer


def unresolved_lmdb(storage: LmdbStorage) -> None:
    with storage.env.begin() as txn:
        db_name = DB_UNRESOLVED
        db = storage.env.open_db(db_name, txn=txn)
        db_id_idx = storage.env.open_db(DB_ID_IDX, txn=txn)

        with (txn.cursor(db) as cursor, txn.cursor(db_id_idx) as cursor_id_idx):
            for key, value in cursor:
                for key2, value2 in cursor_id_idx:
                    if value2 == key:
                        obj_id = key2
                        break

                print(obj_id, storage.idx_class[obj_id.split(b'-')[-1]], value,
                      storage.idx_class[value.split(b'-')[-1]])  # TODO


if __name__ == "__main__":
    import sys

    interesting_members = get_boring_classes()
    with LmdbStorage(Path(sys.argv[1]), ByteSerializer(interesting_members), readonly=True) as storage:
        unresolved_lmdb(storage)
