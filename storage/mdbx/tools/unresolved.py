from pathlib import Path

from domain.netex.services.utils import get_boring_classes
from storage.mdbx.core.implementation import MdbxStorage, DB_UNRESOLVED, DB_ID_IDX, DB_UNRESOLVED_FLAGS, DB_ID_IDX_FLAGS


def unresolved_mdbx(storage: MdbxStorage) -> None:
    with storage.env.ro_transaction() as txn:
        db_name = DB_UNRESOLVED
        db = txn.open_map(db_name, flags=DB_UNRESOLVED_FLAGS)

        with txn.cursor(db) as cursor:
            for key, value in cursor:
                obj = storage.load_object_by_full_key(txn, full_key=key)
                print(obj.id, obj.__class__, value)

if __name__ == "__main__":
    import sys

    interesting_members = get_boring_classes()
    with MdbxStorage(Path(sys.argv[1]), readonly=True) as storage:
        unresolved_mdbx(storage)
