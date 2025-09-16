from pathlib import Path

from domain.netex.services.utils import get_boring_classes
from storage.mdbx.core.implementation import MdbxStorage, DB_ID_IDX

if __name__ == "__main__":
    import sys

    interesting_members = get_boring_classes()
    with MdbxStorage(Path(sys.argv[1]), readonly=False) as storage:
        with storage.env.ro_transaction() as txn:
            with txn.open_map(name=DB_ID_IDX) as db_id_idx:
                with txn.cursor(db_id_idx) as cur:
                    for name, idx in cur.iter():
                        if name.startswith(b"TESO*PASSENGERSTOPASSIGNMENT"):
                            print(name)
                            for clazz, reference_idx in storage._load_references(txn, idx):
                                print("->", storage.load_object(txn, clazz, reference_idx).id)


