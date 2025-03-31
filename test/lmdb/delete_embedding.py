# https://github.com/skinkie/reference/issues/271

import lmdb
from lmdb import Transaction

env = lmdb.open("/tmp/mydb", subdir=False, map_size=2**30, max_dbs=2)
db1 = env.open_db(b"by_parent", dupsort=True)
db2 = env.open_db(b"by_id", dupsort=True)


def delete_inverse_entries(txn: Transaction, parent_id: str, parent_version: str, parent_class: str) -> None:
    """Remove entries from db2 based on parent_id, parent_version, parent_class."""
    key = f"{parent_id}:{parent_version}:{parent_class}".encode()

    with txn.cursor(db1) as cursor1, txn.cursor(db2) as cursor2:
        if cursor1.set_key(key):
            # Iterate over all values that match the key in db1
            for value in cursor1.iternext_dup():
                id_, version, cls = value.split(b":")
                inv_key = f"{id_.decode()}:{version.decode()}:{cls.decode()}".encode()

                # Use MDB_GET_BOTH_RANGE to find the exact key-value pair in db2
                if cursor2.set_range(inv_key) and cursor2.value() == key:
                    cursor2.delete()


def insert_entry(txn: Transaction, parent_id: str, parent_version: str, parent_class: str, id_: str, version: str, cls: str) -> None:
    """Insert a new entry, replacing the old one."""
    delete_inverse_entries(txn, parent_id, parent_version, parent_class)

    key1 = f"{parent_id}:{parent_version}:{parent_class}".encode()
    value1 = f"{id_}:{version}:{cls}".encode()

    key2 = f"{id_}:{version}:{cls}".encode()
    value2 = f"{parent_id}:{parent_version}:{parent_class}".encode()

    txn.put(key1, value1, db=db1, dupdata=True)
    txn.put(key2, value2, db=db2, dupdata=True)


# Example usage
with env.begin(write=True) as txn:
    insert_entry(txn, "P1", "V1", "C1", "I1", "V1", "C2")
    insert_entry(txn, "P1", "V1", "C1", "I2", "V2", "C3")  # Overwrites existing
