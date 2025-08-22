import itertools
from pathlib import Path
from types import TracebackType
from typing import Optional, Type, Literal

import lmdb

from netexio.serializer import Serializer
from storage.interface import Storage

DB_CLASS_IDX = b'_class_idx'
DB_UNRESOLVED = b'_unresolved'
DB_ID_IDX = b'_id_idx'
DB_REFERENCE_FORWARD = b'_reference_forward'
DB_REFERENCE_INWARD = b'_reference_inward'

class LmdbStorage(Storage):
    readonly: bool
    max_dbs: int
    initial_size: int

    def __init__(self, path: Path, serializer: Serializer, readonly: bool = True):
        if readonly and not path.exists():
            raise

        self.path = path
        self.serializer = serializer
        self.readonly = readonly
        self.max_dbs = 128
        self.initial_size = 4 * 1024**3
        self.last_entry = itertools.count() # TODO: start based on entries or last key in _id_idx

    def __enter__(self) -> Storage:
        new_database = not self.path.exists()

        self.env = lmdb.open(
            str(self.path),
            max_dbs=self.max_dbs,
            map_size=self.initial_size,
            writemap=False,
            metasync=False,
            sync=False,
            subdir=True,
        )

        if new_database:
            self.populate_class_idx()

        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> Literal[False]:
        self.env.close()
        return False  # Allow errors to propagate!

    def populate_class_idx(self) -> None:
        if self.readonly:
            raise

        with self.env.begin(write=True) as txn:
            db_class_idx = self.env.open_db(DB_CLASS_IDX, create=True, txn=txn, integerkey=True)
            for idx, clazz in enumerate(self.serializer.name_object.values()):
                clazz_name = self.serializer.get_object_name(clazz)
                txn.put(idx.to_bytes(2, 'little'), clazz_name.encode('utf-8'), db=db_class_idx)
