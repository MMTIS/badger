from itertools import count
from pathlib import Path
from types import TracebackType
from typing import Optional, Type, Literal, Iterable
import multiprocessing as mp

import lmdb

from domain.netex.services.model_typing import Tid
from domain.netex.services.recursive_attributes import only_references
from domain.utils import get_object_name
from storage.interface import Storage, Serializer

DB_CLASS_IDX = b'_class_idx'
DB_UNRESOLVED = b'_unresolved'
DB_ID_IDX = b'_id_idx'
DB_REFERENCE_FORWARD = b'_reference_forward'
DB_REFERENCE_INWARD = b'_reference_inward'


class LmdbStorage(Storage):
    readonly: bool
    max_dbs: int
    initial_size: int
    queue: mp.Queue  # type: ignore
    writer: mp.Process

    def __init__(self, path: Path, serializer: Serializer, readonly: bool = True):
        if readonly and not path.exists():
            raise

        self.path = path
        self.serializer = serializer
        self.readonly = readonly
        self.max_dbs = 128
        self.initial_size = 4 * 1024**3
        self.last_entry = count()  # TODO: change to context of DB
        self.queue = mp.Queue()

    def _populate_class_idx(self) -> None:
        if self.readonly:
            raise

        with self.env.begin(write=True) as txn:
            db_class_idx = self.env.open_db(key=DB_CLASS_IDX, txn=txn, create=True, integerkey=False)
            for idx, clazz in enumerate(self.serializer.name_object.values()):
                clazz_name = get_object_name(clazz)
                txn.put(idx.to_bytes(2, 'little'), clazz_name.encode('utf-8'), db=db_class_idx)

            self.env.open_db(DB_UNRESOLVED, txn=txn, create=True, integerkey=True, dupsort=True, integerdup=True)
            self.env.open_db(DB_ID_IDX, txn=txn, create=True)
            self.env.open_db(DB_REFERENCE_FORWARD, create=True, txn=txn, integerkey=True, dupsort=True, integerdup=True)
            self.env.open_db(DB_REFERENCE_INWARD, create=True, txn=txn, integerkey=True, dupsort=True, integerdup=True)

    def __enter__(self) -> Storage:
        new_database = not self.path.exists()

        self.env = lmdb.open(
            self.path.as_posix(),
            max_dbs=self.max_dbs,
            map_size=self.initial_size,
            writemap=False,
            metasync=False,
            sync=False,
            subdir=True,
        )

        if new_database:
            self._populate_class_idx()

        if not self.readonly:
            self.writer = mp.Process(target=self.consumer, args=(self.queue, self.path.as_posix(), self.max_dbs, self.initial_size))
            self.writer.start()

        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> Literal[False]:
        if self.writer.is_alive():
            self.queue.put(None)
            self.writer.join()
        self.env.close()
        return False  # Allow errors to propagate!

    def insert_objects_on_queue(self, klass: type[Tid], objects: Iterable[Tid], empty: bool = False) -> None:
        print(klass)

        if self.readonly:
            raise

        class_idx = self.serializer.classes.index(klass)

        with self.env.begin(write=False) as txn:
            db_id_idx = self.env.open_db(DB_ID_IDX, txn=txn)

            # if empty:
            #    txn.drop(db=db, delete=False)

            for obj in objects:
                key = int(next(self.last_entry))

                full_key = ((class_idx << 32) | key).to_bytes(8, 'little')
                for referenced_class_idx, ref, version in only_references(obj, self.serializer):
                    unresolved_value = self.serializer.encode_key(ref, version, referenced_class_idx, include_clazz=True)
                    resolved_idx = txn.get(unresolved_value, db=db_id_idx)
                    if resolved_idx:
                        self.queue.put(
                            (
                                DB_REFERENCE_FORWARD,
                                full_key,
                                resolved_idx,
                            )
                        )
                        self.queue.put(
                            (
                                DB_REFERENCE_INWARD,
                                resolved_idx,
                                full_key,
                            )
                        )
                    else:
                        self.queue.put(
                            (
                                DB_UNRESOLVED,
                                full_key,
                                unresolved_value,
                            )
                        )

                value = self.serializer.marshall(obj, klass)
                self.queue.put(
                    (
                        class_idx.to_bytes(2, 'little'),
                        key.to_bytes(4, 'little'),
                        value,
                    )
                )
                self.queue.put(
                    (
                        DB_ID_IDX,
                        self.serializer.encode_key(str(obj.id), obj.version if hasattr(obj, "version") else None, obj.__class__, include_clazz=True),
                        full_key,
                    )
                )

    @staticmethod
    def consumer(queue: mp.Queue, path: str, max_dbs: int, initial_size: int) -> None:  # type: ignore
        env = lmdb.open(
            path,
            max_dbs=max_dbs,
            map_size=initial_size,
            writemap=False,
            metasync=False,
            sync=False,
            subdir=True,
        )

        while True:
            with env.begin(write=True) as txn:
                while True:
                    try:
                        item = queue.get(timeout=0.05)  # probeer een nieuw item
                    except Exception:
                        # timeout → commit de transactie (door contextmanager) en start opnieuw
                        break

                    if item is None:
                        # commit wat er nog in txn zit, daarna stoppen
                        return

                    klass, key, value = item
                    db = env.open_db(klass, txn=txn)
                    txn.put(key, value, db=db)
