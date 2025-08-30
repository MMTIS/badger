from pathlib import Path
from types import TracebackType
from typing import Optional, Type, Literal, Iterable
import multiprocessing as mp

import lmdb

from domain.netex.services.model_typing import Tid
from domain.netex.services.recursive_attributes import only_references
from storage.interface import Storage
from storage.lmdb.core.implementation import LmdbStorage, DB_ID_IDX, DB_REFERENCE_OUTWARD, DB_REFERENCE_INWARD, DB_UNRESOLVED


class LmdbStorageMP(LmdbStorage):
    queue: mp.Queue  # type: ignore
    writer: mp.Process

    def __init__(self, path: Path, readonly: bool = True, initial_size: int = 8 * 1024**3):
        super().__init__(path, readonly, initial_size)
        self.ctx = mp.get_context("spawn")
        self.manager = self.ctx.Manager()
        self.queue = self.manager.Queue(maxsize=1000)

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

        self._restore_class_idx()
        self.next_entry = self.get_next_key()

        if not self.readonly:
            self.writer = mp.Process(target=self.consumer, args=(self.queue, self.path.as_posix(), self.max_dbs, self.initial_size, self.next_entry))
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

        this_class_idx = self.class_idx[klass]

        with self.env.begin(write=False) as txn:
            db_id_idx = self.env.open_db(DB_ID_IDX, txn=txn)

            # if empty:
            #    txn.drop(db=db, delete=False)

            for obj in objects:
                key = self.next_entry = self.next_entry + 1

                full_key = ((int.from_bytes(this_class_idx, 'little') << 32) | key).to_bytes(8, 'little')
                for referenced_class_idx, ref, version in only_references(obj, self.serializer):
                    unresolved_value = self.serializer.encode_key(ref, version, referenced_class_idx, include_clazz=True)
                    resolved_idx = txn.get(unresolved_value, db=db_id_idx)
                    if resolved_idx:
                        self.queue.put(
                            (
                                DB_REFERENCE_OUTWARD,
                                full_key,
                                resolved_idx,
                            )
                        )
                        # self.queue.put(
                        #    (
                        #        DB_REFERENCE_INWARD,
                        #        resolved_idx,
                        #        full_key,
                        #    )
                        #)
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
                        this_class_idx,
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
    def consumer(queue: mp.Queue, path: str, max_dbs: int, initial_size: int, next_entry: int) -> None:  # type: ignore
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
            dbis: dict[bytes, lmdb.database.Database] = {}
            with env.begin(write=True) as txn:
                while True:
                    try:
                        items = queue.get(timeout=0.05)  # probeer een nieuw item
                    except Exception:
                        # timeout → commit de transactie (door contextmanager) en start opnieuw
                        break

                    if items is None:
                        # commit wat er nog in txn zit, daarna stoppen
                        return

                    for item in items:
                        db_name, key, value = item

                        if db_name == DB_ID_IDX: # or db_name == DB_REFERENCE_INWARD:
                            # DB_ID_IDX, encoded_key, partial
                            # INWARD, resolved_idx, partial
                            value = (value | next_entry).to_bytes(8, 'little')
                        elif db_name == DB_UNRESOLVED or db_name == DB_REFERENCE_OUTWARD:
                            # UNRESOLVED, partial, resolved_value
                            # OUTWARD, partial, resolved_idx
                            key = (key | next_entry).to_bytes(8, 'little')
                        else:
                            # dbname, None, value
                            key = next_entry.to_bytes(4, 'little')

                        dbi = dbis.get(db_name)
                        if dbi is None:
                            dbi = dbis[db_name] = env.open_db(db_name, txn=txn)

                        if value is None:
                            pass

                        txn.put(key, value, db=dbi)

                    # When the entire item is inserted, increment
                    next_entry += 1
