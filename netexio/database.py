from __future__ import annotations

from logging import Logger
import random
from types import TracebackType

import lmdb
import threading
import queue
import os
import cloudpickle
from typing import TypeVar, Iterable, Any, Optional, Type, Literal
from enum import IntEnum

from netex import (
    EntityStructure,
    EntityInVersionStructure,
    VersionOfObjectRefStructure,
)
from netexio.activelrucache import ActiveLRUCache
from netexio.dbaccess import update_embedded_referencing
from netexio.serializer import Serializer
from utils.utils import get_object_name

T = TypeVar("T")
Tid = TypeVar("Tid", bound=EntityStructure)
Tver = TypeVar("Tver", bound=EntityInVersionStructure)
Tref = TypeVar("Tref", bound=VersionOfObjectRefStructure)


class LmdbActions(IntEnum):
    STOP = 0
    WRITE = 1
    DELETE_PREFIX = 2
    CLEAR = 3
    DROP = 4


class Embedding:
    parent_class: str
    parent_id: str
    parent_version: str
    klass: str
    id: str
    version: str
    path: str


class Referencing:
    parent_class: str
    parent_id: str
    parent_version: str
    klass: str
    ref: str
    version: str


class Database:
    task_queue: queue.Queue[tuple[LmdbActions, Optional[lmdb._Database], Optional[Any], Optional[Any]]] | None
    writer_thread: threading.Thread | None

    def __init__(
        self,
        path: str,
        serializer: Serializer,
        readonly: bool = True,
        logger: Logger | None = None,
        initial_size: int = 4 * 1024**3,
        growth_size: int | None = None,
        max_size: int = 36 * 1024**3,
        batch_size: int = 10_000,
        max_mem: int = 4 * 1024**3,
    ):
        self.path = path
        self.logger = logger
        self.initial_size = int(initial_size)
        self.growth_size = growth_size if growth_size else self.initial_size
        self.max_size = max_size
        self.readonly = readonly
        self.dbs: dict[str, lmdb._Database] = {}
        self.batch_size = batch_size
        self.max_mem = max_mem
        self.serializer = serializer
        self.max_dbs = len(self.serializer.name_object) + 2

        self.cache = ActiveLRUCache(100)

    def __enter__(self) -> Database:
        if self.readonly:
            self.env = lmdb.open(self.path, max_dbs=self.max_dbs, readonly=self.readonly)

        else:
            self.initial_size = self.initial_size
            self.max_size = self.max_size
            self.lock = threading.Lock()

            # Threaded writer infrastructure
            self.task_queue = None
            self.writer_thread = None

            self.env = lmdb.open(
                self.path,
                max_dbs=self.max_dbs,
                map_size=self.initial_size,
                writemap=True,
                metasync=False,
                sync=False,
                subdir=True,
            )

        self.db_embedding: lmdb._Database = self.env.open_db(b"_embedding", create=not self.readonly, dupsort=True)
        self.db_embedding_inverse: lmdb._Database = self.env.open_db(b"_embedding_inverse", create=not self.readonly, dupsort=True)
        self.db_referencing: lmdb._Database = self.env.open_db(b"_referencing", create=not self.readonly, dupsort=True)
        self.db_referencing_inwards: lmdb._Database = self.env.open_db(b"_referencing_inwards", create=not self.readonly, dupsort=True)

        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> Literal[False]:
        self.block_until_done()
        self.env.close()
        return False  # Allow errors to propagate!

    def usage(self) -> tuple[int, int]:
        with self.lock:
            allocated_size = self.env.info()["map_size"]
            used_size = os.path.getsize(self.path)
            return allocated_size, used_size

    def guard_free_space(self, percentage: float) -> None:
        allocated_size, used_size = self.usage()
        min_increase = int(allocated_size * percentage)
        if (allocated_size - used_size) < min_increase:
            self._resize_env(min_increase)

    def _resize_env(self, min_increase: int = 0) -> None:
        """Ensures LMDB grows by at least growth_size or min_increase."""
        with self.lock:
            current_size = self.env.info()["map_size"]
            increase = max(self.growth_size, int(min_increase))  # Ensure enough space
            self.initial_size = min(current_size + increase, self.max_size)
            if self.initial_size > current_size:
                print(f"Resizing LMDB from {current_size} to {self.initial_size} bytes")
                self.env.set_mapsize(self.initial_size)
            else:
                raise RuntimeError("LMDB reached max map size, cannot grow further.")

    def _writer(self) -> None:
        """Handles both inserts and deletions from the queue, with retry on failure."""

        assert self.task_queue is not None, "Task queue must not be none"

        action = LmdbActions.STOP

        while True:
            batch: list[tuple[lmdb._Database, Any, Any]] = []
            delete_tasks: list[tuple[lmdb._Database, Any]] = []
            drop_tasks: list[lmdb._Database] = []
            clear_tasks: list[lmdb._Database] = []

            total_size = 0

            try:
                for _ in range(self.batch_size):
                    action, database, key, value = self.task_queue.get(timeout=30)

                    match action:
                        case LmdbActions.WRITE:
                            assert key is not None, "Key must not be none"
                            assert value is not None, "Value must not be none"
                            batch.append((database, key, value))
                            total_size += len(key) + len(value)  # Key + Value size

                        case LmdbActions.DELETE_PREFIX:
                            assert key is not None, "Key must not be none"
                            delete_tasks.append((database, key))

                        case LmdbActions.CLEAR:
                            clear_tasks.append(database)

                        case LmdbActions.DROP:
                            drop_tasks.append(database)

                        case LmdbActions.STOP:
                            break

                    if total_size >= self.max_mem:
                        break  # Commit early if memory limit is reached

            except queue.Empty:
                pass

            if batch or delete_tasks or clear_tasks or drop_tasks:
                self._process_batch(batch, delete_tasks, clear_tasks, drop_tasks, total_size)

            if action == LmdbActions.STOP:
                break

        # Cleanup
        with self.lock:
            self.task_queue = None
            self.writer_thread = None

    def _process_batch(
        self,
        batch: list[tuple[lmdb._Database, Any, Any]],
        delete_tasks: list[tuple[lmdb._Database, Any]],
        clear_task: list[lmdb._Database],
        drop_task: list[lmdb._Database],
        total_size: int,
    ) -> None:
        """Processes a batch of writes and deletions, retrying if needed."""
        while True:
            try:
                with self.env.begin(write=True) as txn:
                    # Process drops
                    for db_handle1 in drop_task:
                        txn.drop(db=db_handle1, delete=True)

                    # Process clears
                    for db_handle1 in clear_task:
                        txn.drop(db=db_handle1, delete=False)

                    # Process deletions
                    for db_handle1, prefix in delete_tasks:
                        cursor = txn.cursor(db=db_handle1)
                        if cursor.set_range(prefix):
                            while bytes(cursor.key()).startswith(prefix):
                                cursor.delete()
                                if not cursor.next():
                                    break

                    # Process insertions
                    for db_handle1, key, value in batch:
                        txn.put(key, value, db=db_handle1)

                break  # Success, exit loop
            except lmdb.MapFullError:
                print("LMDB full, resizing...")
                self._resize_env(total_size)

    def _start_writer_if_needed(self) -> None:
        """Starts the writer thread if it's not already running."""
        with self.lock:
            if self.task_queue is None:
                self.task_queue = queue.Queue(maxsize=1000)  # Shared queue
                self.writer_thread = threading.Thread(target=self._writer, args=(), daemon=True)
                self.writer_thread.start()

    def open_db(self, klass: type[Tid], delete: bool = False) -> lmdb._Database:
        name: str = get_object_name(klass)

        if name in self.dbs:
            return self.dbs[name]
        else:
            name_bytes = name.encode("utf-8")
            try:
                # Try opening in read-only mode first to isolate the issue
                db = self.env.open_db(name_bytes, create=False)
            except lmdb.Error as e:
                if self.readonly or delete:
                    return
                if "MDB_NOTFOUND" in str(e):
                    print(f"Database {name} does not exist, creating it.")
                    db = self.env.open_db(name_bytes, create=True)
                else:
                    raise  # Reraise other LMDB errors
            except Exception as ex:
                print(f"Unexpected error: {ex}")
                raise

            self.dbs[name] = db
            if delete:
                del self.dbs[name]
            return db

    def _reopen_dbs(self) -> None:
        for name in self.dbs.keys():
            name_bytes = name.encode("utf-8")
            self.dbs[name] = self.env.open_db(name_bytes)

    def _insert_embedding_on_queue(self, obj: Tid) -> None:
        assert obj.id is not None, "Object must have an id"
        assert self.task_queue is not None, "Task queue must not be none"

        parent_class: type[Tid]
        parent_id: str
        parent_version: str
        object_class: type[Tid]
        object_id: str
        object_version: str
        path: str | None

        for (
            parent_class,
            parent_id,
            parent_version,
            object_class,
            object_id,
            object_version,
            path,
        ) in update_embedded_referencing(self.serializer, obj):
            if path is not None:
                embedding_inverse_key = self.serializer.encode_key(object_id, object_version, object_class, include_clazz=True)
                embedding_inverse_value = cloudpickle.dumps((get_object_name(parent_class), parent_id, parent_version, path))
                self.task_queue.put(
                    (
                        LmdbActions.WRITE,
                        self.db_embedding_inverse,
                        embedding_inverse_key,
                        embedding_inverse_value,
                    )
                )

                embedding_key = self.serializer.encode_key(parent_id, parent_version, parent_class, include_clazz=True)
                embedding_value = cloudpickle.dumps((get_object_name(object_class), object_id, object_version, path))
                self.task_queue.put(
                    (
                        LmdbActions.WRITE,
                        self.db_embedding,
                        embedding_key,
                        embedding_value,
                    )
                )

            else:
                # TODO: This won't work because of out of order behavior
                # self.task_queue.put((LmdbActions.DELETE_PREFIX, self.db_referencing, key_prefix))

                ref_key = self.serializer.encode_key(parent_id, parent_version, parent_class, include_clazz=True)
                ref_value = cloudpickle.dumps((get_object_name(object_class), object_id, object_version))
                self.task_queue.put((LmdbActions.WRITE, self.db_referencing, ref_key, ref_value))

                ref_key = self.serializer.encode_key(object_id, object_version, object_class, include_clazz=True)
                ref_value = cloudpickle.dumps((get_object_name(parent_class), parent_id, parent_version))
                self.task_queue.put((LmdbActions.WRITE, self.db_referencing_inwards, ref_key, ref_value))

    def insert_objects_on_queue(self, klass: type[Tid], objects: Iterable[Tid], empty: bool = False) -> None:
        """Places objects in the shared queue for writing, starting writer if needed."""
        db_handle = self.open_db(klass)
        if db_handle is None:
            return

        self._start_writer_if_needed()
        assert self.task_queue is not None, "Task queue must not be none"

        if empty:
            self.task_queue.put((LmdbActions.CLEAR, db_handle, None, None))

        for obj in objects:
            assert obj.id is not None, "Object must have an id"
            version = obj.version if hasattr(obj, "version") else None
            key = self.serializer.encode_key(obj.id, version, klass)
            value = self.serializer.marshall(obj, klass)
            self.task_queue.put((LmdbActions.WRITE, db_handle, key, value))
            self._insert_embedding_on_queue(obj)

    def insert_one_object(self, object: Tid) -> None:
        if object.id == 'SBB:DataSource:SBB':
            pass
        return self.insert_objects_on_queue(object.__class__, [object])

    def insert_raw_on_queue(self, objects: Iterable[tuple[lmdb._Database, bytes, bytes]]) -> None:
        """Places a hybrid list of encoded pairs in the shared queue for writing, starting writer if needed."""
        self._start_writer_if_needed()
        assert self.task_queue is not None, "Task queue must not be none"

        for db_handle, key, value in objects:
            self.task_queue.put((LmdbActions.WRITE, db_handle, key, value))

    def clear(self, classes: list[type[Tid]]) -> None:
        if self.readonly:
            return

        self._start_writer_if_needed()
        assert self.task_queue is not None, "Task queue must not be none"

        for klass in classes:
            db_handle = self.open_db(klass)
            if db_handle is None:
                return

            self.task_queue.put((LmdbActions.CLEAR, db_handle, None, None))

    def drop(self, classes: list[type[Tid]], embedding: bool = False) -> None:
        if self.readonly:
            return

        self._start_writer_if_needed()
        assert self.task_queue is not None, "Task queue must not be none"

        for klass in classes:
            db_handle = self.open_db(klass, delete=True)
            if db_handle is None:
                return

            self.task_queue.put((LmdbActions.DROP, db_handle, None, None))

        if embedding:
            self.task_queue.put((LmdbActions.CLEAR, self.db_embedding, None, None))
            self.task_queue.put((LmdbActions.CLEAR, self.db_embedding_inverse, None, None))
            self.task_queue.put((LmdbActions.CLEAR, self.db_referencing, None, None))
            self.task_queue.put((LmdbActions.CLEAR, self.db_referencing_inwards, None, None))

    def delete_by_prefix(self, klass: type[Tid], prefix: bytes) -> None:
        """Schedules deletion of all keys with a given prefix using the writer thread."""
        if self.readonly:
            return

        db_handle = self.open_db(klass)
        if db_handle is None:
            return

        self._start_writer_if_needed()
        assert self.task_queue is not None, "Task queue must not be none"

        self.task_queue.put((LmdbActions.DELETE_PREFIX, db_handle, prefix, None))

    def block_until_done(self) -> None:
        if self.readonly:
            return

        if self.task_queue is not None and self.writer_thread is not None:
            assert self.writer_thread is not None, "Writer thread must not be none"
            assert self.task_queue is not None, "Task queue must not be none"

            self.task_queue.put((LmdbActions.STOP, None, None, None))
            self.writer_thread.join()  # Wait for writer to finish

    def close(self) -> None:
        self.block_until_done()
        self.env.close()

    def vacuum(self) -> None:
        if self.readonly:
            return

        self.block_until_done()

        with self.lock:
            if self.env:
                tmp_file = self.path + "_compacted.mdb"
                self.env.copy(path=tmp_file, compact=True)
                self.env.close()
                os.rename(tmp_file, self.path)
                self.__enter__()
                self._reopen_dbs()

    def get_random(self, clazz: type[Tid]) -> Tid | None:
        db_handle = self.open_db(clazz)
        if db_handle is None:
            return None

        with self.env.begin(buffers=True, write=False) as txn:
            cursor = txn.cursor(db_handle)

            # Move to a random position by skipping N random entries
            if cursor.first():
                rand_skip = random.randint(0, txn.stat(db_handle)["entries"] - 1)
                for _ in range(rand_skip):
                    if not cursor.next():
                        break  # Stop if we reach the end

                return self.serializer.unmarshall(cursor.value(), clazz)

        return None  # If DB is empty

    def get_single(self, clazz: type[Tid], id: str, version: str | None = None) -> Tid | None:
        db = self.open_db(clazz)
        if db is None:
            return None

        prefix = self.serializer.encode_key(id, version, clazz)
        with self.env.begin(write=False, buffers=True, db=db) as txn:
            if version:
                value = txn.get(prefix)
                if value:
                    return self.serializer.unmarshall(value, clazz)
            else:
                cursor = txn.cursor()
                if cursor.set_range(prefix):  # Position cursor at the first key >= prefix
                    for key, value in cursor:
                        if not bytes(key).startswith(prefix):
                            break  # Stop when keys no longer match the prefix

                        # TODO: What about handling the validity too here?
                        return self.serializer.unmarshall(value, clazz)

        return None

    def copy_db(self, target: Database, klass: type[Tid]) -> None:
        """
        Copies a single database from `src_env` to `dst_env` with high throughput.

        - `src_env`: Source LMDB environment
        - `dst_env`: Destination LMDB environment
        """
        if target.readonly:
            return

        target._start_writer_if_needed()
        assert target.task_queue is not None, "Task queue must not be none"

        src_db = self.open_db(klass)
        if src_db is None:
            return

        dst_db = target.open_db(klass)
        if dst_db is None:
            return

        with self.env.begin(write=False, buffers=True, db=src_db) as src_txn:
            cursor = src_txn.cursor()
            for key, value in cursor:
                target.task_queue.put((LmdbActions.WRITE, dst_db, bytes(key), bytes(value)))

    def copy_db_embedding(self, target: Database, classes: list[type[Tid]]) -> None:
        """
        Copies '_referencing' and '_embedding' databases from `self.env` to `target.env` with high throughput.
        """
        if target.readonly:
            return

        target._start_writer_if_needed()
        assert target.task_queue is not None, "Task queue must not be none"

        classes_name = {self.serializer.encode_key(None, None, klass, True) for klass in classes}

        def _copy_db(src_db: lmdb._Database, dst_db: lmdb._Database) -> None:
            """Helper function to copy data between LMDB databases efficiently."""
            if src_db is None:
                return

            if dst_db is None:
                return

            assert target.task_queue is not None, "Task queue must not be none"

            with self.env.begin(write=False, buffers=True, db=src_db) as src_txn:
                cursor = src_txn.cursor()
                for prefix in classes_name:
                    if cursor.set_range(prefix):
                        key = bytes(cursor.key())
                        while key.startswith(prefix):
                            target.task_queue.put((LmdbActions.WRITE, dst_db, key, bytes(cursor.value())))
                            if not cursor.next():
                                break

        # Copy both databases
        _copy_db(self.db_referencing, target.db_referencing)
        _copy_db(self.db_referencing_inwards, target.db_referencing_inwards)
        _copy_db(self.db_embedding, target.db_embedding)
        _copy_db(self.db_embedding_inverse, target.db_embedding_inverse)

    def clean_cache(self) -> None:
        self.cache.drop()

    def get_class_by_name(self, name: str) -> type[Tid]:
        return self.serializer.name_object[name]

    def tables(self, exclusively: set[type[Tid]] | None = None) -> list[type[Tid]]:
        if exclusively is None:
            exclusively = set(self.serializer.interesting_classes)

        tables: set[type[Tid]] = set([])
        with self.env.begin(buffers=True, write=False) as txn:
            cursor = txn.cursor()
            for key, _ in cursor:
                name = bytes(key).decode("utf-8")
                if name[0] != "_":
                    tables.add(self.get_class_by_name(name))

        return sorted(list(tables.intersection(exclusively)), key=lambda v: v.__name__)

    def stats(self) -> None:
        print("stats:")
        with self.env.begin(write=False) as txn:
            cursor = txn.cursor()
            for key, _ in cursor:
                name = bytes(key).decode("utf-8")
                with self.env.begin(db=self.env.open_db(key), write=False) as txn2:
                    cursor2 = txn2.cursor()
                    key_count = sum(1 for _ in cursor2)  # Count keys manually
                    print(name, key_count)

    def referencing(self, exclusively: set[type[Tid]] | None = None) -> list[type[Tid]]:
        if exclusively is None:
            exclusively = set(self.serializer.interesting_classes)

        tables: set[type[Tid]] = set([])
        with self.env.begin(write=False, buffers=True, db=self.db_referencing) as txn:
            cursor = txn.cursor()
            for _key, value in cursor:
                klass, *_ = cloudpickle.loads(value)
                tables.add(self.get_class_by_name(klass))

        return sorted(list(tables.intersection(exclusively)), key=lambda v: v.__name__)

    def embedded(self, exclusively: set[type[Tid]] | None = None) -> list[type[Tid]]:
        if exclusively is None:
            exclusively = set(self.serializer.interesting_classes)

        tables: set[type[Tid]] = set([])
        with self.env.begin(write=False, buffers=True, db=self.db_embedding_inverse) as txn:
            cursor = txn.cursor()
            for _key, value in cursor:
                klass, *_ = cloudpickle.loads(value)
                tables.add(self.get_class_by_name(klass))

        return sorted(list(tables.intersection(exclusively)), key=lambda v: v.__name__)
