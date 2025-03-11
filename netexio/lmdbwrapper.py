from __future__ import annotations

import lmdb
import threading
import queue
import os
import cloudpickle
from typing import TypeVar, Iterable
from enum import IntEnum

from netexio.activelrucache import ActiveLRUCache
from netexio.dbaccess import update_embedded_referencing
from netexio.serializer import Serializer
from utils.utils import get_object_name

T = TypeVar("T")


class LmdbActions(IntEnum):
    WRITE = 1
    DELETE_PREFIX = 2
    CLEAR = 3


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


class LMDBDatabase:
    def __init__(self, path: str, serializer: Serializer, readonly=True, initial_size=256 * 1024 * 1024,
                 growth_size=None,
                 max_size=16 * 1024 ** 3, batch_size=10_000, max_mem=4 * 1024 ** 3):
        self.path = path
        self.initial_size = initial_size
        self.growth_size = growth_size
        self.max_size = max_size
        self.readonly = readonly
        self.dbs: dict[str, object] = {}
        self.batch_size = batch_size
        self.max_mem = max_mem
        self.serializer = serializer
        self.max_dbs = len(self.serializer.name_object)
        self.db_embedding = self.open_db(Embedding)
        self.db_referencing = self.open_db(Referencing)

        self.cache = ActiveLRUCache(100)

    def __enter__(self):
        if self.readonly:
            self.env = lmdb.open(
                self.path,
                readonly=self.readonly
            )

        else:
            self.initial_size = self.initial_size
            self.growth_size = self.growth_size if self.growth_size else self.initial_size  # Linear growth
            self.max_size = self.max_size
            self.lock = threading.Lock()

            # Threaded writer infrastructure
            self.task_queue = queue.Queue(maxsize=1000)  # Shared queue
            self.writer_thread = None
            self.stop_signal = object()

            self.env = lmdb.open(
                self.path,
                max_dbs=self.max_dbs,
                map_size=self.initial_size,
                writemap=True,
                metasync=False,
                sync=False
            )

        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.block_until_done()
        self.env.close()

    def _resize_env(self, min_increase=0):
        """Ensures LMDB grows by at least growth_size or min_increase."""
        with self.lock:
            current_size = self.env.info()["map_size"]
            increase = max(self.growth_size, int(min_increase))  # Ensure enough space
            new_size = min(current_size + increase, self.max_size)
            if new_size > current_size:
                print(f"Resizing LMDB from {current_size} to {new_size} bytes")
                self.env.set_mapsize(new_size)
            else:
                raise RuntimeError("LMDB reached max map size, cannot grow further.")

    def _writer(self):
        """Handles both inserts and deletions from the queue, with retry on failure."""
        while True:
            batch = []
            clear_tasks = []
            delete_tasks = []
            total_size = 0
            item = None

            try:
                for _ in range(self.batch_size):
                    item = self.task_queue.get(timeout=3)
                    if item is self.stop_signal:
                        break

                    if item[0] == LmdbActions.CLEAR:
                        clear_tasks.append(item[1])
                    elif item[0] == LmdbActions.DELETE_PREFIX:
                        delete_tasks.append(item[1:])  # Store deletion request separately
                    else:
                        batch.append(item)
                        total_size += len(item[1]) + len(item[2])  # Key + Value size

                    if total_size >= self.max_mem:
                        break  # Commit early if memory limit is reached

            except queue.Empty:
                pass

            if batch or delete_tasks:
                self._process_batch(batch, delete_tasks, clear_tasks, total_size)

            if item is self.stop_signal:
                break

        # Cleanup
        with self.lock:
            self.task_queue = None
            self.writer_thread = None

    def _process_batch(self, batch, delete_tasks, clear_task, total_size):
        """Processes a batch of writes and deletions, retrying if needed."""
        while True:
            try:
                with self.env.begin(write=True) as txn:
                    # Process clears first
                    for db_handle in clear_task:
                        txn.drop(db=db_handle, delete=False)

                    # Process deletions
                    for db_handle, prefix in delete_tasks:
                        cursor = txn.cursor(db=db_handle)
                        if cursor.set_range(prefix):
                            while cursor.key().startswith(prefix):
                                cursor.delete()
                                if not cursor.next():
                                    break

                    # Process insertions
                    for db_handle, key, value in batch:
                        txn.put(key, value, db=db_handle)

                break  # Success, exit loop
            except lmdb.MapFullError:
                print("LMDB full, resizing...")
                self._resize_env(total_size)

    def _start_writer_if_needed(self):
        """ Starts the writer thread if it's not already running. """
        with self.lock:
            if self.task_queue is None:
                self.writer_thread = threading.Thread(
                    target=self._writer,
                    args=(),
                    daemon=True
                )
                self.writer_thread.start()

    def open_db(self, klass: T):
        name: str = get_object_name(klass)
        if name in self.dbs:
            return self.dbs[name]
        else:
            name_bytes = name.encode('utf-8')
            try:
                # Try opening in read-only mode first to isolate the issue
                db = self.env.open_db(name_bytes, create=False)
            except lmdb.Error as e:
                if "MDB_NOTFOUND" in str(e):
                    if self.readonly:
                        return
                    print(f"Database {name} does not exist, creating it.")
                    db = self.env.open_db(name_bytes, create=True)
                else:
                    raise  # Reraise other LMDB errors
            except Exception as ex:
                print(f"Unexpected error: {ex}")
                raise

            self.dbs[name] = db
            return db

    def _insert_embedding_on_queue(self, obj):
        i, j = 0, 0
        for embedding in update_embedded_referencing(self.serializer, obj):
            key_data = (embedding[0], embedding[1], embedding[2])
            key_prefix = self.serializer.encode_key(*key_data)

            if embedding[7] is not None:
                self.task_queue.put((LmdbActions.DELETE_PREFIX, self.db_embedding, key_prefix))

                embedding_key = key_prefix + bytes([ord('-')]) + str(i).encode()
                embedding_value = cloudpickle.dumps(embedding[3], embedding[4], embedding[5], embedding[6],
                                                    embedding[7])
                self.task_queue.put((self.db_embedding, embedding_key, embedding_value))
                i += 1
            else:
                self.task_queue.put((LmdbActions.DELETE_PREFIX, self.db_referencing, key_prefix))

                ref_key = key_prefix + bytes([ord('-')]) + str(j).encode()
                ref_value = cloudpickle.dumps(embedding[3], embedding[4], embedding[5], embedding[6])
                self.task_queue.put((self.db_referencing, ref_key, ref_value))
                j += 1

    def insert_objects_on_queue(self, klass: T, objects: Iterable):
        """ Places objects in the shared queue for writing, starting writer if needed. """
        db_handle = self.open_db(klass)
        self._start_writer_if_needed()

        for obj in objects:
            version = obj.version if hasattr(obj, 'version') else None
            key = self.serializer.encode_key((obj.id, version, klass))
            value = self.serializer.marshall(obj, klass)
            self.task_queue.put((db_handle, key, value))
            self._insert_embedding_on_queue(obj)

    def insert_one_object(self, object, quiet=False):
        return self.insert_objects_on_queue(object.__class__, [object])

    def insert_raw_on_queue(self, objects: Iterable):
        """ Places a hybrid list of encoded pairs in the shared queue for writing, starting writer if needed. """
        self._start_writer_if_needed()

        for db_handle, key, value in objects:
            self.task_queue.put((db_handle, key, value))

    def clear(self, classes: list[T]):
        if self.readonly:
            return

        self._start_writer_if_needed()
        for klass in classes:
            db_handle = self.open_db(klass)
            self.task_queue.put((LmdbActions.CLEAR, db_handle))

    def delete_by_prefix(self, klass: T, prefix: bytes):
        """Schedules deletion of all keys with a given prefix using the writer thread."""
        if self.readonly:
            return

        db_handle = self.open_db(klass)
        self._start_writer_if_needed()
        self.task_queue.put((LmdbActions.DELETE_PREFIX, db_handle, prefix))

    def block_until_done(self):
        if self.readonly:
            return

        if self.writer_thread:
            self.task_queue.put(self.stop_signal)
            self.writer_thread.join()  # Wait for writer to finish

    def close(self):
        self.block_until_done()
        self.env.close()

    def vacuum(self):
        if self.readonly:
            return

        self.block_until_done()

        with self.lock:
            if self.env:
                tmp_file = self.path + "_compacted.mdb"
                self.env.copy(path=tmp_file, compact=True)
                self.env.close()
                os.rename(tmp_file, self.path)
                self.env = lmdb.open(self.path, map_size=self.initial_size, max_dbs=self.max_dbs,
                                     readonly=self.readonly)

    def get_single(self, clazz: T, id, version=None) -> T | None:
        db = self.open_db(clazz)
        if db is None:
            return None

        prefix = self.serializer.encode_key(id, version, clazz)
        with self.env.begin(write=False, db=db) as txn:
            if version:
                value = txn.get(prefix)
                if value:
                    return self.serializer.unmarshall(value, clazz)
            else:
                cursor = txn.cursor()
                if cursor.set_range(prefix):  # Position cursor at the first key >= prefix
                    for key, value in cursor:
                        if not key.startswith(prefix):
                            break  # Stop when keys no longer match the prefix

                        # TODO: What about handling the validity too here?
                        return self.serializer.unmarshall(value, clazz)

    def copy_db(self, target: LMDBDatabase, klass: T, batch_size=1_000, max_mem=4 * 1024 ** 3):
        """
        Copies a single database from `src_env` to `dst_env` with high throughput.

        - `src_env`: Source LMDB environment
        - `dst_env`: Destination LMDB environment
        - `db_name`: Database name (bytes)
        - `batch_size`: Number of records per transaction commit
        - `max_mem`: Max memory usage before commit (default: 4GB)
        """
        if target.readonly:
            return

        self.block_until_done()
        target.block_until_done()

        with target.lock:
            src_db = self.open_db(klass, readonly=True)
            if src_db is None:
                return

            dst_db = target.open_db(klass)

            total_size = 0  # Track memory usage
            batch = []

            with self.env.begin(db=src_db, write=False) as src_txn:
                cursor = src_txn.cursor()
                dst_txn = target.env.begin(write=True, db=dst_db)  # Open initial write transaction

                for key, value in cursor:
                    batch.append((key, value))
                    total_size += len(key) + len(value)

                    if len(batch) >= batch_size or total_size >= max_mem:
                        for k, v in batch:
                            dst_txn.put(k, v)

                        dst_txn.commit()  # Commit batch
                        dst_txn = target.env.begin(write=True, db=dst_db)  # Start new txn

                        batch.clear()
                        total_size = 0  # Reset memory counter

                # Final commit for remaining records
                if batch:
                    for k, v in batch:
                        dst_txn.put(k, v)
                    dst_txn.commit()

    def copy_db_embedding(self, target: LMDBDatabase, classes: list[T], batch_size=1_000, max_mem=4 * 1024 ** 3):
        """
        Copies '_referencing' and '_embedding' databases from `self.env` to `target.env` with high throughput.

        - `batch_size`: Number of records per transaction commit
        - `max_mem`: Max memory usage before commit (default: 4GB)
        """
        if target.readonly:
            return

        self.block_until_done()
        target.block_until_done()

        classes_name = {get_object_name(klass).encode() + bytes([ord('-')]) for klass in classes}

        def _copy_db(klass: T):
            """ Helper function to copy data between LMDB databases efficiently. """
            src_db = self.open_db(klass)
            dst_db = target.open_db(klass)

            total_size = 0  # Track memory usage
            batch = []

            with self.env.begin(write=False, db=src_db) as src_txn:
                cursor = src_txn.cursor()
                dst_txn = target.env.begin(write=True, db=dst_db)  # Open initial write transaction

                for prefix in classes_name:
                    if cursor.set_range(prefix):
                        while cursor.key().startswith(prefix):
                            key = cursor.key()
                            value = cursor.value()
                            batch.append((key, value))
                            total_size += len(key) + len(value)

                            if len(batch) >= batch_size or total_size >= max_mem:
                                for k, v in batch:
                                    dst_txn.put(k, v)

                                dst_txn.commit()  # Commit batch
                                dst_txn = target.env.begin(write=True, db=dst_db)  # Start new txn

                                batch.clear()
                                total_size = 0  # Reset memory counter

                            if not cursor.next():
                                break

                # Final commit for remaining records
                if batch:
                    for k, v in batch:
                        dst_txn.put(k, v)
                    dst_txn.commit()

        with target.lock:
            # Copy both databases
            _copy_db(Referencing)
            _copy_db(Embedding)

    def clean_cache(self):
        self.cache = {}

    def get_class_by_name(self, name: str):
        return self.serializer.name_object[name]

    def tables(self, exclusively: set[T] = None):
        if exclusively is None:
            exclusively = set(self.serializer.interesting_classes)

        if self.env:
            tables = set([])
            with self.env.begin() as txn:
                cursor = txn.cursor()
                for key, _ in cursor:
                    name = key.decode('utf-8')
                    if name[0] != '_':
                        tables.add(self.get_class_by_name(name))
            return sorted(tables.intersection(exclusively), key=lambda v: v.__name__)

    def referencing(self, exclusively: set[T] = None):
        if exclusively is None:
            exclusively = set(self.serializer.interesting_classes)

        if self.env:
            tables = set([])
            with self.env.begin(write=False, db=self.open_db(Referencing)) as txn:
                cursor = txn.cursor()
                for _key, value in cursor:
                    klass, *_ = cloudpickle.loads(value)
                    tables.add(self.get_class_by_name(klass))

            return sorted(tables.intersection(exclusively), key=lambda v: v.__name__)

    def embedded(self, exclusively: set[T] = None):
        if exclusively is None:
            exclusively = set(self.serializer.interesting_classes)

        if self.env:
            tables = set([])
            with self.env.begin(write=False, db=self.open_db(Embedding)) as txn:
                cursor = txn.cursor()
                for _key, value in cursor:
                    _parent_class, _parent_id, _parent_version, klass, _id, _version, _order, _path = cloudpickle.loads(
                        value)
                    tables.add(self.get_class_by_name(klass))

            return sorted(tables.intersection(exclusively), key=lambda v: v.__name__)


if __name__ == '__main__':
    lmdb_db = LMDBDatabase("/tmp/test.lmdb", initial_size=1000 * 1024)
    for j in range(0, 10000):
        items = [(i * j, [b'a' * 3072]) for i in range(0, 1024)]
        lmdb_db.batched_write(db_name=b"test", items=items, batch_size=10000)

    lmdb_db.close()
