from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from netexio.database import Database

import functools
import pickle
import queue
import threading
from typing import Generator
from netexio.dbaccess import update_embedded_referencing
from netexio.serializer import Serializer
from utils.utils import get_object_name


def embedding_update(db: Database, filter_clazz: list = None):
    # TODO: remove when implemented correctly
    db.block_until_done()

    task_queue = queue.Queue(maxsize=1000)  # Prevents excessive memory usage
    stop_signal = object()  # Special object to signal the writer to stop
    db_embedding = db.env.open_db(b"_embedding")
    db_referencing = db.env.open_db(b"_referencing")

    # Drop existing databases
    with db.env.begin(write=True) as txn:
        txn.drop(db_embedding, delete=False)
        txn.drop(db_referencing, delete=False)

    def reader(table):
        """ Reads from the table and pushes modified data to the queue. """
        with db.env.begin(write=False, buffers=True, db=db.open_db(table)) as txn_ro:
            cursor = txn_ro.cursor()
            for db_key, db_value in cursor:  # Rename outer loop variables
                i, j = 0, 0
                deserialized = db.serializer.unmarshall(db_value, table)

                for embedding in update_embedded_referencing(db.serializer, deserialized):
                    key_data = (embedding[0], embedding[1], embedding[2])
                    if embedding[7] is not None:
                        embedding_key = (*key_data, i)
                        embedding_value = (embedding[3], embedding[4], embedding[5], embedding[6], embedding[7])
                        task_queue.put((b"_embedding", embedding_key, embedding_value))
                        i += 1
                    else:
                        ref_key = (*key_data, j)
                        ref_value = (embedding[3], embedding[4], embedding[5], embedding[6])
                        task_queue.put((b"_referencing", ref_key, ref_value))
                        j += 1

    def writer():
        """ Continuously writes data from the queue to LMDB in batches. """
        while True:
            batch = []
            try:
                # Fetch up to 100 items from the queue
                for _ in range(100):
                    batch.append(task_queue.get(timeout=3))
            except queue.Empty:
                if not batch:
                    break  # Stop if queue remains empty

            # Use a short-lived transaction for each batch
            with db.env.begin(write=True) as txn:
                for db_name, key, value in batch:
                    if db_name == stop_signal:  # Stop signal received
                        return  # Exit writer function

                    db_handle = db_embedding if db_name == b"_embedding" else db_referencing
                    txn.put(pickle.dumps(key), pickle.dumps(value), db=db_handle)

    # Start writer thread
    writer_thread = threading.Thread(target=writer, daemon=True)
    writer_thread.start()

    # Spawn reader threads
    reader_threads = []
    for table in db.tables():
        thread = threading.Thread(target=reader, args=(table,))
        thread.start()
        reader_threads.append(thread)

    # Wait for all readers to finish
    for thread in reader_threads:
        thread.join()

    # Signal the writer to stop and wait for it to finish
    task_queue.put((stop_signal, None, None))
    writer_thread.join()
