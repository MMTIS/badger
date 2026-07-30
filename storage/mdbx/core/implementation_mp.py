from types import TracebackType
from typing import Optional, Type, Literal, Self
import multiprocessing as mp
import queue
from pathlib import Path
from mdbx.mdbx import DBI, Env

from storage.mdbx.core.implementation import DB_ID_IDX, DB_REFERENCE_OUTWARD, DB_REFERENCE_OUTWARD_FLAGS, DB_UNRESOLVED, DB_UNRESOLVED_FLAGS, DB_ID_IDX_FLAGS
from storage.interface import Serializer
from storage.mdbx.core.implementation_queue import MdbxStorageQueue


class MdbxStorageMP(MdbxStorageQueue):
    writer: mp.Process
    queue: queue.Queue[tuple[bytes, bytes, bytes, tuple[bytes, ...]] | None]

    def __init__(self, path: Path, readonly: bool = True):
        self.ctx = mp.get_context("spawn")
        self.manager = self.ctx.Manager()
        self.queue = self.manager.Queue(maxsize=10000)
        super().__init__(path, self.queue, readonly)

    def __enter__(self) -> Self:
        # This should take care of the ground work, like new database, we loose that Env because that must be done in the consumer
        super().__enter__()

        if not self.readonly:
            self.writer = mp.Process(target=self.consumer, args=(self.queue, self.path.as_posix(), self.max_dbs))
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

        return super().__exit__(exception_type, exception_value, exception_traceback)

    @staticmethod
    def consumer(queue: queue.Queue[tuple[bytes, bytes, bytes, tuple[bytes, ...]] | None], path: str, max_dbs: int) -> None:
        # This is the actual database
        env = Env(
            path,
            maxdbs=max_dbs,
        )

        while True:
            i: int = 0
            with env.rw_transaction() as txn:
                db_unresolved = txn.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
                db_id_idx = txn.open_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
                db_reference_outward = txn.open_map(name=DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)

                # Optimisation: only once open a database (table)
                dbis: dict[bytes, DBI] = {}

                while True:
                    try:
                        item = queue.get(timeout=1)  # probeer een nieuw item
                    except Exception:
                        # timeout → commit transaction
                        txn.commit()
                        break

                    if item is True:
                        # explicitly asked for a commit
                        txn.commit()
                        break

                    elif item is None:
                        # queue received terminal, commit and stop
                        txn.commit()
                        dbis = {}
                        return

                    my_id, value, this_clazz_idx, unresolved = item

                    # First: check if the id already exists, then we must overwrite.
                    full_key = db_id_idx.get(txn, my_id)
                    if full_key is not None:
                        idx = Serializer.full_key_to_idx(full_key)
                        try:
                            db_reference_outward.delete(txn, full_key)
                        except:  # noqa: E722
                            pass
                    else:
                        idx = db_id_idx.get_sequence(txn, 1).to_bytes(4, 'little')
                        full_key = Serializer.get_fullkey_by_clazz_idx(idx, this_clazz_idx)

                    for unresolved_value in unresolved:
                        resolved_idx = db_id_idx.get(txn, unresolved_value)
                        if resolved_idx:
                            db_reference_outward.put(txn, full_key, resolved_idx)
                        else:
                            db_unresolved.put(txn, full_key, unresolved_value)

                    dbi = dbis.get(this_clazz_idx)
                    if dbi is None:
                        dbi = dbis[this_clazz_idx] = txn.create_map(this_clazz_idx)

                    dbi.put(txn, idx, value)
                    db_id_idx.put(txn, my_id, full_key)
                    i += 1

                    if i > 5000:
                        txn.commit()
                        break
