import logging
from pathlib import Path
from collections.abc import Iterable
from typing import Any
import multiprocessing as mp

from utils.aux_logging import log_all

from domain.netex.services.model_typing import Tid
from domain.netex.services.recursive_attributes import only_references
from storage.mdbx.core.implementation import MdbxStorage, DB_ID_IDX, DB_REFERENCE_OUTWARD, DB_UNRESOLVED, DB_ID_IDX_FLAGS


class MdbxStorageQueue(MdbxStorage):
    queue: mp.Queue[list[tuple[bytes, Any, Any]]]

    def __init__(self, path: Path, queue: mp.Queue[list[tuple[bytes, Any, Any]]]):
        super().__init__(path, readonly=True)
        self.queue = queue

    def insert_objects_on_queue(self, clazz: type[Tid], objects: Iterable[Tid], empty: bool = False) -> None:
        log_all(logging.DEBUG, f"[queue] insert_objects_on_queue {clazz}")

        this_clazz_idx = self.clazz_idx[clazz]

        with self.env.ro_transaction() as txn:
            db_id_idx = txn.open_map(DB_ID_IDX, flags=DB_ID_IDX_FLAGS)

            for obj in objects:
                # Each insert will receive a unique key, therefore they must be grouped together
                updates: list[tuple[bytes, Any, Any]] = []

                partial_key = int.from_bytes(this_clazz_idx, 'little') << 32
                for referenced_clazz_idx, ref, version in only_references(obj, self.serializer):
                    unresolved_value = self.serializer.encode_key(ref, version, referenced_clazz_idx)
                    resolved_idx = db_id_idx.get(txn, unresolved_value)
                    if resolved_idx:
                        updates.append(
                            (
                                DB_REFERENCE_OUTWARD,
                                partial_key,
                                resolved_idx,
                            )
                        )
                    else:
                        updates.append(
                            (
                                DB_UNRESOLVED,
                                partial_key,
                                unresolved_value,
                            )
                        )

                value = self.serializer.marshall(obj, clazz)
                updates.append(
                    (
                        this_clazz_idx,
                        None,
                        value,
                    )
                )
                updates.append(
                    (
                        DB_ID_IDX,
                        self.serializer.encode_key(str(obj.id), obj.version if hasattr(obj, "version") else None, obj.__class__),
                        partial_key,
                    )
                )

                self.queue.put(updates)
