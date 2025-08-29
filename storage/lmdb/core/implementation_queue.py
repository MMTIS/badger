from pathlib import Path
from typing import Iterable
import multiprocessing as mp

import lmdb

from domain.netex.services.model_typing import Tid
from domain.netex.services.recursive_attributes import only_references
from storage.interface import Storage, Serializer
from storage.lmdb.core.implementation import LmdbStorage, DB_ID_IDX, DB_REFERENCE_OUTWARD, DB_REFERENCE_INWARD, \
    DB_UNRESOLVED


class LmdbStorageQueue(LmdbStorage):
    queue: mp.Queue  # type: ignore

    def __init__(self, path: Path, queue: mp.Queue):
        super().__init__(path, readonly=True)
        self.queue = queue

    def insert_objects_on_queue(self, klass: type[Tid], objects: Iterable[Tid], empty: bool = False) -> None:
        print(klass)

        this_class_idx = self.class_idx[klass]

        with self.env.begin(write=False) as txn:
            db_id_idx = self.env.open_db(DB_ID_IDX, txn=txn)

            # if empty:
            #    txn.drop(db=db, delete=False)

            for obj in objects:
                key = int(next(self.last_entry))

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