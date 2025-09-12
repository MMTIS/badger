from pathlib import Path
from typing import Iterable, Any
import multiprocessing as mp

from domain.netex.services.model_typing import Tid
from domain.netex.services.recursive_attributes import only_references
from storage.mdbx.core.implementation import MdbxStorage, DB_ID_IDX, DB_REFERENCE_OUTWARD, DB_UNRESOLVED


class MdbxStorageQueue(MdbxStorage):
    queue: mp.Queue[list[tuple[bytes, Any, Any]]]

    def __init__(self, path: Path, queue: mp.Queue[list[tuple[bytes, Any, Any]]]):
        super().__init__(path, readonly=True)
        self.queue = queue

    def insert_objects_on_queue(self, klass: type[Tid], objects: Iterable[Tid], empty: bool = False) -> None:
        print(klass)

        this_class_idx = self.class_idx[klass]

        with self.env.ro_transaction() as txn:
            db_id_idx = txn.open_map(DB_ID_IDX)

            for obj in objects:
                # Each insert will receive a unique key, therefore they must be grouped together
                updates: list[tuple[bytes, Any, Any]] = []

                partial_key = int.from_bytes(this_class_idx, 'little') << 32
                # partial_key = ((int.from_bytes(this_class_idx, 'little') << 32) | key).to_bytes(8, 'little')
                for referenced_class_idx, ref, version in only_references(obj, self.serializer):
                    unresolved_value = self.serializer.encode_key(ref, version, referenced_class_idx, include_clazz=True)
                    resolved_idx = db_id_idx.get(txn, unresolved_value)
                    if resolved_idx:
                        updates.append(
                            (
                                DB_REFERENCE_OUTWARD,
                                partial_key,
                                resolved_idx,
                            )
                        )
                        # updates.append(
                        #    (
                        #        DB_REFERENCE_INWARD,
                        #        resolved_idx,
                        #        partial_key,
                        #    )
                        # )
                    else:
                        updates.append(
                            (
                                DB_UNRESOLVED,
                                partial_key,
                                unresolved_value,
                            )
                        )

                value = self.serializer.marshall(obj, klass)
                updates.append(
                    (
                        this_class_idx,
                        None,
                        value,
                    )
                )
                updates.append(
                    (
                        DB_ID_IDX,
                        self.serializer.encode_key(str(obj.id), obj.version if hasattr(obj, "version") else None, obj.__class__, include_clazz=True),
                        partial_key,
                    )
                )

                self.queue.put(updates)
