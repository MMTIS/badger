import logging
import queue
from pathlib import Path
from collections.abc import Iterable
from domain.netex.model import EntityStructure
from utils.aux_logging import log_all
from mdbx.mdbx import TXN
from domain.netex.services.recursive_attributes import only_references
from storage.mdbx.core.implementation import MdbxStorage


class MdbxStorageQueue(MdbxStorage):
    queue: queue.Queue[tuple[bytes, bytes, bytes, tuple[bytes, ...]] | None]

    def __init__(self, path: Path, queue: queue.Queue[tuple[bytes, bytes, bytes, tuple[bytes, ...]] | None], readonly: bool = False):
        super().__init__(path, readonly=readonly)
        self.queue = queue

    def insert_any_object_on_queue(self, txn: TXN, objects: Iterable[EntityStructure]) -> None:
        """
        In this function we are not going to any database work, not reading not writing.
        Everything will be treated like unresolved. This will produce an object and a
        package of relationships, which we won't have to compute at a later stage.
        """
        log_all(logging.DEBUG, "[queue] insert_any_objects_on_queue")

        if self.readonly:
            raise

        for obj in objects:
            # Queue format:
            #         serialised object, unresolved
            # (my_id, value,             [my_id_format])

            my_id = self.serializer.encode_obj(obj)
            value = self.serializer.marshall(obj, obj.__class__)

            # TODO: If we want to do the references minus own embeddding, would be good to start here
            unresolved = tuple(
                {self.serializer.encode_key(ref, version, referenced_clazz) for referenced_clazz, ref, version in only_references(obj, self.serializer)}
            )

            self.queue.put((my_id, value, self.clazz_idx[obj.__class__], unresolved))
