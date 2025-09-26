from typing import Any, Optional, Generator

from PySide6.QtCore import QObject, Slot

from domain.netex.services.model_typing import Tid
from gui.models.storageobject import StorageObject


class StorageController(QObject):
    """Controller die requests van LmdbObject naar storage vertaalt."""

    def __init__(self, storage: Any, parent: Optional[QObject] = None) -> None:
        super().__init__(parent)
        self._storage: Any = storage

    @Slot(type, bytes)
    def handleRequest(self, clazz: type, key: bytes) -> None:
        """Laadt object uit storage en stuurt terug naar de requester."""
        with self._storage.env.ro_transaction() as txn:
            obj: Any = self._storage.load_object(txn, clazz, key)
            if obj:
                sender = self.sender()
                if isinstance(sender, StorageObject):
                    sender.setObj(obj)

    @Slot(type, bytes, int)
    def scan_objects(self, clazz: type[Tid], start_key: bytes | None, limit: int) -> Generator[StorageObject, None, None]:
        with self._storage.env.ro_transaction() as txn:
            for key in self._storage.scan_objects(txn, clazz, start_key, limit):
                yield StorageObject(clazz, key, self)

    @Slot(object)
    def load_references_inwards(self, lmdbo: StorageObject) -> Generator[StorageObject, None, None]:
        with self._storage.env.ro_transaction() as txn:
            for clazz, key in self._storage.load_references_by_clazz_key(txn, lmdbo.clazz, lmdbo.key, True):
                yield StorageObject(clazz, key, self)

    @Slot(object)
    def load_references_outwards(self, lmdbo: StorageObject) -> Generator[StorageObject, None, None]:
        with self._storage.env.ro_transaction() as txn:
            for clazz, key in self._storage.load_references_by_clazz_key(txn, lmdbo.clazz, lmdbo.key, False):
                yield StorageObject(clazz, key, self)

    def db_names(self) -> dict[bytes, type]:
        return self._storage.db_names()
