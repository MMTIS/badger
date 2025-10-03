from __future__ import annotations

from typing import Optional, Any, TYPE_CHECKING

from PySide6.QtCore import QObject, Signal, Slot

from domain.netex.model import MultilingualString, TextType
from domain.netex.services.model_typing import Tid

if TYPE_CHECKING:
    from gui.controllers.storagecontroller import StorageController


class StorageObject(QObject):
    """QObject wrapper dat een key representeert en via de controller laadt."""

    requestLoad: Signal = Signal(type, bytes)  # vraagt data op bij controller
    objLoaded: Signal = Signal(object)  # signaleert dat obj geladen is
    nameChanged: Signal = Signal(str)  # specifiek signaal voor property "name"

    def __init__(self, clazz: type, key: bytes, controller: StorageController, parent: Optional[QObject] = None) -> None:
        super().__init__(parent)
        self.key: bytes = key
        self.clazz: type = clazz
        self._obj: Optional[Any] = None
        self._name: Optional[str] = None

        self.requestLoad.connect(controller.handleRequest)

    def load(self) -> None:
        """Trigger een load via de controller."""
        if self._obj is None:
            self.requestLoad.emit(self.clazz, self.key)

    @Slot(object)
    def setObj(self, obj: Any) -> None:
        """Ontvangt data vanuit de controller en slaat het op."""
        self._obj = obj
        self.objLoaded.emit(obj)

        name = getattr(self._obj, 'name', None)
        if name is not None:
            if isinstance(name, MultilingualString):
                if len(name.content) > 0:
                    if isinstance(name.content[0], str):
                        self._name = name.content[0]
                        self.nameChanged.emit(self._name)

                    elif isinstance(name.content[0], TextType):
                        self._name = name.content[0].value
                        self.nameChanged.emit(self._name)

            elif isinstance(name, str):
                self._name = name
                self.nameChanged.emit(self._name)

        if self._name is None:
            self._name = self._obj.id
            self.nameChanged.emit(self._name)

    @property
    def obj(self) -> Optional[Tid]:
        self.load()
        return self._obj

    @property
    def name(self) -> str:
        self.load()
        return str(self._name)
