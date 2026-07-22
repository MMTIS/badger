from abc import ABC, abstractmethod
from PySide6.QtWidgets import QWidget
from gui.models.storageobject import StorageObject


class DetailPanelProvider(ABC):
    @abstractmethod
    def can_handle(self, lmdbo: StorageObject) -> bool:
        pass

    @abstractmethod
    def create_panel(self) -> tuple[QWidget, str]:
        """Creates the panel widget and returns it with a title."""
        pass

    @abstractmethod
    def update_panel(self, widget: QWidget, lmdbo: StorageObject) -> None:
        """Populates an existing panel widget with data from the object."""
        pass

    @abstractmethod
    def clear_panel(self, widget: QWidget) -> None:
        """Clears the content of the panel widget."""
        pass
