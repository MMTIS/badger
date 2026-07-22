from PySide6.QtCore import QModelIndex, Signal, Qt
from PySide6.QtGui import QMouseEvent
from PySide6.QtWidgets import QListView


class CtrlClickListView(QListView):
    ctrlClicked = Signal(QModelIndex)

    def mousePressEvent(self, event: QMouseEvent) -> None:
        if event.modifiers() & Qt.KeyboardModifier.ControlModifier:
            index = self.indexAt(event.pos())
            if index.isValid():
                self.ctrlClicked.emit(index)
                return
        super().mousePressEvent(event)
