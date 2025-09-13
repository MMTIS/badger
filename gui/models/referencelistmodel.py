from typing import Any, Iterable

from PySide6.QtCore import QAbstractListModel, QModelIndex, QPersistentModelIndex, QObject, Qt, Slot

from gui.models.storageobject import StorageObject


class ReferenceListModel(QAbstractListModel):
    def __init__(self, parent: QObject | None = None):
        super().__init__(parent)
        # Store raw reference identifiers, not objects or availability flags
        self._references: list[StorageObject] = []
        # Cache availability to avoid repeated DB checks for the same item
        self._availability_cache: dict[int, bool] = {}

    @Slot(int)
    def _on_object_loaded(self, row: int) -> None:
        model_index = self.index(row, 0)
        self.dataChanged.emit(model_index, model_index, [Qt.ItemDataRole.DisplayRole])

    def populate(self, references: Iterable[StorageObject]) -> None:
        self.beginResetModel()
        self._references = list(references)
        self._availability_cache.clear()
        self.endResetModel()

        for row, item in enumerate(self._references):
            item.objLoaded.connect(lambda _unused, r=row: self._on_object_loaded(r))

    def rowCount(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> int:
        return len(self._references) if not parent.isValid() else 0

    # def _is_available(self, row: int) -> bool:
    #     """Lazily checks if the object for the given row exists in the DB."""
    #     if self.database is None:
    #         return False
    #
    #     if row not in self._availability_cache:
    #        reference = self._references[row]
    #        # This is a fast point-query, only done for visible items.
    #        key = self.database.serializer.encode_key(id_val, version, clazz)
    #        self._availability_cache[row] = self.database.check_object_by_key(clazz, key)
    #
    #     return self._availability_cache.get(row, False)

    def data(self, index: QModelIndex | QPersistentModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid() or not (0 <= index.row() < len(self._references)):
            return None

        reference = self._references[index.row()]

        if role == Qt.ItemDataRole.DisplayRole and reference.obj is not None:
            return f"{reference.obj.id} ({reference.obj.version})"
        # elif role == Qt.ItemDataRole.ToolTipRole:
        #     if not self._is_available(index.row()):
        #        return "This object reference does not exist in the database"
        elif role == Qt.ItemDataRole.UserRole:
            return reference
        return None

    def flags(self, index: QModelIndex | QPersistentModelIndex) -> Qt.ItemFlag:
        if not index.isValid():
            return Qt.ItemFlag.NoItemFlags

        # if not self._is_available(index.row()):
        #    return Qt.ItemFlag.NoItemFlags
        # else:
        return Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled

    def clear(self):
        self._references.clear()

    def append_batch(self, refs: list[StorageObject]) -> None:
        if not refs:
            return
        start = len(self._references)
        end = start + len(refs) - 1
        self.beginInsertRows(QModelIndex(), start, end)
        self._references.extend(refs)
        self.endInsertRows()