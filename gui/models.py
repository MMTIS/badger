# models.py
from typing import Optional, List
from PySide6.QtCore import QAbstractListModel, QModelIndex, Qt
from netexio.database import Database, LMDBObject, Tid

OBJECT_FETCH_BATCH_SIZE = 50


class LazyObjectListModel(QAbstractListModel):
    def __init__(self, database: Database, parent=None):
        super().__init__(parent)
        self.database = database
        self._cache: List[LMDBObject] = []
        self._can_fetch_more = True
        self.clazz: type[Tid] | None = None
        self._filter_text = ""

    def set_database(self, clazz: type[Tid]):
        self.beginResetModel()
        self._cache = []
        self._can_fetch_more = True
        self.clazz = clazz
        self.endResetModel()
        if self.clazz:
            self.fetchMore(QModelIndex())

    def setFilterText(self, text: str):
        self.beginResetModel()
        self._cache = []
        self._can_fetch_more = True
        self._filter_text = text
        self.endResetModel()
        self.fetchMore(QModelIndex())

    def rowCount(self, parent=QModelIndex()) -> int:
        return len(self._cache) if not parent.isValid() else 0

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or not (0 <= index.row() < len(self._cache)):
            return None
        if role == Qt.DisplayRole:
            obj = self._cache[index.row()]
            id_val, version = obj.get_id_version()
            return f"{id_val} ({version})"
        if role == Qt.UserRole:
            return self._cache[index.row()]
        return None

    def canFetchMore(self, parent=QModelIndex()) -> bool:
        return self._can_fetch_more if not parent.isValid() else False

    def fetchMore(self, parent=QModelIndex()):
        if not self.canFetchMore(parent):
            return
        start_key = self._cache[-1].key if self._cache else None
        new_items = list(self.database.scan_objects(self.clazz, start_key=start_key, limit=OBJECT_FETCH_BATCH_SIZE, filter_text=self._filter_text))
        if not new_items:
            self._can_fetch_more = False
            return
        self.beginInsertRows(QModelIndex(), self.rowCount(), self.rowCount() + len(new_items) - 1)
        self._cache.extend(new_items)
        self.endInsertRows()


class ReferenceListModel(QAbstractListModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._objects: List[LMDBObject] = []

    def populate(self, objects: List[LMDBObject]):
        self.beginResetModel()
        self._objects = objects
        self.endResetModel()

    def rowCount(self, parent=QModelIndex()) -> int:
        return len(self._objects) if not parent.isValid() else 0

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or not (0 <= index.row() < len(self._objects)):
            return None
        reference, available = self._objects[index.row()]
        if role == Qt.DisplayRole:
            clazz, id_val, version = reference
            return f"{id_val} ({version})"
        elif role == Qt.ToolTipRole:
            if not available:
                return "This object is not part of the database"
        elif role == Qt.UserRole:
            return reference
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        if not index.isValid():
            return Qt.NoItemFlags

        reference, available = self._objects[index.row()]
        if not available:
            return Qt.NoItemFlags
        else:
            return Qt.ItemIsEnabled
