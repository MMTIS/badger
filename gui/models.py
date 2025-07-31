# models.py
from typing import Optional, List, Tuple, Dict
from PySide6.QtCore import QAbstractListModel, QModelIndex, Qt, Slot, QThreadPool, QRunnable

from netex import MultilingualString
from netexio.database import Database, LMDBObject, Tid

OBJECT_FETCH_BATCH_SIZE = 50


class _ObjectLoaderTask(QRunnable):
    """A QRunnable task to load a single LMDBObject in the background."""
    def __init__(self, lmdbo: LMDBObject):
        super().__init__()
        self.lmdbo = lmdbo

    def run(self):
        """Accesses the .obj property to trigger the lazy load."""
        # The LMDBObject's dataLoaded signal will be emitted from this thread,
        # and Qt's signal/slot mechanism will handle marshalling it to the
        # main GUI thread if the receiver (the model) lives there.
        _ = self.lmdbo.obj


class LazyObjectListModel(QAbstractListModel):
    def __init__(self, database: Database, parent=None):
        super().__init__(parent)
        self.database = database
        self._cache: List[LMDBObject] = []
        self._can_fetch_more = True
        self.clazz: type[Tid] | None = None
        self._filter_text = ""
        # Background loading infrastructure
        self._thread_pool = QThreadPool.globalInstance()
        self._loading_rows = set()

    def set_database(self, clazz: type[Tid]):
        self.beginResetModel()
        self._cache = []
        self._can_fetch_more = True
        self.clazz = clazz
        # When the database changes, any previous filter is no longer valid.
        self._filter_text = ""
        # Cancel any pending background loads as they are no longer relevant
        self._thread_pool.clear()
        self._loading_rows.clear()
        self.endResetModel()
        if self.clazz:
            self.fetchMore(QModelIndex())

    def setFilterText(self, text: str):
        self.beginResetModel()
        self._cache = []
        self._can_fetch_more = True
        self._filter_text = text
        # Cancel any pending background loads as they are no longer relevant
        self._thread_pool.clear()
        self._loading_rows.clear()
        self.endResetModel()
        self.fetchMore(QModelIndex())

    def rowCount(self, parent=QModelIndex()) -> int:
        return len(self._cache) if not parent.isValid() else 0

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or not (0 <= index.row() < len(self._cache)):
            return None

        lmdbo = self._cache[index.row()]

        if role == Qt.DisplayRole:
            # Check if the full object is loaded without triggering the load
            if lmdbo._obj is not None:
                display_name, version = lmdbo.get_name_or_id()
                return f"{display_name} ({version})"
            else:
                # Before loading, display lean info from the key to keep it fast
                id_val, version = lmdbo.get_id_version()
                return f"{id_val} ({version})"

        if role == Qt.UserRole:
            # Return the LMDBObject itself. Accessing .obj on it from elsewhere
            # will now trigger the load and the connected signal.
            return lmdbo
        return None

    def canFetchMore(self, parent=QModelIndex()) -> bool:
        return self._can_fetch_more if not parent.isValid() else False

    def fetchMore(self, parent=QModelIndex()):
        if not self.canFetchMore(parent):
            return

        start_row = self.rowCount()
        start_key = self._cache[-1].key if self._cache else None
        new_items = list(self.database.scan_objects(self.clazz, start_key=start_key, limit=OBJECT_FETCH_BATCH_SIZE, filter_text=self._filter_text))

        if not new_items:
            self._can_fetch_more = False
            return

        self.beginInsertRows(QModelIndex(), start_row, start_row + len(new_items) - 1)
        self._cache.extend(new_items)
        self.endInsertRows()

        # Connect the signal for each new item to our update slot
        for i, item in enumerate(new_items):
            row = start_row + i
            # Use a lambda with a default argument to capture the current row value
            item.dataLoaded.connect(lambda r=row: self._on_object_loaded(r))

    @Slot(int)
    def _on_object_loaded(self, row: int):
        """Slot to handle the dataLoaded signal from an LMDBObject."""
        # Once loaded, it's no longer in the "loading" state.
        self._loading_rows.discard(row)
        model_index = self.index(row, 0)
        self.dataChanged.emit(model_index, model_index, [Qt.DisplayRole])

    def proactively_load_range(self, start_row: int, end_row: int):
        """
        Queues background loading for objects in the specified row range
        if they are not already loaded or being loaded.
        """
        if start_row < 0:
            return

        for row in range(start_row, end_row + 1):
            if 0 <= row < self.rowCount() and row not in self._loading_rows:
                lmdbo = self._cache[row]
                if lmdbo._obj is None:
                    self._loading_rows.add(row)
                    task = _ObjectLoaderTask(lmdbo)
                    self._thread_pool.start(task)


class ReferenceListModel(QAbstractListModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        # Store raw reference identifiers, not objects or availability flags
        self._references: List[Tuple[type[Tid], str, str]] = []
        self.database: Optional[Database] = None
        # Cache availability to avoid repeated DB checks for the same item
        self._availability_cache: Dict[int, bool] = {}

    def populate(self, references: List[Tuple[type[Tid], str, str]], database: Database):
        self.beginResetModel()
        self._references = references
        self.database = database
        self._availability_cache.clear()
        self.endResetModel()

    def rowCount(self, parent=QModelIndex()) -> int:
        return len(self._references) if not parent.isValid() else 0

    def _is_available(self, row: int) -> bool:
        """Lazily checks if the object for the given row exists in the DB."""
        if self.database is None:
            return False

        if row not in self._availability_cache:
            clazz, id_val, version = self._references[row]
            # This is a fast point-query, only done for visible items.
            key = self.database.serializer.encode_key(id_val, version, clazz)
            self._availability_cache[row] = self.database.check_object_by_key(clazz, key)

        return self._availability_cache.get(row, False)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or not (0 <= index.row() < len(self._references)):
            return None

        reference = self._references[index.row()]

        if role == Qt.DisplayRole:
            clazz, id_val, version = reference
            return f"{id_val} ({version})"
        elif role == Qt.ToolTipRole:
            if not self._is_available(index.row()):
                return "This object reference does not exist in the database"
        elif role == Qt.UserRole:
            return reference
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        if not index.isValid():
            return Qt.NoItemFlags

        if not self._is_available(index.row()):
            return Qt.NoItemFlags
        else:
            return Qt.ItemIsSelectable | Qt.ItemIsEnabled
