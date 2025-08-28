from typing import Any, Generic

from PySide6.QtCore import QAbstractListModel, QModelIndex, Qt, Slot, QThreadPool, QRunnable, QPersistentModelIndex, QObject

from domain.netex.services.model_typing import Tid
from gui.controllers.storagecontroller import StorageController
from gui.models.storageobject import StorageObject

OBJECT_FETCH_BATCH_SIZE = 50


class _ObjectLoaderTask(QRunnable):
    """A QRunnable task to load a single LMDBObject in the background."""

    def __init__(self, lmdbo: StorageObject):
        super().__init__()
        self.lmdbo = lmdbo

    def run(self) -> None:
        """Accesses the .obj property to trigger the lazy load."""
        # The LMDBObject's dataLoaded signal will be emitted from this thread,
        # and Qt's signal/slot mechanism will handle marshalling it to the
        # main GUI thread if the receiver (the model) lives there.
        self.lmdbo.load()


class LazyObjectListModel(QAbstractListModel):
    clazz: Generic[Tid]
    _filter_text: str

    def __init__(self, storage_controller: StorageController, parent: QObject | None = None):
        super().__init__(parent)
        self._storage_controller = storage_controller
        self._cache: list[StorageObject] = []
        self._can_fetch_more = True
        # self._filter_text = ""
        # Background loading infrastructure
        self._thread_pool = QThreadPool.globalInstance()
        self._loading_rows: set[int] = set()

    def set_database(self, clazz: type[Tid]) -> None:
        self.beginResetModel()
        self._cache = []
        self._can_fetch_more = True
        self.clazz = clazz
        # When the database changes, any previous filter is no longer valid.
        # self._filter_text = ""
        # Cancel any pending background loads as they are no longer relevant
        self._thread_pool.clear()
        self._loading_rows.clear()
        self.endResetModel()
        if self.clazz:
            self.fetchMore(QModelIndex())

    """
    def setFilterText(self, text: str) -> None:
        self.beginResetModel()
        self._cache = []
        self._can_fetch_more = True
        self._filter_text = text
        # Cancel any pending background loads as they are no longer relevant
        self._thread_pool.clear()
        self._loading_rows.clear()
        self.endResetModel()
        self.fetchMore(QModelIndex())
    """

    def rowCount(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> int:
        return len(self._cache) if not parent.isValid() else 0
        # I wonder if a pattern like below could give a more reasonable scrollbar, but I expect table scanning is required
        # return self._storage_controller.entries_for_class(self.clazz) if not parent.isValid() else 0

    def data(self, index: QModelIndex | QPersistentModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid() or not (0 <= index.row() < len(self._cache)):
            return None

        lmdbo = self._cache[index.row()]

        if role == Qt.ItemDataRole.DisplayRole:
            # Check if the full object is loaded without triggering the load
            if lmdbo._obj is not None:
                return f"{lmdbo.name} ({lmdbo._obj.version})"
            else:
                # Before loading, display lean info from the key to keep it fast
                return "-"

        if role == Qt.ItemDataRole.UserRole:
            # Return the LMDBObject itself. Accessing .obj on it from elsewhere
            # will now trigger the load and the connected signal.
            return lmdbo
        return None

    def canFetchMore(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> bool:
        return self._can_fetch_more if not parent.isValid() else False

    def fetchMore(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> None:
        if not self.canFetchMore(parent):
            return

        start_row = self.rowCount()
        start_key = self._cache[-1].key if self._cache else None
        # TODO: self.clazz may be uninitialized
        new_items = list(self._storage_controller.scan_objects(self.clazz, start_key=start_key, limit=OBJECT_FETCH_BATCH_SIZE))

        if len(new_items) == 0:
            self._can_fetch_more = False
            return

        self.beginInsertRows(QModelIndex(), start_row, start_row + len(new_items) - 1)
        self._cache.extend(new_items)
        self.endInsertRows()

        # Connect the signal for each new item to our update slot
        for i, item in enumerate(new_items):
            row = start_row + i
            # Use a lambda with a default argument to capture the current row value
            item.objLoaded.connect(lambda _unused, r=row: self._on_object_loaded(r))

    @Slot(int)
    def _on_object_loaded(self, row: int) -> None:
        """Slot to handle the dataLoaded signal from an LMDBObject."""
        # Once loaded, it's no longer in the "loading" state.
        self._loading_rows.discard(row)
        model_index = self.index(row, 0)
        self.dataChanged.emit(model_index, model_index, [Qt.ItemDataRole.DisplayRole])

    def proactively_load_range(self, start_row: int, end_row: int) -> None:
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
