from PySide6.QtCore import Qt, QModelIndex, QTimer, Signal, QObject

class ItemSearcher(QObject):
    finished = Signal(object)   # emits QModelIndex or None
    aborted = Signal()

    def __init__(self, list_view, source_model, search_input):
        super().__init__()
        self.list_view = list_view
        self.source_model = source_model
        self.search_input = search_input
        self._state = None
        self._aborted = False

    def start(self, target_obj):
        """Begin asynchronous search."""
        self.abort()  # stop a running search if needed

        if self.search_input.text():
            self.search_input.clear()

        self._state = {
            "target": target_obj,
            "start_row": 0,
        }
        self._aborted = False
        QTimer.singleShot(0, self._continue)

    def abort(self):
        """Abort any running search."""
        if self._state is not None:
            self._aborted = True
            self._state = None
            self.aborted.emit()

    def _continue(self):
        if self._aborted or self._state is None:
            return

        target_obj = self._state["target"]
        start_row = self._state["start_row"]

        # Step 1: search through current chunk
        for row in range(start_row, self.source_model.rowCount()):
            index = self.source_model.index(row, 0)
            item_obj = index.data(Qt.ItemDataRole.UserRole)
            if item_obj and item_obj.key == target_obj.key:
                # Found!
                self.list_view.setCurrentIndex(index)
                self.list_view.scrollTo(index)
                self._state = None
                self.finished.emit(index)
                return

        # Step 2: fetch more if possible
        rows_before = self.source_model.rowCount()
        if self.source_model.canFetchMore():
            self.source_model.fetchMore(QModelIndex())
            if self.source_model.rowCount() == rows_before:
                # no more items
                self._state = None
                self.finished.emit(None)
                return
            self._state["start_row"] = rows_before
            QTimer.singleShot(0, self._continue)
        else:
            # Nothing left to fetch
            self._state = None
            self.finished.emit(None)