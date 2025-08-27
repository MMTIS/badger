from typing import Iterable

from PySide6.QtCore import QTimer, Qt, QPoint, Slot, QAbstractListModel
from PySide6.QtWidgets import QWidget, QSplitter, QHBoxLayout, QVBoxLayout, QComboBox, QLineEdit, QPushButton, QLabel, QTabWidget

from gui.models.lazyobjectlistmodel import LazyObjectListModel
from gui.models.referencelistmodel import ReferenceListModel
from gui.models.storageobject import StorageObject
from gui.views.ctrlclicklistview import CtrlClickListView


class PerspectiveWidget(QWidget):
    def __init__(self, parent: QWidget):
        super().__init__(parent)

        self.splitter = QSplitter()
        main_layout = QHBoxLayout(self)
        main_layout.addWidget(self.splitter)
        main_layout.setContentsMargins(0, 0, 0, 0)
        self.splitter.addWidget(self._create_left_panel())
        self.splitter.addWidget(self._create_right_panel())
        self.splitter.setSizes([350, 850])

        # When the combobox changes, the controller updates the model. However,
        # the model might not emit `modelReset` or `layoutChanged`, leaving the
        # view unaware of the update. By connecting here, we ensure the
        # proactive loading is triggered. Using QTimer.singleShot ensures this
        # runs *after* the controller's slot has finished updating the model.
        self.db_combo_box.currentIndexChanged.connect(lambda: QTimer.singleShot(0, self.on_left_list_view_updated))

    def _create_left_panel(self) -> QWidget:
        left_widget = QWidget()
        layout = QVBoxLayout(left_widget)
        self.db_combo_box = QComboBox()
        filter_layout = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Filter objecten...")
        self.clear_filter_button = QPushButton("X")
        self.clear_filter_button.setFixedWidth(30)
        filter_layout.addWidget(self.search_input)
        filter_layout.addWidget(self.clear_filter_button)
        self.list_view_left = CtrlClickListView()
        self.list_view_left.setAlternatingRowColors(True)
        layout.addWidget(QLabel("Database:"))
        layout.addWidget(self.db_combo_box)
        layout.addWidget(QLabel("Filter:"))
        layout.addLayout(filter_layout)
        layout.addWidget(self.list_view_left)
        return left_widget

    def _create_right_panel(self) -> QWidget:
        right_widget = QWidget()
        layout = QVBoxLayout(right_widget)
        self.details_tab_widget = QTabWidget()
        ref_widget = QWidget()
        ref_layout = QVBoxLayout(ref_widget)
        ref_layout.setContentsMargins(0, 0, 0, 0)
        ref_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.list_view_incoming = CtrlClickListView()
        self.list_view_outgoing = CtrlClickListView()
        self.incoming_ref_model = ReferenceListModel()
        self.outgoing_ref_model = ReferenceListModel()
        self.list_view_incoming.setModel(self.incoming_ref_model)
        self.list_view_outgoing.setModel(self.outgoing_ref_model)
        incoming_widget = QWidget()
        incoming_layout = QVBoxLayout(incoming_widget)
        incoming_layout.addWidget(QLabel("Incoming References:"))
        incoming_layout.addWidget(self.list_view_incoming)
        outgoing_widget = QWidget()
        outgoing_layout = QVBoxLayout(outgoing_widget)
        outgoing_layout.addWidget(QLabel("Outgoing References:"))
        outgoing_layout.addWidget(self.list_view_outgoing)
        ref_splitter.addWidget(incoming_widget)
        ref_splitter.addWidget(outgoing_widget)
        ref_layout.addWidget(ref_splitter)
        layout.addWidget(QLabel("Details:"))
        layout.addWidget(self.details_tab_widget, stretch=2)
        layout.addWidget(ref_widget, stretch=1)
        return right_widget

    def set_list_model(self, model: QAbstractListModel) -> None:
        # First, disconnect from the old model if it exists.
        old_model = self.list_view_left.model()
        if old_model:
            try:
                old_model.modelReset.disconnect(self.on_left_list_view_updated)
                old_model.layoutChanged.disconnect(self.on_left_list_view_updated)
                self.list_view_left.verticalScrollBar().valueChanged.disconnect(self.on_left_list_view_updated)
            except (RuntimeError, TypeError):
                pass  # It's fine if they were not connected.

        # Now, set the new model.
        self.list_view_left.setModel(model)

        # Finally, connect to the new model's signals.
        if model:
            model.modelReset.connect(self.on_left_list_view_updated)
            model.layoutChanged.connect(self.on_left_list_view_updated)
            self.list_view_left.verticalScrollBar().valueChanged.connect(self.on_left_list_view_updated)

            # Schedule an initial load for the currently visible items.
            # Using QTimer.singleShot ensures this runs after the current event processing,
            # allowing the view to update its layout first.
            QTimer.singleShot(0, self.on_left_list_view_updated)

    def update_reference_lists(self, inwards: Iterable[StorageObject], outwards: Iterable[StorageObject]) -> None:
        self.incoming_ref_model.populate(inwards)
        self.outgoing_ref_model.populate(outwards)

    @Slot()
    def on_left_list_view_updated(self) -> None:
        model = self.list_view_left.model()
        if not isinstance(model, LazyObjectListModel):
            return

        viewport_rect = self.list_view_left.viewport().rect()
        first_index = self.list_view_left.indexAt(viewport_rect.topLeft())
        # Check the bottom-right corner (minus 1px) to robustly find the last visible item.
        last_index = self.list_view_left.indexAt(viewport_rect.bottomRight() - QPoint(1, 1))

        if first_index.isValid():
            start_row = first_index.row()
            end_row = last_index.row() if last_index.isValid() else model.rowCount() - 1
            model.proactively_load_range(start_row, end_row)
