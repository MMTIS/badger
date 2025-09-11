from typing import Optional, Any

from PySide6.QtCore import QObject, Signal, QModelIndex, Qt, Slot
from PySide6.QtWidgets import QWidget, QComboBox

from domain.netex.services.model_typing import Tid
from domain.utils import get_object_name
from gui.controllers.storagecontroller import StorageController
from gui.models.lazyobjectlistmodel import LazyObjectListModel
from gui.models.storageobject import StorageObject
from gui.panels.detailpanel import DetailPanelProvider

# from gui.panels.geojsonpanel import GeoJSONPanelProvider
from gui.panels.netexxmlpanel import NeTExXmlPanelProvider
from gui.panels.treeviewpanel import TreeViewPanelProvider
from gui.widgets.perspective import PerspectiveWidget


class PerspectiveController(QObject):
    request_new_perspective = Signal(object)
    titleChanged = Signal(str)

    def __init__(self, storage_controller: StorageController, widget: PerspectiveWidget):
        super().__init__()
        self._storage_controller = storage_controller
        self.widget = widget  # Gebruik de doorgegeven widget
        self.current_lmdbo: Optional[StorageObject] = None
        self.dirty_panels: set[QWidget] = set()
        for idx, clazz in self._storage_controller.db_names().items():
            self.widget.db_combo_box.addItem(get_object_name(clazz), clazz)
        self.source_model = LazyObjectListModel(self._storage_controller)
        self.widget.set_list_model(self.source_model)
        self.detail_panels: list[tuple[DetailPanelProvider, QWidget]] = []
        self._setup_detail_panels()
        self._connect_signals()

    def _setup_detail_panels(self) -> None:
        """Creates the detail panel widgets once and adds them to the tab view."""
        panel_providers = [TreeViewPanelProvider(), NeTExXmlPanelProvider()]  # TODO: , GeoJSONPanelProvider()]
        for provider in panel_providers:
            widget, title = provider.create_panel()
            self.detail_panels.append((provider, widget))
            _tab_index = self.widget.details_tab_widget.addTab(widget, title)

    def _connect_signals(self) -> None:
        self.widget.db_combo_box.currentIndexChanged.connect(self.on_database_changed)
        # self.widget.search_input.textChanged.connect(self.source_model.setFilterText)
        self.widget.clear_filter_button.clicked.connect(lambda: self.widget.search_input.clear())
        # The main list updates on any selection change (click or keyboard).
        # The selectionModel().currentChanged signal handles this perfectly.
        self.widget.list_view_left.selectionModel().currentChanged.connect(lambda current, previous: self.handle_item_selected(current))
        self.widget.list_view_left.ctrlClicked.connect(self.handle_item_ctrl_clicked)
        # The reference lists can still use the simpler click-to-select handler.
        for list_view in [self.widget.list_view_incoming, self.widget.list_view_outgoing]:
            list_view.clicked.connect(self.handle_item_selected)
            list_view.ctrlClicked.connect(self.handle_item_ctrl_clicked)
        self.widget.details_tab_widget.currentChanged.connect(self._on_detail_tab_changed)

    @staticmethod
    def set_current_index_by_data(combo: QComboBox, value: Any) -> None:
        for index in range(combo.count()):
            if combo.itemData(index) == value:
                combo.setCurrentIndex(index)
                return

    def navigate_to_object(self, lmdbo: StorageObject) -> None:
        if not lmdbo:
            return
        self.current_lmdbo = lmdbo  # Store object for lazy loading panels
        self.titleChanged.emit(lmdbo.name)
        # Use db_name for consistency, as obj.__class__.__name__ might not be the DB key
        if self.widget.db_combo_box.currentData() != lmdbo.obj.__class__:
            self.set_current_index_by_data(self.widget.db_combo_box, lmdbo.obj.__class__)

        # Mark all detail panels as "dirty" - needing an update.
        self.dirty_panels = {widget for _, widget in self.detail_panels}

        # Preserve the currently selected detail tab and trigger its update.
        current_tab_index = self.widget.details_tab_widget.currentIndex()
        if current_tab_index != -1:
            self._on_detail_tab_changed(current_tab_index)

        # Pass the database object to the view so the model can perform lazy checks.
        self.widget.update_reference_lists(self._storage_controller.load_references_inwards(lmdbo), self._storage_controller.load_references_outwards(lmdbo))

    def handle_item_selected(self, index: QModelIndex) -> None:
        if not index.isValid():
            return

        lmdbo = index.data(Qt.ItemDataRole.UserRole)
        # is_from_reference_list = isinstance(lmdbo, tuple)

        # if is_from_reference_list:
        #     clazz, id_val, version = lmdbo
        #     key = self.database.serializer.encode_key(id_val, version, clazz)
        #     lmdbo = self.database. get_object_by_key(clazz, key)

        # if not lmdbo:
        #    return

        self.navigate_to_object(lmdbo)

        # If the click was on a reference list and the referenced object's type
        # matches the type currently displayed in the main list, select it.
        # if is_from_reference_list:
        # After navigate_to_object, the combo box is authoritative.
        #     if self.widget.db_combo_box.currentData() == lmdbo.obj.__class__:
        #        self._find_and_select_item_in_main_list(lmdbo)

    def handle_item_ctrl_clicked(self, index: QModelIndex) -> None:
        lmdbo = index.data(Qt.ItemDataRole.UserRole)
        if lmdbo:
            self.request_new_perspective.emit(lmdbo)

    @Slot(int)
    def on_database_changed(self, index: int) -> None:
        self.source_model.set_database(self.widget.db_combo_box.itemData(index))

    @Slot(int)
    def _on_detail_tab_changed(self, index: int) -> None:
        """
        Updates a detail panel only when its tab becomes visible and it's marked as dirty.
        """
        if not self.current_lmdbo or index < 0 or index >= len(self.detail_panels):
            return

        provider, widget = self.detail_panels[index]

        # Only update if the panel is "dirty" (i.e., a new object has been selected)
        if widget in self.dirty_panels:
            can_handle = provider.can_handle(self.current_lmdbo)
            if can_handle:
                provider.update_panel(widget, self.current_lmdbo)
            else:
                provider.clear_panel(widget)
            widget.setEnabled(can_handle)
            self.dirty_panels.remove(widget)

    def set_initial_state(self, lmdbo_to_show: StorageObject) -> None:
        self.widget.db_combo_box.blockSignals(True)
        self.widget.db_combo_box.setCurrentText(get_object_name(lmdbo_to_show.clazz))
        self.widget.db_combo_box.blockSignals(False)
        self.source_model.set_database(lmdbo_to_show.clazz)
        if lmdbo_to_show:
            self.navigate_to_object(lmdbo_to_show)
            self._find_and_select_item_in_main_list(lmdbo_to_show)

    def set_initial_state_by_class(self, clazz: Tid) -> None:
        self.widget.db_combo_box.blockSignals(True)
        self.widget.db_combo_box.setCurrentText(get_object_name(clazz))
        self.widget.db_combo_box.blockSignals(False)
        self.source_model.set_database(clazz)

    def _find_and_select_item_in_main_list(self, target_obj: StorageObject) -> None:
        """Finds an item in the main list model, fetching more if necessary, and selects it."""
        # To guarantee that we can find the target object, we must first ensure
        # the model is in a clean, unfiltered state, just like when a new
        # perspective is created.
        # We achieve this by programmatically clearing the search input. This
        # triggers the model to reset itself via the existing signal-slot connection,
        # which is the most reliable way to synchronize the UI and the model state.
        if self.widget.search_input.text():
            self.widget.search_input.clear()

        start_row = 0
        while True:
            # Search for the object in the currently cached items, starting from where we left off
            for row in range(start_row, self.source_model.rowCount()):
                index = self.source_model.index(row, 0)
                item_obj = index.data(Qt.ItemDataRole.UserRole)
                if item_obj and item_obj.key == target_obj.key:
                    self.widget.list_view_left.setCurrentIndex(index)
                    self.widget.list_view_left.scrollTo(index)
                    return

            # If not found, check if we can fetch more
            rows_before_fetch = self.source_model.rowCount()
            if self.source_model.canFetchMore():
                self.source_model.fetchMore(QModelIndex())
                start_row = rows_before_fetch
                # If fetchMore didn't add any items, we're at the end
                if start_row == self.source_model.rowCount():
                    break
            else:
                break  # No more items to fetch
