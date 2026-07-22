import dataclasses
import enum
from typing import Any

from PySide6.QtGui import QStandardItemModel, QStandardItem
from PySide6.QtWidgets import QWidget, QTreeView, QAbstractItemView

from gui.models.storageobject import StorageObject
from gui.panels.detailpanel import DetailPanelProvider


class TreeViewPanelProvider(DetailPanelProvider):
    @staticmethod
    def _populate_tree_from_object(parent_item: QStandardItem, data: Any, visited: set[int]) -> None:
        """
        Recursively populates a QStandardItemModel from a Python object,
        with protection against circular references.
        """
        obj_id = id(data)
        if obj_id in visited:
            parent_item.appendRow(QStandardItem("... (Circular Reference) ..."))
            return

        # Only track containers to avoid adding primitives to the visited set.
        is_container = (dataclasses.is_dataclass(data) or isinstance(data, (dict, list)) or hasattr(data, "__dict__")) and not isinstance(data, enum.Enum)

        if is_container:
            visited.add(obj_id)

        try:
            if dataclasses.is_dataclass(data):
                for field_info in dataclasses.fields(data):
                    key = field_info.name
                    value = getattr(data, key)
                    if value is None or value == {} or value == [] or value == '':
                        continue
                    child_item = QStandardItem(str(key))
                    parent_item.appendRow(child_item)
                    TreeViewPanelProvider._populate_tree_from_object(child_item, value, visited)
            elif isinstance(data, dict):
                if len(data) == 0:
                    return

                for key, value in data.items():
                    if value is None:
                        continue
                    child_item = QStandardItem(str(key))
                    parent_item.appendRow(child_item)
                    TreeViewPanelProvider._populate_tree_from_object(child_item, value, visited)
            elif isinstance(data, list):
                if len(data) == 0:
                    return
                for i, value in enumerate(data):
                    if value is None:
                        continue
                    child_item = QStandardItem(f"[{i}]")
                    parent_item.appendRow(child_item)
                    TreeViewPanelProvider._populate_tree_from_object(child_item, value, visited)
            elif hasattr(data, '__dict__') and not isinstance(data, enum.Enum):
                for key, value in data.__dict__.items():
                    if key in ["outgoing_refs", "incoming_refs"] or value is None:
                        continue
                    child_item = QStandardItem(str(key))
                    parent_item.appendRow(child_item)
                    TreeViewPanelProvider._populate_tree_from_object(child_item, value, visited)
            else:
                # This case should not be hit with a None value if the container
                # loops above filter them out, but as a safeguard:
                if data is not None:
                    parent_item.appendRow(QStandardItem(str(data)))
        finally:
            if is_container:
                visited.remove(obj_id)

    def can_handle(self, lmdbo: StorageObject) -> bool:
        return lmdbo.obj is not None

    def create_panel(self) -> tuple[QWidget, str]:
        tree_view = QTreeView()
        tree_view.setHeaderHidden(True)
        tree_view.setEnabled(False)
        tree_view.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        return tree_view, "Object Attributes"

    def update_panel(self, widget: QWidget, lmdbo: StorageObject) -> None:
        tree_view: QTreeView = widget
        model = QStandardItemModel()
        # Initial call with an empty set for tracking visited objects.
        TreeViewPanelProvider._populate_tree_from_object(model.invisibleRootItem(), lmdbo.obj, set())
        tree_view.setModel(model)
        tree_view.expandAll()

    def clear_panel(self, widget: QWidget) -> None:
        tree_view: QTreeView = widget
        tree_view.setModel(None)
