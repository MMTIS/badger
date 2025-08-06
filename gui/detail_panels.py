# detail_panels.py
from abc import ABC, abstractmethod
import json
import pprint
import dataclasses
import enum
from decimal import Decimal
from typing import Any
from PySide6.QtCore import QRegularExpression
from PySide6.QtGui import QStandardItemModel, QStandardItem, QSyntaxHighlighter, QTextCharFormat, QColor, QFont
from PySide6.QtWidgets import QAbstractItemView, QWidget, QTreeView, QTextEdit

from conv.netex_db_to_mbtiles import to_feature
from gui.xmlsyntaxhighlighter import XmlSyntaxHighlighter
from gui.qdatabase import LMDBObject, Tid
from transformers.projection import get_all_geo_elements, reprojection
from utils.utils import get_object_name


class DetailPanelProvider(ABC):
    @abstractmethod
    def can_handle(self, lmdbo: LMDBObject) -> bool:
        pass

    @abstractmethod
    def create_panel(self) -> tuple[QWidget, str]:
        """Creates the panel widget and returns it with a title."""
        pass

    @abstractmethod
    def update_panel(self, widget: QWidget, lmdbo: LMDBObject) -> None:
        """Populates an existing panel widget with data from the object."""
        pass

    @abstractmethod
    def clear_panel(self, widget: QWidget) -> None:
        """Clears the content of the panel widget."""
        pass


def _populate_tree_from_object(parent_item: QStandardItem, data: Any, visited: set) -> None:
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
                _populate_tree_from_object(child_item, value, visited)
        elif isinstance(data, dict):
            if len(data) == 0:
                return

            for key, value in data.items():
                if value is None:
                    continue
                child_item = QStandardItem(str(key))
                parent_item.appendRow(child_item)
                _populate_tree_from_object(child_item, value, visited)
        elif isinstance(data, list):
            if len(data) == 0:
                return
            for i, value in enumerate(data):
                if value is None:
                    continue
                child_item = QStandardItem(f"[{i}]")
                parent_item.appendRow(child_item)
                _populate_tree_from_object(child_item, value, visited)
        elif hasattr(data, '__dict__') and not isinstance(data, enum.Enum):
            for key, value in data.__dict__.items():
                if key in ["outgoing_refs", "incoming_refs"] or value is None:
                    continue
                child_item = QStandardItem(str(key))
                parent_item.appendRow(child_item)
                _populate_tree_from_object(child_item, value, visited)
        else:
            # This case should not be hit with a None value if the container
            # loops above filter them out, but as a safeguard:
            if data is not None:
                parent_item.appendRow(QStandardItem(str(data)))
    finally:
        if is_container:
            visited.remove(obj_id)


class TreeViewPanelProvider(DetailPanelProvider):
    def can_handle(self, lmdbo: LMDBObject) -> bool:
        return lmdbo.obj is not None

    def create_panel(self) -> tuple[QWidget, str]:
        tree_view = QTreeView()
        tree_view.setHeaderHidden(True)
        tree_view.setEnabled(False)
        tree_view.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        return tree_view, "Object Attributes"

    def update_panel(self, widget: QWidget, lmdbo: LMDBObject) -> None:
        tree_view: QTreeView = widget
        model = QStandardItemModel()
        # Initial call with an empty set for tracking visited objects.
        _populate_tree_from_object(model.invisibleRootItem(), lmdbo.obj, set())
        tree_view.setModel(model)
        tree_view.expandAll()

    def clear_panel(self, widget: QWidget) -> None:
        tree_view: QTreeView = widget
        tree_view.setModel(None)


class TextDumpPanelProvider(DetailPanelProvider):
    def can_handle(self, lmdbo: LMDBObject) -> bool:
        return lmdbo.obj is not None

    def create_panel(self) -> tuple[QWidget, str]:
        text_edit = QTextEdit()
        text_edit.setReadOnly(True)

        # Attach the syntax highlighter to the document.
        # We store it on the widget itself to prevent it from being garbage collected,
        # as the C++ side doesn't take ownership.
        text_edit.highlighter = XmlSyntaxHighlighter(text_edit.document())
        text_edit.setEnabled(False)
        return text_edit, "XML Source"

    def update_panel(self, widget: QWidget, lmdbo: LMDBObject) -> None:
        text_edit: QTextEdit = widget

        text_edit.setText(lmdbo._db.serializer.xmlserializer.marshall(lmdbo.obj, lmdbo.obj.__class__, pretty_print=True))

    def clear_panel(self, widget: QWidget) -> None:
        text_edit: QTextEdit = widget
        text_edit.clear()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)

class GeoPanelProvider(DetailPanelProvider):
    geo_classes: set[Tid]

    def __init__(self):
        super().__init__()
        self.geo_classes = set(get_all_geo_elements())

    def can_handle(self, lmdbo: LMDBObject) -> bool:
        return lmdbo.obj is not None and lmdbo.obj.__class__ in self.geo_classes

    def create_panel(self) -> tuple[QWidget, str]:
        text_edit = QTextEdit()
        text_edit.setReadOnly(True)
        text_edit.setEnabled(False)
        return text_edit, "Map"

    def update_panel(self, widget: QWidget, lmdbo: LMDBObject) -> None:
        text_edit: QTextEdit = widget
        text_edit.setText(
            json.dumps(list(to_feature(reprojection(lmdbo.obj, "EPSG:4326"), get_object_name(lmdbo.clazz))), indent=2, cls=DecimalEncoder)
        )

    def clear_panel(self, widget: QWidget) -> None:
        text_edit: QTextEdit = widget
        text_edit.clear()
