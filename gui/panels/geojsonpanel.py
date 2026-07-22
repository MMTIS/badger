import json
from decimal import Decimal
from typing import Any

from PySide6.QtWidgets import QTextEdit, QWidget

from domain.netex.services.model_typing import Tid
from gui.models.storageobject import StorageObject
from gui.panels.detailpanel import DetailPanelProvider


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


class GeoJSONPanelProvider(DetailPanelProvider):
    geo_classes: set[Tid]

    def __init__(self):
        super().__init__()
        # self.geo_classes = set(get_all_geo_elements())

    def can_handle(self, lmdbo: StorageObject) -> bool:
        return lmdbo.obj is not None and lmdbo.obj.__class__ in self.geo_classes

    def create_panel(self) -> tuple[QWidget, str]:
        text_edit = QTextEdit()
        text_edit.setReadOnly(True)
        text_edit.setEnabled(False)
        return text_edit, "GeoJSON"

    def update_panel(self, widget: QWidget, lmdbo: StorageObject) -> None:
        # text_edit: QTextEdit = widget
        # text_edit.setText(json.dumps(list(to_feature(reprojection(lmdbo.obj, "EPSG:4326"), get_object_name(lmdbo.clazz))), indent=2, cls=DecimalEncoder))
        pass

    def clear_panel(self, widget: QWidget) -> None:
        text_edit: QTextEdit = widget
        text_edit.clear()
