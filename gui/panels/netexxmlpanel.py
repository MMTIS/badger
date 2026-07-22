from typing import cast

from PySide6.QtWidgets import QTextEdit, QWidget

from gui.models.storageobject import StorageObject
from gui.panels.detailpanel import DetailPanelProvider
from gui.widgets.netexxmleditor import NeTExXmlEditor
from storage.lxml.serialization.xmlserializer import MyXmlSerializer


class NeTExXmlPanelProvider(DetailPanelProvider):
    xml_serializer: MyXmlSerializer

    def __init__(self):
        super().__init__()
        self.xml_serializer = MyXmlSerializer([])

    def can_handle(self, lmdbo: StorageObject) -> bool:
        return lmdbo.obj is not None

    def create_panel(self) -> tuple[QWidget, str]:
        netex_xml_widget = NeTExXmlEditor()
        netex_xml_widget.setReadOnly(True)
        netex_xml_widget.setEnabled(False)
        return netex_xml_widget, "XML Source"

    def update_panel(self, widget: QWidget, lmdbo: StorageObject) -> None:
        text_edit: QTextEdit = cast(QTextEdit, widget)

        text_edit.setText(self.xml_serializer.marshall(lmdbo.obj, lmdbo.obj.__class__, pretty_print=True))

    def clear_panel(self, widget: QWidget) -> None:
        text_edit: QTextEdit = cast(QTextEdit, widget)
        text_edit.clear()
