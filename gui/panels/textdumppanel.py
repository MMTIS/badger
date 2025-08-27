from PySide6.QtWidgets import QTextEdit, QWidget

from gui.models.storageobject import StorageObject
from gui.panels.detailpanel import DetailPanelProvider
from gui.syntaxhighlighters.xmlsyntaxhighlighter import XmlSyntaxHighlighter
from storage.lxml.serialization.xmlserializer import MyXmlSerializer


class TextDumpPanelProvider(DetailPanelProvider):
    xml_serializer: MyXmlSerializer

    def __init__(self):
        super().__init__()
        self.xml_serializer = MyXmlSerializer([])

    def can_handle(self, lmdbo: StorageObject) -> bool:
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

    def update_panel(self, widget: QWidget, lmdbo: StorageObject) -> None:
        text_edit: QTextEdit = widget

        text_edit.setText(self.xml_serializer.marshall(lmdbo.obj, lmdbo.obj.__class__, pretty_print=True))

    def clear_panel(self, widget: QWidget) -> None:
        text_edit: QTextEdit = widget
        text_edit.clear()
