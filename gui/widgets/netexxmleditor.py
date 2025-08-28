from __future__ import annotations

import re

from PySide6.QtCore import Signal, Qt
from PySide6.QtGui import (
    QColor,
    QMouseEvent,
    QTextCursor,
    QPalette,
)
from PySide6.QtWidgets import QTextEdit, QWidget

from gui.syntaxhighlighters.xmlsyntaxhighlighter import XmlSyntaxHighlighter


class NeTExXmlEditor(QTextEdit):
    """
    QTextEdit met een XML-highlighter en klikbare elementen (<...Ref .../>).
    """

    elementClicked: Signal = Signal(str, object)  # elementnaam, attributen

    # TODO:
    """
    def on_element_clicked(name: str, attrs: dict[str, str]) -> None:
        print(f"Clicked element {name} → {attrs}")
    
    editor.elementClicked.connect(on_element_clicked)
    """

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)

        # Highlighter koppelen
        self.highlighter = XmlSyntaxHighlighter(
            parent=self.document(),
            dark=self.is_dark_background(self),
        )

    @staticmethod
    def is_dark_background(widget: QTextEdit) -> bool:
        bg: QColor = widget.palette().color(QPalette.ColorRole.Base)
        return bg.lightness() < 128

    def _element_at_position(self, pos: int) -> tuple[str, dict[str, str]] | None:
        """
        Hulpje: bepaalt of de cursorpositie in een <...Ref> element staat.
        Retourneert (elementnaam, attributen) of None.
        """
        text: str = self.toPlainText()
        start: int = text.rfind("<", 0, pos)
        end: int = text.find(">", pos)
        if start == -1 or end == -1:
            return None

        snippet: str = text[start : end + 1]
        m = re.match(r"<\s*([A-Za-z0-9:_-]+)", snippet)
        if not m:
            return None
        element: str = m.group(1)
        if not element.endswith("Ref"):
            return None

        attrs: dict[str, str] = {}
        for attr in ["ref", "version", "versionRef", "nameOfRefClass"]:
            am = re.search(fr'\b{attr}="([^"]+)"', snippet)
            if am:
                attrs[attr] = am.group(1)

        return (element, attrs)

    def mousePressEvent(self, event: QMouseEvent) -> None:
        cursor: QTextCursor = self.cursorForPosition(event.pos())
        pos: int = cursor.position()
        element_info = self._element_at_position(pos)
        if element_info:
            element, attrs = element_info
            self.elementClicked.emit(element, attrs)
            return  # selectie voorkomen → als gewenst
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event: QMouseEvent) -> None:
        cursor: QTextCursor = self.cursorForPosition(event.pos())
        pos: int = cursor.position()
        if self._element_at_position(pos):
            self.viewport().setCursor(Qt.CursorShape.PointingHandCursor)
        else:
            self.viewport().setCursor(Qt.CursorShape.IBeamCursor)
        super().mouseMoveEvent(event)
