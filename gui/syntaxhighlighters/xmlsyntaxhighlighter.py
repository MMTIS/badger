from PySide6.QtCore import QRegularExpression, QObject
from PySide6.QtGui import QTextCharFormat, QColor, QFont, QSyntaxHighlighter


class XmlSyntaxHighlighter(QSyntaxHighlighter):
    """
    A syntax highlighter for XML, designed to be used with QTextEdit.
    It provides color and formatting for tags, attributes, values, and comments.
    """

    def __init__(self, dark: bool, parent: QObject):
        super().__init__(parent)

        self.highlighting_rules = []

        if dark:
            tag_color = QColor("#4FC3F7")  # lichtblauw
            attr_color = QColor("#FFD54F")  # geel
            value_color = QColor("#81C784")  # groen
            comment_color = QColor("#9E9E9E")
        else:
            tag_color = QColor("#881798")  # paars
            attr_color = QColor("#a65700")  # bruin
            value_color = QColor("#008000")  # groen
            comment_color = QColor("#707070")

        # Rule for XML tags like <tag> or </tag>
        tag_format = QTextCharFormat()
        tag_format.setForeground(tag_color)
        tag_format.setFontWeight(QFont.Weight.Bold)
        self.highlighting_rules.append((QRegularExpression(r"</?[\w:-]+"), tag_format))
        self.highlighting_rules.append((QRegularExpression(r"[/?]>"), tag_format))

        # Rule for XML attributes like attribute=
        attribute_format = QTextCharFormat()
        attribute_format.setForeground(attr_color)
        self.highlighting_rules.append((QRegularExpression(r"\b[\w:-]+(?=\s*=)"), attribute_format))

        # Rule for XML values like "value"
        value_format = QTextCharFormat()
        value_format.setForeground(value_color)
        self.highlighting_rules.append((QRegularExpression(r"\"[^\"]*\""), value_format))

        # Rule for XML comments <!-- ... -->
        self.comment_format = QTextCharFormat()
        self.comment_format.setForeground(comment_color)
        self.comment_format.setFontItalic(True)
        self.comment_start_expression = QRegularExpression(r"<!--")
        self.comment_end_expression = QRegularExpression(r"-->")

    def highlightBlock(self, text: str) -> None:
        # Apply single-line pattern rules first
        for pattern, text_format in self.highlighting_rules:
            iterator = pattern.globalMatch(text)
            while iterator.hasNext():
                match = iterator.next()
                self.setFormat(match.capturedStart(), match.capturedLength(), text_format)

        # Handle multi-line comments
        self.setCurrentBlockState(0)
        start_index = 0
        if self.previousBlockState() != 1:
            match = self.comment_start_expression.match(text)
            start_index = match.capturedStart() if match.hasMatch() else -1

        while start_index >= 0:
            end_match = self.comment_end_expression.match(text, start_index)
            end_index = end_match.capturedStart()
            if end_index == -1:
                self.setCurrentBlockState(1)
                comment_length = len(text) - start_index
            else:
                comment_length = end_index - start_index + end_match.capturedLength()
            self.setFormat(start_index, comment_length, self.comment_format)
            next_match = self.comment_start_expression.match(text, start_index + comment_length)
            start_index = next_match.capturedStart() if next_match.hasMatch() else -1
