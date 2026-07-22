from PySide6.QtGui import QPixmap, QFont, Qt
from PySide6.QtWidgets import QSplashScreen


class SplashScreen(QSplashScreen):
    def __init__(self, pixmap: QPixmap):
        super().__init__(pixmap)
        self.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        self.setMask(pixmap.mask())
        self.progress = 0
        self.message = ""

    def showMessageOverlay(self, msg: str, progress: int):
        self.message = msg
        self.progress = progress
        self.showMessage(msg, Qt.AlignmentFlag.AlignBottom | Qt.AlignmentFlag.AlignCenter, Qt.GlobalColor.white)
        self.repaint()  # force overlay redraw
