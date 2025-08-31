import sys
import signal
from pathlib import Path

from PySide6.QtWidgets import QApplication

from gui.controllers.maincontroller import MainController
from storage.mdbx.core.implementation import MdbxStorage

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    app = QApplication(sys.argv)

    with MdbxStorage(Path(sys.argv[1]), readonly=True) as storage:
        controller = MainController(app, storage)
        controller.show()
        sys.exit(app.exec())
