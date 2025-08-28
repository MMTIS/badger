import sys
import signal
from pathlib import Path

from PySide6.QtWidgets import QApplication

from gui.controllers.maincontroller import MainController
from storage.lmdb.core.implementation import LmdbStorage

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    app = QApplication(sys.argv)

    with LmdbStorage(Path(sys.argv[1]), readonly=True) as storage:
        controller = MainController(app, storage)
        controller.show()
        sys.exit(app.exec())
