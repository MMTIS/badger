import sys
import signal
from pathlib import Path

from PySide6.QtWidgets import QApplication

from domain.netex.services.utils import get_boring_classes
from gui.controllers.maincontroller import MainController
from storage.lmdb.core.implementation import LmdbStorage
from storage.lmdb.serialization.byteserializer import ByteSerializer

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    app = QApplication(sys.argv)

    interesting_members = get_boring_classes()
    with LmdbStorage(Path(sys.argv[1]), ByteSerializer(interesting_members), readonly=True) as storage:
        controller = MainController(app, storage)
        controller.show()
        sys.exit(app.exec())
