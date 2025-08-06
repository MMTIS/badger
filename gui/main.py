# main.py
import sys
import signal
from PySide6.QtWidgets import QApplication
from gui.controllers import MainController
from gui.qdatabase import QDatabase
from netexio.pickleserializer import MyPickleSerializer

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    app = QApplication(sys.argv)
    with QDatabase(sys.argv[1], MyPickleSerializer(compression=True), readonly=True) as database:
        controller = MainController(app, database)
        controller.show()
        sys.exit(app.exec())
