import sys
import signal
from pathlib import Path
from typing import Optional

from PySide6.QtCore import QObject, Qt
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import QApplication

from gui.views.splashscreen import SplashScreen
from gui.workers.loader import LoaderThread


def main():
    controller: Optional[QObject]  # noqa: F842
    app = QApplication(sys.argv)

    # Splash image
    splash_pix = QPixmap(f"{Path(__file__).resolve().parent}/badger.png")
    scaled_pixmap = splash_pix.scaled(
        500,
        400,  # breedte, hoogte
        Qt.AspectRatioMode.KeepAspectRatio,  # verhouding behouden
        Qt.TransformationMode.SmoothTransformation,  # mooi schalen (anti-alias)
    )
    splash = SplashScreen(scaled_pixmap)
    splash.showMessageOverlay("Starting...", 0)
    splash.show()
    app.processEvents()

    def on_finished():
        global controller
        from storage.mdbx.core.implementation import MdbxStorage

        storage = MdbxStorage(Path(sys.argv[1]), readonly=True)
        from gui.controllers.maincontroller import MainController

        storage.__enter__()
        controller = MainController(app, storage)  # Prevent deletion
        controller.show()
        splash.hide()

    # Loader thread
    loader = LoaderThread(Path(sys.argv[1]), app)
    loader.update.connect(lambda msg, prog: splash.showMessageOverlay(msg, prog))
    loader.finished.connect(on_finished)
    loader.start()

    sys.exit(app.exec())


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    main()
