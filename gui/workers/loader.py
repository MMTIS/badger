from pathlib import Path

from PySide6.QtCore import QThread, Signal
from PySide6.QtWidgets import QApplication


class LoaderThread(QThread):
    update = Signal(str, int)  # tekst update + progress (0-100)
    finished = Signal(object)  # loaded modules

    def __init__(self, filename: Path, app: QApplication):
        super().__init__()
        self.filename = filename
        self.app = app

    def run(self):
        steps = [
            ("Loading MDBX...", "mdbx"),
            ("Loading NeTEx dataclasses...", "domain.netex.model"),
            ("Loading Storage module...", "MdbxStorage"),
            ("Loading GUI...", "MainController"),
        ]
        loaded_modules = {}
        total = len(steps)
        for idx, (msg, code) in enumerate(steps, 1):
            self.update.emit(msg, int(idx / total * 100))
            # Vervang door echte import
            if code.startswith("mdbx"):
                import mdbx

                loaded_modules["mdbx"] = mdbx
            elif code.startswith("domain.netex.model"):
                import domain.netex.model as netex

                loaded_modules["netex"] = netex
            elif code.startswith("MdbxStorage"):
                from storage.mdbx.core.implementation import MdbxStorage

                loaded_modules["MdbxStorage"] = MdbxStorage
            elif code.startswith("MainController"):
                from gui.controllers.maincontroller import MainController  # noqa: F401

        self.update.emit("Done!", 100)
        self.finished.emit(loaded_modules)
