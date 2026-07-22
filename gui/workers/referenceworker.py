from PySide6.QtCore import QObject, Signal, Slot

class ReferenceWorker(QObject):
    referencesFound = Signal(object, bool)   # StorageObject, is_inward
    finished = Signal()

    def __init__(self, storage_controller, lmdbo, batch_size=100):
        super().__init__()
        self._storage_controller = storage_controller
        self._lmdbo = lmdbo
        self._batch_size = batch_size
        self._abort = False

    @Slot()
    def run(self):
        try:
            # Outwards
            batch = []
            for obj in self._storage_controller.load_references_outwards(self._lmdbo):
                if self._abort:
                    break
                batch.append(obj)
                if len(batch) >= self._batch_size:
                    self.referencesFound.emit(batch, False)
                    batch = []
            if batch:
                self.referencesFound.emit(batch, False)

            # Inwards
            if False:
                batch = []
                for obj in self._storage_controller.load_references_inwards(self._lmdbo):
                    if self._abort:
                        break
                    batch.append(obj)
                    if len(batch) >= self._batch_size:
                        self.referencesFound.emit(batch, True)
                        batch = []
                if batch:
                    self.referencesFound.emit(batch, True)
        finally:
            self.finished.emit()

    def abort(self):
        self._abort = True
