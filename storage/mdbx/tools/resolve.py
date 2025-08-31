from pathlib import Path

from domain.netex.services.utils import get_boring_classes
from storage.mdbx.core.implementation import MdbxStorage
from storage.mdbx.core.references import resolve, resolve_embeddings

if __name__ == "__main__":
    import sys

    interesting_members = get_boring_classes()
    with MdbxStorage(Path(sys.argv[1]), readonly=False) as storage:
        resolve(storage)
        # resolve_embeddings(storage)
