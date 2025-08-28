from pathlib import Path

from domain.netex.services.utils import get_boring_classes
from storage.lmdb.core.implementation import LmdbStorage
from storage.lmdb.core.references import resolve, resolve_embeddings

if __name__ == "__main__":
    import sys

    interesting_members = get_boring_classes()
    with LmdbStorage(Path(sys.argv[1]), readonly=False) as storage:
        resolve(storage)
        resolve_embeddings(storage)
