from pathlib import Path

from mdbx import MDBXDBFlags
from tqdm import tqdm

from domain.netex.services.model_typing import Tid
from domain.netex.services.utils import get_boring_classes
from domain.utils import get_object_name
from storage.mdbx.core.implementation import MdbxStorage, DB_ID_IDX, DB_UNRESOLVED, DB_REFERENCE_OUTWARD, DB_CLASS_IDX
import time

def benchmark_mdbx(storage: MdbxStorage) -> None:
    db_names = storage.db_names()
    results = []
    results_metadata = []
    total_entries = 0
    total_elapsed = 0.0

    with storage.env.ro_transaction() as txn:
        for db_name, clazz in db_names.items():
            db = txn.open_map(db_name, flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
            entries = db.get_stat(txn).ms_entries
            start_time = time.perf_counter()

            with (
                txn.cursor(db) as cursor,
                tqdm(
                    total=entries,
                    desc=get_object_name(clazz),
                    bar_format="{desc:<25} {bar} {n_fmt:>6}/{total_fmt:<6} [{elapsed}<{remaining}]",
                    unit="entry",
                ) as pbar,
            ):
                for key, value in cursor.iter():
                    _obj: Tid = storage.serializer.unmarshall(value, clazz)
                    pbar.update(1)

            elapsed = time.perf_counter() - start_time
            results.append((get_object_name(clazz), entries, elapsed))

            total_entries += entries
            total_elapsed += elapsed

        for db_name in (DB_CLASS_IDX, DB_ID_IDX, DB_UNRESOLVED, DB_REFERENCE_OUTWARD):
            db = txn.open_map(db_name, flags=MDBXDBFlags.MDBX_DB_ACCEDE) # TODO: Fix this with the known options

            entries = db.get_stat(txn).ms_entries

            start_time = time.perf_counter()
            with (
                txn.cursor(db) as cursor,
                tqdm(
                    total=entries,
                    desc=db_name.decode('utf-8'),
                    bar_format="{desc:<25} {bar} {n_fmt:>6}/{total_fmt:<6} [{elapsed}<{remaining}]",
                    unit="entry",
                ) as pbar,
            ):
                for key, value in cursor:
                    _value = int.from_bytes(value, 'little')
                    pbar.update(1)

            elapsed = time.perf_counter() - start_time
            results_metadata.append((db_name.decode('utf-8'), entries, elapsed))

    # Markdown-tabel printen
    print("\n### LMDB Benchmark Results")
    print("| Database | Entries | Time (s) |")
    print("|----------|--------:|---------:|")
    for name, entries, elapsed in results:
        if name[0] != '_':
            print(f"| {name} | {entries} | {elapsed:.4f} |")

    print(f"| Total: | {total_entries} | {total_elapsed:.4f} |")

    print("\n## Metadata")
    print("| Database | Entries | Time (s) |")
    print("|----------|--------:|---------:|")
    for name, entries, elapsed in results_metadata:
        if name[0] == '_':
            print(f"| {name} | {entries} | {elapsed:.4f} |")


if __name__ == "__main__":
    import sys

    interesting_members = get_boring_classes()
    with MdbxStorage(Path(sys.argv[1]), readonly=True) as storage:
        benchmark_mdbx(storage)
