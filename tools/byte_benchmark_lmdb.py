import lmdb
import time
from tqdm import tqdm
from pathlib import Path

from netexio.byteserializer import BinarySerializer
from utils.utils import get_boring_classes


def benchmark_lmdb(path: str) -> None:
    results = []
    total_entries = 0
    total_elapsed = 0.0

    serializer = BinarySerializer(classes=get_boring_classes())

    env = lmdb.open(path, readonly=False, max_dbs=128, lock=False, readahead=False, map_size=6 * 1024 ** 3)
    with env.begin(write=False) as txn_read:
        for db_name, _ in txn_read.cursor():
            if len(db_name) == 2:
                db = env.open_db(db_name, txn=txn_read)

                stat = txn_read.stat(db)
                entries = stat["entries"]

                start_time = time.perf_counter()

                name = BinarySerializer.get_object_name(serializer.classes[int.from_bytes(db_name, 'little')])

                with (
                    txn_read.cursor(db) as cursor,
                    tqdm(
                        total=entries,
                        desc=name,
                        bar_format="{desc:<25} {bar} {n_fmt:>6}/{total_fmt:<6} [{elapsed}<{remaining}]",
                        unit="entry",
                    ) as pbar,
                ):
                    for key, value in cursor:
                        pbar.update(1)

                elapsed = time.perf_counter() - start_time
                results.append((name, entries, elapsed))

                if db_name[0] != ord('_'):
                    total_entries += entries
                    total_elapsed += elapsed

        for db_name, _ in txn_read.cursor():
            if db_name[0] == ord('_'):
                db = env.open_db(db_name, txn=txn_read)

                stat = txn_read.stat(db)
                entries = stat["entries"]

                start_time = time.perf_counter()

                name = db_name.decode('utf-8')

                with (
                    txn_read.cursor(db) as cursor,
                    tqdm(
                        total=entries,
                        desc=name,
                        bar_format="{desc:<25} {bar} {n_fmt:>6}/{total_fmt:<6} [{elapsed}<{remaining}]",
                        unit="entry",
                    ) as pbar,
                ):
                    for key, value in cursor:
                        pbar.update(1)

                elapsed = time.perf_counter() - start_time
                results.append((name, entries, elapsed))



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
    for name, entries, elapsed in results:
        if name[0] == '_':
            print(f"| {name} | {entries} | {elapsed:.4f} |")


if __name__ == "__main__":
    import sys

    lmdb_path = Path(sys.argv[1])
    benchmark_lmdb(str(lmdb_path))
