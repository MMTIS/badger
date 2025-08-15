import lmdb
import time
from tqdm import tqdm
from pathlib import Path

import netexio.binaryserializer
from netexio.pickleserializer import MyPickleSerializer


def benchmark_lmdb(path: str) -> None:
    env = lmdb.open(path, readonly=True, lock=False, readahead=False, max_dbs=128)

    results = []

    serializer = MyPickleSerializer(compression=True)
    total_entries = 0
    total_elapsed = 0.0

    with env.begin() as txn_db:
        for db_name, _ in txn_db.cursor():
            if db_name == b'_metadata':
                continue

            clazz = serializer.name_object.get(db_name.decode('utf-8'), None)
            if clazz is not None:
                continue

            with env.begin() as txn:
                db = env.open_db(db_name, txn=txn)
                stat = txn.stat(db)
                entries = stat["entries"]

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
                    if db_name[0] == ord('_'):
                        for _, value in cursor:
                            netexio.binaryserializer.deserialize_relation(value) # deserialiseer
                            pbar.update(1)
                    else:
                        for _, value in cursor:
                            serializer.unmarshall(value, clazz)
                            pbar.update(1)

                elapsed = time.perf_counter() - start_time
                results.append((db_name.decode('utf-8'), entries, elapsed))

                if db_name[0] != ord('_'):
                    total_entries += entries
                    total_elapsed += elapsed

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
