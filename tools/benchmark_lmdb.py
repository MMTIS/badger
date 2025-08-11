import lmdb
import cloudpickle
import time
from tqdm import tqdm
from pathlib import Path
from netexio.pickleserializer import MyPickleSerializer


def benchmark_lmdb(path: str):
    env = lmdb.open(path, readonly=True, lock=False, readahead=False, max_dbs=128)

    results = []

    serializer = MyPickleSerializer(compression=True)

    with env.begin() as txn_db:
        for db_name, _ in txn_db.cursor():
            if db_name == b'_metadata':
                continue

            with env.begin() as txn:
                db = env.open_db(db_name, txn=txn)
                stat = txn.stat(db)
                entries = stat["entries"]

                clazz = serializer.name_object.get(db_name, None)
                start_time = time.perf_counter()

                with txn.cursor(db) as cursor, tqdm(
                    total=entries, desc=db_name.decode('utf-8'), bar_format="{desc:<25} {bar} {n_fmt:>6}/{total_fmt:<6} [{elapsed}<{remaining}]", unit="entry"
                ) as pbar:
                    if db_name[0] == ord('_'):
                        for _, value in cursor:
                            cloudpickle.loads(value)  # deserialiseer
                            pbar.update(1)
                    else:
                        for _, value in cursor:
                            serializer.unmarshall(value, clazz)
                            pbar.update(1)

                elapsed = time.perf_counter() - start_time
                results.append((db_name.decode('utf-8'), entries, elapsed))

    # Markdown-tabel printen
    print("\n### LMDB Benchmark Results")
    print("| Database | Entries | Time (s) |")
    print("|----------|---------|----------|")
    for name, entries, elapsed in results:
        print(f"| {name} | {entries} | {elapsed:.4f} |")


if __name__ == "__main__":
    import sys

    lmdb_path = Path(sys.argv[1])
    benchmark_lmdb(str(lmdb_path))
