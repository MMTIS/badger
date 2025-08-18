import lmdb
import time
from tqdm import tqdm
from pathlib import Path

import netexio.binaryserializer
from netexio.pickleserializer import MyPickleSerializer


def benchmark_lmdb(path: str) -> None:
    env = lmdb.open(path, readonly=False, max_dbs=128, lock=False, readahead=False, map_size=6 * 1024**3)

    # with env.begin() as txn_db:
        # txn_db.drop(db=env.open_db(b'_embedding'), delete=True)
        # txn_db.drop(db=env.open_db(b'_embedding_inverse'), delete=True)
        # txn_db.drop(db=env.open_db(b'_referencing'), delete=True)
        # txn_db.drop(db=env.open_db(b'_referencing_inwards'), delete=True)

    results = []

    serializer = MyPickleSerializer(compression=True)
    total_entries = 0
    total_elapsed = 0.0
    obj_idx = 0

    db_idx = env.open_db(b'_db_idx')
    """
    db_names: list[tuple[bytes, type]] = []
    with env.begin() as txn_db:
        for db_name, _ in txn_db.cursor():
            if db_name == b'_metadata' or db_name == b'_id_idx':
                continue

            clazz = serializer.name_object.get(db_name.decode('utf-8'), None)
            db_names.append((db_name, clazz,))

    for db_name, clazz in db_names:
        # buffer = []

        with (env.begin(write=False) as txn_read, env.begin(write=True, db=db_idx) as txn_write):
            db = env.open_db(db_name, txn=txn_read)

            stat = txn_read.stat(db)
            entries = stat["entries"]

            start_time = time.perf_counter()

            with (
                txn_read.cursor(db) as cursor,
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
                        obj = serializer.unmarshall(value, clazz)
                        id = serializer.encode_key(obj.id, obj.version if hasattr(obj, 'version') else None, clazz)
                        # buffer.append(id)
                        txn_write.put(id, obj_idx.to_bytes(4, 'little'))
                        obj_idx += 1
                        pbar.update(1)

            elapsed = time.perf_counter() - start_time
            results.append((db_name.decode('utf-8'), entries, elapsed))

            if db_name[0] != ord('_'):
                total_entries += entries
                total_elapsed += elapsed
    """

    with env.begin() as txn:
        stat = txn.stat(db_idx)
        entries = stat["entries"]

        start_time = time.perf_counter()

        with (
            txn.cursor(db_idx) as cursor,
            tqdm(
                total=entries,
                desc="db_idx",
                bar_format="{desc:<25} {bar} {n_fmt:>6}/{total_fmt:<6} [{elapsed}<{remaining}]",
                unit="entry",
            ) as pbar,
        ):
            for _, value in cursor:
                value = int.from_bytes(value, 'little')
                pbar.update(1)



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
