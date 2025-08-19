import lmdb
import cloudpickle
import time
from tqdm import tqdm
from pathlib import Path

import netex
from netex import Line, ServiceJourney
from netexio.attributes import resolve_attr
from netexio.binaryserializer import get_numeric_path, serialize_relation, deserialize_relation
from netexio.database import Database
from netexio.dbaccess import load_local, load_referencing_inwards, load_referencing
from netexio.pickleserializer import MyPickleSerializer
from transformers.references import split_path


def benchmark_lmdb(path: str) -> None:
    """
    env = lmdb.open(path, readonly=True, lock=False, readahead=False, max_dbs=128)

    results = []

    serializer = MyPickleSerializer(compression=True)
    total_entries = 0
    total_elapsed = 0.0

    with env.begin() as txn_db:
        with env.begin() as txn:
            db = env.open_db(b"_embedding", txn=txn)
            with txn.cursor(db) as cursor:
                for _, value in cursor:
                    print(len(value))
                    clazz_name, parent_id, parent_version, path = cloudpickle.loads(value)
                    clazz = serializer.name_object[clazz_name]
                    print(clazz_name, parent_id, parent_version, path)
    """

    with Database("/mnt/storage/compressed/wsf.lmdb", serializer=MyPickleSerializer(compression=True)) as source_db:
        sj = load_local(source_db, ServiceJourney, 1)[0]

        for parent_id, parent_version, parent_class, path in load_referencing(source_db, ServiceJourney, sj.id, sj.version):
            split = split_path(path)
            attribute = resolve_attr(sj, split)
            print(parent_id, parent_version, parent_class, path, attribute)

            path = get_numeric_path(sj, attribute)
            serialized = serialize_relation(netex.__all__.index(attribute.__class__.__name__), attribute.ref, attribute.version, path)
            print('our', len(serialized))
            clazz_idx, resolved_id, resolved_version, resolved_path = deserialize_relation(serialized)
            print(clazz_idx, resolved_id, resolved_version, resolved_path)

if __name__ == "__main__":
    import sys

    lmdb_path = Path(sys.argv[1])
    benchmark_lmdb(str(lmdb_path))
