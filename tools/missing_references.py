import logging

from netexio.database import Database
from netexio.pickleserializer import MyPickleSerializer
from utils.aux_logging import prepare_logger, log_all
import netexio.binaryserializer

def main(source_database_file: str):
    with Database(source_database_file, MyPickleSerializer(compression=True), readonly=True) as source_db:
        with source_db.env.begin(buffers=True, write=False) as txn:
            cursor = txn.cursor(source_db.db_referencing)
            all_referenced_elements = set([])
            for key, value in cursor:
                object_clazz, object_id, object_version, _embedding_path = netexio.binaryserializer.deserialize_relation(value)
                needle = (object_clazz, object_id, object_version)
                if needle in all_referenced_elements:
                    continue

                all_referenced_elements.add(needle)
                object_clazz = source_db.get_class_by_name(object_clazz)
                lookup_key = source_db.serializer.encode_key(object_id, object_version, object_clazz)
                if not source_db.check_object_by_key(object_clazz, lookup_key):
                    print(needle)

if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Check an LMDB for missing references")
    parser.add_argument("source", type=str, help="lmdb file to use as input.")
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.source)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
