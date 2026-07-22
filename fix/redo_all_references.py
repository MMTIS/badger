import logging

from netexio.database import Database
from netexio.pickleserializer import MyPickleSerializer
from utils.aux_logging import prepare_logger, log_all
import time

def main(source_database_file: str):
    with Database(source_database_file, MyPickleSerializer(compression=True), readonly=False) as source_db:
        source_db._resize_env()
        # with source_db.env.begin(write=True) as txn:
        #     txn.put(b"test", b"test", db=source_db.db_metadata, dupdata=False)
        # print("hello")
        source_db.redo_all_embedding_and_references()


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
