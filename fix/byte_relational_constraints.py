import logging
from pathlib import Path

from netexio.byteserializer import Database, BinarySerializer
from utils.aux_logging import prepare_logger, log_all
from utils.utils import get_boring_classes


def main(source_database_file: Path):
    # This function tries to resolve invalid and missing references, typically due to a missing nameOfRefClass, or out of order inserts.

    with Database(source_database_file, BinarySerializer(classes=get_boring_classes()), readonly=False) as source_db:
        source_db.resolve()


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Check an LMDB for missing references")
    parser.add_argument("source", type=str, help="lmdb file to use as input.")
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(Path(args.source))
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e