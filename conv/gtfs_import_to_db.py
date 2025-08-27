import logging
from pathlib import Path

from domain.gtfs.services.gtfs_to_duckdb import load_gtfs_to_duckdb
from utils.aux_logging import log_all, prepare_logger
import os

def main(gtfs_file: Path, database_file: Path) -> None:
    # Workaround for https://github.com/duckdb/duckdb/issues/8261
    try:
        os.remove(database_file.resolve())
    except OSError:
        pass

    load_gtfs_to_duckdb(gtfs_file, database_file)


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description='GTFS import into DuckDB')
    parser.add_argument('gtfs', type=str, help='GTFS file to import, for example: gtfs.zip')
    parser.add_argument('database', type=str, help='DuckDB file to overwrite and store contents of the import.')
    parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(Path(args.gtfs), Path(args.database))
    except Exception as e:
        log_all(logging.ERROR, f'{e}  {traceback.format_exc()}')
        raise e
