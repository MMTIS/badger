import logging
from pathlib import Path

from domain.gtfs.services.gtfs_to_duckdb import load_gtfs_to_duckdb
from utils.aux_logging import log_all, prepare_logger
import os


def gtfs_import_to_db(source: Path, target: Path) -> None:
    # Remove target file if it exists (workaround for DuckDB issue)
    try:
        target.resolve().unlink(missing_ok=True)
    except OSError as e:
        print(f"Warning: Could not remove target file {target}: {e}")

    # Ensure temporary files are cleaned up before operation
    temp_files = ['_tmp', '_tmp2']  # Add any other temp files you use
    for temp_file in temp_files:
        try:
            Path(temp_file).unlink(missing_ok=True)
        except OSError:
            pass
    load_gtfs_to_duckdb(source, target)


def main(gtfs_file: str, database_file: str) -> None:
    gtfs_path = Path(gtfs_file)
    if not gtfs_path.exists():
        log_all(logging.ERROR, f"{gtfs_path} does not exist.")

    else:
        gtfs_import_to_db(gtfs_path, Path(database_file))


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
        main(args.gtfs, args.database)
    except Exception as e:
        log_all(logging.ERROR, f'{e}  {traceback.format_exc()}')
        raise e
