import logging
from pathlib import Path
from typing import cast

from domain.gtfs.services.duckdb_to_storage import to_storage
from storage.lmdb.core.implementation import LmdbStorage
from storage.lmdb.core.references import resolve
from storage.lmdb.serialization.byteserializer import ByteSerializer
from utils.aux_logging import prepare_logger, log_all
from domain.netex.services.utils import get_boring_classes


def main(database_gtfs: Path, database_netex: Path) -> None:
    interesting_members = get_boring_classes()
    with LmdbStorage(database_netex, ByteSerializer(interesting_members), readonly=False) as storage:  # type: ignore
        to_storage(database_gtfs, storage)
        resolve(cast(LmdbStorage, storage))



if __name__ == '__main__':
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description='Convert a GTFS database to a NeTEx database')
    parser.add_argument('gtfs', type=str, help='GTFS database to convert, for example: gtfs-import.duckdb')
    parser.add_argument('database', type=str, help='Storage file to overwrite and store contents of the conversion.')
    parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    try:
        main(Path(args.gtfs), Path(args.database))
    except Exception as e:
        log_all(logging.ERROR, f'{e} {traceback.format_exc()}')
        raise e
