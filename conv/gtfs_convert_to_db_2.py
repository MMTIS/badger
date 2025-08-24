import logging
from pathlib import Path
import domain.netex.model
from domain.gtfs.services.duckdb_to_storage import to_storage
from storage.lmdb.core.implementation import LmdbStorage
from storage.lmdb.serialization.byteserializer import ByteSerializer
from utils.aux_logging import prepare_logger, log_all
from domain.netex.services import monkey_patching  # noqa: F401


def main(database_gtfs: Path, database_netex: Path) -> None:
    with LmdbStorage(database_netex, ByteSerializer(domain.netex.model.interesting_members), readonly=False) as storage:  # type: ignore
        to_storage(database_gtfs, storage)


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
