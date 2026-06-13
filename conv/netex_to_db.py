import traceback
from pathlib import Path

from storage.mdbx.core.references import resolve, resolve_embeddings
from storage.lxml.core.implementation import XmlStorage
from storage.lxml.core.insert import insert_database, get_interesting_classes
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import log_all, log_flush
import logging


def netex_to_db(filenames: set[Path], database: Path, clean_database: bool = True) -> None:
    with MdbxStorage(database, readonly=False) as storage:
        """
        if clean_database:
            print("Is cleaned!")
            storage.clean()
        """

        interesting_classes = get_interesting_classes()
        for filename in filenames:
            xml_storage = XmlStorage(filename)
            for sub_file, real_filename in xml_storage.open_netex_file():
                log_all(logging.INFO, f"[netex_to_db] loading {real_filename}")
                insert_database(storage, interesting_classes, sub_file)

        log_all(logging.INFO, "[netex_to_db] resolving references")
        resolve(storage)
        log_all(logging.INFO, "[netex_to_db] resolving embeddings")
        resolve_embeddings(storage)
        log_all(logging.INFO, f"[netex_to_db] done: {database}")


def main(filenames: list[str], database: str, clean_database: bool = True) -> None:
    # if filenames is not a list of str  => error
    if not (isinstance(filenames, list) and all(isinstance(item, str) for item in filenames)):
        log_all(logging.ERROR, 'filenames parameter must be a [] of file names.')
        log_flush()
        exit(1)

    paths: set[Path] = set([])
    for filename in filenames:
        path = Path(filename)
        if not path.exists():
            log_all(logging.WARNING, f'{filename} does not exist.')
        else:
            paths.add(path)

    netex_to_db(paths, Path(database), clean_database)


if __name__ == '__main__':
    import argparse

    from utils.aux_logging import prepare_logger

    argument_parser = argparse.ArgumentParser(description='Import any NeTEx source into lmdb')
    argument_parser.add_argument('netex', nargs='+', default=[], help='NeTEx files')
    argument_parser.add_argument('database', type=str, help='The lmdb to be overwritten with the NeTEx context')
    argument_parser.add_argument('--clean_database', action="store_true", help='Clean the current file', default=True)
    argument_parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = argument_parser.parse_args()
    prepare_logger(logging.INFO, args.log_file)

    try:
        main(args.netex, args.database, args.clean_database)
    except Exception as e:
        log_all(logging.ERROR, traceback.format_exc())
        raise e
