from pathlib import Path

from storage.mdbx.core.references import resolve, resolve_embeddings
from storage.lxml.core.implementation import XmlStorage
from storage.lxml.core.insert import insert_database, get_interesting_classes
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import *


def main(filenames: list[str], database: str, clean_database: bool = True) -> None:
    # if filenames is not a list of str  => error
    if not (isinstance(filenames, list) and all(isinstance(item, str) for item in filenames)):
        log_all(logging.ERROR, f'filenames parameter must be a [] of file names.')
        log_flush()
        exit(1)

    with MdbxStorage(Path(database), readonly=False) as storage:
        """
        if clean_database:
            print("Is cleaned!")
            storage.clean()
        """

        interesting_classes = get_interesting_classes()
        for filename in filenames:
            xml_storage = XmlStorage(Path(filename))
            for sub_file, real_filename in xml_storage.open_netex_file():
                insert_database(storage, interesting_classes, sub_file)

        resolve(storage)
        resolve_embeddings(storage)

if __name__ == '__main__':
    import argparse

    argument_parser = argparse.ArgumentParser(description='Import any NeTEx source into lmdb')
    argument_parser.add_argument('netex', nargs='+', default=[], help='NeTEx files')
    argument_parser.add_argument('database', type=str, help='The lmdb to be overwritten with the NeTEx context')
    argument_parser.add_argument('--clean_database', action="store_true", help='Clean the current file', default=True)
    args = argument_parser.parse_args()

    main(args.netex, args.database, args.clean_database)
