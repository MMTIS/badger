import logging

from netexio.database import Database
from netexio.dbaccess import setup_database, open_netex_file, insert_database
from netexio.pickleserializer import MyPickleSerializer
from utils.utils import get_interesting_classes
from utils.aux_logging import *
from netexio.dbaccess import check_referencing

def main(filenames: list[str], database: str, clean_database: bool = True) -> None:
    # if filenames is not a list of str  => error
    if not (isinstance(filenames, list) and all(isinstance(item, str) for item in filenames)):
        log_all(logging.ERROR, f'filenames parameter must be a [] of file names.')
        log_flush()
        exit(1)

    with Database(database, MyPickleSerializer(compression=True), readonly=False,
                  logger=logging.getLogger("script_runner")) as db:
        classes = get_interesting_classes()

        if clean_database:
            print("Is cleaned!")
            setup_database(db, classes, clean_database)

        for filename in filenames:
            for sub_file in open_netex_file(filename):
                insert_database(db, classes, sub_file)

        db.block_until_done()

        # check_referencing(db)


if __name__ == '__main__':
    import argparse

    argument_parser = argparse.ArgumentParser(description='Import any NeTEx source into lmdb')
    argument_parser.add_argument('netex', nargs='+', default=[], help='NeTEx files')
    argument_parser.add_argument('database', type=str, help='The lmdb to be overwritten with the NeTEx context')
    argument_parser.add_argument('--clean_database', action="store_true", help='Clean the current file', default=True)
    args = argument_parser.parse_args()

    main(args.netex, args.database, args.clean_database)
