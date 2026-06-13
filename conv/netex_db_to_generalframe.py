import logging
from pathlib import Path


from storage.mdbx.core.implementation import MdbxStorage
from transformers.generalframe import export_to_general_frame
from storage.lxml.serialization.xml import export_publication_delivery_xml
from utils.aux_logging import log_all, prepare_logger


def netex_db_to_generalframe(source: Path, target: Path) -> None:
    log_all(logging.INFO, f"[netex_db_to_generalframe] exporting {source} to {target}")
    with MdbxStorage(source) as storage:
        with storage.env.ro_transaction() as txn:
            publication_delivery = export_to_general_frame(storage, txn)
            export_publication_delivery_xml(publication_delivery, target)


def main(source: str, target: str) -> None:
    source_path = Path(source)
    if not source_path.exists():
        log_all(logging.ERROR, f"{source_path} does not exist.")

    else:
        netex_db_to_generalframe(source_path, Path(target))


if __name__ == '__main__':
    import argparse

    argument_parser = argparse.ArgumentParser(description='Export any lmdb file into a NeTEx GeneralFrame')
    argument_parser.add_argument('database', type=str, help='lmdb to be read from')
    argument_parser.add_argument('output_filename', type=str, help='The output XML file')
    argument_parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = argument_parser.parse_args()
    prepare_logger(logging.INFO, args.log_file)

    main(args.database, args.output_filename)
