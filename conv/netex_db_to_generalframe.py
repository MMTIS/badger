from pathlib import Path


from storage.mdbx.core.implementation import MdbxStorage
from transformers.generalframe import export_to_general_frame
from storage.lxml.serialization.xml import export_publication_delivery_xml


def main(database: Path, output_filename: str) -> None:
    with MdbxStorage(database) as storage:
        with storage.env.ro_transaction() as txn:
            publication_delivery = export_to_general_frame(storage, txn)
            export_publication_delivery_xml(publication_delivery, output_filename)


if __name__ == '__main__':
    import argparse

    argument_parser = argparse.ArgumentParser(description='Export any lmdb file into a NeTEx GeneralFrame')
    argument_parser.add_argument('database', type=str, help='lmdb to be read from')
    argument_parser.add_argument('output_filename', type=str, help='The output XML file')
    args = argument_parser.parse_args()

    main(Path(args.database), args.output_filename)
