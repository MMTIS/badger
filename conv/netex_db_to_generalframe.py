from netexio.database import Database
from netexio.pickleserializer import MyPickleSerializer
from transformers.generalframe import export_to_general_frame
from netexio.xml import export_publication_delivery_xml


def main(database: str, output_filename: str):
    with Database(database, MyPickleSerializer(compression=True), readonly=True) as db_read:
        publication_delivery = export_to_general_frame(db_read)
        export_publication_delivery_xml(publication_delivery, output_filename)


if __name__ == '__main__':
    import argparse

    argument_parser = argparse.ArgumentParser(description='Export any lmdb file into a NeTEx GeneralFrame')
    argument_parser.add_argument('database', type=str, help='lmdb to be read from')
    argument_parser.add_argument('output_filename', type=str, help='The output XML file')
    args = argument_parser.parse_args()

    main(args.database, args.output_filename)
