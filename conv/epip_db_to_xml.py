from netex import PublicationDelivery
from netexio.database import Database
from netexio.pickleserializer import MyPickleSerializer
from netexio.xml import export_publication_delivery_xml

from utils.aux_logging import *
from transformers.epip import export_epip_network_offer


def main(database_epip: str, output_filename: str):
    with Database(database_epip, serializer=MyPickleSerializer(compression=True)) as db_epip:
        publication_delivery: PublicationDelivery = export_epip_network_offer(db_epip)
        export_publication_delivery_xml(publication_delivery, output_filename)


if __name__ == '__main__':
    import argparse
    import traceback

    argument_parser = argparse.ArgumentParser(description='Export a prepared EPIP export XML')
    argument_parser.add_argument('epip', type=str, help='The EPIP lmdb database')
    argument_parser.add_argument('output', type=str, help='The NeTEx output filename, for example: netex-epip.xml.gz')
    argument_parser.add_argument('--log_file', type=str, required=False, help='the logfile')

    args = argument_parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    try:
        main(args.epip, args.output)
    except Exception as e:
        log_all(logging.ERROR, f'{e} {traceback.format_exc()}')
        raise e
