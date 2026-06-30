from pathlib import Path

from domain.netex.model import PublicationDelivery
from storage.lxml.serialization.xml import export_publication_delivery_xml
from utils.aux_logging import prepare_logger, log_all
from transformers.epip import export_epip_network_offer
from storage.mdbx.core.implementation import MdbxStorage
import logging


def epip_db_to_xml(database_epip: Path, output_filename: Path) -> None:
    with MdbxStorage(database_epip) as db_epip:
        with db_epip.env.ro_transaction() as txn:
            log_all(logging.INFO, f"[epip_db_to_xml] building EPIP publication delivery from {database_epip}")
            publication_delivery: PublicationDelivery = export_epip_network_offer(db_epip, txn)
            log_all(logging.INFO, f"[epip_db_to_xml] serialising to {output_filename}")
            export_publication_delivery_xml(publication_delivery, output_filename)
            log_all(logging.INFO, f"[epip_db_to_xml] done: {output_filename}")


def main(source: str, target: str) -> None:
    source_path = Path(source)
    if not source_path.exists():
        log_all(logging.ERROR, f"{source_path} does not exist.")

    else:
        epip_db_to_xml(source_path, Path(target))


if __name__ == "__main__":
    import argparse
    import traceback

    argument_parser = argparse.ArgumentParser(description="Export a prepared EPIP export XML")
    argument_parser.add_argument("epip", type=str, help="The EPIP MDBX database")
    argument_parser.add_argument(
        "output",
        type=str,
        help="The NeTEx output filename, for example: netex-epip.xml.gz",
    )
    argument_parser.add_argument("--log_file", type=str, required=False, help="the logfile")

    args = argument_parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    try:
        main(args.epip, args.output)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
