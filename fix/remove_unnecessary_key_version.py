# For non relevant objects the id/version shall be removed from the mdbx
# e.g. Location and Centroid are not "first-class" objects and should never be referenced. Therefore they don't need id/version
# needed for some French data


import logging
from pathlib import Path
from typing import Generator

from domain.netex.model import ServiceJourney
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import prepare_logger, log_all


def main(source_database_file: str) -> None:
    # The function removes id/version from a set of Classes that are not important.
    with MdbxStorage(Path(source_database_file), readonly=False) as source_db:
        with source_db.env.rw_transaction() as txn_write:

            def all_elements() -> Generator[T[id], None, None]:
                # iterate through all elements of one of the classes we don't want to see in the data Location and Centroid for now

                yield from xxx


            source_db.insert_any_object_on_queue(txn_write, all_sj())
            txn_write.commit()


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Check an MDBX for not correctly set DayOffsets. It will transform 25:00 to 01:00 with DayOffset=1.")
    parser.add_argument("source", type=str, help="mdbx file to use as input.")
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(Path(args.source))
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
