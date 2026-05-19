"""

"""

import logging
from collections import defaultdict
from pathlib import Path

from xsdata.models.datatype import XmlDateTime

from domain.netex.model import (
    DayType,
    DayTypeAssignment,
    DayTypeRef,
    DayTypeRefsRelStructure,
    Line,
    Route,
    ServiceCalendar,
    ServiceJourney,
    ServiceJourneyPattern,
    UicOperatingPeriod,
    UicOperatingPeriodRef,
)
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import prepare_logger, log_all


def fmt_dt(dt: XmlDateTime | None) -> str:
    return dt.to_datetime().isoformat() if dt else "None"


def fix_lines(database: Path) -> None:

    with MdbxStorage(database, readonly=False) as db:
        with db.env.ro_transaction() as txn:
            for journey in db.iter_only_objects(txn, ServiceJourney):
                print(journey.day_types.day_type_ref)


def main(source_database_file: str) -> None:
    return fix_lines(Path(source_database_file))


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Fix Mentz line versions")
    parser.add_argument("source", type=str, help="mdbx file to use as input.")
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.source)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e