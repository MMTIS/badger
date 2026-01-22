import logging
import traceback
from pathlib import Path

from domain.netex.model import (
    DataSource,
    Codespace,
    StopPlace,
    PassengerStopAssignment,
    ScheduledStopPoint,
    StopArea,
    InterchangeRule,
    Version,
    ServiceCalendar,
)
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import prepare_logger, log_all

# from netex import DataSource, Codespace, StopPlace, PassengerStopAssignment, ScheduledStopPoint, Version, StopArea, InterchangeRule
# from netexio.database import Database
# from netexio.dbaccess import setup_database, load_local, copy_table
# from netexio.pickleserializer import MyPickleSerializer
# from utils.utils import get_interesting_classes
# from utils.profiles import GTFS_CLASSES
from transformers.gtfs import gtfs_operator_line_memory, gtfs_sj_processing, gtfs_generate_deprecated_version
from transformers.projection import reprojection_update


def gtfs_db_to_db(source_database: Path, target_database: Path, clean_database: bool):
    # classes = get_interesting_classes(GTFS_CLASSES)

    with MdbxStorage(target_database, readonly=False) as db_write:
        # Target requires: Version, DataSource, Codespace, Authority, Operator, Branding, StopPlace, PassengerStopAssignment, ScheduledStopPoint, Line, DayType, ServiceJourney, TemplateServiceJourney, JourneyMeeting, ServiceJourneyInterchange
        with db_write.env.rw_transaction() as txn_write:

            with MdbxStorage(source_database) as db_read:
                # Copy tables that we don't change as-is.
                with db_read.env.ro_transaction() as txn_read:
                    for clazz in [DataSource, Codespace, StopPlace, PassengerStopAssignment, ScheduledStopPoint, StopArea, InterchangeRule, Version]:
                        db_write.copy_map(txn_read, db_write, txn_write, clazz)

                    # service_calendars: List[ServiceCalendar] = list(db_read.iter_only_objects(txn_read, ServiceCalendar))

                    # Flatten the Operator, Authority, Branding, ResponsibilitySet; Provides Line and Operator
                    db_write.insert_any_object_on_queue(txn_write, gtfs_operator_line_memory(db_read, txn_read, {}))
                    db_write.insert_any_object_on_queue(txn_write, gtfs_sj_processing(db_read, txn_read))

                    # apply_availability_conditions_via_day_type_ref(db_read, db_write)

                    # rewrite to override the db_write
                    # gtfs_calls_generator(db_read, db_write, {})

                    # Extract calendar information
                    # gtfs_calendar_generator(db_read, db_write, {})

                    versions = list(db_read.iter_only_objects(txn_read, Version, limit=1))
                    if len(versions) == 0:
                        db_write.insert_any_object_on_queue(txn_write, gtfs_generate_deprecated_version(db_write, txn_write))

                    # Our target database must be reprojected to WGS84
                    db_write.insert_any_object_on_queue(txn_write, reprojection_update(db_write, txn_write, "urn:ogc:def:crs:EPSG::4326"))
                    txn_write.commit()


def main(source_database_file: str, target_database_file: str, clean_database: bool = True) -> None:
    source_database = Path(source_database_file)
    if not source_database.exists():
        log_all(logging.ERROR, f"{source_database} does not exist.")

    else:
        try:
            gtfs_db_to_db(source_database, Path(target_database_file), clean_database)
        except Exception as e:
            log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
            raise e


if __name__ == '__main__':
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description='Transform the input into mandatory objects for the export of GTFS')
    parser.add_argument('source', type=str, help='DuckDB file to use as input of the transformation.')
    parser.add_argument('target', type=str, help='DuckDB file to overwrite and store contents of the transformation.')
    parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.source, args.target)
    except Exception as e:
        log_all(logging.ERROR, f'{e} {traceback.format_exc()}')
        raise e
