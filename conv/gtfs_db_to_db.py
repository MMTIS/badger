import logging

from utils.aux_logging import prepare_logger, log_all
from netex import DataSource, Codespace, StopPlace, PassengerStopAssignment, ScheduledStopPoint, Version, StopArea, InterchangeRule
from netexio.database import Database
from netexio.dbaccess import setup_database, load_local, copy_table
from netexio.pickleserializer import MyPickleSerializer
from utils.utils import get_interesting_classes
from transformers.gtfs import GTFS_CLASSES, gtfs_operator_line_memory, gtfs_sj_processing, gtfs_generate_deprecated_version
from transformers.projection import reprojection_update


def main(source_database_file: str, target_database_file: str, clean_database: bool = True) -> None:
    classes = get_interesting_classes(GTFS_CLASSES)

    with Database(target_database_file, serializer=MyPickleSerializer(compression=True), readonly=False) as db_write:
        # Target requires: Version, DataSource, Codespace, Authority, Operator, Branding, StopPlace, PassengerStopAssignment, ScheduledStopPoint, Line, DayType, ServiceJourney, TemplateServiceJourney, JourneyMeeting, ServiceJourneyInterchange

        setup_database(db_write, classes, True)

        with Database(source_database_file, serializer=MyPickleSerializer(compression=True), readonly=True) as db_read:
            # Copy tables that we don't change as-is.
            copy_table(
                db_read,
                db_write,
                [DataSource, Codespace, StopPlace, PassengerStopAssignment, ScheduledStopPoint, StopArea, InterchangeRule, Version],
                clean=True,
            )

            # Flatten the Operator, Authority, Branding, ResponsibilitySet; Provides Line and Operator
            gtfs_operator_line_memory(db_read, db_write, {})

            gtfs_sj_processing(db_read, db_write)

            # apply_availability_conditions_via_day_type_ref(db_read, db_write)

            # rewrite to override the db_write
            # gtfs_calls_generator(db_read, db_write, {})

            # Extract calendar information
            # gtfs_calendar_generator(db_read, db_write, {})

            versions = load_local(db_write, Version, 1)
            if len(versions) == 0:
                gtfs_generate_deprecated_version(db_write)

        # Our target database must be reprojected to WGS84            apply_availability_conditions_via_day_type_ref(db_read, db_write)

        db_write.block_until_done()

        reprojection_update(db_write, crs_to="urn:ogc:def:crs:EPSG::4326")


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
