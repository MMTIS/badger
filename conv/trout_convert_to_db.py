import logging
from pathlib import Path

from domain.netex.model import (
    Operator,
    Line,
    StopArea,
    ScheduledStopPoint,
    TimeDemandType,
    ServiceJourney,
    DestinationDisplay,
    ServiceJourneyPattern,
    TypeOfProductCategory,
    AvailabilityCondition,
    Connection,
    SiteConnection,
    TimingLink,
)
from domain.trout.transform.to_netex import (
    get_operators,
    get_lines,
    get_stopareas,
    get_scheduledstoppoint,
    get_timedemandtype,
    get_vehiclejourney,
    get_destinationdisplays,
    get_servicejourneypattern,
    get_productcategory,
    get_validitypatterns,
    get_connections,
    get_footpaths,
    get_timinglink,
)
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import prepare_logger, log_all

from domain.trout.model.tryeartimetable_pb2 import TYearTimetable


def load_from_file(yeartimetable: Path) -> TYearTimetable:
    timetable = TYearTimetable()
    with open(yeartimetable, "rb") as f:
        timetable.ParseFromString(f.read())
    return timetable


def main(yeartimetable: Path, target_database_file: Path) -> None:
    timetable = load_from_file(yeartimetable)

    with MdbxStorage(target_database_file, readonly=False) as db_write:
        # StopArea has a circular relationship to ScheduledStopPoints, removed.
        db_write.insert_objects_on_queue(StopArea, get_stopareas(timetable), empty=True)

        # SiteConnection refers to StopArea
        db_write.insert_objects_on_queue(SiteConnection, get_footpaths(timetable), empty=True)

        # ScheduledStopPoint refers to StopAreas
        db_write.insert_objects_on_queue(ScheduledStopPoint, get_scheduledstoppoint(timetable), empty=True)

        # Connection refers to ScheduledStopPoint
        db_write.insert_objects_on_queue(Connection, get_connections(timetable), empty=True)

        # TimingLinks do not exist in trout. Inference refers to ScheduledStopPoints
        db_write.insert_objects_on_queue(TimingLink, get_timinglink(timetable), empty=True)

        # TimeDemandType refers to TimingLinks (RunTime) and ScheduledStopPoints (WaitTime)
        db_write.insert_objects_on_queue(TimeDemandType, get_timedemandtype(timetable), empty=True)

        # Operator does not have references
        db_write.insert_objects_on_queue(Operator, get_operators(timetable), empty=True)

        # Line refers to Operator
        db_write.insert_objects_on_queue(Line, get_lines(timetable), empty=True)

        # DestinationDisplay has no references
        db_write.insert_objects_on_queue(DestinationDisplay, get_destinationdisplays(timetable), empty=True)

        # ServiceJourneyPattern refers to ScheduledStopPoint and DestinationDisplay
        db_write.insert_objects_on_queue(ServiceJourneyPattern, get_servicejourneypattern(timetable), empty=True)

        # AvailabilityCondition has no references
        db_write.insert_objects_on_queue(AvailabilityCondition, get_validitypatterns(timetable), empty=True)

        # ProductCategory has no references
        db_write.insert_objects_on_queue(TypeOfProductCategory, get_productcategory(timetable), empty=True)

        # ServiceJourney refers to Line, TimeDemandType, TypeOfProductCategory, ServiceJourneyPattern and AvailabilityCondition
        db_write.insert_objects_on_queue(ServiceJourney, get_vehiclejourney(timetable), empty=True)

        # There must not be any unresolved entities at this point.


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description='Trout YearTimetable to NeTEx')
    parser.add_argument('yeartimetable', type=str, help='yeartimetable.pb file to import, for example: yeartimetable.pb')
    parser.add_argument(
        "target",
        type=str,
        help="mdbx path to overwrite and store contents of the transformation.",
    )
    parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        tt = Path(args.yeartimetable)
        if tt.exists():
            main(tt, Path(args.target))
        else:
            mylogger.error("File does not exist")
    except Exception as e:
        log_all(logging.ERROR, f'{e}  {traceback.format_exc()}')
        raise e
