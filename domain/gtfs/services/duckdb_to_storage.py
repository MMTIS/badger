from pathlib import Path
import duckdb

from domain.gtfs.transform.codespace import getCodespace
from domain.gtfs.transform.datasource import getDataSource
from domain.gtfs.transform.daytype import getDayTypes
from domain.gtfs.transform.line import getLines
from domain.gtfs.transform.operator import getOperators
from domain.gtfs.transform.scheduledstoppoint import getScheduledStopPoints
from domain.gtfs.transform.servicejourney import getServiceJourneys
from domain.gtfs.transform.stoparea import getStopAreas
from domain.gtfs.transform.stopplace import getStopPlaces
from domain.gtfs.transform.version import getVersion
from domain.netex.model import (
    Codespace,
    DataSource,
    Operator,
    Line,
    ServiceJourney,
    StopArea,
    ScheduledStopPoint,
    StopPlace,
    PassengerStopAssignment,
    DayType,
    OperatingPeriod,
    DayTypeAssignment,
)
from storage.interface import Storage


def to_storage(database_file: Path, storage: Storage) -> None:
    with duckdb.connect(database=database_file, read_only=True) as con:
        version = getVersion(con)
        assert version

        # valid_between = getValidBetween(con)
        codespace = getCodespace(con)
        assert codespace

        datasource = getDataSource(con, codespace, version)
        assert datasource

        storage.insert_objects_on_queue(Codespace, [codespace])
        storage.insert_objects_on_queue(DataSource, [datasource])
        storage.insert_objects_on_queue(Operator, getOperators(con, codespace, version))
        storage.insert_objects_on_queue(Line, getLines(con, codespace, version))

        stop_areas = getStopAreas(con, codespace, version)
        storage.insert_objects_on_queue(StopArea, stop_areas)
        storage.insert_objects_on_queue(ScheduledStopPoint, getScheduledStopPoints(con, codespace, version, stop_areas))
        del stop_areas

        stop_places, passenger_stop_assignments = getStopPlaces(con, codespace, version)
        storage.insert_objects_on_queue(StopPlace, stop_places)
        storage.insert_objects_on_queue(PassengerStopAssignment, passenger_stop_assignments)
        del stop_places
        del passenger_stop_assignments

        day_types, day_type_assignments, operating_periods = getDayTypes(con, codespace, version)
        storage.insert_objects_on_queue(DayType, day_types)
        storage.insert_objects_on_queue(OperatingPeriod, operating_periods)
        storage.insert_objects_on_queue(DayTypeAssignment, day_type_assignments)
        del day_types
        del day_type_assignments
        del operating_periods

    # Because we want to create trip_times, and remove it.
    with duckdb.connect(database=database_file, read_only=False) as con:
        storage.insert_objects_on_queue(ServiceJourney, getServiceJourneys(con, codespace, version))
