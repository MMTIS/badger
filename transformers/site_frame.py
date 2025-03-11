import sys
from itertools import groupby
from typing import List, Generator, Tuple

from netexio.database import Database
from netexio.dbaccess import load_local, write_objects, write_generator, load_embedding_generator, load_generator
from utils.refs import getIndex, getRef
from utils.utils import project

from netex import ScheduledStopPoint, StopArea, StopPlace, Quay, QuaysRelStructure, SimplePointVersionStructure, \
    PassengerStopAssignment, QuayRef, StopPlaceRef, ScheduledStopPointRef


def infer_stop_place_from_scheduled_stop_point(scheduled_stop_point: ScheduledStopPoint) -> (
StopPlace, List[PassengerStopAssignment]):
    quay: Quay = project(scheduled_stop_point, Quay)
    quay.centroid = SimplePointVersionStructure(location=scheduled_stop_point.location)
    stop_place: StopPlace = project(scheduled_stop_point, StopPlace)
    stop_place.centroid = SimplePointVersionStructure(location=scheduled_stop_point.location)
    stop_place.quays = QuaysRelStructure(taxi_stand_ref_or_quay_ref_or_quay=[quay])
    passenger_stop_assignment: PassengerStopAssignment = project(scheduled_stop_point, PassengerStopAssignment)
    passenger_stop_assignment.order = 1
    passenger_stop_assignment.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point = getRef(
        scheduled_stop_point)
    passenger_stop_assignment.taxi_stand_ref_or_quay_ref_or_quay = getRef(quay)
    return stop_place, [passenger_stop_assignment]


def infer_stop_place_from_stop_area(stop_area: StopArea, scheduled_stop_points: List[ScheduledStopPoint]) -> (
StopPlace, List[PassengerStopAssignment]):
    def ssp_to_quay(scheduled_stop_point: ScheduledStopPoint):
        quay: Quay = project(scheduled_stop_point, Quay)
        quay.centroid = SimplePointVersionStructure(location=scheduled_stop_point.location)
        passenger_stop_assignment: PassengerStopAssignment = project(scheduled_stop_point, PassengerStopAssignment)
        passenger_stop_assignment.order = 1
        passenger_stop_assignment.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point = getRef(
            scheduled_stop_point)
        passenger_stop_assignment.taxi_stand_ref_or_quay_ref_or_quay = getRef(quay)
        return quay, passenger_stop_assignment

    stop_place: StopPlace = project(stop_area, StopPlace)
    quay_psa = [ssp_to_quay(ssp) for ssp in scheduled_stop_points]
    stop_place.quays = QuaysRelStructure(taxi_stand_ref_or_quay_ref_or_quay=[quay_psa[0] for quay_psa in quay_psa])
    return stop_place, [quay_psa[1] for quay_psa in quay_psa]


def groupby_stop_area(stop_areas: List[StopArea], scheduled_stop_points: List[ScheduledStopPoint]) -> Generator[
    Tuple[StopPlace, List[PassengerStopAssignment]], None, None]:
    stop_areas = getIndex(stop_areas)
    ssp_to_stop_area_ref = groupby(
        [ssp for ssp in scheduled_stop_points if ssp.stop_areas is not None and len(ssp.stop_areas.stop_area_ref) > 0],
        lambda x: x.stop_areas.stop_area_ref[0])
    for stop_area_ref, ssps in ssp_to_stop_area_ref:
        yield infer_stop_place_from_stop_area(stop_areas[stop_area_ref], ssps)
    for ssp in [ssp for ssp in scheduled_stop_points if
                ssp.stop_areas is None or len(ssp.stop_areas.stop_area_ref) == 0]:
        yield infer_stop_place_from_scheduled_stop_point(ssp)


def infer_stop_places(db_read: Database, db_write: Database, generator_defaults: dict):
    print(sys._getframe().f_code.co_name)

    stop_areas = load_local(db_read, StopArea)
    scheduled_stop_points = load_local(db_read, ScheduledStopPoint)
    write_generator(db_write, StopPlace, [], True)
    write_generator(db_write, PassengerStopAssignment, [], True)

    # TODO: How to write to the database, a pair of input from a generator?
    for stop_place, psas in groupby_stop_area(stop_areas, scheduled_stop_points):
        write_objects(db_write, [stop_place], False, False)
        write_objects(db_write, psas, False, False)


# Create an inverse index for ScheduledStopPoint to StopPlace
def index_scheduled_stopplace(db_read: Database) -> Generator[Tuple[ScheduledStopPoint, StopPlaceRef], None, None]:
    index_quay = {}

    # Get all quays embedded in a StopPlace
    for parent, child in load_embedding_generator(db_read, StopPlace, cursor=True):
        if isinstance(child, QuayRef):
            index_quay[child] = parent

    for psa in load_generator(db_read, PassengerStopAssignment):
        psa: PassengerStopAssignment
        if isinstance(psa.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point,
                      ScheduledStopPointRef):
            if isinstance(psa.taxi_rank_ref_or_stop_place_ref_or_stop_place, StopPlaceRef):
                yield psa.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point, psa.taxi_rank_ref_or_stop_place_ref_or_stop_place
            elif isinstance(psa.taxi_stand_ref_or_quay_ref_or_quay, QuayRef):
                yield psa.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point, index_quay[
                    psa.taxi_stand_ref_or_quay_ref_or_quay]
