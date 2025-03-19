from typing import Iterable, Dict, Any

from netexio.database import Database
from netexio.dbaccess import load_generator
from netex import PassengerStopAssignment, StopPlace, LocationStructure2, Quay, ScheduledStopPoint, ScheduledStopPointRef, QuayRef, StopPlaceRef


def infer_locations_from_quay_or_stopplace_and_apply(db_read: Database, db_write: Database, generator_defaults: dict[str, Any]) -> None:
    mapping: Dict[str, LocationStructure2] = {}
    ssp_location: Dict[str, LocationStructure2] = {}

    def process(ssp: ScheduledStopPoint, generator_defaults: dict[str, str]) -> ScheduledStopPoint:
        assert ssp.id is not None, f"ScheduledStopPoint without id"
        if ssp.location is None:
            location: LocationStructure2 | None = ssp_location.get(ssp.id, None)
            if location is not None:
                ssp.location = location

        # TODO: The question here is can we just do something like a virtual table?
        return ssp

    def query(db_read: Database) -> Iterable[ScheduledStopPoint]:
        _load_generator = load_generator(db_read, ScheduledStopPoint)
        for ssp in _load_generator:
            new_ssp = process(ssp, generator_defaults)
            if new_ssp is not None:
                yield new_ssp

    sp: StopPlace
    for sp in load_generator(db_read, StopPlace):
        if sp.centroid is not None:
            assert sp.id is not None, f"StopPlace without id"
            mapping[sp.id] = getattr(sp.centroid, "location")
        if sp.quays is not None:
            for quay in sp.quays.taxi_stand_ref_or_quay_ref_or_quay:
                if isinstance(quay, Quay):
                    assert quay.id is not None, f"Quay without id"
                    if quay.centroid is not None:
                        mapping[quay.id] = getattr(quay.centroid, "location")
                    elif sp.centroid is not None:
                        mapping[quay.id] = getattr(sp.centroid, "location")

    ssp_location = {}

    psa: PassengerStopAssignment
    for psa in load_generator(db_read, PassengerStopAssignment):
        if isinstance(psa.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point,
                      ScheduledStopPointRef):
            if isinstance(psa.taxi_stand_ref_or_quay_ref_or_quay, QuayRef):
                if psa.taxi_stand_ref_or_quay_ref_or_quay.ref in mapping:
                    ssp_location[
                        psa.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point.ref] = \
                    mapping[psa.taxi_stand_ref_or_quay_ref_or_quay.ref]
            if isinstance(psa.taxi_rank_ref_or_stop_place_ref_or_stop_place, StopPlaceRef):
                if psa.taxi_rank_ref_or_stop_place_ref_or_stop_place.ref in mapping:
                    ssp_location[
                        psa.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point.ref] = \
                    mapping[psa.taxi_rank_ref_or_stop_place_ref_or_stop_place.ref]

    db_write.insert_objects_on_queue(ScheduledStopPoint, query(db_read))
