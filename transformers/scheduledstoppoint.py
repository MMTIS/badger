from typing import Generator, Dict, Any

from mdbx.mdbx import TXN

from domain.netex.model import (
    PassengerStopAssignment,
    StopPlace,
    LocationStructure2,
    Quay,
    ScheduledStopPoint,
    ScheduledStopPointRef,
    QuayRef,
    StopPlaceRef,
    ServiceJourney,
)

from storage.mdbx.core.implementation import MdbxStorage

from domain.netex.services.model_typing import Tid


def infer_locations_from_quay_or_stopplace_and_apply(db_read: MdbxStorage, txn: TXN, generator_defaults: dict[str, Any]) -> Generator[Tid, None, None]:
    mapping: Dict[str, LocationStructure2] = {}
    ssp_location: Dict[str, LocationStructure2] = {}

    def process(ssp: ScheduledStopPoint, generator_defaults: dict[str, str]) -> Generator[ScheduledStopPoint, None, None]:
        assert ssp.id is not None, f"ScheduledStopPoint without id"
        ssp.projections = None  # TODO: Somewhere else
        ssp.stop_areas = None  # TODO: Somewhere else
        if ssp.location is None:
            location: LocationStructure2 | None = ssp_location.get(ssp.id, None)
            if location is not None:
                ssp.location = location

        # TODO: The question here is can we just do something like a virtual table?
        yield ssp

    def query(db_read: MdbxStorage, txn: TXN) -> Generator[ScheduledStopPoint, None, None]:
        for ssp in db_read.iter_only_objects(txn, ScheduledStopPoint):
            yield from process(ssp, generator_defaults)

    sp: StopPlace
    for _key, sp in db_read.iter_objects(txn, StopPlace):
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
    for _key, psa in db_read.iter_objects(txn, PassengerStopAssignment):
        if isinstance(psa.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point, ScheduledStopPointRef):
            if isinstance(psa.taxi_stand_ref_or_quay_ref_or_quay, QuayRef):
                if psa.taxi_stand_ref_or_quay_ref_or_quay.ref in mapping:
                    ssp_location[psa.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point.ref] = mapping[
                        psa.taxi_stand_ref_or_quay_ref_or_quay.ref
                    ]
            if isinstance(psa.taxi_rank_ref_or_stop_place_ref_or_stop_place, StopPlaceRef):
                if psa.taxi_rank_ref_or_stop_place_ref_or_stop_place.ref in mapping:
                    ssp_location[psa.fare_scheduled_stop_point_ref_or_scheduled_stop_point_ref_or_scheduled_stop_point.ref] = mapping[
                        psa.taxi_rank_ref_or_stop_place_ref_or_stop_place.ref
                    ]

    yield from query(db_read, txn)
