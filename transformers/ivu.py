from typing import Generator

from mdbx.mdbx import TXN

from domain.netex.model import (
    Line,
    ServiceJourneyPattern,
    ServiceJourney,
    Route,
    RouteRef,
    RouteView,
    LineRef,
    FlexibleLineRef,
    OperatorRef,
    ServiceJourneyPatternRef,
    VehicleType,
    MultilingualString,
    StopPlace,
    Quay, TextType,
)
import re
from storage.mdbx.core.implementation import MdbxStorage


def avv_service_journey_operator(db_read: MdbxStorage, txn: TXN) -> Generator[ServiceJourney, None, None]:
    line_operator_ref = {line.id: line.operator_ref for line in db_read.iter_only_objects(txn, Line)}
    route_operator_ref = {route.id: line_operator_ref[route.line_ref.ref] for route in db_read.iter_only_objects(txn, Route) if route.line_ref}

    sjp_operator_ref = {}
    sjp: ServiceJourneyPattern
    for sjp in db_read.iter_only_objects(txn, ServiceJourneyPattern):
        if isinstance(sjp.route_ref_or_route_view, RouteRef):
            sjp_operator_ref[sjp.id] = route_operator_ref[sjp.route_ref_or_route_view.ref]

        elif isinstance(sjp.route_ref_or_route_view, RouteView):
            if isinstance(sjp.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view, LineRef):
                sjp_operator_ref[sjp.id] = line_operator_ref[sjp.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view.ref]

            elif isinstance(sjp.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view, FlexibleLineRef):
                sjp_operator_ref[sjp.id] = line_operator_ref[sjp.route_ref_or_route_view.flexible_line_ref_or_line_ref_or_line_view.ref]

    sj: ServiceJourney
    for sj in db_read.iter_only_objects(txn, ServiceJourney):
        if not isinstance(sj.operator_ref_or_operator_view, OperatorRef):
            if isinstance(sj.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view, LineRef):
                sj.operator_ref_or_operator_view = line_operator_ref[sj.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view.ref]

            elif isinstance(sj.route_ref, RouteRef):
                sj.operator_ref_or_operator_view = route_operator_ref[sj.route_ref.ref]

            elif isinstance(sj.journey_pattern_ref, ServiceJourneyPatternRef):
                sj.operator_ref_or_operator_view = sjp_operator_ref[sj.journey_pattern_ref.ref]

        yield sj


def abbreviate_initials(text: str, max_len: int = 5) -> str:
    words = re.findall(r"[A-Za-z0-9]+", text)
    initials = "".join(w[0] for w in words)
    return initials[:max_len] if len(initials) > max_len else initials


def abbreviate_hybrid(text: str, max_len: int = 5) -> str:
    words = re.findall(r"[A-Za-z0-9]+", text)
    if not words:
        return ""

    abb = words[0][0]  # first letter of first word

    # Append letters from the rest, including same word
    for w in words:
        for ch in w[1:]:
            abb += ch
            if len(abb) >= max_len:
                return str(abb.upper())

    return str(abb.upper())


def make_abbreviation(text: str, max_len: int = 5) -> str:
    text = text.strip()

    if len(text) <= max_len:
        return text.upper()

    abb = abbreviate_initials(text, max_len)
    if len(abb) < max_len:
        abb = abbreviate_hybrid(text, max_len)

    return abb.upper()


def avv_vehicle_type_short_name(db_read: MdbxStorage, txn: TXN) -> Generator[VehicleType, None, None]:
    vt: VehicleType
    for vt in db_read.iter_only_objects(txn, VehicleType):
        if vt.name:
            vt.short_name = MultilingualString(content=[TextType(value=make_abbreviation(str(vt.name.content[0]), 5))])
            yield vt


def avv_quay_name(db_read: MdbxStorage, txn: TXN) -> Generator[StopPlace, None, None]:
    sp: StopPlace
    for sp in db_read.iter_only_objects(txn, StopPlace):
        if sp.quays:
            for quay in sp.quays.taxi_stand_ref_or_quay_ref_or_quay:
                if isinstance(quay, Quay):
                    if not quay.name:
                        quay.name = sp.name
        yield sp
