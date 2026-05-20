"""
Resolves the Mentz line versions into actual operating days.

Mentz' EPIP exporter writes variations of a journey not as separate operating
periods but as a completely new "version" of a line, even though it's not the
line that changes. It looks like this:

Line A: v1
  validity: 2026-01-01 -> 2026-01-31
  Route A1:
    ServiceJourneyPattern A1:
      Journey A1
        dayTypes: Period1

Line A: v2
  validity: 2026-02-01 -> 2026-02-31
  Route A2:
    ServiceJourneyPattern A2:
      Journey A2
        dayTypes: Period1

This means you must create new operating periods for each version of the line
and assign the correct day types to each version of the journey.

This is very unfortunate and extremely convoluted to untangle.

Further reading:
 https://public.3.basecamp.com/p/fVFt3mGJiK52ewcsr8nFgu6o
 https://github.com/noi-techpark/opendatahub-mentor-otp/issues/291

"""

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from xsdata.models.datatype import XmlDateTime

from domain.netex.model import (
    CompositeFrame,
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
    ValidBetween,
)
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import log_all, prepare_logger


def fmt_dt(dt: XmlDateTime | None) -> str:
    return dt.to_datetime().isoformat() if dt else "None"


def _xml_date(d: date) -> XmlDateTime:
    return XmlDateTime(d.year, d.month, d.day, 0, 0, 0)


@dataclass
class LineValidity:
    from_dt: XmlDateTime
    to_dt: XmlDateTime

    @property
    def from_date(self) -> date:
        return self.from_dt.to_datetime().date()

    @property
    def to_date(self) -> date:
        return self.to_dt.to_datetime().date()


@dataclass
class NewPeriod:
    from_xml: XmlDateTime
    to_xml: XmlDateTime
    bits: str
    from_date: date
    to_date: date

    @property
    def n_days(self) -> int:
        return (self.to_date - self.from_date).days + 1


def _build_version_validity(
    lines: defaultdict[str, list[Line]],
    frame_end: XmlDateTime,
) -> dict[tuple[str, str | None], LineValidity]:
    result: dict[tuple[str, str | None], LineValidity] = {}
    for line_id, line_versions in lines.items():
        if len(line_versions) <= 1:
            continue
        for line in line_versions:
            for vb in line.validity_conditions_or_valid_between:
                if hasattr(vb, 'from_date') and vb.from_date is not None:
                    result[(line_id, line.version)] = LineValidity(
                        from_dt=vb.from_date,
                        to_dt=vb.to_date if vb.to_date is not None else frame_end,
                    )
                    break
    return result


def _compute_period(
    existing_period: UicOperatingPeriod | None,
    validity: LineValidity,
) -> NewPeriod:
    if existing_period is not None and isinstance(existing_period.from_operating_day_ref_or_from_date, XmlDateTime):
        period_from_date = existing_period.from_operating_day_ref_or_from_date.to_datetime().date()
        period_to_ref = existing_period.to_operating_day_ref_or_to_date
        period_to_date = period_to_ref.to_datetime().date() if isinstance(period_to_ref, XmlDateTime) else validity.to_date

        new_from_date = max(validity.from_date, period_from_date)
        new_to_date = min(validity.to_date, period_to_date)

        if new_from_date <= new_to_date:
            offset = (new_from_date - period_from_date).days
            n_days = (new_to_date - new_from_date).days + 1
            existing_bits = existing_period.valid_day_bits or ''
            bits = existing_bits[offset:offset + n_days].ljust(n_days, '0')
            from_xml = validity.from_dt if new_from_date == validity.from_date else existing_period.from_operating_day_ref_or_from_date
            to_xml = validity.to_dt if new_to_date == validity.to_date else period_to_ref
            return NewPeriod(from_xml=from_xml, to_xml=to_xml, bits=bits, from_date=new_from_date, to_date=new_to_date)

    # No existing period or no overlap — cover the full line validity with all days active.
    n_days = max(1, (validity.to_date - validity.from_date).days + 1)
    return NewPeriod(
        from_xml=validity.from_dt,
        to_xml=validity.to_dt,
        bits='1' * n_days,
        from_date=validity.from_date,
        to_date=validity.to_date,
    )


def _make_calendar_objects(
    line_id: str,
    existing_dt_id: str,
    safe_version: str,
    period: NewPeriod,
) -> tuple[DayType, UicOperatingPeriod, DayTypeAssignment]:
    new_day_type_id = f"{line_id}:{existing_dt_id}:{safe_version}"
    new_uic_period_id = f"{line_id}:{existing_dt_id}:UicOperatingPeriod:{safe_version}"
    new_dta_id = f"{line_id}:{existing_dt_id}:DayTypeAssignment:{safe_version}"

    print(f"  Creating {new_day_type_id} [{period.from_date} .. {period.to_date}] ({period.n_days} days)")

    return (
        DayType(id=new_day_type_id, version='1'),
        UicOperatingPeriod(
            id=new_uic_period_id,
            version='1',
            from_operating_day_ref_or_from_date=period.from_xml,
            to_operating_day_ref_or_to_date=period.to_xml,
            valid_day_bits=period.bits,
        ),
        DayTypeAssignment(
            id=new_dta_id,
            version='1',
            day_type_ref=DayTypeRef(ref=new_day_type_id, version='1'),
            uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date=UicOperatingPeriodRef(
                ref=new_uic_period_id,
                version='1',
            ),
        ),
    )


def _process_journey(
    journey: ServiceJourney,
    line_id: str,
    line_version: str | None,
    safe_version: str,
    validity: LineValidity,
    day_type_assignments: dict[str, DayTypeAssignment],
    uic_periods: dict[str, UicOperatingPeriod],
    new_objects: list,
    created: dict[tuple[str, str | None, str], str],
) -> list[DayTypeRef]:
    new_refs: list[DayTypeRef] = []
    for dt_ref in journey.day_types.day_type_ref:  # type: ignore[union-attr]
        existing_dt_id = dt_ref.ref
        cache_key = (line_id, line_version, existing_dt_id)

        if cache_key in created:
            new_refs.append(DayTypeRef(ref=created[cache_key], version='1'))
            continue

        dta = day_type_assignments.get(existing_dt_id)
        existing_period: UicOperatingPeriod | None = None
        if dta is not None:
            period_ref = dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date
            if hasattr(period_ref, 'ref'):
                existing_period = uic_periods.get(period_ref.ref)

        period = _compute_period(existing_period, validity)
        objs = _make_calendar_objects(line_id, existing_dt_id, safe_version, period)
        new_objects.extend(objs)

        created[cache_key] = objs[0].id
        new_refs.append(DayTypeRef(ref=objs[0].id, version='1'))
    return new_refs


def _resolve_journeys(
    lines: defaultdict[str, list[Line]],
    routes: defaultdict[str, list[Route]],
    sjps: defaultdict[str, list[ServiceJourneyPattern]],
    journeys: defaultdict[str, list[ServiceJourney]],
    version_validity: dict[tuple[str, str | None], LineValidity],
    day_type_assignments: dict[str, DayTypeAssignment],
    uic_periods: dict[str, UicOperatingPeriod],
) -> tuple[list[DayType | UicOperatingPeriod | DayTypeAssignment], list[ServiceJourney]]:
    new_objects: list[DayType | UicOperatingPeriod | DayTypeAssignment] = []
    updated_journeys: list[ServiceJourney] = []
    created: dict[tuple[str, str | None, str], str] = {}

    for line_id, line_versions in lines.items():
        if len(line_versions) <= 1:
            continue

        for route in routes.get(line_id, []):
            validity = version_validity.get((line_id, route.line_ref.version))
            if validity is None:
                print(f"  Skipping route {route.id}: no validity for line {line_id} v={route.line_ref.version}")
                continue

            safe_version = (route.line_ref.version or 'unknown').replace(':', '_')

            for sjp in sjps.get(route.id, []):
                for journey in journeys.get(sjp.id, []):
                    if journey.day_types is None:
                        continue

                    new_refs = _process_journey(
                        journey, line_id, route.line_ref.version, safe_version,
                        validity, day_type_assignments, uic_periods, new_objects, created,
                    )
                    if new_refs:
                        journey.day_types = DayTypeRefsRelStructure(day_type_ref=new_refs)
                        updated_journeys.append(journey)

    return new_objects, updated_journeys


def fix_lines(database: Path) -> None:
    lines: defaultdict[str, list[Line]] = defaultdict(list)

    with MdbxStorage(database, readonly=False) as db:
        with db.env.rw_transaction() as txn:
            for line in db.iter_only_objects(txn, Line):
                lines[line.id].append(line)

            duplicate_line_ids = {key for key, values in lines.items() if len(values) > 1}
            print(f"{len(duplicate_line_ids)} of {len(lines)} lines have duplicates")

            routes: defaultdict[str, list[Route]] = defaultdict(list)
            for route in db.iter_only_objects(txn, Route):
                if route.line_ref is not None and route.line_ref.ref in duplicate_line_ids:
                    routes[route.line_ref.ref].append(route)

            duplicate_route_ids = {route.id for route_list in routes.values() for route in route_list}

            sjps: defaultdict[str, list[ServiceJourneyPattern]] = defaultdict(list)
            for sjp in db.iter_only_objects(txn, ServiceJourneyPattern):
                if sjp.route_ref_or_route_view is not None and sjp.route_ref_or_route_view.ref in duplicate_route_ids:
                    sjps[sjp.route_ref_or_route_view.ref].append(sjp)

            duplicate_sjp_ids = {sjp.id for sjp_list in sjps.values() for sjp in sjp_list}

            journeys: defaultdict[str, list[ServiceJourney]] = defaultdict(list)
            day_type_ids: set[str] = set()
            for journey in db.iter_only_objects(txn, ServiceJourney):
                if journey.journey_pattern_ref is not None and journey.journey_pattern_ref.ref in duplicate_sjp_ids:
                    journeys[journey.journey_pattern_ref.ref].append(journey)
                    if journey.day_types is not None:
                        for ref in journey.day_types.day_type_ref:
                            day_type_ids.add(ref.ref)

            uic_period_ids: set[str] = set()
            day_type_assignments: dict[str, DayTypeAssignment] = {}
            uic_periods: dict[str, UicOperatingPeriod] = {}

            for calendar in db.iter_only_objects(txn, ServiceCalendar):
                if calendar.day_type_assignments is not None:
                    for dta in calendar.day_type_assignments.day_type_assignment:
                        if dta.day_type_ref is not None and dta.day_type_ref.ref in day_type_ids:
                            day_type_assignments[dta.day_type_ref.ref] = dta
                            ref = dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date
                            if hasattr(ref, 'ref'):
                                uic_period_ids.add(ref.ref)

                if calendar.operating_periods is not None:
                    for entry in calendar.operating_periods.uic_operating_period_ref_or_operating_period_ref_or_operating_period_or_uic_operating_period:
                        if isinstance(entry, UicOperatingPeriod) and entry.id in uic_period_ids:
                            uic_periods[entry.id] = entry

        # we really should be getting this from CompositeFrame, but that doesn't appear to be stored
        frame_end: XmlDateTime = XmlDateTime(2026, 12, 14, 23, 59, 59)
        print(f"The feed's end date is {frame_end}")

        version_validity = _build_version_validity(lines, frame_end)

        new_objects, updated_journeys = _resolve_journeys(
            lines, routes, sjps, journeys, version_validity, day_type_assignments, uic_periods,
        )

        print(f"Writing {len(new_objects)} new calendar objects and {len(updated_journeys)} updated journeys")

        with db.env.rw_transaction() as txn:
            db.insert_any_object_on_queue(txn, new_objects)
            db.insert_any_object_on_queue(txn, updated_journeys)
            txn.commit()


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
