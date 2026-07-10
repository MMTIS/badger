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
from collections.abc import Generator
from datetime import timedelta
from pathlib import Path

from xsdata.models.datatype import XmlDateTime

from domain.netex.model import (
    CompositeFrame,
    DayType,
    DayTypeAssignment,
    DayTypeRef,
    DayTypeRefsRelStructure,
    Line,
    OperatingPeriod,
    OperatingPeriodRef,
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


def _xml_dt(dt) -> XmlDateTime:
    return XmlDateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)


def _compute_default_segments(
    from_dt: XmlDateTime,
    default_end: XmlDateTime,
    overrides: list[tuple[XmlDateTime, XmlDateTime]],
) -> list[ValidBetween]:
    """Return non-overlapping ValidBetween segments for a default version, excluding override windows."""
    segments: list[ValidBetween] = []
    current = from_dt.to_datetime()
    end = default_end.to_datetime()

    for od_from, od_to in sorted(overrides, key=lambda x: x[0].to_datetime()):
        od_start = od_from.to_datetime()
        od_end = od_to.to_datetime()
        if od_start >= end:
            break
        if od_end <= current:
            continue
        if current < od_start:
            segments.append(ValidBetween(
                from_date=_xml_dt(current),
                to_date=_xml_dt(od_start - timedelta(seconds=1)),
            ))
        current = od_end + timedelta(seconds=1)

    if current < end:
        segments.append(ValidBetween(from_date=_xml_dt(current), to_date=_xml_dt(end)))

    return segments


def _iter_version_validity(
    lines: defaultdict[str, list[Line]],
    frame_end: XmlDateTime,
) -> Generator[tuple[tuple[str, str | None], list[ValidBetween]], None, None]:
    """Yield (line_id, version) → list of ValidBetween segments.

    Versions without a ToDate are "defaults": they run continuously from their
    FromDate (until the next default's FromDate, or frame_end), interrupted only
    by "override" versions that carry an explicit ToDate.

    Versions with a ToDate are "overrides": they run exactly during their window.
    """
    for line_id, line_versions in lines.items():
        if len(line_versions) <= 1:
            continue

        raw: list[tuple[str | None, XmlDateTime, XmlDateTime | None]] = []
        for line in line_versions:
            for vb in line.validity_conditions_or_valid_between:
                if hasattr(vb, 'from_date') and vb.from_date is not None:
                    raw.append((line.version, vb.from_date, vb.to_date))
                    break

        defaults = sorted(
            [(ver, from_dt) for ver, from_dt, to_dt in raw if to_dt is None],
            key=lambda x: x[1].to_datetime(),
        )
        overrides = [(ver, from_dt, to_dt) for ver, from_dt, to_dt in raw if to_dt is not None]

        for ver, from_dt, to_dt in raw:
            if to_dt is not None:
                yield (line_id, ver), [ValidBetween(from_date=from_dt, to_date=to_dt)]
            else:
                idx = next(i for i, (v, _) in enumerate(defaults) if v == ver)
                if idx + 1 < len(defaults):
                    default_end = _xml_dt(defaults[idx + 1][1].to_datetime() - timedelta(seconds=1))
                else:
                    default_end = frame_end
                override_windows = [(od_from, od_to) for _, od_from, od_to in overrides]
                segments = _compute_default_segments(from_dt, default_end, override_windows)
                if segments:
                    yield (line_id, ver), segments


def _make_calendar_objects(
    line_id: str,
    existing_dt_id: str,
    safe_version: str,
    existing_period: UicOperatingPeriod | None,
    validity: ValidBetween,
) -> tuple[DayType, UicOperatingPeriod | OperatingPeriod, DayTypeAssignment]:
    new_day_type_id = f"{line_id}:{existing_dt_id}:{safe_version}"
    new_dta_id = f"{line_id}:{existing_dt_id}:DayTypeAssignment:{safe_version}"

    line_from_date = validity.from_date.to_datetime().date()
    line_to_date = validity.to_date.to_datetime().date()

    period: UicOperatingPeriod | OperatingPeriod
    period_ref: UicOperatingPeriodRef | OperatingPeriodRef

    if existing_period is not None and isinstance(existing_period.from_operating_day_ref_or_from_date, XmlDateTime):
        period_from_date = existing_period.from_operating_day_ref_or_from_date.to_datetime().date()
        period_to_ref = existing_period.to_operating_day_ref_or_to_date
        period_to_date = period_to_ref.to_datetime().date() if isinstance(period_to_ref, XmlDateTime) else line_to_date

        new_from_date = max(line_from_date, period_from_date)
        new_to_date = min(line_to_date, period_to_date)

        if new_from_date <= new_to_date:
            offset = (new_from_date - period_from_date).days
            n_days = (new_to_date - new_from_date).days + 1
            existing_bits = existing_period.valid_day_bits or ''
            bits = existing_bits[offset:offset + n_days].ljust(n_days, '0')
            from_xml = validity.from_date if new_from_date == line_from_date else existing_period.from_operating_day_ref_or_from_date
            to_xml = validity.to_date if new_to_date == line_to_date else period_to_ref

            new_period_id = f"{line_id}:{existing_dt_id}:UicOperatingPeriod:{safe_version}"
            print(f"  Creating {new_day_type_id} [{new_from_date} .. {new_to_date}] ({n_days} days)")
            period = UicOperatingPeriod(id=new_period_id, version='1', from_operating_day_ref_or_from_date=from_xml, to_operating_day_ref_or_to_date=to_xml, valid_day_bits=bits)
            period_ref = UicOperatingPeriodRef(ref=new_period_id, version='1')

            return (
                DayType(id=new_day_type_id, version='1'),
                period,
                DayTypeAssignment(id=new_dta_id, version='1', day_type_ref=DayTypeRef(ref=new_day_type_id, version='1'), uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date=period_ref),
            )

    # No existing period or no overlap — all days in the line's validity window are active.
    new_period_id = f"{line_id}:{existing_dt_id}:OperatingPeriod:{safe_version}"
    n_days = max(1, (line_to_date - line_from_date).days + 1)
    print(f"  Creating {new_day_type_id} [{line_from_date} .. {line_to_date}] ({n_days} days)")
    period = OperatingPeriod(id=new_period_id, version='1', from_operating_day_ref_or_from_date=validity.from_date, to_operating_day_ref_or_to_date=validity.to_date)
    period_ref = OperatingPeriodRef(ref=new_period_id, version='1')

    return (
        DayType(id=new_day_type_id, version='1'),
        period,
        DayTypeAssignment(id=new_dta_id, version='1', day_type_ref=DayTypeRef(ref=new_day_type_id, version='1'), uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date=period_ref),
    )


def _process_journey(
    journey: ServiceJourney,
    line_id: str,
    line_version: str | None,
    safe_version: str,
    validity: ValidBetween,
    day_type_assignments: dict[str, DayTypeAssignment],
    uic_periods: dict[str, UicOperatingPeriod],
    new_objects: list,
    created: dict[tuple[str, str, str], str],
) -> list[DayTypeRef]:
    new_refs: list[DayTypeRef] = []
    for dt_ref in journey.day_types.day_type_ref:  # type: ignore[union-attr]
        existing_dt_id = dt_ref.ref
        cache_key = (line_id, safe_version, existing_dt_id)

        if cache_key in created:
            new_refs.append(DayTypeRef(ref=created[cache_key], version='1'))
            continue

        dta = day_type_assignments.get(existing_dt_id)
        existing_period: UicOperatingPeriod | None = None
        if dta is not None:
            period_ref = dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date
            if hasattr(period_ref, 'ref'):
                existing_period = uic_periods.get(period_ref.ref)

        objs = _make_calendar_objects(line_id, existing_dt_id, safe_version, existing_period, validity)
        new_objects.extend(objs)

        created[cache_key] = objs[0].id
        new_refs.append(DayTypeRef(ref=objs[0].id, version='1'))
    return new_refs


def _resolve_journeys(
    lines: defaultdict[str, list[Line]],
    routes: defaultdict[str, list[Route]],
    sjps: defaultdict[str, list[ServiceJourneyPattern]],
    journeys: defaultdict[str, list[ServiceJourney]],
    version_validity: dict[tuple[str, str | None], list[ValidBetween]],
    day_type_assignments: dict[str, DayTypeAssignment],
    uic_periods: dict[str, UicOperatingPeriod],
) -> tuple[list[DayType | UicOperatingPeriod | OperatingPeriod | DayTypeAssignment], list[ServiceJourney]]:
    new_objects: list[DayType | UicOperatingPeriod | OperatingPeriod | DayTypeAssignment] = []
    updated_journeys: list[ServiceJourney] = []
    created: dict[tuple[str, str, str], str] = {}

    for line_id, line_versions in lines.items():
        if len(line_versions) <= 1:
            continue

        for route in routes.get(line_id, []):
            validities = version_validity.get((line_id, route.line_ref.version))
            if not validities:
                print(f"  Skipping route {route.id}: no validity for line {line_id} v={route.line_ref.version}")
                continue

            base_safe = (route.line_ref.version or 'unknown').replace(':', '_')

            for sjp in sjps.get(route.id, []):
                for journey in journeys.get(sjp.id, []):
                    if journey.day_types is None:
                        continue

                    all_new_refs: list[DayTypeRef] = []
                    for seg_idx, validity in enumerate(validities):
                        safe_version = f"{base_safe}_{seg_idx}"
                        new_refs = _process_journey(
                            journey, line_id, route.line_ref.version, safe_version,
                            validity, day_type_assignments, uic_periods, new_objects, created,
                        )
                        all_new_refs.extend(new_refs)

                    if all_new_refs:
                        journey.day_types = DayTypeRefsRelStructure(day_type_ref=all_new_refs)
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

        version_validity = dict(_iter_version_validity(lines, frame_end))

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
