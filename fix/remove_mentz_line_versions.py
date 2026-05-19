"""
Resolves the Mentz line versions into actual operating days

https://public.3.basecamp.com/p/fVFt3mGJiK52ewcsr8nFgu6o
"""

import logging
from collections import defaultdict
from pathlib import Path

from xsdata.models.datatype import XmlDateTime

from domain.netex.model import (
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
)
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import prepare_logger, log_all


def fmt_dt(dt: XmlDateTime | None) -> str:
    return dt.to_datetime().isoformat() if dt else "None"


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

            # Collect existing DayTypeAssignments and UicOperatingPeriods for those day types.
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

        new_objects: list[DayType | UicOperatingPeriod | DayTypeAssignment] = []
        updated_journeys: list[ServiceJourney] = []

        # Cache: (line_id, line_version, existing_dt_id) -> new day_type_id
        created: dict[tuple[str, str | None, str], str] = {}

        # Pre-index line validity by (line_id, version).
        version_validity: dict[tuple[str, str | None], tuple[XmlDateTime, XmlDateTime | None]] = {}
        for line_id, line_versions in lines.items():
            if len(line_versions) <= 1:
                continue
            for line in line_versions:
                for vb in line.validity_conditions_or_valid_between:
                    if hasattr(vb, 'from_date') and vb.from_date is not None:
                        version_validity[(line_id, line.version)] = (vb.from_date, vb.to_date)
                        break

        for line_id, line_versions in lines.items():
            if len(line_versions) <= 1:
                continue

            for route in routes.get(line_id, []):
                validity = version_validity.get((line_id, route.line_ref.version))
                if validity is None:
                    print(f"  Skipping route {route.id}: no validity for line {line_id} v={route.line_ref.version}")
                    continue

                line_from, line_to = validity
                line_from_date = line_from.to_datetime().date()
                line_to_date = line_to.to_datetime().date() if line_to is not None else line_from_date
                safe_version = (route.line_ref.version or 'unknown').replace(':', '_')

                for sjp in sjps.get(route.id, []):
                    for journey in journeys.get(sjp.id, []):
                        if journey.day_types is None:
                            continue

                        new_refs: list[DayTypeRef] = []
                        for dt_ref in journey.day_types.day_type_ref:
                            existing_dt_id = dt_ref.ref
                            cache_key = (line_id, route.line_ref.version, existing_dt_id)

                            if cache_key in created:
                                new_refs.append(DayTypeRef(ref=created[cache_key], version='1'))
                                continue

                            # Derive the new UicOperatingPeriod by intersecting the existing period
                            # with the line version's validity window.
                            dta = day_type_assignments.get(existing_dt_id)
                            existing_period: UicOperatingPeriod | None = None
                            if dta is not None:
                                period_ref = dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date
                                if hasattr(period_ref, 'ref'):
                                    existing_period = uic_periods.get(period_ref.ref)

                            if existing_period is not None and isinstance(existing_period.from_operating_day_ref_or_from_date, XmlDateTime):
                                period_from_date = existing_period.from_operating_day_ref_or_from_date.to_datetime().date()
                                period_to_ref = existing_period.to_operating_day_ref_or_to_date
                                period_to_date = period_to_ref.to_datetime().date() if isinstance(period_to_ref, XmlDateTime) else line_to_date

                                new_from_date = max(line_from_date, period_from_date)
                                new_to_date = min(line_to_date, period_to_date)

                                if new_from_date <= new_to_date:
                                    # Slice valid_day_bits to the intersection window.
                                    offset = (new_from_date - period_from_date).days
                                    n_days = (new_to_date - new_from_date).days + 1
                                    existing_bits = existing_period.valid_day_bits or ''
                                    new_bits = existing_bits[offset:offset + n_days].ljust(n_days, '0')

                                    new_from_xml: XmlDateTime = line_from if new_from_date == line_from_date else existing_period.from_operating_day_ref_or_from_date
                                    new_to_xml: XmlDateTime | None = line_to if new_to_date == line_to_date else period_to_ref
                                else:
                                    # No overlap — fall back to full line validity with all days active.
                                    new_from_date, new_to_date = line_from_date, line_to_date
                                    n_days = max(1, (new_to_date - new_from_date).days + 1)
                                    new_bits = '1' * n_days
                                    new_from_xml, new_to_xml = line_from, line_to
                            else:
                                # No existing period — cover the full line validity.
                                new_from_date, new_to_date = line_from_date, line_to_date
                                n_days = max(1, (new_to_date - new_from_date).days + 1)
                                new_bits = '1' * n_days
                                new_from_xml, new_to_xml = line_from, line_to

                            new_day_type_id = f"{line_id}:{existing_dt_id}:{safe_version}"
                            new_uic_period_id = f"{line_id}:{existing_dt_id}:UicOperatingPeriod:{safe_version}"
                            new_dta_id = f"{line_id}:{existing_dt_id}:DayTypeAssignment:{safe_version}"

                            print(f"  Creating {new_day_type_id} [{new_from_date} .. {new_to_date}] ({n_days} days)")

                            new_objects.append(DayType(id=new_day_type_id, version='1'))
                            new_objects.append(UicOperatingPeriod(
                                id=new_uic_period_id,
                                version='1',
                                from_operating_day_ref_or_from_date=new_from_xml,
                                to_operating_day_ref_or_to_date=new_to_xml,
                                valid_day_bits=new_bits,
                            ))
                            new_objects.append(DayTypeAssignment(
                                id=new_dta_id,
                                version='1',
                                day_type_ref=DayTypeRef(ref=new_day_type_id, version='1'),
                                uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date=UicOperatingPeriodRef(
                                    ref=new_uic_period_id,
                                    version='1',
                                ),
                            ))

                            created[cache_key] = new_day_type_id
                            new_refs.append(DayTypeRef(ref=new_day_type_id, version='1'))

                        if new_refs:
                            journey.day_types = DayTypeRefsRelStructure(day_type_ref=new_refs)
                            updated_journeys.append(journey)

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