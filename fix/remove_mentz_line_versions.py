"""
Resolves the Mentz line versions into actual operating days

https://public.3.basecamp.com/p/fVFt3mGJiK52ewcsr8nFgu6o
"""

import logging
from collections import defaultdict
from pathlib import Path

from xsdata.models.datatype import XmlDateTime

from domain.netex.model import DayTypeAssignment, Line, Route, ServiceCalendar, ServiceJourney, ServiceJourneyPattern, UicOperatingPeriod
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import prepare_logger, log_all


def fmt_dt(dt: XmlDateTime | None) -> str:
    return dt.to_datetime().isoformat() if dt else "None"


def list_lines(database: Path):
    lines: defaultdict[str, list[Line]] = defaultdict(list)
    with MdbxStorage(database, readonly=True) as db:
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

            day_type_ids: set[str] = set()
            journeys: defaultdict[str, list[ServiceJourney]] = defaultdict(list)
            for journey in db.iter_only_objects(txn, ServiceJourney):
                if journey.journey_pattern_ref is not None and journey.journey_pattern_ref.ref in duplicate_sjp_ids:
                    journeys[journey.journey_pattern_ref.ref].append(journey)
                    if journey.day_types is not None:
                        for ref in journey.day_types.day_type_ref:
                            day_type_ids.add(ref.ref)

            uic_period_ids: set[str] = set()
            day_type_assignments: defaultdict[str, DayTypeAssignment] = {}
            uic_periods: dict[str, UicOperatingPeriod] = {}

            for calendar in db.iter_only_objects(txn, ServiceCalendar):
                if calendar.day_type_assignments is None:
                    continue
                for dta in calendar.day_type_assignments.day_type_assignment:
                    if dta.day_type_ref is not None and dta.day_type_ref.ref in day_type_ids:
                        day_type_assignments[dta.day_type_ref.ref] = dta
                        ref = dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date
                        if hasattr(ref, 'ref'):
                            uic_period_ids.add(ref.ref)

                if calendar.operating_periods is None:
                    continue

                for entry in calendar.operating_periods.uic_operating_period_ref_or_operating_period_ref_or_operating_period_or_uic_operating_period:
                    if isinstance(entry, UicOperatingPeriod) and entry.id in uic_period_ids:
                        uic_periods[entry.id] = entry

            for key, values in sorted(lines.items()):
                if len(values) > 1:
                    print(f"Duplicate line ID: {key}")
                    for line in values:
                        dates = [(fmt_dt(v.from_date), fmt_dt(v.to_date)) for v in line.validity_conditions_or_valid_between if hasattr(v, 'from_date')]
                        print(f"  version={line.version} dates={dates}")
                    for route in routes.get(key, []):
                        print(f"  Route: {route.id} lineRef={route.line_ref.ref} v={route.line_ref.version}")
                        for sjp in sjps.get(route.id, []):
                            print(f"    ServiceJourneyPattern: {sjp.id} version={sjp.version}")
                            for journey in journeys.get(sjp.id, []):
                                if journey.day_types is None:
                                    continue
                                for dt_ref in journey.day_types.day_type_ref:
                                    print(f"      Journey: {journey.id} DayType: {dt_ref.ref}")
                                    dta = day_type_assignments.get(dt_ref.ref)

                                    ref = dta.uic_operating_period_ref_or_operating_period_ref_or_operating_day_ref_or_date
                                    period = uic_periods.get(ref.ref) if hasattr(ref, 'ref') else None


def main(source_database_file: str):
    return list_lines(Path(source_database_file))


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
