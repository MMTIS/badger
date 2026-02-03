"""
There are currently datasets available that do not meet the NeTEx requirements.
This script validates that a ServiceJourney/timetabledPassingTimes has a reference to a StopPointInJourneyPattern.
This script does not validate the existance of an existing StopPointInJourneyPatternRef, as this would already be a referential constraint violation.
"""

import logging
from pathlib import Path
from typing import Generator

from mdbx.mdbx import TXN

from domain.netex.model import (
    ServiceJourney,
    TimetabledPassingTime,
    DeadRunJourneyPattern,
    ServicePattern,
    ServiceJourneyPattern,
    JourneyPattern,
    PointInJourneyPatternRef,
    TimingPointInJourneyPatternRef,
    StopPointInJourneyPatternRef,
    FarePointInPatternRef,
    PointInSingleJourneyPathRef,
)
from domain.netex.services.refs import getRef
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import prepare_logger, log_all


def point_in_sequence_ref(unknown: ServiceJourneyPattern | ServicePattern | DeadRunJourneyPattern | JourneyPattern):
    if isinstance(unknown, ServiceJourneyPattern):
        sjp: ServiceJourneyPattern = unknown
        return [getRef(pis) for pis in sjp.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern]
    elif isinstance(unknown, JourneyPattern):
        jp: JourneyPattern = unknown
        return [getRef(pis) for pis in jp.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern]
    elif isinstance(unknown, DeadRunJourneyPattern):
        drjp: DeadRunJourneyPattern = unknown
        return [getRef(pis) for pis in drjp.points_in_sequence.point_in_journey_pattern_or_stop_point_in_journey_pattern_or_timing_point_in_journey_pattern]
    elif isinstance(unknown, ServicePattern):
        sp: ServicePattern = unknown
        return [getRef(pis) for pis in sp.points_in_sequence.stop_point_in_journey_pattern]


def fix_stop_point_in_journey_pattern_ref(database: Path):
    with MdbxStorage(database, readonly=False) as db:
        with db.env.rw_transaction() as txn:

            def query(txn: TXN) -> Generator[ServiceJourney, None, None]:
                sj: ServiceJourney
                for sj in db.iter_only_objects(txn, ServiceJourney):
                    jp: ServiceJourneyPattern | ServicePattern | DeadRunJourneyPattern | JourneyPattern | None = None
                    jpps: list[
                        PointInSingleJourneyPathRef
                        | FarePointInPatternRef
                        | StopPointInJourneyPatternRef
                        | TimingPointInJourneyPatternRef
                        | PointInJourneyPatternRef
                    ]
                    # Validate that this ServiceJourney has timetabledPassingTimes
                    if sj.passing_times and len(sj.passing_times.timetabled_passing_time) > 0:
                        write = False
                        for i in range(0, len(sj.passing_times.timetabled_passing_time)):
                            # Validate that a passingTime exists without reference to a StopPointInJourneyPattern
                            ttpt: TimetabledPassingTime = sj.passing_times.timetabled_passing_time[i]
                            if ttpt.point_in_journey_pattern_ref is None:
                                if jp is None:
                                    jp = db.load_object_by_reference(txn, sj.journey_pattern_ref)
                                    jpps = point_in_sequence_ref(jp)
                                    if len(jpps) != len(sj.passing_times.timetabled_passing_time):
                                        log_all(
                                            logging.ERROR,
                                            f"{sj.id} has unreferenced stops and has an invalid number of PassingTimes with respect to {jp.id}, can't fix.",
                                        )
                                        break

                                ttpt.point_in_journey_pattern_ref = jpps[i]
                                write = True
                        if write:
                            yield sj

            db.insert_any_object_on_queue(txn, query(txn))
            txn.commit()


def main(source_database_file: str):
    return fix_stop_point_in_journey_pattern_ref(Path(source_database_file))


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Check an MDBX for invalid Timetabled Passing Times without PointInJourneyPatternRef")
    parser.add_argument("source", type=str, help="mdbx file to use as input.")
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.source)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
