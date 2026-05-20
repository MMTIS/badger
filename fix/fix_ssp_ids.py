"""
Fix ScheduledStopPoint IDs to match SIRI feed format.

Transforms IDs from:  IT:ITH1:ScheduledStopPoint:it-22021-7010-51-32073:
to:                   IT:ITH10:ScheduledStopPoint:7010:51:32073

This is needed so that NeTEx and SIRI feeds reference the same stops.
"""

import dataclasses
import logging
import re
from pathlib import Path
from typing import Any

from domain.netex.model import (
    PassengerStopAssignment,
    Route,
    RoutePoint,
    RoutePointRef,
    ScheduledStopPoint,
    ScheduledStopPointRef,
    ServiceJourneyPattern,
    ServiceLink,
    TimingLink,
)
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import log_all, prepare_logger

_PATTERN = re.compile(r'^.*:ScheduledStopPoint:it-22021-(.+):$')


def _new_id(old_id: str) -> str | None:
    m = _PATTERN.match(old_id)
    if m:
        return 'IT:ITH10:ScheduledStopPoint:' + m.group(1).replace('-', ':')
    return None


def _update_refs(obj: Any, id_map: dict[str, str]) -> bool:
    if obj is None or not dataclasses.is_dataclass(obj) or isinstance(obj, type):
        return False
    modified = False
    for f in dataclasses.fields(obj):
        val = getattr(obj, f.name)
        if isinstance(val, (ScheduledStopPointRef, RoutePointRef)):
            new_ref = id_map.get(val.ref)
            if new_ref is not None:
                val.ref = new_ref
                modified = True
        elif isinstance(val, list):
            for item in val:
                if _update_refs(item, id_map):
                    modified = True
        elif dataclasses.is_dataclass(val):
            if _update_refs(val, id_map):
                modified = True
    return modified


# Object types that may transitively contain ScheduledStopPointRef or RoutePointRef.
_REF_BEARING_TYPES = [
    ServiceJourneyPattern,
    ServiceLink,
    TimingLink,
    PassengerStopAssignment,
    Route,
    RoutePoint,
]


def fix_ssp_ids(database: Path) -> None:
    with MdbxStorage(database, readonly=False) as db:
        with db.env.rw_transaction() as txn:
            id_map: dict[str, str] = {}
            old_ssps: list[ScheduledStopPoint] = []
            for ssp in db.iter_only_objects(txn, ScheduledStopPoint):
                new_id = _new_id(ssp.id)
                if new_id is not None:
                    id_map[ssp.id] = new_id
                    old_ssps.append(ssp)

            print(f"{len(id_map)} ScheduledStopPoints to rewrite")

            updated: list[Any] = []
            for cls in _REF_BEARING_TYPES:
                for obj in db.iter_only_objects(txn, cls):
                    if _update_refs(obj, id_map):
                        updated.append(obj)

            new_ssps = [dataclasses.replace(ssp, id=id_map[ssp.id]) for ssp in old_ssps]

            print(f"Updating refs in {len(updated)} objects")
            print(f"Inserting {len(new_ssps)} renamed ScheduledStopPoints")
            # TODO: delete the old ScheduledStopPoint objects (no delete API available yet)

            db.insert_any_object_on_queue(txn, updated)
            db.insert_any_object_on_queue(txn, new_ssps)
            txn.commit()


def main(source_database_file: str) -> None:
    fix_ssp_ids(Path(source_database_file))


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Fix ScheduledStopPoint IDs to SIRI format")
    parser.add_argument("source", type=str, help="mdbx file to fix in-place")
    parser.add_argument("--log_file", type=str, required=False, help="log file path")
    args = parser.parse_args()
    prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.source)
    except Exception as e:
        log_all(logging.ERROR, f"{e}")
        raise e
