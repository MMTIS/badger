"""
Fix ScheduledStopPoint IDs to match SIRI feed format.

Transforms IDs from:  IT:ITH1:ScheduledStopPoint:it-22021-7010-51-32073:
to:                   IT:ITH10:ScheduledStopPoint:7010:51:32073

This is needed so that NeTEx and SIRI feeds reference the same stops.

It is a mystery to me why this cannot be fixed at the source.
"""

import dataclasses
import logging
import re
from collections.abc import Generator
from pathlib import Path
from typing import Any

from domain.netex.model import (
    EntityStructure,
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
from domain.netex.services.recursive_attributes import recursive_attributes
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import log_all, prepare_logger

_PATTERN = re.compile(r'^.*:ScheduledStopPoint:it-22021-(.+):$')


def _new_id(old_id: str) -> str | None:
    m = _PATTERN.match(old_id)
    if m:
        return 'IT:ITH10:ScheduledStopPoint:' + m.group(1).replace('-', ':')
    return None


# Object types that may transitively contain ScheduledStopPointRef or RoutePointRef.
_REF_BEARING_TYPES: list[type[EntityStructure]] = [
    ServiceJourneyPattern,
    ServiceLink,
    TimingLink,
    PassengerStopAssignment,
    Route,
    RoutePoint,
]

# Classes that are rewritten explicitly rather than copied verbatim.
_MODIFIED_TYPES: set[type[EntityStructure]] = {*_REF_BEARING_TYPES, ScheduledStopPoint}


def _iter_updated_objects(
    db: MdbxStorage, txn: Any,
) -> Generator[Any, None, None]:
    for cls in _REF_BEARING_TYPES:
        for obj in db.iter_only_objects(txn, cls):
            for ref, _path in recursive_attributes(obj, []):
                if isinstance(ref, (ScheduledStopPointRef, RoutePointRef)):
                    new_ref = _new_id(ref.ref)
                    if new_ref is not None:
                        ref.ref = new_ref
            yield obj


def _iter_ssps(
    db: MdbxStorage, txn: Any,
) -> Generator[ScheduledStopPoint, None, None]:
    for ssp in db.iter_only_objects(txn, ScheduledStopPoint):
        new_id = _new_id(ssp.id)
        yield dataclasses.replace(ssp, id=new_id) if new_id is not None else ssp


def fix_ssp_ids(source_database: Path, target_database: Path) -> None:
    with MdbxStorage(source_database) as source_db:
        with source_db.env.ro_transaction() as txn_read:
            with MdbxStorage(target_database, readonly=False) as target_db:
                with target_db.env.rw_transaction() as txn_write:
                    for clazz in source_db.db_names_iter(txn_read):
                        if clazz not in _MODIFIED_TYPES:
                            source_db.copy_map(txn_read, target_db, txn_write, clazz)

                    target_db.insert_any_object_on_queue(txn_write, _iter_updated_objects(source_db, txn_read))
                    target_db.insert_any_object_on_queue(txn_write, _iter_ssps(source_db, txn_read))
                    txn_write.commit()


def main(source_database_file: str, target_database_file: str) -> None:
    fix_ssp_ids(Path(source_database_file), Path(target_database_file))


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Fix ScheduledStopPoint IDs to SIRI format")
    parser.add_argument("source", type=str, help="mdbx file to use as input of the transformation")
    parser.add_argument("target", type=str, help="mdbx file to write the transformed contents to")
    parser.add_argument("--log_file", type=str, required=False, help="log file path")
    args = parser.parse_args()
    prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.source, args.target)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
