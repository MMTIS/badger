"""
Add TrainNumbers to the Trenitalia feed using ServiceJourney/Name

This fix gives every Trenitalia journey a real TrainNumber: it reads each ServiceJourney's
number from its name, creates one first-class TrainNumber per distinct (deduplicated) number,
and points the journey at it via `trainNumbers`. This aligns with the Austrian and Swiss datasets.

Usage:
    uv run python -m fix.trenitalia.add_train_numbers path/to/it.lmdb
"""

import logging
import re
from pathlib import Path
from typing import Optional

from domain.netex.model import (
    ServiceJourney,
    TextType,
    TrainNumber,
    TrainNumberRef,
    TrainNumberRefsRelStructure,
)
from storage.mdbx.core.implementation import MdbxStorage
from storage.mdbx.core.references import resolve_embeddings
from utils.aux_logging import log_all, prepare_logger

_DIGITS = re.compile(r"\d+")


def normalize_train_number(num: Optional[str]) -> Optional[str]:
    if num is None:
        return None
    n = num.strip()
    m = _DIGITS.search(n)
    return str(int(m.group())) if m else None


def train_number_for_journey(sj: ServiceJourney) -> Optional[str]:
    if sj.name is not None and sj.name.content:
        first = sj.name.content[0]
        text = first.value if isinstance(first, TextType) else str(first)
        return normalize_train_number(text)
    return None


def _tn_id(num: str) -> str:
    return f"IT:TrainNumber:{num}"


def add_train_numbers(db: MdbxStorage) -> None:
    # Read pass: journeys that don't yet carry a train number
    with db.env.ro_transaction() as rtx:
        todo: list[tuple[ServiceJourney, str]] = []
        for sj in db.iter_only_objects(rtx, ServiceJourney):
            if sj.train_numbers is not None:
                continue
            num = train_number_for_journey(sj)
            if not num:
                continue
            todo.append((sj, num))

    # One first-class TrainNumber per distinct number (many journeys share a number).
    train_numbers: dict[str, TrainNumber] = {}
    for sj, num in todo:
        if num not in train_numbers:
            train_numbers[num] = TrainNumber(id=_tn_id(num), version=sj.version, for_advertisement=num)

    log_all(logging.INFO, f"[add_train_numbers] {len(todo)} Trenitalia journeys -> " f"{len(train_numbers)} distinct train numbers")

    with db.env.rw_transaction() as wtx:
        db.insert_any_object_on_queue(wtx, train_numbers.values())
        for sj, num in todo:
            train_number = train_numbers[num]
            sj.train_numbers = TrainNumberRefsRelStructure(train_number_ref=[TrainNumberRef(ref=train_number.id, version=train_number.version)])
        db.insert_any_object_on_queue(wtx, [sj for sj, _ in todo])
        wtx.commit()

    resolve_embeddings(db)
    log_all(logging.INFO, f"[add_train_numbers] done: added {len(train_numbers)} TrainNumbers, " f"linked {len(todo)} Trenitalia journeys")


def main(source_database_file: str) -> None:
    with MdbxStorage(Path(source_database_file), readonly=False) as db:
        return add_train_numbers(db)


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(
        description="Add NeTEx TrainNumber objects to the Trenitalia journeys (read each "
        "journey's number from its name) so it matches the Austrian/Swiss feeds."
    )
    parser.add_argument("source", type=str, help="Trenitalia NeTEx mdbx file")
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.source)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
