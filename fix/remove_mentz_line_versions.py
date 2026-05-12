"""
Resolves the Mentz line versions into actual operating days

https://public.3.basecamp.com/p/fVFt3mGJiK52ewcsr8nFgu6o
"""

import logging
from collections import defaultdict
from pathlib import Path

from xsdata.models.datatype import XmlDateTime

from domain.netex.model import Line
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
    for key, values in sorted(lines.items()):
        if len(values) > 1:
            print(f"Duplicate line ID: {key}")
            for line in values:
                dates = [(fmt_dt(v.from_date), fmt_dt(v.to_date)) for v in line.validity_conditions_or_valid_between if hasattr(v, 'from_date')]
                print(f"  version={line.version} dates={dates}")


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
