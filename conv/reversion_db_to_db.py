import logging
from pathlib import Path
from typing import Generator

from mdbx.mdbx import TXN

from domain.netex import EntityStructure, Line
from domain.netex.services.model_typing import Tid
from domain.netex.services.recursive_attributes import recursive_attributes
from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import log_all, prepare_logger

def reversion_object(deserialized: EntityStructure, version: str | None) -> EntityStructure:
    if version is None:
        return deserialized

    if deserialized.__class__ == Line:
        pass
    for obj, path in recursive_attributes(deserialized, []):
        if hasattr(obj, "version"):
            obj.version = version
        if hasattr(obj, "data_source_ref"):
            obj.data_source_ref = None
        if hasattr(obj, "responsibility_set_ref"):
            obj.responsibility_set_ref = None

    if hasattr(deserialized, "version"):
        deserialized.version = version
    if hasattr(deserialized, "data_source_ref"):
        deserialized.data_source_ref = None
    if hasattr(deserialized, "responsibility_set_ref"):
        deserialized.responsibility_set_ref = None

    return deserialized

def reversion_update(db: MdbxStorage, txn: TXN, version: int | None) -> Generator[Tid, None, None]:
    # Within this function we are reading and writing towards the target database.
    # This effectively means that if we would need to resize for whatever reason,
    # we cannot hold the cursor since access has to be disabled.
    # We will first validate that we do have remaining capacity.

    clazz: EntityStructure
    for clazz in db.db_names(txn).values():
        obj: Tid
        for _key, obj in db.iter_objects(txn, clazz):
            yield reversion_object(obj, version)

def reversion_db_to_db(source_database_files: set[Path], target_database_file: Path, version: int | None) -> None:
    version = str(version) if version else None

    with MdbxStorage(target_database_file, readonly=False) as target_db:
        with target_db.env.rw_transaction() as txn_write:
            for source_database_file in source_database_files:
                with MdbxStorage(source_database_file, readonly=True) as source_db:
                    with source_db.env.ro_transaction() as txn_read:
                        target_db.insert_any_object_on_queue(txn_write, reversion_update(source_db, txn_read, version))
            txn_write.commit()

def main(source: list[str], target: str, version: int) -> None:
    source_paths: set[Path] = set()
    for s in source:
        source_path = Path(s)
        if not source_path.exists():
            log_all(logging.ERROR, f"{source_path} does not exist.")
        else:
            source_paths.add(source_path)

    else:
        reversion_db_to_db(source_paths, Path(target), version)


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Transform the input and set all version attributes to the same version")
    parser.add_argument("--source", nargs='+', default=[], help="mdbx file(s) to use as input of the transformation.")
    parser.add_argument(
        "--target",
        type=str,
        help="mdbx file to overwrite and store contents of the transformation.",
    )
    parser.add_argument("--version", nargs='?', type=int, help="optional version to set, if empty all sources are copied untouched.")
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    try:
        main(args.source, args.target, args.version)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
