import logging
from pathlib import Path

from storage.mdbx.core.implementation import MdbxStorage
from utils.aux_logging import log_all, prepare_logger
from storage.mdbx.core.references import resolve, resolve_embeddings_index
from transformers.multilingualstring import TransformMultilingualString


def multilingualstringv1_db_to_db(source_database_files: set[Path], target_database_file: Path) -> None:
    with MdbxStorage(target_database_file, readonly=False) as target_db:
        with target_db.env.rw_transaction() as txn_write:
            for source_database_file in source_database_files:
                with MdbxStorage(source_database_file, readonly=True) as source_db:
                    with source_db.env.ro_transaction() as txn_read:
                        target_db.insert_any_object_on_queue(txn_write, TransformMultilingualString.iter_to_v1(source_db, txn_read, only_changed=False))
            txn_write.commit()

        resolve(target_db)
        resolve_embeddings_index(target_db)


def main(source: list[str], target: str) -> None:
    source_paths: set[Path] = set()
    for s in source:
        source_path = Path(s)
        if not source_path.exists():
            log_all(logging.ERROR, f"{source_path} does not exist.")
        else:
            source_paths.add(source_path)

    else:
        multilingualstringv1_db_to_db(source_paths, Path(target))


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Transform the MultilingualStrings in the input to NeTEx 1.x")
    parser.add_argument("--source", nargs='+', default=[], help="mdbx file(s) to use as input of the transformation.")
    parser.add_argument(
        "--target",
        type=str,
        help="mdbx file to overwrite and store contents of the transformation.",
    )
    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    try:
        main(args.source, args.target)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
