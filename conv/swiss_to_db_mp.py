import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from storage.mdbx.core.implementation import MdbxStorage
from storage.mdbx.core.implementation_mp import MdbxStorageMP
from storage.mdbx.core.references import resolve, resolve_embeddings_index
from storage.lxml.core.implementation import XmlStorage
from utils.aux_logging import log_all, prepare_logger
from domain.netex.services.profiles import SWISS_CLASSES
import multiprocessing as mp

n_proc = 10


def parse_and_enqueue(target: Path, queue: mp.Queue, source: Path, sub_filename: str) -> None:
    """Runs in a subprocess: parse XML and enqueue objects."""

    import zipfile
    from storage.mdbx.core.implementation_queue import MdbxStorageQueue
    from storage.lxml.core.insert import get_interesting_classes, insert_database

    print(f"[{mp.current_process().name}] Parsing {sub_filename}")

    with zipfile.ZipFile(source) as zip_file:
        with zip_file.open(sub_filename) as sub_file:
            interesting_classes = get_interesting_classes(SWISS_CLASSES)
            with MdbxStorageQueue(target, queue) as storage:
                print(sub_filename)
                insert_database(storage, interesting_classes, sub_file)


def swiss_to_db_mp(source: Path, target: Path, clean_database: bool = True) -> None:
    xml_storage = XmlStorage(source)
    all_names = xml_storage.list_netex_files()

    with MdbxStorageMP(target, readonly=False) as storage:
        with ProcessPoolExecutor(max_workers=n_proc, mp_context=storage.ctx) as executor:
            futures = []
            for sub_filename in all_names:
                if "_RESOURCE_" in sub_filename or '_SITE_' in sub_filename or '_SERVICECALENDAR_' in sub_filename:
                    futures.append(executor.submit(parse_and_enqueue, target, storage.queue, source, sub_filename))

            for future in as_completed(futures):
                _res = future.result()

            futures = []
            for sub_filename in all_names:
                if "_SERVICE_" in sub_filename or '_COMMON_' in sub_filename:
                    futures.append(executor.submit(parse_and_enqueue, target, storage.queue, source, sub_filename))

            for future in as_completed(futures):
                _res = future.result()

            futures = []
            for sub_filename in all_names:
                if "_TIMETABLE_" in sub_filename:
                    futures.append(executor.submit(parse_and_enqueue, target, storage.queue, source, sub_filename))

            for future in as_completed(futures):
                _res = future.result()

    with MdbxStorage(target, readonly=False) as storage:
        resolve(storage)
        resolve_embeddings_index(storage)


def main(source: str, target: str, clean_database: bool = True) -> None:
    if not source.endswith('.zip'):
        log_all(logging.ERROR, f"{source} does not end with .zip")
        return

    source_path = Path(source)
    if not source_path.exists():
        log_all(logging.ERROR, f"{source_path} does not exist.")

    else:
        swiss_to_db_mp(source_path, Path(target), clean_database)


if __name__ == '__main__':
    import argparse
    import traceback

    argument_parser = argparse.ArgumentParser(description='Import a Swiss NeTEx ZIP archive into mdbx')
    argument_parser.add_argument('swiss_zip_file', type=str, help='The NeTEx zip file')
    argument_parser.add_argument('database', type=str, help='The mdbx to be overwritten with the NeTEx context')
    argument_parser.add_argument('--clean_database', action="store_true", help='Clean the current file', default=True)
    argument_parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = argument_parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.swiss_zip_file, args.database, args.clean_database)
    except Exception as e:
        log_all(logging.ERROR, traceback.format_exc())
        raise e
