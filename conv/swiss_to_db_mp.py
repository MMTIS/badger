import logging
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from storage.mdbx.core.implementation import MdbxStorage
from storage.mdbx.core.implementation_mp import MdbxStorageMP
from storage.mdbx.core.references import resolve, resolve_embeddings
from storage.lxml.core.insert import get_interesting_classes, insert_database
from storage.lxml.core.implementation import XmlStorage
from utils.aux_logging import log_all, prepare_logger
from domain.netex.services.profiles import SWISS_CLASSES
import multiprocessing as mp

n_proc = 10


def first_pattern_index(name: str, patterns: list[str]) -> tuple[int, str] | None:
    """
    Return (pattern_index, matched_token) if any token appears in `name`,
    otherwise None. Matching is substring-based and case-sensitive.
    """

    for i, token in enumerate(patterns):
        if token in name:
            return i, token
    return None


def parse_and_enqueue(database: str, queue: mp.Queue, filename: str, sub_filename: str):
    """Runs in a subprocess: parse XML and enqueue objects."""

    import zipfile
    from storage.mdbx.core.implementation_queue import MdbxStorageQueue
    from storage.lxml.core.insert import get_interesting_classes, insert_database

    print(f"[{mp.current_process().name}] Parsing {sub_filename}")

    with zipfile.ZipFile(filename) as zip_file:
        with zip_file.open(sub_filename) as sub_file:
            interesting_classes = get_interesting_classes(SWISS_CLASSES)
            with MdbxStorageQueue(Path(database), queue) as storage:
                print(sub_filename)
                insert_database(storage, interesting_classes, sub_file)


def main(filename: str, database: str, clean_database: bool = True) -> None:
    if not filename.endswith('.zip'):
        return

    interesting_classes = get_interesting_classes(SWISS_CLASSES)
    xml_storage = XmlStorage(Path(filename))
    # Stap 1: alleen bestandsnamen ophalen
    all_names = xml_storage.list_netex_files()

    """
    with LmdbStorage(Path(database), readonly=False, initial_size=initial_size) as storage:
        if clean_database:
            print("Is cleaned!")
            storage.clean()

        patterns = [
            "_RESOURCE_",
            "_SITE_",
            "_SERVICE_",
            "_SERVICECALENDAR_"
        ]

        # Stap 2: filter + sort op patronen
        bucket: list[tuple[int, str]] = []
        for real_filename in all_names:
            match = first_pattern_index(real_filename, patterns)
            if match:
                prio, token = match
                # eventueel check of sub_file == real_filename in jouw context
                bucket.append((prio, real_filename))

        bucket.sort(key=lambda x: (x[0], x[1]))

        zip_file = zipfile.ZipFile(filename)
        for _, sub_filename in bucket:
            print(sub_filename)
            sub_file = zip_file.open(sub_filename)
            insert_database(storage, interesting_classes, sub_file)
    """

    with MdbxStorageMP(Path(database), readonly=False) as storage:
        with ProcessPoolExecutor(max_workers=n_proc, mp_context=storage.ctx) as executor:
            futures = []
            for sub_filename in all_names:
                if "_RESOURCE_" in sub_filename or '_SITE_' in sub_filename or '_SERVICECALENDAR_' in sub_filename:
                    futures.append(executor.submit(parse_and_enqueue, database, storage.queue, filename, sub_filename))

            for future in as_completed(futures):
                _res = future.result()

            futures = []
            for sub_filename in all_names:
                if "_SERVICE_" in sub_filename:
                    futures.append(executor.submit(parse_and_enqueue, database, storage.queue, filename, sub_filename))

            for future in as_completed(futures):
                _res = future.result()

            futures = []
            for sub_filename in all_names:
                if "_TIMETABLE_" in sub_filename:
                    futures.append(executor.submit(parse_and_enqueue, database, storage.queue, filename, sub_filename))

            for future in as_completed(futures):
                _res = future.result()

    with MdbxStorage(Path(database), readonly=False) as storage:
        zip_file = zipfile.ZipFile(filename)
        for sub_filename in all_names:
            if '_COMMON_' in sub_filename:
                print(sub_filename)
                sub_file = zip_file.open(sub_filename)
                insert_database(storage, interesting_classes, sub_file)
        resolve(storage)
        resolve_embeddings(storage)


if __name__ == '__main__':
    import argparse
    import traceback

    argument_parser = argparse.ArgumentParser(description='Import a Swiss NeTEx ZIP archive into lmdb')
    argument_parser.add_argument('swiss_zip_file', type=str, help='The NeTEx zip file')
    argument_parser.add_argument('database', type=str, help='The lmdb to be overwritten with the NeTEx context')
    argument_parser.add_argument('--clean_database', action="store_true", help='Clean the current file', default=True)
    argument_parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = argument_parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.swiss_zip_file, args.database, args.clean_database)
    except Exception as e:
        log_all(logging.ERROR, traceback.format_exc())
        raise e
