import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from storage.mdbx.core.implementation import MdbxStorage
from storage.mdbx.core.implementation_mp import MdbxStorageMP
from storage.mdbx.core.references import resolve, resolve_embeddings_index
from storage.lxml.core.implementation import XmlStorage
from utils.aux_logging import log_all, prepare_logger, log_flush
import multiprocessing as mp
import queue

n_proc = 10


def parse_and_enqueue(target: Path, queue: queue.Queue[list[tuple[bytes, bytes, bytes, tuple[bytes, ...]]] | None], source: Path, sub_filename: str) -> None:
    """Runs in a subprocess: parse XML and enqueue objects."""

    import zipfile
    from storage.mdbx.core.implementation_queue import MdbxStorageQueue
    from storage.lxml.core.insert import get_interesting_classes, insert_database

    # This essentially copies XmlStorage
    source_str = str(source)
    if source_str.endswith('.zip'):
        with zipfile.ZipFile(source) as zip_file:
            with zip_file.open(sub_filename) as sub_file:
                interesting_classes = get_interesting_classes()
                with MdbxStorageQueue(target, queue) as storage:
                    print(f"[{mp.current_process().name}] Parsing {sub_filename}")
                    insert_database(storage, interesting_classes, sub_file)
    elif source_str.endswith('.xml.gz'):
        from isal import igzip_threaded

        interesting_classes = get_interesting_classes()
        with MdbxStorageQueue(target, queue) as storage:
            print(f"[{mp.current_process().name}] Parsing {source}")
            sub_file = igzip_threaded.open(source, "rb", compresslevel=3, threads=3)
            insert_database(storage, interesting_classes, sub_file)
    elif source_str.endswith('.xml'):
        interesting_classes = get_interesting_classes()
        with MdbxStorageQueue(target, queue) as storage:
            print(f"[{mp.current_process().name}] Parsing {source}")
            sub_file = source.open("rb")
            insert_database(storage, interesting_classes, sub_file)


def netex_to_db_mp(filenames: list[Path], target: Path, clean_database: bool = True) -> None:

    fork_ctx = mp.get_context("fork")
    with ProcessPoolExecutor(max_workers=n_proc, mp_context=fork_ctx) as executor:
        with MdbxStorageMP(target, readonly=False) as storage:
            futures = []
            for source in filenames:
                xml_storage = XmlStorage(source)
                all_names = xml_storage.list_netex_files()

                for sub_filename in all_names:
                    futures.append(executor.submit(parse_and_enqueue, target, storage.queue, source, sub_filename))

                for future in as_completed(futures):
                    _res = future.result()

            storage.queue.put(None)

    with MdbxStorage(target, readonly=False) as storage:
        resolve(storage)
        resolve_embeddings_index(storage)


def main(filenames: list[str], target: str, clean_database: bool = True) -> None:
    # if filenames is not a list of str  => error
    if not (isinstance(filenames, list) and all(isinstance(item, str) for item in filenames)):
        log_all(logging.ERROR, 'filenames parameter must be a [] of file names.')
        log_flush()
        exit(1)

    paths: list[Path] = []
    for filename in filenames:
        path = Path(filename)
        if path in paths:
            log_all(logging.WARNING, f'{filename} is a duplicate.')

        else:
            if not path.exists():
                log_all(logging.WARNING, f'{filename} does not exist.')
            else:
                paths.append(path)

    if len(paths) == 0:
        exit(1)

    netex_to_db_mp(paths, Path(target), clean_database)


if __name__ == '__main__':
    import argparse
    import traceback

    argument_parser = argparse.ArgumentParser(description='Import a NeTEx NeTEx ZIP archive into mdbx')
    argument_parser.add_argument('netex', nargs='+', default=[], help='NeTEx files')
    argument_parser.add_argument('database', type=str, help='The mdbx to be overwritten with the NeTEx context')
    argument_parser.add_argument('--clean_database', action="store_true", help='Clean the current file', default=True)
    argument_parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = argument_parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.netex, args.database, args.clean_database)
    except Exception as e:
        log_all(logging.ERROR, traceback.format_exc())
        raise e
