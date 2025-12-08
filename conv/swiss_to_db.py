import logging
import zipfile
from pathlib import Path

from storage.lxml.core.implementation import XmlStorage
from storage.lxml.core.insert import get_interesting_classes, insert_database
from storage.mdbx.core.implementation import MdbxStorage
from storage.mdbx.core.references import resolve, resolve_embeddings
from utils.aux_logging import log_all, prepare_logger
from domain.netex.services.profiles import SWISS_CLASSES


def first_pattern_index(name: str) -> tuple[int, str] | None:
    """
    Return (pattern_index, matched_token) if any token appears in `name`,
    otherwise None. Matching is substring-based and case-sensitive.
    """

    patterns = [
        "_RESOURCE_",
        "_SITE_",
        "_SERVICE_",
        "_SERVICECALENDAR_",
        "_TIMETABLE_",
        "_COMMON_",
    ]

    for i, token in enumerate(patterns):
        if token in name:
            return i, token
    return None


def swiss_to_db(source: Path, target: Path, clean_database: bool = True) -> None:
    with MdbxStorage(Path(target), readonly=False) as storage:
        """
        if clean_database:
            print("Is cleaned!")
            storage.clean()
        """

        interesting_classes = get_interesting_classes(SWISS_CLASSES)
        xml_storage = XmlStorage(source)

        # Stap 1: alleen bestandsnamen ophalen
        all_names = xml_storage.list_netex_files()

        # Stap 2: filter + sort op patronen
        bucket: list[tuple[int, str]] = []
        for real_filename in all_names:
            match = first_pattern_index(real_filename)
            if match:
                prio, token = match
                # eventueel check of sub_file == real_filename in jouw context
                bucket.append((prio, real_filename))

        bucket.sort(key=lambda x: (x[0], x[1]))

        zip_file = zipfile.ZipFile(source)
        for _, sub_filename in bucket:
            print(sub_filename)
            sub_file = zip_file.open(sub_filename)
            insert_database(storage, interesting_classes, sub_file)

        resolve(storage)
        resolve_embeddings(storage)


def main(source: str, target: str, clean_database: bool = True) -> None:
    if not source.endswith('.zip'):
        log_all(logging.ERROR, f"{source} does not end with .zip")
        return

    source_path = Path(source)
    if not source_path.exists():
        log_all(logging.ERROR, f"{source_path} does not exist.")

    else:
        swiss_to_db(source_path, Path(target), clean_database)


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
