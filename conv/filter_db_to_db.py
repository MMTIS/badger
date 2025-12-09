import logging
from collections import defaultdict
from pathlib import Path
from typing import TypeVar, Any, Iterator, Generator

from domain.netex.model import (
    Route,
    ServiceJourneyPattern,
    Line,
    PassengerStopAssignment,
    ScheduledStopPoint,
    EntityStructure,
    DayTypeAssignment,
    DayType,
    UicOperatingPeriod,
)
from domain.netex.services.recursive_attributes import recursive_attributes
from old.netexio.dbaccess import recursive_resolve
from storage.mdbx.core.implementation import MdbxStorage, DB_ID_IDX

# from netexio.attributes import update_attr
# from netexio.database import Database
# from netexio.dbaccess import recursive_resolve, load_local, load_referencing_inwards
# from netexio.pickleserializer import MyPickleSerializer
from transformers.references import split_path

# from utils.profiles import EPIP_CLASSES
from utils.aux_logging import log_all, prepare_logger

Tid = TypeVar("Tid", bound=EntityStructure)


def filter_db_to_db(source_database_file: Path, target_database_file: Path, clazz: type[EntityStructure], object_filter: str) -> None:
    with MdbxStorage(source_database_file) as db_read:
        filter_set = {Route, ServiceJourneyPattern, Line, ScheduledStopPoint, PassengerStopAssignment, DayType, DayTypeAssignment, UicOperatingPeriod}
        filter_set.add(clazz)

        with db_read.env.ro_transaction() as txn:
            my_id = db_read.serializer.encode_key(object_filter, None, clazz, include_clazz=True)

            # First: check if the id already exists, then we must overwrite.
            db_id_idx = txn.open_map(name=DB_ID_IDX)
            full_key = db_id_idx.get(txn, my_id)

            obj = db_read.load_object_by_full_key(txn, full_key)

            with MdbxStorage(target_database_file, readonly=False) as db_write:
                with db_write.env.rw_transaction() as txn_write:
                    def only_values(test: Iterator[Tid]) -> Generator[Tid, None, None]:
                        for x,y in test:
                            yield y

                    db_write.insert_any_object_on_queue(txn_write, only_values(db_read.load_references_by_object(txn, obj)))

        with Database(target_database_file, serializer=MyPickleSerializer(compression=True), readonly=False) as db_write:
            # TODO: This is memory intensive, ideally we only keep what we have resolved and yield the objects to write them into the database
            resolved: list[Any] = []
            for obj in objs:
                assert obj.id is not None, "Object without id"
                recursive_resolve(db_read, obj, resolved, obj.id, filter_set)

            for obj in resolved:
                db_write.insert_one_object(obj)

    # TODO: It would be interesting to take the objects not being in the EPIP classes, remove the references from the objects that reference them.
    with Database(target_database_file, serializer=MyPickleSerializer(compression=True), readonly=False) as db_write:
        result: dict[tuple[str, str, Any], list[str]] = defaultdict(list)

        # TODO: For now EPIP
        removable_classes = db_write.tables() - EPIP_CLASSES
        for removable_class in removable_classes:
            for parent_id, parent_version, parent_class, path in load_referencing_inwards(db_write, removable_class):
                parent_klass: type[Any] = db_write.get_class_by_name(parent_class)  # TODO: refactor at load_referencing_*
                if parent_klass in EPIP_CLASSES:
                    # Aggregate all parent_ids, so we prevent concurrency issues, and the cost of deserialisation and serialisation
                    key = (parent_id, parent_version, parent_klass)
                    result[key].append(path)
                    print("REMOVABLE", removable_class, key, path)

        # TODO: Once removed the export should have less elements in the GeneralFrame, and only the relevant extra elements
        for key, paths in result.items():
            parent_id, parent_version, parent_klass = key
            print("1", parent_klass, parent_id, parent_version, path)
            obj = db_write.get_single(parent_klass, parent_id, parent_version)
            for path in paths:
                split = split_path(path)
                update_attr(obj, split, None)

            db_write.insert_one_object(obj)


def main(source: str, target: str, object_type: str, object_filter: str) -> None:
    source_path = Path(source)
    if not source_path.exists():
        log_all(logging.ERROR, f"{source_path} does not exist.")

    else:
        clazz: type[EntityStructure] | None
        with MdbxStorage(source_path) as db_read:
            clazz = db_read.idx_class.get(
                db_read.class_name_idx.get(object_type, None), None
            )
            if clazz is None:
                log_all(logging.ERROR, "{object_type} does not exist.")
                return

        filter_db_to_db(source_path, Path(target), clazz, object_filter)


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Filter the input by an object")
    parser.add_argument("source", type=str, help="MDBX file to use as input of the transformation.")

    parser.add_argument('object_type', type=str, help='The NeTEx object type to filter, for example ServiceJourney')
    parser.add_argument('object_filter', type=str, help='The object filter to apply.')

    parser.add_argument(
        "target",
        type=str,
        help="MDBX file to overwrite and store contents of the transformation.",
    )

    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    try:
        main(args.source, args.target, args.object_type, args.object_filter)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
