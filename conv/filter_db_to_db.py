import logging
from collections import defaultdict
from typing import TypeVar, Any

from netex import (
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
from netexio.attributes import update_attr
from netexio.database import Database
from netexio.dbaccess import recursive_resolve, load_local, load_referencing_inwards
from netexio.pickleserializer import MyPickleSerializer
from transformers.references import split_path
from utils.profiles import EPIP_CLASSES
from utils.aux_logging import log_all, prepare_logger

Tid = TypeVar("Tid", bound=EntityStructure)


def main(source_database_file: str, target_database_file: str, object_type: str, object_filter: str) -> None:
    with Database(source_database_file, serializer=MyPickleSerializer(compression=True), readonly=True) as db_read:
        filter_set = {Route, ServiceJourneyPattern, Line, ScheduledStopPoint, PassengerStopAssignment, DayType, DayTypeAssignment, UicOperatingPeriod}
        filter_set.add(db_read.get_class_by_name(object_type))

        objs: list[Any] = load_local(db_read, db_read.get_class_by_name(object_type), filter_id=object_filter)

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

        # TODO: Once removed the export should have less elements in the GeneralFrame, and only the relevant extra elements
        for (parent_id, parent_version, parent_klass), paths in result.items():
            print(parent_klass, parent_id, parent_version, path)
            obj = db_write.get_single(parent_klass, parent_id, parent_version)
            for path in paths:
                split = split_path(path)
                update_attr(obj, split, None)
            db_write.insert_one_object(obj)
            # TODO: db_write.delete_embedding


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Filter the input by an object")
    parser.add_argument("source", type=str, help="lmdb file to use as input of the transformation.")

    parser.add_argument('object_type', type=str, help='The NeTEx object type to filter, for example ServiceJourney')
    parser.add_argument('object_filter', type=str, help='The object filter to apply.')

    parser.add_argument(
        "target",
        type=str,
        help="lmdb file to overwrite and store contents of the transformation.",
    )

    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.source, args.target, args.object_type, args.object_filter)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
