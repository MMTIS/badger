import logging
from typing import TypeVar, Any

from netex import Route, ServiceJourneyPattern, Line, PassengerStopAssignment, ScheduledStopPoint, EntityStructure
from netexio.database import Database
from netexio.dbaccess import recursive_resolve, load_local
from netexio.pickleserializer import MyPickleSerializer
from utils.aux_logging import log_all, prepare_logger

Tid = TypeVar("Tid", bound=EntityStructure)


def main(source_database_file: str, target_database_file: str, object_type: str, object_filter: str) -> None:
    with Database(source_database_file, serializer=MyPickleSerializer(compression=True), readonly=True) as db_read:
        filter_set = {Route, ServiceJourneyPattern, Line, ScheduledStopPoint, PassengerStopAssignment}
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

            db_write.block_until_done()


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
