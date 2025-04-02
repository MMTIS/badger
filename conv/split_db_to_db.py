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
    UicOperatingPeriod, PublicationDelivery, TypeOfFrameRef,
)
from netexio.attributes import update_attr
from netexio.database import Database
from netexio.dbaccess import recursive_resolve, load_referencing_inwards, load_generator
from netexio.pickleserializer import MyPickleSerializer
from netexio.xml import export_publication_delivery_xml
from transformers.epip import export_epip_network_offer
from transformers.references import split_path
from utils.profiles import EPIP_CLASSES
from utils.aux_logging import log_all, prepare_logger

Tid = TypeVar("Tid", bound=EntityStructure)


def main(source_database_file: str, target_database_file: str, object_type: str) -> None:
    with Database(source_database_file, serializer=MyPickleSerializer(compression=True), readonly=True) as db_read:
        # filter_set = {Route, ServiceJourneyPattern, Line, ScheduledStopPoint, PassengerStopAssignment, DayType, UicOperatingPeriod}
        filter_set = set({ServiceJourneyPattern, PassengerStopAssignment, DayType})
        filter_set.add(db_read.get_class_by_name(object_type))

        split_by: Tid
        for split_by in load_generator(db_read, db_read.get_class_by_name(object_type)):
            assert split_by.id is not None

            a, b = target_database_file.split('.lmdb')
            new_target_database_file = a + '_' + split_by.id.replace(':', '_') + '.lmdb'
            new_xml_file = a + '_' + split_by.id.replace(':', '_') + '.xml.gz'

            with Database(new_target_database_file, serializer=MyPickleSerializer(compression=True), readonly=False) as db_write:

                # TODO: This is memory intensive, ideally we only keep what we have resolved and yield the objects to write them into the database
                resolved: list[Any] = []
                recursive_resolve(db_read, split_by, resolved, split_by.id, filter_set)

                for obj in resolved:
                    db_write.insert_one_object(obj)

                result: dict[tuple[str, str, Any], list[str]] = defaultdict(list)

                db_write.block_until_done()

                # TODO: For now EPIP
                # TODO: It seems that the ValueSet for some reason removes BISON:TypeOfResponsibilityRole:financing
                removable_classes = db_write.tables() - EPIP_CLASSES
                for removable_class in removable_classes:
                    for parent_id, parent_version, parent_class, path in load_referencing_inwards(db_write, removable_class):
                        parent_klass: type[Any] = db_write.get_class_by_name(parent_class)  # TODO: refactor at load_referencing_*
                        if parent_klass in EPIP_CLASSES:
                            # Aggregate all parent_ids, so we prevent concurrency issues, and the cost of deserialisation and serialisation
                            key = (parent_id, parent_version, parent_klass)
                            result[key].append(path)
                            # print(removable_class, key, path)

                # TODO: Once removed the export should have less elements in the GeneralFrame, and only the relevant extra elements
                for key, paths in result.items():
                    parent_id, parent_version, parent_klass = key
                    obj = db_write.get_single(parent_klass, parent_id, parent_version)
                    if obj:
                        for path in paths:
                            split = split_path(path)
                            update_attr(obj, split, None)

                        db_write.insert_one_object(obj, delete_embedding=True)

                db_write.block_until_done()

                publication_delivery: PublicationDelivery = export_epip_network_offer(db_write, composite_frame_id=split_by.id, type_of_frame_ref=TypeOfFrameRef(ref='epip:EU_PI_LINE_OFFER', version_ref='1.0'))
                export_publication_delivery_xml(publication_delivery, new_xml_file)
                print(new_xml_file)

                break


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Filter the input by an object")
    parser.add_argument("source", type=str, help="lmdb file to use as input of the transformation.")

    parser.add_argument('object_type', type=str, help='The NeTEx object type to filter, for example ServiceJourney')

    parser.add_argument(
        "target",
        type=str,
        help="lmdb file to overwrite and store contents of the transformation.",
    )

    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.source, args.target, args.object_type)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
