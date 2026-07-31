import logging
from functools import partial
from pathlib import Path
from typing import Any, cast
from collections.abc import Callable, Generator
from mdbx.mdbx import TXN
from domain.utils import get_object_name


from domain.netex.model import (
    Route,
    ServiceJourneyPattern,
    ServiceJourneyInterchange,
    Line,
    Operator,
    PassengerStopAssignment,
    ScheduledStopPoint,
    ServiceJourney,
    EntityStructure,
    DayTypeAssignment,
    NoticeAssignment,
    AllPublicTransportModesEnumeration,
    StopPlace,
    SiteConnection,
)

# from domain.netex.services.recursive_attributes import recursive_attributes
# from old.netexio.dbaccess import recursive_resolve
from storage.mdbx.core.implementation import MdbxStorage

# from netexio.attributes import update_attr
# from netexio.database import Database
# from netexio.dbaccess import recursive_resolve, load_local, load_referencing_inwards
# from netexio.pickleserializer import MyPickleSerializer
# from transformers.references import split_path

# from utils.profiles import EPIP_CLASSES
from utils.aux_logging import log_all, prepare_logger

import re
from storage.mdbx.core.references import resolve, resolve_embeddings_index

_TOKEN_RE = re.compile(r"([^.[]+)|\[(\d*|\*)\]")


def safe_attrgetter(path: str, default: Any = None) -> Callable[[object], Any]:
    """
    Supports:

        id
        name.value
        quays[0].id
        quays[].id
        quays[*].id
        stop_places[].quays[].id

    Missing attributes return `default`.
    """

    operations: list[str | int | None] = []

    for match in _TOKEN_RE.finditer(path):
        attr, index = match.groups()

        if attr is not None:
            operations.append(attr)
        elif index in ("", "*"):
            operations.append(None)  # wildcard
        else:
            operations.append(int(index))

    def apply(obj: Any, pos: int) -> Any:
        if obj is default:
            return default

        if pos == len(operations):
            return [obj]

        op = operations[pos]

        try:
            if op is None:
                # Wildcard
                if obj is None:
                    return default

                result = []

                for item in obj:
                    value = apply(item, pos + 1)

                    if value is default:
                        continue

                    if isinstance(value, list):
                        result.extend(value)
                    else:
                        result.append(value)

                return result if result else default

            elif isinstance(op, int):
                return apply(obj[op], pos + 1)

            else:
                if isinstance(obj, dict):
                    return apply(obj.get(op, default), pos + 1)

                return apply(getattr(obj, op), pos + 1)

        except (AttributeError, IndexError, KeyError, TypeError):
            return default

    return lambda obj: apply(obj, 0)


def id_filter(db_read: MdbxStorage, txn: TXN, clazz: type[EntityStructure], object_filters: set[str]) -> Generator[tuple[bytes, EntityStructure], None, None]:
    for object_filter in object_filters:
        pair = db_read.load_object_by_id_version(txn, object_filter, clazz)
        if pair is not None:
            yield pair


def attribute_filter(
    db_read: MdbxStorage,
    txn: TXN,
    clazz: type[EntityStructure],
    getter: Callable[[object], Any],
    allowed_values: set[str],
) -> Generator[tuple[bytes, EntityStructure], None, None]:
    for key, obj in db_read.iter_objects(txn, clazz):
        if not set([str(x) for x in getter(obj)]).isdisjoint(allowed_values):
            # I don't think this should belong here...
            this_clazz_idx = db_read.clazz_idx[clazz]
            full_key = key + this_clazz_idx.ljust(4, b'\x00')
            yield full_key, obj


def custom_filter(db_read: MdbxStorage, txn: TXN) -> Generator[tuple[bytes, EntityStructure], None, None]:
    obj: Line
    for key, obj in db_read.iter_objects(txn, Line):
        if obj.authority_ref and obj.authority_ref.ref == 'NL:DOVA:Authority:LMB' and obj.transport_mode == AllPublicTransportModesEnumeration.BUS:
            this_clazz_idx = db_read.clazz_idx[obj.__class__]
            full_key = key + this_clazz_idx.ljust(4, b'\x00')
            yield full_key, obj


def filter_db_to_db(
    source_database_file: Path,
    target_database_file: Path,
    filter_function: Callable[
        [MdbxStorage, TXN],
        Generator[tuple[bytes, EntityStructure], None, None],
    ],
    inward_classes: set[type[EntityStructure]],
    conditional_inward_classes: set[tuple[type[EntityStructure], type[EntityStructure]]],
) -> None:
    with MdbxStorage(source_database_file, readonly=False) as db_write:
        # Assure we have a inward index.
        resolve(db_write)
        resolve_embeddings_index(db_write)
        with db_write.env.rw_transaction() as txn:
            db_write._index_references_inwards(txn, force=True)
            txn.commit()

    with MdbxStorage(source_database_file) as db_read:
        # Assure we have a inward index.
        with db_read.env.rw_transaction() as txn:
            db_read._index_references_inwards(txn, force=True)
            txn.commit()

        with db_read.env.ro_transaction() as txn:
            visited: set[bytes] = set()
            full_keys = [full_key for full_key, obj in filter_function(db_read, txn)]
            with MdbxStorage(target_database_file, readonly=False) as db_write:

                with db_write.env.rw_transaction() as txn_write:
                    db_write.insert_any_object_on_queue(
                        txn_write,
                        db_read.load_references_by_object_values_dfs(txn, full_keys, inward_classes, conditional_inward_classes, visited),
                    )
                    txn_write.commit()

                resolve(db_write)
                resolve_embeddings_index(db_write)

    """
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
                parent_clazz: type[Any] = db_write.get_class_by_name(parent_class)  # TODO: refactor at load_referencing_*
                if parent_clazz in EPIP_CLASSES:
                    # Aggregate all parent_ids, so we prevent concurrency issues, and the cost of deserialisation and serialisation
                    key = (parent_id, parent_version, parent_clazz)
                    result[key].append(path)
                    print("REMOVABLE", removable_class, key, path)

        # TODO: Once removed the export should have less elements in the GeneralFrame, and only the relevant extra elements
        for key, paths in result.items():
            parent_id, parent_version, parent_clazz = key
            print("1", parent_clazz, parent_id, parent_version, path)
            obj = db_write.get_single(parent_clazz, parent_id, parent_version)
            for path in paths:
                split = split_path(path)
                update_attr(obj, split, None)

            db_write.insert_one_object(obj)
    """


filter_templates: dict[type[EntityStructure], dict[str, list[tuple[type[EntityStructure], type[EntityStructure]]] | list[type[EntityStructure]]]] = {
    ServiceJourney: {
        "conditional_inward_classes": [
            (PassengerStopAssignment, ScheduledStopPoint),
        ],
        "inward_classes": [DayTypeAssignment, NoticeAssignment],
    },
    ServiceJourneyInterchange: {
        "conditional_inward_classes": [
            (PassengerStopAssignment, ScheduledStopPoint),
        ],
        "inward_classes": [DayTypeAssignment, NoticeAssignment],
    },
    Line: {
        "conditional_inward_classes": [
            (Route, Line),
            (Line, Route),
            (Route, ServiceJourneyPattern),
            (ServiceJourneyPattern, ServiceJourney),
            (PassengerStopAssignment, ScheduledStopPoint),
        ],
        "inward_classes": [DayTypeAssignment, NoticeAssignment],
    },
    Operator: {
        "conditional_inward_classes": [
            (Route, Line),
            (Line, Route),
            (Operator, Line),
            (Line, Operator),
            (Route, ServiceJourneyPattern),
            (ServiceJourneyPattern, ServiceJourney),
            (PassengerStopAssignment, ScheduledStopPoint),
        ],
        "inward_classes": [DayTypeAssignment, NoticeAssignment],
    },
    StopPlace: {
        "conditional_inward_classes": [
            (SiteConnection, StopPlace),
            (StopPlace, SiteConnection),
        ]
    },
}


def main(
    source: str,
    target: str,
    object_type: str,
    attributes: list[str],
    inwards_object_types: list[str] | None,
    conditional_inward_object_types: list[list[str]] | None,
    use_template: bool,
) -> None:
    source_path = Path(source)
    target_path = Path(target)
    clazz: type[EntityStructure] | None

    # handling Stuff coming from script runner
    if isinstance(inwards_object_types,str) and inwards_object_types=="None":
        inwards_object_types=None
    if isinstance(conditional_inward_object_types,str) and conditional_inward_object_types=="None":
        conditional_inward_object_types=None
    if not source_path.exists():
        log_all(logging.ERROR, f"{source_path} does not exist.")

    else:
        with MdbxStorage(source_path) as db_read:
            # TODO: would be a good idea to have this done better.
            clazz = db_read.serializer.class_by_name(object_type)
            if clazz is None:
                log_all(logging.ERROR, "{object_type} does not exist.")
                return

            inward_classes: set[type[EntityStructure]] = set({})
            conditional_inward_classes: set[tuple[type[EntityStructure], type[EntityStructure]]] = set()

            if use_template:
                for cic in filter_templates.get(clazz, {}).get("conditional_inward_classes", []):
                    conditional_inward_classes.add(cast(tuple[type[EntityStructure], type[EntityStructure]], cic))

                for ic in filter_templates.get(clazz, {}).get("inward_classes", []):
                    inward_classes.add(cast(type[EntityStructure], ic))

            if conditional_inward_object_types:
                for referencing_type, inwards_object_type in conditional_inward_object_types:
                    referencing_type_clazz = db_read.serializer.class_by_name(referencing_type)
                    if referencing_type_clazz is None:
                        log_all(logging.ERROR, "{referencing_type} does not exist. {referencing_type} {inwards_object_type} not added.")
                        continue

                    inwards_object_type_clazz = db_read.serializer.class_by_name(inwards_object_type)
                    if inwards_object_type_clazz is None:
                        log_all(logging.ERROR, "{referencing_type} does not exist. {referencing_type} {inwards_object_type} not added.")
                        continue

                    conditional_inward_classes.add((referencing_type_clazz, inwards_object_type_clazz))

            log_all(logging.INFO, f"inward_classes: {', '.join([get_object_name(c) for c in inward_classes])}")
            log_all(
                logging.INFO, f"conditional_inward_classes: {', '.join([get_object_name(r) + '-' + get_object_name(i) for r, i in conditional_inward_classes])}"
            )

            if inwards_object_types:
                for inwards_object_type in inwards_object_types:
                    inwards_object_type_clazz = db_read.serializer.class_by_name(inwards_object_type)
                    if inwards_object_type_clazz is None:
                        log_all(logging.ERROR, "{inwards_object_type_clazz} does not exist.")
                        continue

                    inward_classes.add(inwards_object_type_clazz)

        if attributes[0] == 'id':
            filter_db_to_db(
                source_path, target_path, partial(id_filter, clazz=clazz, object_filters=set(attributes[1:])), inward_classes, conditional_inward_classes
            )

        elif attributes[0] == 'custom':
            # TODO, fix argument
            filter_db_to_db(source_path, target_path, partial(custom_filter), inward_classes, conditional_inward_classes)

        elif attributes is not None:
            getter = safe_attrgetter(attributes[0], set())
            filter_db_to_db(
                source_path,
                target_path,
                partial(attribute_filter, clazz=clazz, getter=getter, allowed_values=set(attributes[1:])),
                inward_classes,
                conditional_inward_classes,
            )

        with MdbxStorage(target_path) as db_read:
            with db_read.env.ro_transaction() as txn:
                log_all(logging.DEBUG, f"{get_object_name(clazz)}: {db_read.count_objects(txn, clazz)}")
                if clazz != ServiceJourney:
                    log_all(logging.DEBUG, f"{get_object_name(ServiceJourney)}: {db_read.count_objects(txn, ServiceJourney)}")


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Filter the input by an object")
    parser.add_argument("source", type=str, help="MDBX file to use as input of the transformation.")

    parser.add_argument('object_type', type=str, help='The NeTEx object type to filter, for example ServiceJourney')

    parser.add_argument(
        "attribute",
        nargs=2,
        metavar=("ATTRIBUTE", "VALUE"),
        help="Filter on an attribute and its value.",
    )

    parser.add_argument(
        "target",
        type=str,
        help="MDBX file to overwrite and store contents of the transformation.",
    )

    parser.add_argument(
        '--use-template',
        action="store_true",
        help=f'Use predefined templates ({', '.join([get_object_name(c) for c in filter_templates.keys()])}) for relational selections ',
        default=False,
    )

    parser.add_argument("inwards_object_types", nargs="*", type=str, help="Optional list of additional object types to be inwards selected")

    parser.add_argument(
        "--conditional_inwards",
        nargs=2,
        action="append",
        metavar=("REFERENCING_TYPE", "INWARDS_OBJECT_TYPE"),
        help="Apply conditional inward resolving",
    )

    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    try:
        main(args.source, args.target, args.object_type, args.attribute, args.inwards_object_types, args.conditional_inwards, args.use_template)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
