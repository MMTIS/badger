import logging
from collections import defaultdict
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import TypeVar, Any, Iterator, Generator
from operator import attrgetter

from domain.netex import ResponsibilitySet
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
    NoticeAssignment,
)
# from domain.netex.services.recursive_attributes import recursive_attributes
# from old.netexio.dbaccess import recursive_resolve
from storage.mdbx.core.implementation import MdbxStorage, DB_ID_IDX

# from netexio.attributes import update_attr
# from netexio.database import Database
# from netexio.dbaccess import recursive_resolve, load_local, load_referencing_inwards
# from netexio.pickleserializer import MyPickleSerializer
# from transformers.references import split_path

# from utils.profiles import EPIP_CLASSES
from utils.aux_logging import log_all, prepare_logger

Tid = TypeVar("Tid", bound=EntityStructure)

import re
from collections.abc import Callable
from typing import Any
from storage.mdbx.core.references import resolve, resolve_embeddings, resolve_embeddings_index

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
            operations.append(None)          # wildcard
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

def id_filter(db_read: MdbxStorage, txn, clazz: type[EntityStructure], object_filters: set[str]) -> Generator[tuple[bytes, EntityStructure], None, None]:
    for object_filter in object_filters:
        pair = db_read.load_object_by_id_version(txn, object_filter, clazz)
        if pair:
            yield pair

def attribute_filter(
    db_read: MdbxStorage,
    txn,
    clazz: type[EntityStructure],
    getter: Callable[[EntityStructure], str],
    allowed_values: set[str],
) -> Generator[tuple[bytes, EntityStructure], None, None]:
    for key, obj in db_read.iter_objects(txn, clazz):
        attrs = set(getter(obj))
        if not set([str(x) for x in getter(obj)]).isdisjoint(allowed_values):
            # I don't think this should belong here...
            this_class_idx = db_read.class_idx[clazz]
            full_key = key + this_class_idx.ljust(4, b'\x00')
            yield full_key, obj

def filter_db_to_db(source_database_file: Path, target_database_file: Path, filter_function: callable, inward_classes: set[type[EntityStructure]], conditional_inward_classes: set[tuple[type[EntityStructure], type[EntityStructure]]]) -> None:
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
    """

def main(source: str, target: str, object_type: str, attributes: list[str], inwards_object_types: list[str], conditional_inward_object_types: list[str]) -> None:
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

            inward_classes: set[type[EntityStructure]] = {NoticeAssignment, PassengerStopAssignment, DayTypeAssignment}
            conditional_inward_classes: set[tuple[type[EntityStructure], type[EntityStructure]]] = {(Route, ServiceJourneyPattern), (PassengerStopAssignment, ScheduledStopPoint)}
            for inwards_object_type in inwards_object_types:
                idx = db_read.class_name_idx.get(inwards_object_type, None)
                if not idx:
                    log_all(logging.WARNING, f"{inwards_object_type} is not a (known) NeTEx class")
                else:
                    inward_classes.add(db_read.idx_class[idx])

        print(f'source_path: {source_path}')
        print(f'target_path: {Path(target)}')
        print(inward_classes)
        if attributes[0] == 'id':
            filter_db_to_db(source_path, Path(target), partial(id_filter, clazz=clazz, object_filters=set(attributes[1:])), inward_classes, conditional_inward_classes)
        elif attributes is not None:
            getter = safe_attrgetter(attributes[0], set())
            filter_db_to_db(source_path, Path(target), partial(attribute_filter, clazz=clazz, getter=getter, allowed_values=set(attributes[1:])), inward_classes, conditional_inward_classes)

            # if clazz == ResponsibilitySet and ResponsibilitySet in inward_classes:
            #    with MdbxStorage(Path(target)) as db_read:
            #        with db_read.env.ro_transaction() as txn:
            #            refs = [obj.id for obj in db_read.iter_only_objects(txn, ResponsibilitySet)]
            #            getter = safe_attrgetter("responsibility_set", set())
            #            filter_db_to_db(source_path, Path(target), partial(attribute_filter, clazz=Line, getter=getter, allowed_values=set(attributes[1:])), inward_classes)

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

    parser.add_argument('object_type', type=str, help='The NeTEx object type to filter, for example ServiceJourney')
    parser.add_argument('object_filter', type=str, help='The object filter to apply.')

    parser.add_argument(
    "inwards_object_types",
        nargs="*",
        type=str,
        help="Optional list of additional object types to be inwards selected"
    )

    parser.add_argument(
        "--conditional_inwards",
        nargs=2,
        metavar=("REFERENCING_TYPE", "INWARDS_OBJECT_TYPE"),
        help="Apply conditional inward resolving",
    )

    parser.add_argument("--log_file", type=str, required=False, help="the logfile")
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)

    try:
        main(args.source, args.target, args.object_type, args.attribute, args.inwards_object_types, args.conditional_inwards)
    except Exception as e:
        log_all(logging.ERROR, f"{e} {traceback.format_exc()}")
        raise e
