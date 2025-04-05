from __future__ import annotations

from typing import TYPE_CHECKING, IO, Any, Literal, Generator, get_origin, get_args, Union
import inspect
from dataclasses import fields, is_dataclass
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from netexio.database import Database

import warnings
from typing import TypeVar, List

import cloudpickle

from isal import igzip_threaded
from xsdata.formats.dataclass.context import XmlContext
from xsdata.formats.dataclass.parsers import XmlParser
from xsdata.formats.dataclass.parsers.config import ParserConfig
from xsdata.formats.dataclass.parsers.handlers import LxmlEventHandler

import netex
from netexio.attributes import resolve_attr
from netex import (
    VersionFrameDefaultsStructure,
    VersionOfObjectRef,
    VersionOfObjectRefStructure,
    EntityInVersionStructure,
    ResponsibilitySetRef,
    DataSourceRefStructure,
    EntityStructure, ScheduledStopPoint,
)
from netexio.serializer import Serializer
from netexio.xmlserializer import MyXmlSerializer
from transformers.references import replace_with_reference_inplace
from utils.utils import get_object_name
from utils.aux_logging import log_all
import logging
from lxml import etree

from xsdata.models.datatype import XmlDateTime, XmlTime


class XmlTimeZoned(XmlTime):
    """Extended XmlTime with explicit timezone."""

    zoneinfo: ZoneInfo | None

    def __new__(
        cls, hour: int, minute: int, second: int, fractional_second: int = 0, offset: int | None = None, zoneinfo: ZoneInfo | None = None
    ) -> XmlTimeZoned:
        instance = super().__new__(cls, hour, minute, second, fractional_second, offset)
        instance.zoneinfo = zoneinfo
        return instance


class XmlDateTimeZoned(XmlDateTime):
    """Extended XmlTime with explicit timezone."""

    zoneinfo: ZoneInfo | None

    def __new__(
        cls,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
        second: int,
        fractional_second: int = 0,
        offset: int | None = None,
        zoneinfo: ZoneInfo | None = None,
    ) -> XmlDateTimeZoned:
        instance = super().__new__(cls, year, month, day, hour, minute, second, fractional_second, offset)
        instance.zoneinfo = zoneinfo
        return instance


T = TypeVar("T")
Tid = TypeVar("Tid", bound=EntityStructure)
Tver = TypeVar("Tver", bound=EntityInVersionStructure)

ns_map = {"": "http://www.netex.org.uk/netex", "gml": "http://www.opengis.net/gml/3.2"}

context = XmlContext()
config = ParserConfig(fail_on_unknown_properties=False)
parser = XmlParser(context=context, config=config, handler=LxmlEventHandler)


# TODO: For all load_ functions filter by id + version, not only id


def load_referencing(db: Database, clazz: type[Tid], filter_id: str | None = None) -> Generator[tuple[str, str, str, str], None, None]:
    prefix = db.serializer.encode_key(filter_id, None, clazz, include_clazz=True)

    with db.env.begin(db=db.db_referencing, buffers=True, write=False) as txn:
        cursor = txn.cursor()
        if cursor.set_range(prefix):  # Position cursor at the first key >= prefix
            for key, value in cursor:
                if not bytes(key).startswith(prefix):
                    break  # Stop when keys no longer match the prefix

                referencing_class, referencing_id, referencing_version, path = cloudpickle.loads(value)

                yield referencing_id, referencing_version, referencing_class, path


def load_referencing_inwards(db: Database, clazz: type[Tid], filter_id: str | None = None) -> Generator[tuple[str, str, str, str], None, None]:
    prefix = db.serializer.encode_key(filter_id, None, clazz, include_clazz=True)

    with db.env.begin(db=db.db_referencing_inwards, buffers=True, write=False) as txn:
        cursor = txn.cursor()
        if cursor.set_range(prefix):  # Position cursor at the first key >= prefix
            for key, value in cursor:
                if not bytes(key).startswith(prefix):
                    break  # Stop when keys no longer match the prefix

                parent_class, parent_id, parent_version, path = cloudpickle.loads(value)

                yield parent_id, parent_version, parent_class, path


def load_local(
    db: Database,
    clazz: type[Tid],
    limit: int | None = None,
    filter_id: str | None = None,
    cursor: bool = False,
    embedding: bool = True,
    embedded_parent: bool = False,
    cache: bool = True,
) -> list[Tid]:
    return list(load_generator(db, clazz, limit, filter_id, embedding, embedded_parent, cache))


def recursive_resolve(
    db: Database,
    parent: Tid,
    resolved: list[Any],
    filter: str | Literal[False] | None = None,
    filter_class: set[type[Tid]] = set([]),
    inwards: bool = True,
    outwards: bool = True,
    filter_set_assignment: dict[type[Tid]: set[type[Tid]]] = {}
) -> None:
    resolved_objs: list[Any]

    for x in resolved:
        if parent.id == x.id and parent.__class__ == x.__class__:
            return

    resolved.append(parent)

    if inwards and parent.__class__ in filter_class:
        if parent.__class__ == ScheduledStopPoint:
            pass
        assert parent.id is not None, "Parent.id must not be none"
        # print("INWARDS", parent.id)
        resolved_parents = list(load_referencing_inwards(db, parent.__class__, filter_id=parent.id))  # TODO: replace resolved_parents with named attributes
        my_filterset = filter_set_assignment.get(parent.__class__, None)

        for y in resolved_parents:
            y_class: type[Tid] = db.get_class_by_name(y[2])
            already_done = False
            for x in resolved:
                if (
                    y[0] == x.id and y_class == x.__class__
                ):  #  or y_class in filter_class: This seems to be an issue to get the inward relationships to work, starting from Line
                    already_done = True
                    break

            if not already_done and (y_class in my_filterset if my_filterset is not None else True):
                resolved_objs = load_local(
                    db,
                    db.get_class_by_name(y[2]),
                    filter_id=y[0],
                    embedding=True,
                    embedded_parent=True,
                )
                if len(resolved_objs) > 0:
                    recursive_resolve(
                        db,
                        resolved_objs[0],
                        resolved,
                        filter,
                        filter_class,
                        inwards,
                        outwards,
                        filter_set_assignment=filter_set_assignment
                    )  # TODO: not only consider the first

    # In principle this would already take care of everything recursive_attributes could find, but now does it inwards.
    if outwards:
        assert parent.id is not None, "parent.id must not be none"
        # print("OUTWARDS", parent.id)
        resolved_parents = load_referencing(db, parent.__class__, filter_id=parent.id)
        for y in resolved_parents:
            already_done = False
            for x in resolved:
                if y[0] == x.id and db.get_class_by_name(y[2]) == x.__class__:
                    already_done = True
                    break

            if not already_done:
                resolved_objs = load_local(
                    db,
                    db.get_class_by_name(y[2]),
                    filter_id=y[0],
                    embedding=True,
                    embedded_parent=True,
                )
                if len(resolved_objs) > 0:
                    recursive_resolve(
                        db,
                        resolved_objs[0],
                        resolved,
                        filter,
                        filter_class,
                        inwards,
                        outwards,
                        filter_set_assignment=filter_set_assignment
                    )  # TODO: not only consider the first

        for obj, path in recursive_attributes(parent, []):
            if hasattr(obj, "id"):
                continue

            elif hasattr(obj, "name_of_ref_class"):
                if obj.name_of_ref_class is None:
                    # Hack, because NeTEx does not define the default name of ref class yet
                    if obj.__class__.__name__.endswith("RefStructure"):
                        obj.name_of_ref_class = obj.__class__.__name__[0:-12]
                    elif obj.__class__.__name__.endswith("Ref"):
                        obj.name_of_ref_class = obj.__class__.__name__[0:-3]

                assert obj.ref is not None, "Object ref must not be none"
                assert obj.name_of_ref_class is not None, "Object name of ref class must not be none"
                if not hasattr(netex, obj.name_of_ref_class):
                    # hack for non-existing structures
                    log_all(
                        logging.WARN,
                        f"No attribute found in module {netex} for {obj.name_of_ref_class}.",
                    )

                    continue

                clazz = getattr(netex, obj.name_of_ref_class)

                # TODO: do this via a hash function
                # if obj in resolved:
                #    continue
                already_done = False
                for x in resolved:
                    if obj.ref == x.id and clazz == x.__class__:
                        already_done = True
                        break

                if not already_done:
                    resolved_objs = load_local(
                        db,
                        clazz,
                        filter_id=obj.ref,
                        embedding=True,
                        embedded_parent=True,
                    )
                    if len(resolved_objs) > 0:
                        recursive_resolve(
                            db,
                            resolved_objs[0],
                            resolved,
                            filter,
                            filter_class,
                            inwards,
                            outwards,
                            filter_set_assignment=filter_set_assignment
                        )  # TODO: not only consider the first
                    else:
                        # print(obj.ref)
                        resolved_parents = load_embedded_transparent_generator(db, clazz, filter=obj.ref)
                        for y in resolved_parents:
                            already_done = False
                            for x in resolved:
                                if y[0] == x.id and db.get_class_by_name(y[2]) == x.__class__:
                                    already_done = True
                                    break

                            if not already_done:
                                resolved_objs = load_local(
                                    db,
                                    db.get_class_by_name(y[2]),
                                    filter_id=y[0],
                                    embedding=True,
                                    embedded_parent=True,
                                )
                                if len(resolved_objs) > 0:
                                    recursive_resolve(
                                        db,
                                        resolved_objs[0],
                                        resolved,
                                        filter,
                                        filter_class,
                                        inwards,
                                        outwards,
                                        filter_set_assignment=filter_set_assignment
                                    )  # TODO: not only consider the first
                        else:
                            log_all(
                                logging.WARN,
                                f"Cannot resolve embedded {obj.ref}",
                            )


def fetch_references_classes_generator(db: Database, classes: list[type[Tid]]) -> Generator[Tid, None, None]:
    list_classes = {get_object_name(clazz) for clazz in classes}
    processed = set()

    # Find all embeddings and objects the target profile, elements must not be added directly later, but referenced.
    existing_ids = set()
    existing_global_ids = set()
    with db.env.begin(db=db.db_embedding, buffers=True, write=False) as src1_txn:
        cursor = src1_txn.cursor()
        for _key, value in cursor:
            clazz, ref, version, *_ = cloudpickle.loads(value)
            existing_ids.add(db.serializer.encode_key(ref, version, db.get_class_by_name(clazz)))

    del clazz, ref, version

    for clazz in classes:
        # print(clazz)
        db_name = db.open_db(clazz, readonly=True)
        if not db_name:
            continue

        with db.env.begin(db=db_name, buffers=True, write=False) as src2_txn:
            cursor = src2_txn.cursor()
            for key, _value in cursor:
                existing_ids.add(bytes(key))
                existing_global_ids.add(clazz.__name__.upper().encode('utf-8') + b"-" + bytes(key))  # TODO: This is a hack

    with db.env.begin(db=db.db_referencing, buffers=True, write=False) as src3_txn:
        cursor = src3_txn.cursor()
        for key, value in cursor:
            if key not in existing_global_ids:
                continue

            ref_class, ref_id, ref_version, path = cloudpickle.loads(value)  # TODO: check if this goes right
            if ref_class not in list_classes:
                results: list[Tid] = load_local(
                    db,
                    db.get_class_by_name(ref_class),
                    limit=1,
                    filter_id=ref_id,
                    cursor=True,
                    embedding=True,
                    embedded_parent=True,
                )
                if len(results) > 0:
                    assert results[0].id is not None, "results[0].id must not be none"
                    needle = get_object_name(results[0].__class__) + "|" + results[0].id
                    if results[0].__class__ in classes:  # Don't export classes, which are part of the main delivery
                        pass
                    elif needle in processed:  # Don't export classes which have been exported already, maybe this can be solved at the database layer
                        pass
                    else:
                        processed.add(needle)

                        with db.env.begin(db=db.db_embedding, buffers=True, write=False) as src_txn2:
                            # TODO: Very expensive sequential scan Solved??
                            cursor2 = src_txn2.cursor()

                            prefix = db.serializer.encode_key(ref_id, ref_version, db.get_class_by_name(ref_class))
                            if cursor2.set_range(prefix):  # Position cursor at the first key >= prefix
                                for key2, value2 in cursor2:
                                    if not bytes(key2).startswith(prefix):
                                        break  # Stop when keys no longer match the prefix

                                    (
                                        embedding_class,
                                        embedding_id,
                                        embedding_version,
                                        embedding_path,
                                    ) = cloudpickle.loads(value2)
                                    if (
                                        embedding_class,
                                        db.serializer.encode_key(
                                            embedding_id,
                                            embedding_version,
                                            db.get_class_by_name(embedding_class),
                                        ),
                                    ) in existing_ids:
                                        replace_with_reference_inplace(results[0], embedding_path)

                        yield results[0]

                        # An element may obviously also include other references.
                        resolved: list[Tid] = []
                        filter_set = {results[0].__class__}.union(classes)
                        recursive_resolve(
                            db,
                            results[0],
                            resolved,
                            results[0].id,
                            filter_set,
                            False,
                            True,
                        )

                        for resolve in resolved:
                            assert resolve.id is not None, "resolve.id must not be none"
                            needle = get_object_name(resolve.__class__) + "|" + resolve.id
                            if resolve.__class__ in classes:  # Don't export classes, which are part of the main delivery
                                pass
                            elif needle in processed:  # Don't export classes which have been exported already, maybe this can be solved at the database layer
                                pass
                            else:
                                processed.add(needle)
                                # We can do two things here, query the database for embeddings, or recursively iterate over the object.

                                with db.env.begin(db=db.db_embedding, buffers=True, write=False) as src_txn2:
                                    # TODO: Very expensive sequential scan Solved??
                                    cursor2 = src_txn2.cursor()

                                    prefix = db.serializer.encode_key(
                                        resolve.id,
                                        getattr(resolve, "version", None),
                                        resolve.__class__,
                                    )
                                    if cursor2.set_range(prefix):  # Position cursor at the first key >= prefix
                                        for key2, value2 in cursor2:
                                            if not bytes(key2).startswith(prefix):
                                                break  # Stop when keys no longer match the prefix

                                            (
                                                embedding_class,
                                                embedding_id,
                                                embedding_version,
                                                embedding_path,
                                            ) = cloudpickle.loads(value2)
                                            if (
                                                embedding_class,
                                                db.serializer.encode_key(
                                                    embedding_id,
                                                    embedding_version,
                                                    db.get_class_by_name(embedding_class),
                                                ),
                                            ) in existing_ids:
                                                replace_with_reference_inplace(resolve, embedding_path)

                                yield resolve


def load_generator(
    db: Database,
    clazz: type[Tid],
    limit: int | None = None,
    filter_id: str | None = None,
    embedding: bool = True,
    parent: bool = False,
    cache: bool = True,
) -> Generator[Tid, None, None]:
    if db.env and db.open_db(clazz, readonly=True) is not None:
        with db.env.begin(write=False, buffers=True, db=db.open_db(clazz, readonly=True)) as txn:
            cursor = txn.cursor()
            if filter_id:
                prefix = db.serializer.encode_key(filter_id, None, clazz)
                if cursor.set_range(prefix):  # Position cursor at the first key >= prefix
                    for key, value in cursor:
                        if not bytes(key).startswith(prefix):
                            break  # Stop when keys no longer match the prefix

                        yield db.serializer.unmarshall(value, clazz)

            elif limit is not None:
                i = 0
                for _key, value in cursor:
                    if i < limit:
                        value = db.serializer.unmarshall(value, clazz)
                        yield value
                    else:
                        break
                    i += 1
            else:
                for _key, value in cursor:
                    value = db.serializer.unmarshall(value, clazz)
                    yield value

    if embedding:
        yield from load_embedded_transparent_generator(db, clazz, limit, filter_id, parent, cache)


def load_embedded_transparent_generator(
    db: Database,
    clazz: type[Tid],
    limit: int | None = None,
    filter: str | None = None,
    parent: bool = False,
    cache: bool = True,
) -> Generator[Tid, None, None]:
    # TODO: Expensive for classes that are not available, it will do a complete sequential scan and for each time it will depicle each individual object

    if db.env:
        with db.env.begin(db=db.db_embedding_inverse, buffers=True, write=False) as txn:
            i = 0
            prefix = db.serializer.encode_key(filter, None, clazz, True)
            # print(prefix)
            cursor = txn.cursor()
            if cursor.set_range(prefix):  # Position cursor at the first key >= prefix
                for key, value in cursor:
                    if not bytes(key).startswith(prefix):
                        break  # Stop when keys no longer match the prefix

                    _tmp = cloudpickle.loads(value)
                    parent_clazz, parent_id, parent_version, embedding_path = _tmp

                    if limit is None or i < limit:
                        parent_clazz = db.get_class_by_name(parent_clazz)
                        cache_key = db.serializer.encode_key(parent_id, parent_version, parent_clazz, True)
                        obj = db.cache.get(
                            cache_key,
                            lambda: db.get_single(parent_clazz, parent_id, parent_version),
                        )

                        if obj is not None:
                            if parent:
                                yield obj
                                if filter:
                                    break
                            else:
                                # TODO: separate function
                                split = []
                                for p in embedding_path.split("."):
                                    if p.isnumeric():
                                        p = int(p)
                                    split.append(p)
                                yield resolve_attr(obj, split)
                                if filter:
                                    break
                        i += 1


def copy_table(db_read: Database, db_write: Database, classes: list[type[Tid]], clean: bool = False, embedding: bool = True, metadata: bool = True) -> None:
    for klass in classes:
        # print(klass.__name__)
        db_read.copy_db(db_write, klass)

    if embedding:
        db_read.copy_db_embedding(db_write, classes)

    if metadata:
        db_read.copy_db_metadata(db_write)


def missing_class_update(source_db: Database, target_db: Database) -> None:
    # TODO: As written in #223 some of the objects have not been copied at this point, but are still referenced.
    target_classes: set[type[EntityStructure]] = target_db.tables()
    referencing_classes: set[type[EntityStructure]] = set(target_db.referencing())
    embedded_classes: set[type[EntityStructure]] = set(target_db.embedded())
    missing_classes: set[type[EntityStructure]] = referencing_classes - (
        target_classes.union(embedded_classes)
    )  # This is naive because there may be indirections
    copy_table(source_db, target_db, list(missing_classes))


def setup_database(db: Database, classes: tuple[list[str], list[str], list[Any]], clean: bool = False) -> None:
    clean_element_names, interesting_element_names, interesting_classes = classes

    if clean:
        db.drop(interesting_classes, embedding=True)
        # db.vacuum()


def get_local_name(element: type[Tid]) -> str:
    meta = getattr(element, "Meta", None)
    if meta:
        return getattr(meta, "name", element.__name__)

    return element.__name__


def update_embedded_referencing(serializer: Serializer, deserialized: Tid) -> Generator[tuple[bool, type[Tid], str, str, type[Tid], str, str, str], None, None]:
    assert deserialized.id is not None, "deserialised.id must not be none"

    for obj, path in recursive_attributes(deserialized, []):
        if hasattr(obj, "id") and obj.id is not None:
            if obj.__class__ in serializer.interesting_classes:
                assert obj.id is not None, "Object.id must not be none"
                yield (
                    True,
                    deserialized.__class__,
                    deserialized.id,
                    getattr(deserialized, "version", "any"),
                    obj.__class__,
                    obj.id,
                    getattr(obj, "version", "any"),
                    ".".join([str(s) for s in path]),
                )

        elif hasattr(obj, "ref"):
            assert obj.ref is not None, "Object ref must not be none"
            if obj.name_of_ref_class is None:
                # Hack, because NeTEx does not define the default name of ref class yet
                if obj.__class__.__name__.endswith("RefStructure"):
                    obj.name_of_ref_class = obj.__class__.__name__[0:-12]
                elif obj.__class__.__name__.endswith("Ref"):
                    obj.name_of_ref_class = obj.__class__.__name__[0:-3]

            yield (
                False,
                deserialized.__class__,
                deserialized.id,
                getattr(deserialized, "version", "any"),
                # The object that contains the reference
                serializer.name_object[obj.name_of_ref_class],  # The object that the reference is towards
                obj.ref,
                getattr(obj, "version", "any"),
                ".".join([str(s) for s in path]),
            )


def is_xml_time_type(t: T) -> bool:
    """Checks for the type XmlTime is, including Optional[XmlTime]."""
    return t is XmlTime or (get_origin(t) is Union and XmlTime in get_args(t))


def is_xml_date_time_type(t: T) -> bool:
    """Checks for the type XmlTime is, including Optional[XmlTime]."""
    return t is XmlTime or (get_origin(t) is Union and XmlTime in get_args(t))


def convert_xml_time(value: XmlTime | XmlDateTime, zoneinfo: ZoneInfo) -> Any:
    """Transform XmlTime to XmlTimeZone When applicable."""
    if type(value) is XmlTime:
        return XmlTimeZoned(value.hour, value.minute, value.second, value.fractional_second, value.offset, zoneinfo)

    if type(value) is XmlDateTime:
        return XmlDateTimeZoned(value.year, value.month, value.day, value.hour, value.minute, value.second, value.fractional_second, value.offset, zoneinfo)

    return value


def replace_xml_time_with_timezone(obj: Any, zoneinfo: ZoneInfo) -> None:
    """Replace alle XmlTime instances by XmlTimeZone, recursive in dataclasses and lists."""
    if not is_dataclass(obj):
        return

    for field in fields(obj):
        value = getattr(obj, field.name)

        # Algemene verwerking zonder duplicatie
        if isinstance(value, (XmlTime, XmlDateTime, list, tuple)) or is_dataclass(value):
            object.__setattr__(obj, field.name, recursive_replace(value, zoneinfo))


def recursive_replace(value: Any, zoneinfo: ZoneInfo) -> Any:
    """Recursive replacement of XmlTime in lists, tuples and dataclasses."""
    if type(value) in (XmlTime, XmlDateTime):
        return convert_xml_time(value, zoneinfo)

    if type(value) is list:
        return [recursive_replace(v, zoneinfo) for v in value]

    if type(value) is tuple:
        return tuple(recursive_replace(v, zoneinfo) for v in value)

    if is_dataclass(value):
        # Applies directly
        replace_xml_time_with_timezone(value, zoneinfo)

    return value


def class_contains_xml_time(cls: Any) -> bool:
    """Recursieve functie om te bepalen of een dataclass ergens een XmlTime bevat."""
    if not is_dataclass(cls):
        return False

    for field in fields(cls):
        field_type = field.type
        if is_xml_time_type(field_type):  # Direct een XmlTime of Optional[XmlTime]
            return True
        if is_xml_date_time_type(field_type):  # Direct een XmlTime of Optional[XmlTime]
            return True
        if get_origin(field_type) is list:  # Lijst met dataclass-objecten
            field_type = get_args(field_type)[0]
        if is_dataclass(field_type) and class_contains_xml_time(field_type):  # Recursief checken
            return True
    return False


def insert_database(
    db: Database,
    classes: tuple[list[str], list[str], list[Any]],
    f: IO[Any] | None = None,
    type_of_frame_filter: list[str] | None = None,
    cursor: bool = False,
    direct_embedding: bool = False,
) -> None:
    xml_serializer = MyXmlSerializer()
    clsmembers = inspect.getmembers(netex, inspect.isclass)
    all_frames = [
        get_local_name(x[1])
        for x in clsmembers
        if hasattr(x[1], "Meta") and hasattr(x[1].Meta, "namespace") and netex.VersionFrameVersionStructure in x[1].__mro__
    ]

    all_with_id = [
        get_local_name(x[1])
        for x in clsmembers
        if hasattr(x[1], "id")
    ]

    # See: https://github.com/NeTEx-CEN/NeTEx/issues/788
    # all_datasource_refs = [x[0] for x in clsmembers if hasattr(x[1], 'Meta') and hasattr(x[1].Meta, 'namespace') and hasattr(x[1], 'data_source_ref_attribute')]
    all_datasource_refs = [
        get_local_name(x[1])
        for x in clsmembers
        if hasattr(x[1], "Meta") and hasattr(x[1].Meta, "namespace") and netex.DataManagedObjectStructure in x[1].__mro__
    ]
    all_responsibility_set_refs = [
        get_local_name(x[1]) for x in clsmembers if hasattr(x[1], "Meta") and hasattr(x[1].Meta, "namespace") and netex.EntityInVersionStructure in x[1].__mro__
    ]
    all_srs_name = [get_local_name(x[1]) for x in clsmembers if hasattr(x[1], "Meta") and hasattr(x[1], "srs_name")]

    all_classes_with_xml_time = [get_local_name(x[1]) for x in clsmembers if hasattr(x[1], "Meta") and class_contains_xml_time(x[1])]

    frame_defaults_stack: list[VersionFrameDefaultsStructure | None] = []
    if f is None:
        return

    clean_element_names, interesting_element_names, interesting_classes = classes
    clazz_by_name = {}

    for i in range(0, len(interesting_element_names)):
        clazz_by_name[interesting_element_names[i]] = interesting_classes[i]

    events = ("start", "end")
    context = etree.iterparse(f, events=events, remove_blank_text=True)
    current_frame_id = None
    current_element_tag = None
    current_framedefaults = None
    current_datasource_ref = None
    current_responsibility_set_ref = None
    current_location_system = None
    current_zoneinfo: ZoneInfo | None = None
    last_id = None
    last_version = None
    skip_frame = False

    location_srsName = None
    for event, element in context:
        localname = element.tag.split("}")[-1]  # localname

        if event == "start":
            if current_element_tag is None and element.tag in interesting_element_names:
                current_element_tag = element.tag

            if localname in all_with_id:
                id = element.attrib.get("id", None)
                if id is not None:
                    last_id = (localname, id)
                elif last_id is not None:
                    element.attrib['id'] = last_id[1].replace(last_id[0], element.tag)

                version = element.attrib.get("version", None)
                if version is not None:
                    last_version = version

            elif localname == "TypeOfFrameRef":
                if type_of_frame_filter is not None and element.attrib["ref"] not in type_of_frame_filter:
                    # TODO: log a single warning that an unknown TypeOfFrame is found, and is not processed
                    print(f"{element.attrib['ref']} is not a known TypeOfFrame")
                    skip_frame = True

            if localname in all_frames:
                current_frame_id = (element.attrib['id'], element.attrib['version'])
                frame_defaults_stack.append(None)

            elif localname == "Location":
                if "srsName" in element.attrib:
                    location_srsName = element.attrib["srsName"]

        elif event == "end":
            # current_element_tag = element.tag
            if localname == "FrameDefaults":
                xml = etree.tostring(element, encoding="unicode")
                frame_defaults: VersionFrameDefaultsStructure = parser.from_string(xml, VersionFrameDefaultsStructure)
                frame_defaults_stack[-1] = frame_defaults
                current_framedefaults = frame_defaults
                if current_framedefaults.default_data_source_ref is not None:
                    current_datasource_ref = current_framedefaults.default_data_source_ref.ref
                if current_framedefaults.default_responsibility_set_ref is not None:
                    current_responsibility_set_ref = current_framedefaults.default_responsibility_set_ref.ref
                if current_framedefaults.default_location_system is not None:
                    current_location_system = current_framedefaults.default_location_system
                if current_framedefaults.default_locale and current_framedefaults.default_locale.time_zone:
                    current_zoneinfo = ZoneInfo(current_framedefaults.default_locale.time_zone)

                if current_frame_id is not None:
                    db.insert_metadata_on_queue([(current_frame_id[0], current_frame_id[1], frame_defaults)])

                continue

            elif localname in all_frames:
                # This is the end of the frame, pop the frame_defaults stack
                frame_defaults_stack.pop()
                filtered = [fd for fd in frame_defaults_stack if fd is not None]
                current_framedefaults = filtered[-1] if len(filtered) > 0 else None

                current_datasource_ref = None
                current_responsibility_set_ref = None
                current_location_system = None
                current_zoneinfo = None
                for fd in reversed(filtered):
                    if current_datasource_ref is None:
                        if fd.default_data_source_ref is not None:
                            current_datasource_ref = fd.default_data_source_ref.ref
                    if current_responsibility_set_ref is None:
                        if fd.default_responsibility_set_ref is not None:
                            current_responsibility_set_ref = fd.default_responsibility_set_ref.ref
                    if current_location_system is None:
                        if fd.default_location_system is not None:
                            current_location_system = fd.default_location_system
                    if current_zoneinfo is None:
                        if fd.default_locale and fd.default_locale.time_zone:
                            current_zoneinfo = ZoneInfo(fd.default_locale.time_zone)

                last_id = None
                last_version = None

                skip_frame = False
                continue

            if skip_frame:
                continue

            if current_framedefaults is not None:
                if current_datasource_ref is not None and localname in all_datasource_refs:
                    if "dataSourceRef" not in element.attrib:
                        element.attrib["dataSourceRef"] = current_datasource_ref

                if current_responsibility_set_ref is not None and localname in all_responsibility_set_refs:
                    if "responsibilitySetRef" not in element.attrib:
                        element.attrib["responsibilitySetRef"] = current_responsibility_set_ref

                if current_location_system is not None:
                    if localname in all_srs_name:
                        if "srsName" not in element.attrib:
                            element.attrib["srsName"] = location_srsName if location_srsName is not None else current_location_system

                    if localname == "Location":
                        if "srsName" not in element.attrib:
                            element.attrib["srsName"] = current_location_system

                        location_srsName = None

            if (
                current_element_tag == element.tag
            ):  # https://stackoverflow.com/questions/65935392/why-does-elementtree-iterparse-sometimes-retrieve-xml-elements-incompletely
                if "id" not in element.attrib:
                    current_element_tag = None
                    # print(xml)
                    continue

                clazz = clazz_by_name[element.tag]

                id = element.attrib["id"]

                version = element.attrib.get("version", None)
                if version is not None:
                    last_version = version

                order = element.attrib.get("order", None)
                object = xml_serializer.unmarshall(element, clazz)

                if False and current_zoneinfo is not None:  # TODO: Fix this after we can do this in xsData
                    if localname in all_classes_with_xml_time:
                        recursive_replace(object, current_zoneinfo)

                if hasattr(clazz, "order"):
                    if order is None:
                        warnings.warn(f"{localname} {id} does not have a required order, setting it to 1.")
                        order = 1
                        object.order = order

                    if version is None:
                        version = last_version
                        object.version = version
                        warnings.warn(f"{localname} {id} does not have a required version, inheriting it {version}.")

                    try:
                        db.insert_one_object(object)
                    except:
                        print("1", etree.tostring(element))
                        raise
                        pass

                elif hasattr(clazz, "version"):
                    if version is None:
                        version = last_version
                        object.version = version
                        warnings.warn(f"{localname} {id} does not have a required version, inheriting it {version}.")

                    try:
                        db.insert_one_object(object)
                    except:
                        print("2", etree.tostring(element), object)
                        raise
                        pass

                else:
                    try:
                        db.insert_one_object(object)
                    except:
                        print("3", etree.tostring(element))
                        raise
                        pass

                current_element_tag = None


def recursive_attributes(obj: Tid, depth: List[int | str]) -> Generator[tuple[Any, list[int | str]], None, None]:
    # qprint(obj.__class__.__name__)

    data_source_ref_attribute = getattr(obj, "data_source_ref_attribute", None)
    if data_source_ref_attribute:
        yield DataSourceRefStructure(ref=data_source_ref_attribute), depth + ["data_source_ref_attribute"]

    responsibility_set_ref_attribute = getattr(obj, "responsibility_set_ref_attribute", None)
    if responsibility_set_ref_attribute:
        yield ResponsibilitySetRef(ref=responsibility_set_ref_attribute), depth + ["responsibility_set_ref_attribute"]

    mydepth: list[int | str] = depth.copy()
    mydepth.append(0)
    for key in obj.__dataclass_fields__.keys():
        mydepth[-1] = key
        v = getattr(obj, key, None)
        if v is not None:
            # print(v)
            if issubclass(v.__class__, VersionOfObjectRef) or issubclass(v.__class__, VersionOfObjectRefStructure):
                yield v, mydepth

            else:
                if hasattr(v, "__dataclass_fields__") and v.__class__.__name__ in netex.set_all:  # type: ignore
                    if hasattr(v, "id"):
                        yield v, mydepth
                    yield from recursive_attributes(v, mydepth)
                elif v.__class__ in (list, tuple):
                    mydepth.append(0)
                    for j in range(0, len(v)):
                        mydepth[-1] = j
                        x = v[j]
                        if x is not None:
                            if issubclass(x.__class__, VersionOfObjectRef) or issubclass(x.__class__, VersionOfObjectRefStructure):
                                yield x, mydepth  # TODO: mydepth result is incorrect when list() but not as iterator
                            elif hasattr(x, "__dataclass_fields__") and x.__class__.__name__ in netex.set_all:  # type: ignore
                                if hasattr(x, "id"):
                                    yield x, mydepth
                                yield from recursive_attributes(x, mydepth)
                    mydepth.pop()


def open_netex_file(filename: str) -> Generator[IO[Any], None, None]:
    if filename.endswith(".xml.gz"):
        yield igzip_threaded.open(filename, "rb", compresslevel=3, threads=3)  # type: ignore
    elif filename.endswith(".xml"):
        yield open(filename, "rb")
    elif filename.endswith(".zip"):
        import zipfile

        zip = zipfile.ZipFile(filename)
        for zipfilename in zip.filelist:
            l_zipfilename = zipfilename.filename.lower()
            if l_zipfilename.endswith(".xml.gz") or l_zipfilename.endswith(".xml"):
                yield zip.open(zipfilename)
