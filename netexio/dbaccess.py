from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, IO, Any
import inspect

if TYPE_CHECKING:
    from netexio.database import Database

import pickle
import warnings
from typing import TypeVar, List, Generator, Tuple

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
    DataManagedObject,
    ResponsibilitySetRef,
    DataSourceRefStructure,
)
from netexio.serializer import Serializer
from netexio.xmlserializer import MyXmlSerializer
from transformers.references import replace_with_reference_inplace
from utils.utils import get_object_name
from utils.aux_logging import log_all
import logging
from lxml import etree

T = TypeVar("T")

ns_map = {"": "http://www.netex.org.uk/netex", "gml": "http://www.opengis.net/gml/3.2"}

context = XmlContext()
config = ParserConfig(fail_on_unknown_properties=False)
parser = XmlParser(context=context, config=config, handler=LxmlEventHandler)


# TODO: For all load_ functions filter by id + version, not only id


# TODO: This must be fixed, this is an incorrect implementation!!
def load_embedded(db: Database, clazz: T, filter, cursor=False):
    # TODO: maybe return something here, which includes *ALL* objects that are embedded within this object, so it does not have to be resolved anymore
    objectname = get_object_name(clazz)

    result = []

    with db.env.begin(
        db=db.env.open_db(b"_embedding"), buffers=True, write=False
    ) as txn:
        cursor = txn.cursor()
        for key, value in cursor:
            # parent_class, parent_id, parent_version, *_ = pickle.loads(key)
            (
                parent_class,
                parent_id,
                parent_version,
                embedding_class,
                embedding_id,
                embedding_version,
                *_,
            ) = pickle.loads(value)
            if embedding_id == filter and embedding_class == objectname:
                result.append(
                    (
                        parent_id,
                        parent_version,
                        parent_class,
                    )
                )

    return result


def load_referencing(db: Database, clazz: T, filter, cursor=False):
    result = []

    prefix = db.serializer.encode_key(filter, None, clazz, include_clazz=True)

    with db.env.begin(db=db.db_referencing, buffers=True, write=False) as txn:
        cursor = txn.cursor()
        if cursor.set_range(prefix):  # Position cursor at the first key >= prefix
            for key, value in cursor:
                if not bytes(key).startswith(prefix):
                    break  # Stop when keys no longer match the prefix

                referencing_class, referencing_id, referencing_version = (
                    cloudpickle.loads(value)
                )
                result.append(
                    (
                        referencing_id,
                        referencing_version,
                        referencing_class,
                    )
                )

    return result


def load_referencing_inwards(db: Database, clazz: T, filter, cursor=False):
    result = []

    prefix = db.serializer.encode_key(filter, None, clazz, include_clazz=True)

    with db.env.begin(db=db.db_referencing_inwards, buffers=True, write=False) as txn:
        cursor = txn.cursor()
        if cursor.set_range(prefix):  # Position cursor at the first key >= prefix
            for key, value in cursor:
                if not bytes(key).startswith(prefix):
                    break  # Stop when keys no longer match the prefix

                parent_class, parent_id, parent_version = cloudpickle.loads(value)
                result.append(
                    (
                        parent_id,
                        parent_version,
                        parent_class,
                    )
                )

    return result


def load_local(
    db: Database,
    clazz: T,
    limit=None,
    filter=None,
    cursor=False,
    embedding=True,
    embedded_parent=False,
    cache=True,
) -> list[T]:
    return list(
        load_generator(db, clazz, limit, filter, embedding, embedded_parent, cache)
    )


def recursive_resolve(
    db: Database,
    parent,
    resolved,
    filter=None,
    filter_class=set([]),
    inwards=True,
    outwards=True,
):
    for x in resolved:
        if parent.id == x.id and parent.__class__ == x.__class__:
            return

    resolved.append(parent)

    if inwards and (
        filter is False or filter == parent.id or parent.__class__ in filter_class
    ):
        resolved_parents = load_referencing_inwards(
            db, parent.__class__, filter=parent.id
        )
        if len(resolved_parents) > 0:
            for y in resolved_parents:
                already_done = False
                for x in resolved:
                    y_class = db.get_class_by_name(y[2])
                    if (
                        y[0] == x.id and y_class == x.__class__
                    ) or y_class in filter_class:
                        already_done = True
                        break

                if not already_done:
                    resolved_objs = load_local(
                        db,
                        db.get_class_by_name(y[2]),
                        filter=y[0],
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
                        )  # TODO: not only consider the first

    # In principle this would already take care of everything recursive_attributes could find, but now does it inwards.
    if outwards:
        resolved_parents = load_referencing(db, parent.__class__, filter=parent.id)
        if len(resolved_parents) > 0:
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
                        filter=y[0],
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
                        )  # TODO: not only consider the first
        # else:
        #      print(f"Cannot resolve referencing {parent.id}")

        for obj in recursive_attributes(parent, []):
            if hasattr(obj, "id"):
                continue

            elif hasattr(obj, "name_of_ref_class"):
                if obj.name_of_ref_class is None:
                    # Hack, because NeTEx does not define the default name of ref class yet
                    if obj.__class__.__name__.endswith("RefStructure"):
                        obj.name_of_ref_class = obj.__class__.__name__[0:-12]
                    elif obj.__class__.__name__.endswith("Ref"):
                        obj.name_of_ref_class = obj.__class__.__name__[0:-3]

                if not hasattr(netex, obj.name_of_ref_class):
                    # hack for non-existing structures
                    log_all(
                        logging.WARN,
                        "related_explorer",
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
                        db, clazz, filter=obj.ref, embedding=True, embedded_parent=True
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
                        )  # TODO: not only consider the first
                    else:
                        # print(obj.ref)
                        resolved_parents = load_embedded(db, clazz, filter=obj.ref)
                        if len(resolved_parents) > 0:
                            for y in resolved_parents:
                                already_done = False
                                for x in resolved:
                                    if (
                                        y[0] == x.id
                                        and db.get_class_by_name(y[2]) == x.__class__
                                    ):
                                        already_done = True
                                        break

                                if not already_done:
                                    resolved_objs = load_local(
                                        db,
                                        db.get_class_by_name(y[2]),
                                        filter=y[0],
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
                                        )  # TODO: not only consider the first
                        else:
                            log_all(
                                logging.WARN,
                                "related_explorer",
                                f"Cannot resolve embedded {obj.ref}",
                            )


def fetch_references_classes_generator(db: Database, classes: list):
    list_classes = {get_object_name(clazz) for clazz in classes}
    processed = set()

    # Find all embeddings and objects the target profile, elements must not be added directly later, but referenced.
    existing_ids = set()
    with db.env.begin(db=db.db_embedding, buffers=True, write=False) as src1_txn:
        cursor = src1_txn.cursor()
        for _key, value in cursor:
            clazz, ref, version, *_ = cloudpickle.loads(value)
            existing_ids.add(
                db.serializer.encode_key(ref, version, db.get_class_by_name(clazz))
            )

    for clazz in classes:
        # print(clazz)
        db_name = db.open_db(clazz)
        if not db_name:
            continue

        with db.env.begin(db=db_name, buffers=True, write=False) as src2_txn:
            cursor = src2_txn.cursor()
            for key, _value in cursor:
                existing_ids.add(key)

    with db.env.begin(db=db.db_referencing, buffers=True, write=False) as src3_txn:
        cursor = src3_txn.cursor()
        for _key, value in cursor:
            ref_class, ref_id, ref_version = cloudpickle.loads(
                value
            )  # TODO: check if this goes right
            if ref_class not in list_classes:
                results = load_local(
                    db,
                    db.get_class_by_name(ref_class),
                    limit=1,
                    filter=ref_id,
                    cursor=True,
                    embedding=True,
                    embedded_parent=True,
                )
                if len(results) > 0:
                    needle = get_object_name(results[0].__class__) + "|" + results[0].id
                    if (
                        results[0].__class__ in classes
                    ):  # Don't export classes, which are part of the main delivery
                        pass
                    elif (
                        needle in processed
                    ):  # Don't export classes which have been exported already, maybe this can be solved at the database layer
                        pass
                    else:
                        processed.add(needle)

                        with db.env.begin(
                            db=db.db_embedding, buffers=True, write=False
                        ) as src_txn2:
                            # TODO: Very expensive sequential scan Solved??
                            cursor2 = src_txn2.cursor()

                            prefix = db.serializer.encode_key(
                                ref_id, ref_version, db.get_class_by_name(ref_class)
                            )
                            if cursor2.set_range(
                                prefix
                            ):  # Position cursor at the first key >= prefix
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
                                        replace_with_reference_inplace(
                                            results[0], embedding_path
                                        )

                        yield results[0]

                        # An element may obviously also include other references.
                        resolved = []
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
                            needle = (
                                get_object_name(resolve.__class__) + "|" + resolve.id
                            )
                            if (
                                resolve.__class__ in classes
                            ):  # Don't export classes, which are part of the main delivery
                                pass
                            elif (
                                needle in processed
                            ):  # Don't export classes which have been exported already, maybe this can be solved at the database layer
                                pass
                            else:
                                processed.add(needle)
                                # We can do two things here, query the database for embeddings, or recursively iterate over the object.

                                with db.env.begin(
                                    db=db.db_embedding, buffers=True, write=False
                                ) as src_txn2:
                                    # TODO: Very expensive sequential scan Solved??
                                    cursor2 = src_txn2.cursor()

                                    prefix = db.serializer.encode_key(
                                        resolve.id, resolve.version, resolve.__class__
                                    )
                                    if cursor2.set_range(
                                        prefix
                                    ):  # Position cursor at the first key >= prefix
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
                                                    db.get_class_by_name(
                                                        embedding_class
                                                    ),
                                                ),
                                            ) in existing_ids:
                                                replace_with_reference_inplace(
                                                    resolve, embedding_path
                                                )

                                yield resolve


def load_generator(
    db: Database,
    clazz: T,
    limit=None,
    filter=None,
    embedding=True,
    parent=False,
    cache=True,
):
    if db.env and db.open_db(clazz) is not None:
        with db.env.begin(write=False, buffers=True, db=db.open_db(clazz)) as txn:
            cursor = txn.cursor()
            if filter:
                prefix = db.serializer.encode_key(filter, None, clazz)
                if cursor.set_range(
                    prefix
                ):  # Position cursor at the first key >= prefix
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
        yield from load_embedded_transparent_generator(
            db, clazz, limit, filter, parent, cache
        )


def load_embedded_transparent_generator(
    db: Database, clazz: T, limit=None, filter=None, parent=False, cache=True
):
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
                        cache_key = db.serializer.encode_key(
                            parent_id, parent_version, parent_clazz, True
                        )
                        obj = db.cache.get(
                            cache_key,
                            lambda: db.get_single(
                                parent_clazz, parent_id, parent_version
                            ),
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


def write_objects(
    db: Database, objs, empty=False, many=False, silent=False, cursor=False
):
    if len(objs) == 0:
        return

    clazz = objs[0].__class__
    db.insert_many_objects(clazz, objs)


def write_generator(db: Database, clazz, generator: Generator, empty=False):
    if empty:
        db.clear([clazz])

    db.insert_objects_on_queue(clazz, generator)


def copy_table(
    db_read: Database, db_write: Database, classes: list, clean=False, embedding=False
):
    for klass in classes:
        # print(klass.__name__)
        db_read.copy_db(db_write, klass)

    if embedding:
        db_read.copy_db_embedding(db_write, classes)


def missing_class_update(source_db: Database, target_db: Database):
    # TODO: As written in #223 some of the objects have not been copied at this point, but are still referenced.
    target_classes = set(target_db.tables())
    referencing_classes = set(target_db.referencing())
    embedded_classes = set(target_db.embedded())
    missing_classes = referencing_classes - (
        target_classes.union(embedded_classes)
    )  # This is naive because there may be indirections
    copy_table(source_db, target_db, missing_classes)


def update_generator(db: Database, clazz, generator: Generator):
    db.insert_many_objects(clazz, generator)


def setup_database(db: Database, classes, clean=False):
    clean_element_names, interesting_element_names, interesting_classes = classes

    if clean:
        db.drop(interesting_classes, embedding=True)
        # db.vacuum()


def get_local_name(element):
    if hasattr(element, "Meta") and hasattr(element.Meta, "name"):
        return element.Meta.name
    return element.__name__


def update_embedded_referencing(
    serializer: Serializer, deserialized
) -> Generator[list[str], None, None]:
    for obj, path in recursive_attributes(deserialized, []):
        if hasattr(obj, "id"):
            if obj.id is not None and obj.__class__ in serializer.interesting_classes:
                yield [
                    deserialized.__class__,
                    deserialized.id,
                    deserialized.version,
                    obj.__class__,
                    obj.id,
                    (
                        obj.version
                        if hasattr(obj, "version") and obj.version is not None
                        else "any"
                    ),
                    ".".join([str(s) for s in path]),
                ]

        elif hasattr(obj, "ref"):
            if obj.ref is not None:
                if obj.name_of_ref_class is None:
                    # Hack, because NeTEx does not define the default name of ref class yet
                    if obj.__class__.__name__.endswith("RefStructure"):
                        obj.name_of_ref_class = obj.__class__.__name__[0:-12]
                    elif obj.__class__.__name__.endswith("Ref"):
                        obj.name_of_ref_class = obj.__class__.__name__[0:-3]

                yield [
                    deserialized.__class__,
                    deserialized.id,
                    deserialized.version,
                    # The object that contains the reference
                    serializer.name_object[
                        obj.name_of_ref_class
                    ],  # The object that the reference is towards
                    obj.ref,
                    (
                        obj.version
                        if hasattr(obj, "version") and obj.version is not None
                        else "any"
                    ),
                    None,
                ]


def insert_database(
    db: Database,
    classes,
    f=None,
    type_of_frame_filter=None,
    cursor=False,
    direct_embedding=False,
):
    xml_serializer = MyXmlSerializer()
    clsmembers = inspect.getmembers(netex, inspect.isclass)
    all_frames = [
        get_local_name(x[1])
        for x in clsmembers
        if hasattr(x[1], "Meta")
        and hasattr(x[1].Meta, "namespace")
        and netex.VersionFrameVersionStructure in x[1].__mro__
    ]

    # See: https://github.com/NeTEx-CEN/NeTEx/issues/788
    # all_datasource_refs = [x[0] for x in clsmembers if hasattr(x[1], 'Meta') and hasattr(x[1].Meta, 'namespace') and hasattr(x[1], 'data_source_ref_attribute')]
    all_datasource_refs = [
        get_local_name(x[1])
        for x in clsmembers
        if hasattr(x[1], "Meta")
        and hasattr(x[1].Meta, "namespace")
        and netex.DataManagedObjectStructure in x[1].__mro__
    ]
    all_responsibility_set_refs = [
        get_local_name(x[1])
        for x in clsmembers
        if hasattr(x[1], "Meta")
        and hasattr(x[1].Meta, "namespace")
        and netex.EntityInVersionStructure in x[1].__mro__
    ]
    all_srs_name = [
        get_local_name(x[1])
        for x in clsmembers
        if hasattr(x[1], "Meta") and hasattr(x[1], "srs_name")
    ]

    frame_defaults_stack = []
    if f is None:
        return

    clean_element_names, interesting_element_names, interesting_classes = classes
    clazz_by_name = {}

    for i in range(0, len(interesting_element_names)):
        clazz_by_name[interesting_element_names[i]] = interesting_classes[i]

    events = ("start", "end")
    context = etree.iterparse(f, events=events, remove_blank_text=True)
    current_element_tag = None
    current_framedefaults = None
    current_datasource_ref = None
    current_responsibility_set_ref = None
    current_location_system = None
    last_version = None
    skip_frame = False

    location_srsName = None
    for event, element in context:
        localname = element.tag.split("}")[-1]  # localname

        if event == "start":
            if current_element_tag is None and element.tag in interesting_element_names:
                current_element_tag = element.tag

            elif localname == "TypeOfFrameRef":
                if (
                    type_of_frame_filter is not None
                    and element.attrib["ref"] not in type_of_frame_filter
                ):
                    # TODO: log a single warning that an unknown TypeOfFrame is found, and is not processed
                    print(f"{element.attrib['ref']} is not a known TypeOfFrame")
                    skip_frame = True

            if localname in all_frames:
                frame_defaults_stack.append(None)

            elif localname == "Location":
                if "srsName" in element.attrib:
                    location_srsName = element.attrib["srsName"]

        elif event == "end":
            # current_element_tag = element.tag
            if localname == "FrameDefaults":
                xml = etree.tostring(element, encoding="unicode")
                frame_defaults: VersionFrameDefaultsStructure = parser.from_string(
                    xml, VersionFrameDefaultsStructure
                )
                frame_defaults_stack[-1] = frame_defaults
                current_framedefaults = frame_defaults
                if current_framedefaults.default_data_source_ref is not None:
                    current_datasource_ref = (
                        current_framedefaults.default_data_source_ref.ref
                    )
                if current_framedefaults.default_responsibility_set_ref is not None:
                    current_responsibility_set_ref = (
                        current_framedefaults.default_responsibility_set_ref.ref
                    )
                if current_framedefaults.default_location_system is not None:
                    current_location_system = (
                        current_framedefaults.default_location_system
                    )

                continue

            elif localname in all_frames:
                # This is the end of the frame, pop the frame_defaults stack
                frame_defaults_stack.pop()
                filtered = [fd for fd in frame_defaults_stack if fd is not None]
                current_framedefaults = filtered[-1] if len(filtered) > 0 else None

                current_datasource_ref = None
                current_responsibility_set_ref = None
                current_location_system = None
                for fd in reversed(filtered):
                    if current_datasource_ref is None:
                        if fd.default_data_source_ref is not None:
                            current_datasource_ref = fd.default_data_source_ref.ref
                    if current_responsibility_set_ref is None:
                        if fd.default_responsibility_set_ref is not None:
                            current_responsibility_set_ref = (
                                fd.default_responsibility_set_ref.ref
                            )
                    if current_location_system is None:
                        if fd.default_location_system is not None:
                            current_location_system = fd.default_location_system
                last_version = None

                skip_frame = False
                continue

            if skip_frame:
                continue

            if current_framedefaults is not None:
                if (
                    current_datasource_ref is not None
                    and localname in all_datasource_refs
                ):
                    if "dataSourceRef" not in element.attrib:
                        element.attrib["dataSourceRef"] = current_datasource_ref

                if (
                    current_responsibility_set_ref is not None
                    and localname in all_responsibility_set_refs
                ):
                    if "responsibilitySetRef" not in element.attrib:
                        element.attrib["responsibilitySetRef"] = (
                            current_responsibility_set_ref
                        )

                if current_location_system is not None:
                    if localname in all_srs_name:
                        if "srsName" not in element.attrib:
                            element.attrib["srsName"] = (
                                location_srsName
                                if location_srsName is not None
                                else current_location_system
                            )

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

                if hasattr(clazz, "order"):
                    if order is None:
                        warnings.warn(
                            f"{localname} {id} does not have a required order, setting it to 1."
                        )
                        order = 1
                        object.order = order

                    if version is None:
                        version = last_version
                        object.version = version
                        warnings.warn(
                            f"{localname} {id} does not have a required version, inheriting it {version}."
                        )

                    try:
                        db.insert_one_object(object)
                    except:
                        print(etree.tostring(element))
                        raise
                        pass

                elif hasattr(clazz, "version"):
                    if version is None:
                        version = last_version
                        object.version = version
                        warnings.warn(
                            f"{localname} {id} does not have a required version, inheriting it {version}."
                        )

                    try:
                        db.insert_one_object(object)
                    except:
                        print(etree.tostring(element))
                        raise
                        pass

                else:
                    try:
                        db.insert_one_object(object)
                    except:
                        print(etree.tostring(element))
                        raise
                        pass

                current_element_tag = None


def recursive_attributes(obj, depth: List[int]) -> Tuple[object, List[int]]:
    # qprint(obj.__class__.__name__)
    if (
        issubclass(obj.__class__, EntityInVersionStructure)
        and obj.data_source_ref_attribute is not None
    ):
        yield DataSourceRefStructure(ref=obj.data_source_ref_attribute), depth + [
            "data_source_ref_attribute"
        ]

    if (
        issubclass(obj.__class__, DataManagedObject)
        and obj.responsibility_set_ref_attribute is not None
    ):
        yield ResponsibilitySetRef(ref=obj.responsibility_set_ref_attribute), depth + [
            "responsibility_set_ref_attribute"
        ]

    mydepth = depth.copy()
    mydepth.append(0)
    for key in obj.__dataclass_fields__.keys():
        mydepth[-1] = key
        v = getattr(obj, key, None)
        if v is not None:
            # print(v)
            if issubclass(v.__class__, VersionOfObjectRef) or issubclass(
                v.__class__, VersionOfObjectRefStructure
            ):
                yield v, mydepth

            else:
                if (
                    hasattr(v, "__dataclass_fields__")
                    and v.__class__.__name__ in netex.set_all
                ):
                    if hasattr(v, "id"):
                        yield v, mydepth
                    yield from recursive_attributes(v, mydepth)
                elif v.__class__ in (list, tuple):
                    mydepth.append(0)
                    for j in range(0, len(v)):
                        mydepth[-1] = j
                        x = v[j]
                        if x is not None:
                            if issubclass(
                                x.__class__, VersionOfObjectRef
                            ) or issubclass(x.__class__, VersionOfObjectRefStructure):
                                yield x, mydepth
                            elif (
                                hasattr(x, "__dataclass_fields__")
                                and x.__class__.__name__ in netex.set_all
                            ):
                                if hasattr(x, "id"):
                                    yield x, mydepth
                                yield from recursive_attributes(x, mydepth)
                    mydepth.pop()


def open_netex_file(filename : str) -> Iterable[IO[Any]]:
    if filename.endswith(".xml.gz"):
        yield igzip_threaded.open(filename, "rb", compresslevel=3, threads=3)
    elif filename.endswith(".xml"):
        yield open(filename, "rb")
    elif filename.endswith(".zip"):
        import zipfile

        zip = zipfile.ZipFile(filename)
        for zipfilename in zip.filelist:
            l_zipfilename = zipfilename.filename.lower()
            if l_zipfilename.endswith(".xml.gz") or l_zipfilename.endswith(".xml"):
                yield zip.open(zipfilename)
