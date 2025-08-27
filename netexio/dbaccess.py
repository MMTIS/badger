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
    EntityStructure,
    ScheduledStopPoint,
)
from netexio.serializer import Serializer
from netexio.xmlserializer import MyXmlSerializer
from transformers.references import replace_with_reference_inplace, split_path
from utils.utils import get_object_name
from utils.aux_logging import log_all, log_once
import logging
from lxml import etree

from xsdata.models.datatype import XmlDateTime, XmlTime




T = TypeVar("T")
Tid = TypeVar("Tid", bound=EntityStructure)
Tver = TypeVar("Tver", bound=EntityInVersionStructure)

ns_map = {"": "http://www.netex.org.uk/netex", "gml": "http://www.opengis.net/gml/3.2"}

context = XmlContext()
config = ParserConfig(fail_on_unknown_properties=False)
parser = XmlParser(context=context, config=config, handler=LxmlEventHandler)


# TODO: For all load_ functions filter by id + version, not only id


def load_referencing(
    db: Database, clazz: type[Tid], filter_id: str | None = None, filter_version: str | None = None
) -> Generator[tuple[str, str, str, str], None, None]:
    prefix = db.serializer.encode_key(filter_id, filter_version, clazz, include_clazz=True)

    with db.env.begin(db=db.db_referencing, buffers=True, write=False) as txn:
        cursor = txn.cursor()
        if cursor.set_range(prefix):  # Position cursor at the first key >= prefix
            for key, value in cursor:
                if not bytes(key).startswith(prefix):
                    break  # Stop when keys no longer match the prefix

                referencing_class, referencing_id, referencing_version, path = cloudpickle.loads(value)

                yield referencing_id, referencing_version, referencing_class, path


def load_referencing_inwards(
    db: Database, clazz: type[Tid], filter_id: str | None = None, filter_version: str | None = None
) -> Generator[tuple[str, str, str, str], None, None]:
    prefix = db.serializer.encode_key(filter_id, filter_version, clazz, include_clazz=True)

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
    filter_set_assignment: dict[type[Tid] : set[type[Tid]]] = {},
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
                        db, resolved_objs[0], resolved, filter, filter_class, inwards, outwards, filter_set_assignment=filter_set_assignment
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
                        db, resolved_objs[0], resolved, filter, filter_class, inwards, outwards, filter_set_assignment=filter_set_assignment
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
                            db, resolved_objs[0], resolved, filter, filter_class, inwards, outwards, filter_set_assignment=filter_set_assignment
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
                                        db, resolved_objs[0], resolved, filter, filter_class, inwards, outwards, filter_set_assignment=filter_set_assignment
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
        db_name = db.open_database(clazz, readonly=True)
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
    if db.env and db.open_database(clazz, readonly=True) is not None:
        with db.env.begin(write=False, buffers=True, db=db.open_database(clazz, readonly=True)) as txn:
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

            if obj.name_of_ref_class not in serializer.name_object:
                log_once(logging.WARN, "unknown name_of_ref_class", "Reference Class cannot be found in serializer")
                continue

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







from functools import lru_cache

@lru_cache(maxsize=None)
def _dc_field_names(cls: type) -> tuple[str, ...]:
    return tuple(cls.__dataclass_fields__.keys())

def recursive_attributes(obj: Tid, depth: List[int | str]) -> Generator[tuple[Any, list[int | str]], None, None]:
    # We skip data_source_ref_attribute and  responsibility_set_ref_attribute later in the pipeline
    # data_source_ref_attribute = getattr(obj, "data_source_ref_attribute", None)
    # if data_source_ref_attribute:
    #     yield DataSourceRefStructure(ref=data_source_ref_attribute), depth + ["data_source_ref_attribute"]

    # responsibility_set_ref_attribute = getattr(obj, "responsibility_set_ref_attribute", None)
    # if responsibility_set_ref_attribute:
    #     yield ResponsibilitySetRef(ref=responsibility_set_ref_attribute), depth + ["responsibility_set_ref_attribute"]

    mydepth = depth
    mydepth.append(0)
    for key in _dc_field_names(obj.__class__):
        mydepth[-1] = key
        v = getattr(obj, key, None)
        if v is not None:
            # print(v)
            if v.__class__ in netex.set_ref_types:
                yield v, mydepth

            else:
                if hasattr(v, "__dataclass_fields__") and v.__class__.__name__ in netex.set_all:  # type: ignore
                    if hasattr(v, "id"):
                        yield v, list(mydepth)
                    yield from recursive_attributes(v, mydepth)
                elif v.__class__ in (list, tuple):
                    mydepth.append(0)
                    for j, x in enumerate(v):
                        mydepth[-1] = j
                        if x is not None:
                            if x.__class__ in netex.set_ref_types:
                                yield x, list(mydepth)  # TODO: mydepth result is incorrect when list() but not as iterator
                            elif hasattr(x, "__dataclass_fields__") and x.__class__.__name__ in netex.set_all:  # type: ignore
                                if hasattr(x, "id"):
                                    yield x, list(mydepth)
                                yield from recursive_attributes(x, mydepth)
                    mydepth.pop()
    mydepth.pop()




def all_subclasses(cls):
    return set(cls.__subclasses__()).union(s for c in cls.__subclasses__() for s in all_subclasses(c))


def check_referencing(db: Database) -> None:
    """
    The ambition of this function is to make sure that all references are valid, but also to remove the "any" version
    towards a resolved version currently found in the database.

    This could also be a place to resolve conditional valdity (valid between)
    """

    result = None
    prev_key = None
    prev_key_update = False

    with db.env.begin(db=db.db_referencing, write=False) as txn:
        with txn.cursor(db.db_referencing_inwards) as cursor_referencing_inwards, txn.cursor(db.db_referencing) as cursor_referencing:
            for key, value in cursor_referencing:
                if prev_key != key:
                    if prev_key_update:
                        # print(f"Would update {result.id} {attribute}")
                        # TODO: we must aggregate all operations per single referencing object, otherwise concurrency will prevent any update
                        db.insert_one_object(result, delete_embedding=False)
                        prev_key_update = False
                    # print(key)

                referencing_class, referencing_id, referencing_version, path = cloudpickle.loads(value)
                result_referencing_class = referencing_class
                orig_check_class = check_class = db.get_class_by_name(referencing_class)
                result_reference = db.get_single(check_class, referencing_id, referencing_version)
                if result_reference is None:
                    alternatives = getattr(netex, referencing_class + 'RefStructure')
                    for sc in all_subclasses(alternatives):
                        try:
                            check_class = getattr(netex, sc.__name__.replace('RefStructure', ''))

                            result_reference = db.get_single(check_class, referencing_id, referencing_version)
                            if result_reference:
                                result_referencing_class = get_object_name(check_class)
                                break
                        except AttributeError:
                            pass

                if result_reference is None:
                    print(f"referencing {key} {path} {referencing_id} cannot be found")

                elif not path.endswith("_attribute") and (result_reference.version != referencing_version or result_referencing_class != referencing_class):
                    # print(result_reference.version, referencing_version)
                    inv_key = db.serializer.encode_key(referencing_id, referencing_version, orig_check_class, True)

                    if cursor_referencing_inwards.set_range(inv_key):
                        for inv_value in cursor_referencing_inwards.iternext_dup():
                            parent_clazz, parent_id, parent_version, embedding_path = cloudpickle.loads(inv_value)
                            parent_class = db.get_class_by_name(parent_clazz)

                            check_key = db.serializer.encode_key(parent_id, parent_version, parent_class, True)
                            if check_key == key:
                                if not prev_key_update:
                                    result = db.get_single(parent_class, parent_id, parent_version)
                                split = split_path(embedding_path)
                                attribute = resolve_attr(result, split)
                                # print(embedding_path)
                                attribute.name_of_ref_class = result_referencing_class
                                attribute.version = result_reference.version
                                prev_key_update = True

                                db.delete_key_value_on_queue(db.db_referencing, key, value)

                prev_key = key

            if prev_key_update:
                # print(f"Would update {result.id} {attribute}")
                # TODO: we must aggregate all operations per single referencing object, otherwise concurrency will prevent any update
                db.insert_one_object(result, delete_embedding=False)
                prev_key_update = False
            # print(key)
