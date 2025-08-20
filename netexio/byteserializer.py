from __future__ import annotations
import re
import netex
netex.set_all = frozenset(netex.__all__)  # type: ignore # This is the true performance step

def _all_subclasses(cls):
    seen = set()
    stack = [cls]
    while stack:
        c = stack.pop()
        for s in c.__subclasses__():
            if s not in seen:
                seen.add(s)
                stack.append(s)
    return seen


netex.set_ref_types = frozenset(
    {netex.VersionOfObjectRef, netex.VersionOfObjectRefStructure}
    | _all_subclasses(netex.VersionOfObjectRef)
    | _all_subclasses(netex.VersionOfObjectRefStructure)
)

# The LMDB format follows the following databases:
#
# Database: _class_idx
# Is created when the database is initialised, allows for distribution with different ABI.
# key: class_name: uint16_t
# value: class_idx: uint16_t
#
# Database: _id_idx
# Mapping from an object identifier towards a class and unique internal idx
# key: (encoded) id must allow for prefer search of an identifier, without prior knowledge of version or class.
# value: a struct consisting of a class_idx: uint16_t and unique_idx: uint32_t
#
# Database: [[class_idx]] (the numeric value of the class to be stored)
# key: unique_idx
# value: lz4, pickle encoded, python object
#
# Database: _referencing_outwards
# key: unique_idx
# value: a struct consisting of a class_idx | is_embedded: uint16_t and unique_idx: uint32_t
#
# Database: _referencing_inwards
# key: unique_idx
# value: a struct consisting of a class_idx: uint16_t and unique_idx: uint32_t
#
# Database: _unresolved
# key: unique_idx of the object holding the reference
# value: a tuple consisting of the id, version and class_idx of the referenced object
#
# Database: _embedding
# key: unique_idx
# value: struct consisting of a class_idx: uint16_t and unique_idx: uint32_t, path_indices list[uint_8]
import itertools
import string
import struct
from pathlib import Path
from types import TracebackType
from typing import T, cast, Generator, Any, Optional, Type, Literal, Iterable

import cloudpickle
import lmdb
import lz4.frame
from functools import lru_cache

from utils.utils import get_interesting_classes


class BinarySerializer:
    SEPARATOR = ord("-")
    SPECIAL_CHAR = ord("*")
    WORD_MASK = "#"

    classes: list[type]
    names: list[str]

    @staticmethod
    def get_object_name(clazz: type[T]) -> str:
        return getattr(getattr(clazz, "Meta", None), "name", str(clazz.__name__))

    @staticmethod
    def encode_string(value: str, obj_name: str | None, mask: bool = True) -> bytes:
        """Encodes a string by replacing special characters and masking the object name."""
        if mask and obj_name is not None:
            value = re.sub(rf"\b{re.escape(obj_name)}\b", BinarySerializer.WORD_MASK, value.upper())
        return bytes(
            (ord(char) if char in string.ascii_uppercase or char in string.digits or char == BinarySerializer.WORD_MASK else BinarySerializer.SPECIAL_CHAR)
            for char in value
        )

    def encode_key(self, id: str | None, version: str | None, clazz: type[T], include_clazz: bool = False) -> bytes:
        obj_name = BinarySerializer.get_object_name(clazz).upper()
        encoded_bytes = bytearray()

        if id is not None:
            encoded_bytes.extend(BinarySerializer.encode_string(id, obj_name))
            encoded_bytes.append(BinarySerializer.SEPARATOR)

        if version is not None and version != "any":
            encoded_bytes.extend(BinarySerializer.encode_string(version, obj_name))
            encoded_bytes.append(BinarySerializer.SEPARATOR)

        if include_clazz:
            encoded_bytes.extend(self.classes.index(clazz).to_bytes(2, 'little'))

        return bytes(encoded_bytes)

    def split_key(self, key: bytes) -> list[bytes]:
        return key.split(bytes([BinarySerializer.SEPARATOR]))

    def __init__(self, classes: list[type]):
        self.classes = classes
        self.name_object = {BinarySerializer.get_object_name(x): x for x in classes}

DB_CLASS_IDX = b'_class_idx'
DB_UNRESOLVED = b'_unresolved'
DB_ID_IDX = b'_id_idx'
DB_REFERENCE_FORWARD = b'_reference_forward'
DB_REFERENCE_INWARD = b'_reference_inward'

class Database:
    readonly: bool
    max_dbs: int
    initial_size: int

    def __init__(self, path: Path, serializer: BinarySerializer, readonly: bool = True):
        if readonly and not path.exists():
            raise

        self.path = path
        self.serializer = serializer
        self.readonly = readonly
        self.max_dbs = 128
        self.initial_size = 4 * 1024**3
        self.last_entry = itertools.count() # TODO: start based on entries or last key in _id_idx


    def __enter__(self) -> Database:
        new_database = not self.path.exists()

        self.env = lmdb.open(
            str(self.path),
            max_dbs=self.max_dbs,
            map_size=self.initial_size,
            writemap=False,
            metasync=False,
            sync=False,
            subdir=True,
        )

        if new_database:
            self.populate_class_idx()

        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> Literal[False]:
        self.env.close()
        return False  # Allow errors to propagate!

    def populate_class_idx(self) -> None:
        if self.readonly:
            raise

        with self.env.begin(write=True) as txn:
            db_class_idx = self.env.open_db(DB_UNRESOLVED, create=True, txn=txn, integerkey=True)
            for idx, clazz_name in enumerate(self.serializer.name_object.keys()):
                txn.put(idx.to_bytes(2, 'little'), clazz_name.encode('utf-8'), db=db_class_idx)


    @staticmethod
    @lru_cache(maxsize=None)
    def _dc_field_names(cls: type) -> tuple[str, ...]:
        return tuple(cls.__dataclass_fields__.keys())

    @staticmethod
    def recursive_attributes(obj: T, depth: list[int]) -> Generator[tuple[Any, tuple[int, ...]], None, None]:
        # We skip data_source_ref_attribute and  responsibility_set_ref_attribute later in the pipeline
        # data_source_ref_attribute = getattr(obj, "data_source_ref_attribute", None)
        # if data_source_ref_attribute:
        #     yield DataSourceRefStructure(ref=data_source_ref_attribute), depth + ["data_source_ref_attribute"]

        # responsibility_set_ref_attribute = getattr(obj, "responsibility_set_ref_attribute", None)
        # if responsibility_set_ref_attribute:
        #     yield ResponsibilitySetRef(ref=responsibility_set_ref_attribute), depth + ["responsibility_set_ref_attribute"]

        mydepth = depth
        mydepth.append(0)
        field_names = Database._dc_field_names(obj.__class__)
        for col_idx, field_name in enumerate(field_names):
            mydepth[-1] = col_idx
            v = getattr(obj, field_name, None)
            if v is not None:
                if v.__class__ in netex.set_ref_types:  # type: ignore
                    yield v, tuple(mydepth)

                else:
                    if hasattr(v, "__dataclass_fields__") and v.__class__.__name__ in netex.set_all:  # type: ignore
                        if hasattr(v, "id"):
                            yield v, tuple(mydepth)
                        yield from Database.recursive_attributes(v, mydepth)
                    elif v.__class__ in (list, tuple):
                        mydepth.append(0)
                        for j, x in enumerate(v):
                            mydepth[-1] = j
                            if x is not None:
                                if x.__class__ in netex.set_ref_types:  # type: ignore
                                    yield x, tuple(
                                        mydepth)  # TODO: mydepth result is incorrect when list() but not as iterator
                                elif hasattr(x,
                                             "__dataclass_fields__") and x.__class__.__name__ in netex.set_all:  # type: ignore
                                    if hasattr(x, "id"):
                                        yield x, tuple(mydepth)
                                    yield from Database.recursive_attributes(x, mydepth)
                        mydepth.pop()
        mydepth.pop()

    def only_references(self, deserialized: T) -> Generator[tuple[type[T], str, str], None, None]:
        assert deserialized.id is not None, "deserialised.id must not be none"

        for obj, path in Database.recursive_attributes(deserialized, []):
            if hasattr(obj, "ref"):
                assert obj.ref is not None, "Object ref must not be none"
                if obj.name_of_ref_class is None:
                    # Hack, because NeTEx does not define the default name of ref class yet
                    if obj.__class__.__name__.endswith("RefStructure"):
                        obj.name_of_ref_class = obj.__class__.__name__[0:-12]
                    elif obj.__class__.__name__.endswith("Ref"):
                        obj.name_of_ref_class = obj.__class__.__name__[0:-3]

                if obj.name_of_ref_class not in self.serializer.name_object.keys():
                    # log_once(logging.WARN, "unknown name_of_ref_class", "Reference Class cannot be found in serializer")
                    continue

                yield (
                    self.serializer.name_object[obj.name_of_ref_class],  # The object that the reference is towards
                    obj.ref,
                    getattr(obj, "version", "any"),
                )

    def insert_objects_on_queue(self, klass: type[T], objects: Iterable[T], empty: bool = False) -> None:
        print(klass)

        if self.readonly:
            raise

        class_idx = self.serializer.classes.index(klass)

        with self.env.begin(write=True) as txn:
            db = self.env.open_db(class_idx.to_bytes(2, 'little'), txn=txn, create=True, integerkey=True)
            db_unresolved = self.env.open_db(DB_UNRESOLVED, txn=txn, create=True, integerkey=True, dupsort=True, integerdup=True)
            db_idx = self.env.open_db(DB_ID_IDX, txn=txn, create=True, )

            if empty:
                txn.drop(db=db, delete=False)

            for obj in objects:
                key = next(self.last_entry)

                full_key = struct.pack("<HI", class_idx, key)
                for referenced_class_idx, ref, version in self.only_references(obj):
                    unresolved_value = self.serializer.encode_key(ref, version, referenced_class_idx, include_clazz=True)
                    txn.put(full_key, unresolved_value, db=db_unresolved)

                value = cast(bytes, lz4.frame.compress(cloudpickle.dumps(obj)))
                txn.put(key.to_bytes(4, 'little'), value, db=db)
                txn.put(self.serializer.encode_key(obj.id, obj.version if hasattr(obj, "version") else None, obj.__class__, include_clazz=True), full_key, db=db_idx)


    def write_object(self, obj: T, idx: bytes | None = None) -> None:
        if self.readonly:
            raise

        if idx is not None:
            # TODO: we may need to clean the existing references
            pass

        class_idx = self.serializer.classes.index(obj.__class__).to_bytes(2, 'little')
        key = (idx if idx else next(self.last_entry)).to_bytes(4, 'little')

        with self.env.begin(write=True) as txn:
            db_unresolved = self.env.open_db(DB_UNRESOLVED, txn=txn,create=True, integerkey=True, dupsort=True, integerdup=True)
            full_key = struct.pack("<HI", class_idx, key)
            for referenced_class_idx, ref, version in Database.only_references(obj):
                unresolved_value = self.serializer.encode_key(ref, version, referenced_class_idx, include_clazz=True)
                txn.put(full_key, unresolved_value, db=db_unresolved)

            db = self.env.open_db(class_idx, txn=txn, create=True, integerkey=True)
            value = cast(bytes, lz4.frame.compress(cloudpickle.dumps(obj)))
            txn.put(key, value, db=db)

            db = self.env.open_db(DB_ID_IDX, txn=txn, create=True)
            txn.put(obj.id.encode("utf-8"), full_key, db=db) # TODO: smart encoding key

    def resolve(self) -> None:
        if self.readonly:
            raise

        with self.env.begin(write=True) as txn:
            db_unresolved = self.env.open_db(DB_UNRESOLVED, create=False, txn=txn, integerkey=True, dupsort=True, integerdup=True)
            db_id_idx = self.env.open_db(DB_ID_IDX, txn=txn, create=False)
            db_reference_forward = self.env.open_db(DB_REFERENCE_FORWARD, create=False, txn=txn, integerkey=True, dupsort=True, integerdup=True)
            db_reference_inward = self.env.open_db(DB_REFERENCE_INWARD, create=False, txn=txn, integerkey=True, dupsort=True, integerdup=True)

            if not db_unresolved or not db_id_idx:
                return

            unresolved_cursor = db_unresolved.cursor()
            for idx, value in unresolved_cursor:
                resolved_idx = txn.get(value, db=db_id_idx) # This will be the id + version + class check
                if not resolved_idx:
                    parts = self.serializer.split_key(value)
                    parts[-1] = ''
                    prefix = bytes([BinarySerializer.SEPARATOR]).join(parts)
                    cursor = txn.cursor(db=db_id_idx)
                    if cursor.set_range(prefix): # This will be the id + version check
                        while bytes(cursor.key()).startswith(prefix):
                            resolved_idx = cursor.key()
                            break

                    if not resolved_idx:
                        parts.pop()
                        parts[-1] = ''
                        prefix = bytes([BinarySerializer.SEPARATOR]).join(parts)
                        cursor = txn.cursor(db=db_id_idx)
                        if cursor.set_range(prefix): # This will be the id check
                            while bytes(cursor.key()).startswith(prefix):
                                resolved_idx = cursor.key()
                                break

                if resolved_idx:
                    unresolved_cursor.delete()
                    txn.put(idx, resolved_idx, db=db_reference_forward)
                    txn.put(resolved_idx, idx, db=db_reference_inward)

# _, _, interesting_classes = get_interesting_classes()
# db = Database(Path("/tmp/test.lmdb"), BinarySerializer(classes=interesting_classes), readonly=False)