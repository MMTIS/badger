from itertools import count
from pathlib import Path
from types import TracebackType
from typing import Optional, Type, Literal, Iterable, Generator, Self, Any

import lmdb

from domain.netex.services.model_typing import Tid
from domain.netex.services.recursive_attributes import only_references
from domain.netex.services.utils import get_boring_classes
from domain.utils import get_object_name
from storage.interface import Storage
from storage.lmdb.serialization.byteserializer import ByteSerializer

DB_CLASS_IDX = b'_class_idx'
DB_UNRESOLVED = b'_unresolved'
DB_ID_IDX = b'_id_idx'
DB_REFERENCE_OUTWARD = b'_reference_outward'
DB_REFERENCE_INWARD = b'_reference_inward'


class LmdbStorage(Storage):
    readonly: bool
    max_dbs: int
    initial_size: int
    class_idx: dict[type, bytes]
    idx_class: dict[bytes, type]
    class_name_idx: dict[str, bytes]
    serializer: ByteSerializer

    def __init__(self, path: Path, readonly: bool = True, initial_size: int = 4 * 1024**3):
        if readonly and not path.exists():
            raise

        self.path = path
        self.readonly = readonly
        self.max_dbs = 128
        self.initial_size = initial_size
        self.last_entry = count() # TODO: change to context of DB
        self.class_idx = {}
        self.idx_class = {}
        self.class_name_idx = {}
        self.serializer = ByteSerializer(get_boring_classes())

    def _populate_class_idx(self) -> None:
        if self.readonly:
            raise

        with self.env.begin(write=True) as txn:
            db_class_idx = self.env.open_db(key=DB_CLASS_IDX, txn=txn, create=True, integerkey=False)
            for idx, clazz in enumerate(self.serializer.name_object.values()):
                clazz_name = get_object_name(clazz)
                txn.put(idx.to_bytes(2, 'little'), clazz_name.encode('utf-8'), db=db_class_idx)

            self.env.open_db(DB_UNRESOLVED, txn=txn, create=True, integerkey=True, dupsort=True)
            self.env.open_db(DB_ID_IDX, txn=txn, create=True)
            self.env.open_db(DB_REFERENCE_OUTWARD, create=True, txn=txn, integerkey=True, dupsort=True, integerdup=True)
            self.env.open_db(DB_REFERENCE_INWARD, create=True, txn=txn, integerkey=True, dupsort=True, integerdup=True)

    def _restore_class_idx(self) -> None:
        with self.env.begin(write=False) as txn:
            db_class_idx = self.env.open_db(key=DB_CLASS_IDX, txn=txn, create=True, integerkey=False)
            for idx, name in txn.cursor(db=db_class_idx):
                clazz = self.serializer.name_object[name.decode('utf-8')]
                self.idx_class[idx] = clazz
                self.class_name_idx[get_object_name(clazz)] = idx
                self.class_idx[clazz] = idx

        self.serializer.set_class_idx(self.class_idx)

    def __enter__(self) -> Self:
        new_database = not self.path.exists()

        self.env = lmdb.open(
            self.path.as_posix(),
            max_dbs=self.max_dbs,
            map_size=self.initial_size,
            writemap=False,
            metasync=False,
            sync=False,
            subdir=True,
        )

        if new_database:
            self._populate_class_idx()

        self._restore_class_idx()

        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> Literal[False]:
        self.env.close()
        return False  # Allow errors to propagate!

    def db_names(self) -> dict[bytes, type]:
        db_names: dict[bytes, type] = {}
        with self.env.begin(write=False) as txn:
            for db_name, _ in txn.cursor():
                if db_name in (DB_CLASS_IDX, DB_UNRESOLVED, DB_ID_IDX, DB_UNRESOLVED, DB_REFERENCE_INWARD, DB_REFERENCE_OUTWARD):
                    continue

                clazz = self.idx_class.get(db_name, None)
                if clazz is not None:
                    db_names[db_name] = clazz
        return db_names

    def clean(self) -> None:
        with self.env.begin(write=True) as txn:
            for db_name, _ in txn.cursor():
                db = self.env.open_db(db_name, txn=txn)
                if db:
                    txn.drop(db=db, delete=True)
        self._populate_class_idx()

    def insert_objects_on_queue(self, klass: type[Tid], objects: Iterable[Tid], empty: bool = False) -> None:
        print(klass)

        if self.readonly:
            raise

        this_class_idx = self.class_idx[klass]

        with self.env.begin(write=True) as txn:
            db = self.env.open_db(this_class_idx, txn=txn)
            db_unresolved = self.env.open_db(DB_UNRESOLVED, txn=txn, create=False)
            db_id_idx = self.env.open_db(DB_ID_IDX, txn=txn, create=False)
            db_reference_forward = self.env.open_db(DB_REFERENCE_OUTWARD, txn=txn, create=False)
            db_reference_inward = self.env.open_db(DB_REFERENCE_INWARD, txn=txn, create=False)

            if empty:
                txn.drop(db=db, delete=False)

            for obj in objects:
                key = int(next(self.last_entry))

                full_key = ((int.from_bytes(this_class_idx, 'little') << 32) | key).to_bytes(8, 'little')
                for referenced_class_idx, ref, version in only_references(obj, self.serializer):
                    unresolved_value = self.serializer.encode_key(ref, version, referenced_class_idx, include_clazz=True)
                    resolved_idx = txn.get(unresolved_value, db=db_id_idx)
                    if resolved_idx:
                        txn.put(full_key, resolved_idx, db=db_reference_forward)
                        txn.put(resolved_idx, full_key, db=db_reference_inward)
                    else:
                        txn.put(full_key, unresolved_value, db=db_unresolved)

                value = self.serializer.marshall(obj, klass)
                txn.put(key.to_bytes(4, 'little'), value, db=db)
                my_id = self.serializer.encode_key(str(obj.id), obj.version if hasattr(obj, "version") else None, obj.__class__, include_clazz=True)
                txn.put(my_id, full_key, db=db_id_idx)

    def _load_references(self, full_key: bytes, db_direction: bytes) -> Generator[tuple[type, bytes], None, None]:
        with self.env.begin(write=False) as txn:
            db_reference = self.env.open_db(db_direction, txn=txn, create=False)

            cursor = txn.cursor(db_reference)
            if cursor.set_key(full_key):
                for reference_key in cursor.iternext_dup():
                    class_idx, reference_local_key = ByteSerializer.full_key_to_idx(reference_key)
                    yield self.idx_class[class_idx], reference_local_key

    def load_references_by_clazz_key(self, clazz: type, key: bytes, inwards: bool) -> Generator[tuple[type, bytes], None, None]:
        db_direction = DB_REFERENCE_INWARD if inwards else DB_REFERENCE_OUTWARD
        this_class_idx = self.class_idx[clazz]
        full_key = ((int.from_bytes(this_class_idx, 'little') << 32) | int.from_bytes(key, 'little')).to_bytes(8, 'little')
        yield from self._load_references(full_key, db_direction)

    def load_references_by_object(self, obj: Tid, inwards: bool) -> Generator[tuple[type, bytes], None, None]:
        db_direction = DB_REFERENCE_INWARD if inwards else DB_REFERENCE_OUTWARD
        if hasattr(obj, 'idx'):
            full_key = obj.idx
            yield from self._load_references(full_key, db_direction)
        else:
            with self.env.begin(write=False) as txn:
                db_id_idx = self.env.open_db(DB_ID_IDX, txn=txn, create=False)
                key = self.serializer.encode_key(str(obj.id), obj.version if hasattr(obj, "version") else None, obj.__class__, include_clazz=True)
                full_key = txn.get(key, db=db_id_idx)
                yield from self._load_references(full_key, db_direction)

    def load_object_by_full_key(self, full_key: bytes) -> Any:
        this_clazz_idx, key = self.serializer.full_key_to_idx(full_key)
        clazz = self.idx_class[this_clazz_idx]
        with self.env.begin(write=False) as txn:
            db = self.env.open_db(this_clazz_idx, txn=txn, create=False)
            value = txn.get(key, db=db)
            obj: Tid = self.serializer.unmarshall(value, clazz)
            return obj

    def load_object(self, clazz: type[Tid], key: bytes) -> Tid:
        this_class_idx = self.class_idx[clazz]
        with self.env.begin(write=False) as txn:
            db = self.env.open_db(this_class_idx, txn=txn, create=False)
            value = txn.get(key, db=db)
            obj = self.serializer.unmarshall(value, clazz)
            # idx = ((int.from_bytes(this_class_idx, 'little') << 32) | int.from_bytes(key, 'little')).to_bytes(8, 'little')
            return obj

    def scan_objects(self, clazz: type[Tid], start_key: bytes | None, limit: int) -> Generator[bytes, None, None]:
        with self.env.begin(write=False) as txn:
            db = self.env.open_db(self.class_idx[clazz], txn=txn, create=False)
            if not db:
                return

            cursor = txn.cursor(db)
            # Position the cursor at the correct starting point
            if start_key:
                # set_range() positions cursor at or after start_key.
                # If it lands exactly on start_key, we need to move to the next item
                # to avoid fetching the last item of the previous page again.
                if cursor.set_range(start_key):
                    if cursor.key() == start_key:
                        if not cursor.next():
                            return  # start_key was the last item
                else:
                    return  # No keys at or after start_key
            elif not cursor.first():
                return  # Database is empty

            count = 0

            # Iterate over keys only for maximum efficiency
            for key in cursor.iternext(keys=True, values=False):
                yield key
                count += 1
                if count >= limit:
                    break