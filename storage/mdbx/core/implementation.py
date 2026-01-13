from datetime import datetime
from pathlib import Path
from sys import exception
from types import TracebackType
from typing import Optional, Type, Literal, Iterable, Generator, Self, Any

from mdbx import Env, MDBXDBFlags
from mdbx.mdbx import TXN, MDBXErrorExc

from domain.netex.model import (
    VersionOfObjectRefStructure,
    EntityStructure,
    NoticeAssignment,
    PassengerStopAssignment,
    ServiceJourneyPattern,
    ServiceJourney,
    DayTypeAssignment,
)
from domain.netex.services.model_typing import Tid
from domain.netex.services.recursive_attributes import only_references
from domain.netex.services.utils import get_boring_classes
from domain.utils import get_object_name
from storage.mdbx.serialization.byteserializer import ByteSerializer

DB_CLASS_IDX = bytes(b'_class_idx')
DB_UNRESOLVED = bytes(b'_unresolved')
DB_ID_IDX = bytes(b'_id_idx')
DB_REFERENCE_OUTWARD = bytes(b'_reference_outward')

DB_UNRESOLVED_FLAGS = MDBXDBFlags.MDBX_INTEGERKEY | MDBXDBFlags.MDBX_DUPSORT
DB_ID_IDX_FLAGS = MDBXDBFlags.MDBX_DB_DEFAULTS
DB_REFERENCE_OUTWARD_FLAGS = MDBXDBFlags.MDBX_INTEGERKEY | MDBXDBFlags.MDBX_DUPSORT | MDBXDBFlags.MDBX_DUPFIXED | MDBXDBFlags.MDBX_INTEGERDUP

class MdbxStorage:
    readonly: bool
    max_dbs: int
    initial_size: int
    class_idx: dict[type[EntityStructure], bytes]
    idx_class: dict[bytes, type[EntityStructure]]
    class_name_idx: dict[str, bytes]
    serializer: ByteSerializer

    def __init__(self, path: Path, readonly: bool = True, initial_size: int = 8 * 1024**3):
        if readonly and not path.exists():
            raise

        self.path = path
        self.readonly = readonly
        self.max_dbs = 128
        self.initial_size = initial_size
        self.class_idx = {}
        self.idx_class = {}
        self.class_name_idx = {}
        self.serializer = ByteSerializer(get_boring_classes())

    def _populate_class_idx(self) -> None:
        if self.readonly:
            raise

        with self.env.rw_transaction() as txn:
            with txn.create_map(name=DB_CLASS_IDX, flags= DB_ID_IDX_FLAGS) as db_class_idx:
                for idx, clazz in enumerate(self.serializer.name_object.values()):
                    clazz_name = get_object_name(clazz)
                    db_class_idx.put(txn, idx.to_bytes(2, 'little'), clazz_name.encode('utf-8'))

            txn.create_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            txn.create_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
            txn.create_map(name=DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)
            txn.commit()

    def _restore_class_idx(self) -> None:
        with self.env.ro_transaction() as txn:
            with txn.open_map(name=DB_CLASS_IDX, flags=DB_ID_IDX_FLAGS) as db_class_idx:
                with txn.cursor(db_class_idx) as cur:
                    for idx, name in cur.iter():
                        clazz = self.serializer.name_object[name.decode('utf-8')]
                        self.idx_class[idx] = clazz
                        self.class_name_idx[get_object_name(clazz)] = idx
                        self.class_idx[clazz] = idx

        self.serializer.set_class_idx(self.class_idx)

    def __enter__(self) -> Self:
        new_database = not self.path.exists()

        self.env = Env(
            self.path.as_posix(),
            maxdbs=self.max_dbs,
            # map_size=self.initial_size,
            # writemap=True,
            # metasync=True,
            # sync=True,
            # subdir=True,
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

    def db_names(self, txn: TXN = None) -> dict[bytes, Tid]:
        db_names: dict[bytes, Tid] = {}
        if txn is None:
            txn = self.env.ro_transaction()
        with txn.cursor(db=None) as cur:
            for db_name, _ in cur.iter():
                if db_name in (DB_CLASS_IDX, DB_UNRESOLVED, DB_ID_IDX, DB_UNRESOLVED, DB_REFERENCE_OUTWARD):
                    continue

                clazz = self.idx_class.get(db_name, None)
                if clazz is not None:
                    db_names[db_name] = clazz
        return db_names

    def db_names_iter(self, txn: TXN) -> Generator[Tid, None, None]:
        db_names: dict[bytes, Tid] = {}
        with txn.cursor(db=None) as cur:
            for db_name, _ in cur.iter():
                if db_name in (DB_CLASS_IDX, DB_UNRESOLVED, DB_ID_IDX, DB_UNRESOLVED, DB_REFERENCE_OUTWARD):
                    continue

                clazz = self.idx_class.get(db_name, None)
                if clazz is not None:
                    yield clazz

    def clean(self) -> None:
        with self.env.rw_transaction() as txn:
            with txn.cursor() as cur:
                for db_name, _ in cur.iter():
                    dbi = txn.open_map(name=db_name, flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
                    if dbi:
                        dbi.drop(delete=True)
            txn.commit()
        self._populate_class_idx()

    def fetch_all_references_by_class(
        self, txn: TXN, clazzes: set[type[EntityStructure]], skip_existing: bool = False
    ) -> Generator[type[EntityStructure], None, None]:
        # Scan for all collected objects, this delivers their keys, a full key needs to be created for the lookup in reference outward
        # Referenced objects may by itself introduce new references, hence it should be checked if the set contains (already) those
        # When the scan is complete, all referenced objects should be made available via the generator.

        # TODO: filter clazzes on the classes that are actually in the database, this limits the set.

        yielded_set: set[bytes] = set([])
        partial: set[bytes] = set([])

        db_reference_outward = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)
        cursor = txn.cursor(db_reference_outward)
        for it in cursor.iter_dupsort_rows():
            for referencing_key, reference_key in it:
                referencing_class_idx, _ = ByteSerializer.full_key_to_idx(referencing_key)
                reference_class_idx, _ = ByteSerializer.full_key_to_idx(reference_key)

                # print(self.idx_class[referencing_class_idx], "->", self.idx_class[reference_class_idx])

                if self.idx_class[referencing_class_idx] in clazzes:
                    # if self.idx_class[reference_class_idx] not in clazzes:
                    if reference_key not in yielded_set:
                        yielded_set.add(reference_key)

                        # Why is this separate: we don't want to expose objects that we already export,
                        # but we do want to search if there are any references used.
                        partial.add(reference_key)
                    # print(self.idx_class[referencing_class_idx], "->", self.idx_class[reference_class_idx])
                else:
                    # print(self.idx_class[referencing_class_idx])
                    pass

        # Our selected objects may contain references themselves, obviously we need to have those too
        partial_new: set[bytes]

        while True:
            partial_new = set([])
            # print(f'Partial length: {len(partial)}')
            for referencing_key in partial:
                referencing_class_idx, _ = ByteSerializer.full_key_to_idx(referencing_key)
                # print("STEP1", self.idx_class[referencing_class_idx], referencing_key)
                toc=0
                for t in cursor.iter_dupsort_rows(start_key=referencing_key):
                    # print(f'{referencing_key}')
                    toc=toc+1
                    for referencing_key2, reference_key in t:
                        referencing_class_idx2, _ = ByteSerializer.full_key_to_idx(referencing_key2)
                        # print("STEP2", self.idx_class[referencing_class_idx2], referencing_key2)
                        if referencing_key2 != referencing_key:
                            break
                        reference_class_idx, _ = ByteSerializer.full_key_to_idx(reference_key)
                        # print("    STEP2", self.idx_class[reference_class_idx], reference_key)
                        if self.idx_class[reference_class_idx] not in clazzes:
                            if reference_key not in partial_new and reference_key not in yielded_set:
                                partial_new.add(reference_key)
                    break
                # print(f'  end loop 2: {toc}   {str(datetime.now())}')
            # print(f'  end loop 1: {len(partial_new)} -  {str(datetime.now())}')

            if len(partial_new) == 0:
                break
            else:
                yielded_set.update(partial_new)
                partial = partial_new.copy()

        # TODO: we are still missing the objects that are referenced from the reference

        for full_reference in yielded_set:
            # TODO: We can optimise this by grouping the objects per class, and then fetch the groups in one access pattern
            obj = self.load_object_by_full_key(txn, full_reference)
            if skip_existing:
                if obj.__class__ not in clazzes:
                    print(obj.__class__, clazzes)
                    yield obj
            else:
                yield obj

    # TODO: Rename
    def other_classes(self, txn: TXN, clazzes: set[Tid]) -> Generator[Tid, None, None]:
        other_classes = set(self.db_names_iter(txn))
        other_classes -= clazzes

        for clazz in other_classes:
            yield from self.iter_only_objects(txn, clazz)

    def insert_any_object_on_queue(self, txn: TXN, objects: Iterable[Tid]) -> None:
        if self.readonly:
            raise

        db_unresolved = txn.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
        db_id_idx = txn.open_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
        db_reference_outward = txn.open_map(name=DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)

        for obj in objects:
            this_class_idx = self.class_idx[obj.__class__]
            db = txn.create_map(name=this_class_idx)

            my_id = self.serializer.encode_key(str(obj.id), obj.version if hasattr(obj, "version") else None, obj.__class__, include_clazz=True)

            # First: check if the id already exists, then we must overwrite.
            full_key = db_id_idx.get(txn, my_id)
            if full_key is not None:
                full_int = int.from_bytes(full_key, 'little')
                key = full_int & 0xFFFFFFFF
                try:
                    db_reference_outward.delete(txn, full_key)
                except:
                    pass
            else:
                key = db_id_idx.get_sequence(txn, 1)
                full_key = ((int.from_bytes(this_class_idx, 'little') << 32) | key).to_bytes(8, 'little')

            for referenced_class, ref, version in only_references(obj, self.serializer):
                unresolved_value = self.serializer.encode_key(ref, version, referenced_class, include_clazz=True)
                resolved_idx = db_id_idx.get(txn, unresolved_value)
                if resolved_idx:
                    db_reference_outward.put(txn, full_key, resolved_idx)
                else:
                    db_unresolved.put(txn, full_key, unresolved_value)

            value = self.serializer.marshall(obj, obj.__class__)
            db.put(txn, key.to_bytes(4, 'little'), value)
            db_id_idx.put(txn, my_id, full_key)

    # Deprecate this one
    def insert_objects_on_queue(self, klass: type[Tid], objects: Iterable[Tid], empty: bool = False) -> None:
        if self.readonly:
            raise

        this_class_idx = self.class_idx[klass]

        with self.env.rw_transaction() as txn:
            db = txn.create_map(name=this_class_idx)
            db_unresolved = txn.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            db_id_idx = txn.open_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
            db_reference_outward = txn.open_map(name=DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)

            if empty:
                db.drop(txn, delete=False)

            for obj in objects:
                my_id = self.serializer.encode_key(str(obj.id), obj.version if hasattr(obj, "version") else None, obj.__class__, include_clazz=True)

                # First: check if the id already exists, then we must overwrite.
                full_key = db_id_idx.get(txn, my_id)
                if full_key is not None:
                    full_int = int.from_bytes(full_key, 'little')
                    key = full_int & 0xFFFFFFFF
                    try:
                        db_reference_outward.delete(txn, full_key)
                    except:
                        pass
                else:
                    key = db_id_idx.get_sequence(txn, 1)
                    full_key = ((int.from_bytes(this_class_idx, 'little') << 32) | key).to_bytes(8, 'little')

                for referenced_class, ref, version in only_references(obj, self.serializer):
                    unresolved_value = self.serializer.encode_key(ref, version, referenced_class, include_clazz=True)
                    resolved_idx = db_id_idx.get(txn, unresolved_value)
                    if resolved_idx:
                        db_reference_outward.put(txn, full_key, resolved_idx)
                    else:
                        db_unresolved.put(txn, full_key, unresolved_value)

                value = self.serializer.marshall(obj, klass)
                db.put(txn, key.to_bytes(4, 'little'), value)
                db_id_idx.put(txn, my_id, full_key)

            txn.commit()

    def _load_references_by_fullkey(self, txn: TXN, full_key: bytes) -> Generator[bytes, None, None]:
        db = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)
        cursor = txn.cursor(db)
        for it in cursor.iter_dupsort_rows(start_key=full_key):
            for referencing_key, reference_key in it:
                if referencing_key != full_key:
                    break

                yield reference_key
            break

    def _load_references(self, txn: TXN, full_key: bytes) -> Generator[tuple[type[EntityStructure], bytes], None, None]:
        for full_key in self._load_references_by_fullkey(txn, full_key):
            class_idx, reference_local_key = ByteSerializer.full_key_to_idx(full_key)
            yield self.idx_class[class_idx], reference_local_key

    def _load_references_inwards_by_fullkey(self, txn: TXN, full_key: bytes) -> Generator[bytes, None, None]:
        db = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)
        cursor = txn.cursor(db)
        for it in cursor.iter_dupsort_rows():
            for referencing_key, reference_key in it:
                if reference_key == full_key:
                    yield referencing_key

    def _load_references_inwards_by_fullkeys(self, txn: TXN, full_keys: set[bytes]) -> Generator[bytes, None, None]:
        # This will do everything in one sequential scan
        db = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)
        cursor = txn.cursor(db)
        for it in cursor.iter_dupsort_rows():
            for referencing_key, reference_key in it:
                if reference_key in full_keys:
                    yield referencing_key

    def _load_references_inwards(self, txn: TXN, full_key: bytes) -> Generator[tuple[type[EntityStructure], bytes], None, None]:
        for full_key in self._load_references_inwards_by_fullkey(txn, full_key):
            class_idx, referencing_local_key = ByteSerializer.full_key_to_idx(full_key)
            yield self.idx_class[class_idx], referencing_local_key

    def load_references_by_clazz_full_key(self, txn: TXN, full_key: bytes, inwards: bool) -> Generator[bytes, None, None]:
        if inwards:
            yield from self._load_references_inwards_by_fullkey(txn, full_key)
        else:
            yield from self._load_references_by_fullkey(txn, full_key)

    def load_references_by_clazz_key(self, txn: TXN, clazz: type, key: bytes, inwards: bool) -> Generator[tuple[type[EntityStructure], bytes], None, None]:
        this_class_idx = self.class_idx[clazz]
        full_key = ((int.from_bytes(this_class_idx, 'little') << 32) | int.from_bytes(key, 'little')).to_bytes(8, 'little')
        for full_referenced_key in self.load_references_by_clazz_full_key(txn, full_key, inwards):
            referenced_clazz_idx, referenced_key = self.serializer.full_key_to_idx(full_referenced_key)
            yield self.idx_class[referenced_clazz_idx], referenced_key

    def load_references_by_clazz_keys(
        self, txn: TXN, clazz: type, key: set[bytes], inwards: bool
    ) -> Generator[tuple[type[EntityStructure], bytes], None, None]:
        this_class_idx = self.class_idx[clazz]
        full_key = ((int.from_bytes(this_class_idx, 'little') << 32) | int.from_bytes(key, 'little')).to_bytes(8, 'little')
        for full_referenced_key in self.load_references_by_clazz_full_key(txn, full_key, inwards):
            referenced_clazz_idx, referenced_key = self.serializer.full_key_to_idx(full_key)
            yield self.idx_class[referenced_clazz_idx], referenced_key

    def load_references_by_object(self, txn: TXN, obj: Tid, inwards: bool) -> Generator[tuple[type[EntityStructure], bytes], None, None]:
        if hasattr(obj, 'idx'):
            full_key = obj.idx
            if inwards:
                yield from self._load_references_inwards(txn, full_key)
            else:
                yield from self._load_references(txn, full_key)
        else:
            with txn.open_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS) as db_id_idx:
                key = self.serializer.encode_key(str(obj.id), obj.version if hasattr(obj, "version") else None, obj.__class__, include_clazz=True)
                full_key = db_id_idx.get(txn, key)
                if inwards:
                    yield from self._load_references_inwards(txn, full_key)
                else:
                    yield from self._load_references(txn, full_key)

    def load_references_by_object_values(self, txn: TXN, obj: Tid, inwards: bool) -> Generator[EntityStructure, None, None]:
        for clazz, key in self.load_references_by_object(txn, obj, inwards):
            yield self.load_object(txn, clazz, key)

    def load_references_by_object_values_dfs(
        self,
        txn: TXN,
        full_key: bytes,
        inward_classes: set[type[EntityStructure]] = {NoticeAssignment, PassengerStopAssignment, DayTypeAssignment},
    ) -> Generator[EntityStructure, None, None]:

        stack = [full_key]
        visited: set[bytes] = set()

        # Ideally we would only check objects that would make sense to check
        clazz_idxs = [self.class_idx[clazz] for clazz in inward_classes]

        while stack:
            to_visit_inwards: set[bytes] = set([])
            while stack:
                identifier = stack.pop()

                if identifier not in visited:
                    visited.add(identifier)

                    full_key = identifier
                    obj = self.load_object_by_full_key(txn, full_key)
                    if obj:
                        yield obj

                        this_clazz_idx, key = self.serializer.full_key_to_idx(full_key)
                        if this_clazz_idx in clazz_idxs:
                            to_visit_inwards.add(full_key)

                        for _referenced_full_key in self.load_references_by_clazz_full_key(txn, full_key, False):
                            stack.append(_referenced_full_key)

            for _referenced_full_key in self._load_references_inwards_by_fullkeys(txn, to_visit_inwards):
                if _referenced_full_key not in visited:
                    stack.append(_referenced_full_key)

    def load_object_by_id_version(
        self, txn: TXN, id: str, clazz: type[EntityStructure], version: Optional[str] = None
    ) -> Optional[tuple[bytes, Optional[EntityStructure]]]:
        my_id = self.serializer.encode_key(id, version, clazz, include_clazz=True)

        # TODO: Abstract this because
        if version is not None:
            db_id_idx = txn.open_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
            full_key = db_id_idx.get(txn, my_id)
            return full_key, self.load_object_by_full_key(txn, full_key)

        else:
            prefix, _, _ = self.serializer.split_key(my_id)
            cursor = txn.cursor(db=DB_ID_IDX)
            for check_key, resolved_idx in cursor.iter(prefix):
                if check_key.startswith(prefix):
                    return resolved_idx, self.load_object_by_full_key(txn, resolved_idx)

    def load_object_by_full_key(self, txn: TXN, full_key: bytes) -> Optional[EntityStructure]:
        this_clazz_idx, key = self.serializer.full_key_to_idx(full_key)
        clazz = self.idx_class[this_clazz_idx]
        with txn.open_map(name=this_clazz_idx, flags=MDBXDBFlags.MDBX_DB_DEFAULTS) as db:
            value = db.get(txn, key)
            if value:
                obj: EntityStructure = self.serializer.unmarshall(value, clazz)
                return obj

        return None

    def load_object(self, txn: TXN, clazz: type[Tid], key: bytes) -> Tid:
        this_class_idx = self.class_idx[clazz]
        with txn.open_map(name=this_class_idx, flags=MDBXDBFlags.MDBX_DB_DEFAULTS) as db:
            value = db.get(txn, key)
            assert value is not None
            # if value is None:
            #    print(clazz, key)

            obj = self.serializer.unmarshall(value, clazz)
            # idx = ((int.from_bytes(this_class_idx, 'little') << 32) | int.from_bytes(key, 'little')).to_bytes(8, 'little')
            return obj

    def load_object_by_reference(self, txn: TXN, ref: VersionOfObjectRefStructure) -> Optional[EntityStructure]:
        with txn.open_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS) as db_id_idx:
            if ref.name_of_ref_class is not None:
                # The optimal situation, we can search for the id class in the right place
                name_of_ref_class = str(ref.name_of_ref_class.value if hasattr(ref.name_of_ref_class, 'value') else ref.name_of_ref_class)
                key = self.serializer.encode_key(
                    str(ref.ref), ref.version if hasattr(ref, "version") else None, self.idx_class[self.class_name_idx[name_of_ref_class]], include_clazz=True
                )
                full_key = db_id_idx.get(txn, key)
                return self.load_object_by_full_key(txn, full_key)

            if True:
                print("Fallback...")
                prefix = self.serializer.encode_key(str(ref.ref), ref.version if hasattr(ref, "version") else None)
                cursor = txn.cursor(db=DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
                for check_key, resolved_idx in cursor.iter(prefix):
                    if check_key.startswith(prefix):
                        referenced_class_idx, referenced_key = self.serializer.full_key_to_idx(resolved_idx)
                        # We now want to check if the referenced_class_idx actually matches what should be "possible"

                        return self.load_object(txn, self.idx_class[referenced_class_idx], referenced_key)
                    else:
                        break
        return None

    def scan_objects(self, txn: TXN, clazz: type[Tid], start_key: bytes | None = None, limit: int | None = None) -> Generator[bytes, None, None]:
        with txn.open_map(name=self.class_idx[clazz], flags=MDBXDBFlags.MDBX_DB_DEFAULTS) as db:
            with txn.cursor(db) as cursor:
                count = 0

                # Iterate over keys only for maximum efficiency
                for key, _value in cursor.iter(start_key=start_key):  # TODO: MDBX_SET
                    yield key
                    if limit:
                        count += 1
                        if count >= limit:
                            break

    def iter_objects(
        self, txn: TXN, clazz: type[EntityStructure], start_key: bytes | None = None, limit: int | None = None
    ) -> Generator[tuple[bytes, Tid], None, None]:
        try:
            db = txn.open_map(name=self.class_idx[clazz], flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
        except:  # TODO: Better catching by pymdbx proper exceptions
            return

        with txn.cursor(db) as cursor:
            count = 0

            for key, value in cursor.iter(start_key=start_key):
                yield key, self.serializer.unmarshall(value, clazz)
                if limit:
                    count += 1
                    if count >= limit:
                        break

    def iter_only_objects(self, txn: TXN, clazz: type[Tid], start_key: bytes | None = None, limit: int | None = None) -> Generator[Tid, None, None]:
        for _key, obj in self.iter_objects(txn, clazz, start_key, limit):
            yield obj

    def copy_map(self, txn: TXN, remote_storage: "MdbxStorage", remote_txn: TXN, clazz: type[EntityStructure]) -> None:
        remote_storage.insert_any_object_on_queue(remote_txn, self.iter_only_objects(txn, clazz))
        """
        We missen hier de afhandeling van db_id's etc.
        with remote_txn.create_map(name=remote_storage.class_idx[clazz]) as db_destination:
            try:
                with txn.open_map(name=self.class_idx[clazz], flags=MDBXDBFlags.MDBX_DB_DEFAULTS) as db_source:
                    with txn.cursor(db_source) as cursor:
                        for key, value in cursor.iter():
                            db_destination.put(remote_txn, key, value)
            except MDBXErrorExc as e:
                if e.errno == -30798:
                    pass
                else:
                    raise
        """
