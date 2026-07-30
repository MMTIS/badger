import logging
from collections import defaultdict
from pathlib import Path
from types import TracebackType
from collections.abc import Iterable, Generator
from typing import Optional, Type, Literal, Self, cast

from mdbx import Env, MDBXDBFlags
from mdbx.mdbx import TXN

from domain.netex.model import (
    VersionOfObjectRefStructure,
    EntityStructure,
    NoticeAssignment,
    PassengerStopAssignment,
    ScheduledStopPoint,
    DayTypeAssignment,
    NameOfClass,
)
from domain.netex.services.model_typing import Tid
from domain.netex.services.recursive_attributes import only_references
from domain.netex.services.utils import get_boring_classes
from domain.utils import get_object_name
from storage.mdbx.serialization.combinedserializer import CombinedSerializer
from utils.aux_logging import log_all
from storage.interface import Serializer
from ctypes import c_uint64

DB_CLAZZ_IDX = bytes(b'_clazz_idx')
DB_UNRESOLVED = bytes(b'_unresolved')
DB_ID_IDX = bytes(b'_id_idx')
DB_REFERENCE_OUTWARD = bytes(b'_reference_outward')
DB_REFERENCE_INWARD = bytes(b'_reference_inwards')
DB_EMBEDDED_ID_IDX = bytes(b'_embedded_id_idx')

DB_UNRESOLVED_FLAGS = MDBXDBFlags.MDBX_INTEGERKEY | MDBXDBFlags.MDBX_DUPSORT
DB_ID_IDX_FLAGS = MDBXDBFlags.MDBX_DB_DEFAULTS
DB_EMBEDDED_ID_IDX_FLAGS = MDBXDBFlags.MDBX_DB_DEFAULTS
DB_REFERENCE_OUTWARD_FLAGS = MDBXDBFlags.MDBX_INTEGERKEY | MDBXDBFlags.MDBX_DUPSORT | MDBXDBFlags.MDBX_DUPFIXED | MDBXDBFlags.MDBX_INTEGERDUP
DB_REFERENCE_INWARD_FLAGS = MDBXDBFlags.MDBX_INTEGERKEY | MDBXDBFlags.MDBX_DUPSORT | MDBXDBFlags.MDBX_DUPFIXED | MDBXDBFlags.MDBX_INTEGERDUP


class MdbxStorage:
    readonly: bool
    max_dbs: int
    initial_size: int
    clazz_idx: dict[type[EntityStructure], bytes]
    idx_clazz: dict[bytes, type[EntityStructure]]

    def __init__(self, path: Path, readonly: bool = True, initial_size: int = 8 * 1024**3):
        if readonly and not path.exists():
            raise

        self.path = path
        self.readonly = readonly
        self.max_dbs = 128
        self.initial_size = initial_size
        self.clazz_idx = {}
        self.idx_clazz = {}
        self.serializer = CombinedSerializer(get_boring_classes())

    def _populate_clazz_idx(self) -> None:
        if self.readonly:
            raise

        with self.env.rw_transaction() as txn:
            with txn.create_map(name=DB_CLAZZ_IDX, flags=DB_ID_IDX_FLAGS) as db_clazz_idx:
                for idx, clazz in enumerate(self.serializer.name_object.values()):
                    clazz_name = get_object_name(clazz)
                    try:
                        _name_of_class = NameOfClass(clazz_name)
                        db_clazz_idx.put(txn, idx.to_bytes(2, 'little'), clazz_name.encode('utf-8'))
                    except ValueError:
                        pass

            txn.create_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            txn.create_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
            txn.create_map(name=DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)
            txn.commit()

    def _restore_clazz_idx(self) -> None:
        with self.env.ro_transaction() as txn:
            with txn.open_map(name=DB_CLAZZ_IDX, flags=DB_ID_IDX_FLAGS) as db_clazz_idx:
                with txn.cursor(db_clazz_idx) as cur:
                    for idx, name in cur.iter():
                        clazz = self.serializer.name_object[name.decode('utf-8')]
                        self.idx_clazz[idx] = clazz
                        self.clazz_idx[clazz] = idx

        self.serializer.set_clazz_idx(self.clazz_idx, self.idx_clazz)

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
            self._populate_clazz_idx()

        self._restore_clazz_idx()

        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        exception_traceback: Optional[TracebackType],
    ) -> Literal[False]:
        self.env.close()
        return False  # Allow errors to propagate!

    def db_names(self, txn: TXN = None) -> dict[bytes, type[EntityStructure]]:
        db_names: dict[bytes, type[EntityStructure]] = {}
        if txn is None:
            txn = self.env.ro_transaction()
        with txn.cursor(db=None) as cur:
            for db_name, _ in cur.iter():
                if db_name in (DB_CLAZZ_IDX, DB_UNRESOLVED, DB_ID_IDX, DB_UNRESOLVED, DB_REFERENCE_OUTWARD):
                    continue

                clazz = self.idx_clazz.get(db_name, None)
                if clazz is not None:
                    db_names[db_name] = clazz
        return db_names

    def db_names_iter(self, txn: TXN) -> Generator[type[EntityStructure], None, None]:
        with txn.cursor(db=None) as cur:
            for db_name, _ in cur.iter():
                if db_name in (DB_CLAZZ_IDX, DB_UNRESOLVED, DB_ID_IDX, DB_UNRESOLVED, DB_REFERENCE_OUTWARD):
                    continue

                clazz = self.idx_clazz.get(db_name, None)
                if clazz is not None:
                    yield clazz

    def clean(self) -> None:
        log_all(logging.INFO, "[storage] cleaning database")
        with self.env.rw_transaction() as txn:
            with txn.cursor(db=None) as cur:
                for db_name, _ in cur.iter():
                    dbi = txn.open_map(name=db_name, flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
                    if dbi:
                        dbi.drop(txn, delete=True)
            txn.commit()
        self._populate_clazz_idx()

    def fetch_all_references_by_class(
        self, txn: TXN, clazzes: set[type[EntityStructure]], skip_existing: bool = False
    ) -> Generator[EntityStructure, None, None]:
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
                referencing_clazz_idx = Serializer.full_key_to_clazz(referencing_key)
                reference_clazz_idx = Serializer.full_key_to_clazz(reference_key)

                # print(self.idx_clazz[referencing_clazz_idx], "->", self.idx_clazz[reference_clazz_idx])

                if self.idx_clazz[referencing_clazz_idx] in clazzes:
                    # if self.idx_clazz[reference_clazz_idx] not in clazzes:
                    if reference_key not in yielded_set:
                        yielded_set.add(reference_key)

                        # Why is this separate: we don't want to expose objects that we already export,
                        # but we do want to search if there are any references used.
                        partial.add(reference_key)
                    # print(self.idx_clazz[referencing_clazz_idx], "->", self.idx_clazz[reference_clazz_idx])
                else:
                    # print(self.idx_clazz[referencing_clazz_idx])
                    pass

        # Our selected objects may contain references themselves, obviously we need to have those too
        partial_new: set[bytes]

        while True:
            partial_new = set([])
            for referencing_key in partial:
                referencing_clazz_idx = Serializer.full_key_to_clazz(referencing_key)
                for t in cursor.iter_dupsort_rows(start_key=referencing_key):
                    for referencing_key2, reference_key in t:
                        # referencing_clazz_idx2 = Serializer.full_key_to_clazz(referencing_key2) # TODO: Waarom stond deze hier?
                        # we skip when we can't find a matching key
                        if referencing_key2 != referencing_key:
                            break
                        reference_clazz_idx = Serializer.full_key_to_clazz(reference_key)
                        if self.idx_clazz[reference_clazz_idx] not in clazzes:
                            if reference_key not in partial_new and reference_key not in yielded_set:
                                partial_new.add(reference_key)
                    break  # We only want the single needle, which is found by the start_key.
            if len(partial_new) == 0:
                break
            else:
                yielded_set.update(partial_new)
                partial = partial_new.copy()

        # TODO: we are still missing the objects that are referenced from the reference

        for full_reference in yielded_set:
            # TODO: We can optimise this by grouping the objects per class, and then fetch the groups in one access pattern
            obj = self.load_object_by_full_key(txn, full_reference)
            if obj:
                if skip_existing:
                    if obj.__class__ not in clazzes:
                        log_all(logging.DEBUG, f"yielding unexpected class {obj.__class__} not in interesting classes {clazzes}")
                        yield obj
                else:
                    yield obj

    # TODO: Rename
    def other_classes(self, txn: TXN, clazzes: set[type[EntityStructure]]) -> Generator[EntityStructure, None, None]:
        other_classes: set[type[EntityStructure]] = set(self.db_names_iter(txn))
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
            idx: bytes  # The serial index in the object tables
            full_key: bytes  # The clazz_idx + serial
            this_clazz_idx = self.clazz_idx[obj.__class__]
            db = txn.create_map(name=this_clazz_idx)

            my_id = self.serializer.encode_obj(obj)

            # First: check if the id already exists, then we must overwrite.
            full_key = db_id_idx.get(txn, my_id)
            if full_key is not None:
                idx = Serializer.full_key_to_idx(full_key)
                try:
                    db_reference_outward.delete(txn, full_key)
                except:  # noqa: E722
                    pass
            else:
                idx = db_id_idx.get_sequence(txn, 1).to_bytes(4, 'little')
                full_key = Serializer.get_fullkey_by_clazz_idx(idx, this_clazz_idx)

            for referenced_clazz, ref, version in only_references(obj, self.serializer):
                unresolved_value = self.serializer.encode_key(ref, version, referenced_clazz)
                resolved_idx = db_id_idx.get(txn, unresolved_value)
                if resolved_idx:
                    db_reference_outward.put(txn, full_key, resolved_idx)
                else:
                    db_unresolved.put(txn, full_key, unresolved_value)

            value = self.serializer.marshall(obj, obj.__class__)
            db.put(txn, idx, value)
            db_id_idx.put(txn, my_id, full_key)

    # Deprecate this one
    def insert_objects_on_queue(self, clazz: type[EntityStructure], objects: Iterable[EntityStructure], empty: bool = False) -> None:
        if self.readonly:
            raise

        this_clazz_idx = self.clazz_idx[clazz]

        with self.env.rw_transaction() as txn:
            db = txn.create_map(name=this_clazz_idx)
            db_unresolved = txn.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            db_id_idx = txn.open_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
            db_reference_outward = txn.open_map(name=DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)

            if empty:
                db.drop(txn, delete=False)

            for obj in objects:
                my_id = self.serializer.encode_obj(obj)

                # First: check if the id already exists, then we must overwrite.
                full_key = db_id_idx.get(txn, my_id)
                if full_key is not None:
                    idx = Serializer.full_key_to_idx(full_key)
                    try:
                        db_reference_outward.delete(txn, full_key)
                    except:  # noqa: E722
                        pass
                else:
                    idx = db_id_idx.get_sequence(txn, 1).to_bytes(4, 'little')
                    full_key = Serializer.get_fullkey_by_clazz_idx(idx, this_clazz_idx)

                for referenced_clazz, ref, version in only_references(obj, self.serializer):
                    unresolved_value = self.serializer.encode_key(ref, version, referenced_clazz)
                    resolved_idx = db_id_idx.get(txn, unresolved_value)
                    if resolved_idx:
                        db_reference_outward.put(txn, full_key, resolved_idx)
                    else:
                        db_unresolved.put(txn, full_key, unresolved_value)

                value = self.serializer.marshall(obj, clazz)
                db.put(txn, idx, value)
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
        for reference_full_key in self._load_references_by_fullkey(txn, full_key):
            clazz_idx, idx = Serializer.full_key_to_clazz_idx(reference_full_key)
            yield self.idx_clazz[clazz_idx], idx

    def _load_references_inwards_by_fullkey(self, txn: TXN, full_key: bytes) -> Generator[bytes, None, None]:
        db = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)
        cursor = txn.cursor(db)
        for it in cursor.iter_dupsort_rows():
            for referencing_key, reference_key in it:
                if reference_key == full_key:
                    yield referencing_key

    def _load_references_inwards_by_fullkeys(self, txn: TXN, full_keys: set[bytes]) -> Generator[tuple[bytes, bytes], None, None]:
        # This will do everything in one sequential scan
        db = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)
        cursor = txn.cursor(db)
        for it in cursor.iter_dupsort_rows():
            for referencing_key, reference_key in it:
                if reference_key in full_keys:
                    yield reference_key, referencing_key

    def _index_references_inwards(self, txn: TXN, force: bool = False) -> None:
        create = True
        try:
            db_inward = txn.open_map(DB_REFERENCE_INWARD, flags=DB_REFERENCE_INWARD_FLAGS)
            create = db_inward.get_stat(txn).ms_entries > 0 or force
        except:  # noqa: E722
            pass
        finally:
            if create:
                db_inward = txn.create_map(DB_REFERENCE_INWARD, flags=DB_REFERENCE_INWARD_FLAGS)
                db_inward.drop(txn, delete=False)
                db = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)
                cursor = txn.cursor(db)

                for it in cursor.iter_dupsort_rows():
                    for referencing_key, reference_key in it:
                        db_inward.put(txn, reference_key, referencing_key)

    def _load_references_inwards_by_fullkeys_index(self, txn: TXN, full_keys: set[bytes]) -> Generator[tuple[bytes, bytes], None, None]:
        if not full_keys:
            return

        db = txn.open_map(DB_REFERENCE_INWARD, flags=DB_REFERENCE_INWARD_FLAGS)
        cursor = txn.cursor(db)

        # Maybe sort full_keys?
        for full_key in full_keys:
            for it in cursor.iter_dupsort_rows(start_key=full_key):
                for reference_key, referencing_key in it:
                    if reference_key != full_key:
                        break

                    yield reference_key, referencing_key
                break

    def _load_references_inwards(self, txn: TXN, full_key: bytes) -> Generator[tuple[type[EntityStructure], bytes], None, None]:
        for referencing_full_key in self._load_references_inwards_by_fullkey(txn, full_key):
            clazz_idx, idx = Serializer.full_key_to_clazz_idx(referencing_full_key)
            yield self.idx_clazz[clazz_idx], idx

    def load_references_by_clazz_full_key(self, txn: TXN, full_key: bytes, inwards: bool) -> Generator[bytes, None, None]:
        if inwards:
            yield from self._load_references_inwards_by_fullkey(txn, full_key)
        else:
            yield from self._load_references_by_fullkey(txn, full_key)

    def load_references_by_clazz_key(self, txn: TXN, clazz: type, key: bytes, inwards: bool) -> Generator[tuple[type[EntityStructure], bytes], None, None]:
        this_clazz_idx = self.clazz_idx[clazz]
        full_key = Serializer.get_fullkey_by_clazz_idx(key, this_clazz_idx)
        for full_referenced_key in self.load_references_by_clazz_full_key(txn, full_key, inwards):
            referenced_clazz_idx, referenced_idx = Serializer.full_key_to_clazz_idx(full_referenced_key)
            yield self.idx_clazz[referenced_clazz_idx], referenced_idx

    def load_references_by_clazz_keys(
        self, txn: TXN, clazz: type, keys: set[bytes], inwards: bool
    ) -> Generator[tuple[type[EntityStructure], bytes], None, None]:
        this_clazz_idx = self.clazz_idx[clazz]
        for key in keys:
            full_key = Serializer.get_fullkey_by_clazz_idx(key, this_clazz_idx)
            for full_referenced_key in self.load_references_by_clazz_full_key(txn, full_key, inwards):
                referenced_clazz_idx, referenced_idx = Serializer.full_key_to_clazz_idx(full_referenced_key)
                yield self.idx_clazz[referenced_clazz_idx], referenced_idx

    def load_references_by_object(self, txn: TXN, obj: Tid, inwards: bool) -> Generator[tuple[type[EntityStructure], bytes], None, None]:
        if hasattr(obj, 'idx'):
            full_key = obj.idx
            if inwards:
                yield from self._load_references_inwards(txn, full_key)
            else:
                yield from self._load_references(txn, full_key)
        else:
            with txn.open_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS) as db_id_idx:
                my_id = self.serializer.encode_key(str(obj.id), obj.version if hasattr(obj, "version") else None, obj.__class__)
                full_key = db_id_idx.get(txn, my_id)
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
        full_keys: list[bytes],
        inward_classes: set[type[EntityStructure]] | None = None,
        conditional_inward_classes: set[tuple[type[EntityStructure], type[EntityStructure]]] | None = None,
        visited: set[bytes] | None = None,
    ) -> Generator[EntityStructure, None, None]:
        if visited is None:
            visited = set()

        if inward_classes is None:
            inward_classes = {NoticeAssignment, DayTypeAssignment}

        if conditional_inward_classes is None:
            conditional_inward_classes = {(PassengerStopAssignment, ScheduledStopPoint)}

        stack = list(full_keys)

        # Ideally we would only check objects that would make sense to check
        clazz_idxs = [self.clazz_idx[clazz] for clazz in inward_classes]

        conditional = defaultdict(set)
        for f, t in conditional_inward_classes:
            conditional[self.clazz_idx[t]].add(self.clazz_idx[f])

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

                        this_clazz_idx, idx = Serializer.full_key_to_clazz_idx(full_key)
                        if this_clazz_idx in clazz_idxs or this_clazz_idx in conditional:
                            to_visit_inwards.add(full_key)

                        for referenced_full_key in self.load_references_by_clazz_full_key(txn, full_key, False):
                            # TODO: We could move the visited check here? we could also make our stack a set?
                            stack.append(referenced_full_key)

            for referencing_full_key, referenced_full_key in self._load_references_inwards_by_fullkeys_index(txn, to_visit_inwards):
                # print("by_fullkeys", referenced_full_key)
                if referenced_full_key not in visited:
                    referenced_clazz_idx, _referenced_idx = Serializer.full_key_to_clazz_idx(referenced_full_key)
                    if referenced_clazz_idx in clazz_idxs:
                        stack.append(referenced_full_key)
                    else:
                        referencing_clazz_idx, _referencing_key = Serializer.full_key_to_clazz_idx(referencing_full_key)
                        if referencing_clazz_idx in conditional.get(referenced_clazz_idx, {}):
                            # print("by conditional", referenced_full_key)
                            stack.append(referenced_full_key)

    def load_object_by_id_version(
        self, txn: TXN, id: str, clazz: type[EntityStructure], version: Optional[str] = None
    ) -> Optional[tuple[bytes, EntityStructure]]:
        my_id = self.serializer.encode_key(id, version, clazz)

        # TODO: Abstract this because
        if version is not None:
            db_id_idx = txn.open_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
            full_key = db_id_idx.get(txn, my_id)
            if full_key is None:
                return None

            obj = self.load_object_by_full_key(txn, full_key)
            if obj is None:
                return None

            return full_key, obj

        else:
            prefix, _, _ = self.serializer.split_key(my_id)
            cursor = txn.cursor(db=DB_ID_IDX)
            for check_key, resolved_idx in cursor.iter(prefix):
                if check_key.startswith(prefix):
                    obj = self.load_object_by_full_key(txn, resolved_idx)
                    if obj is None:
                        return None
                    return resolved_idx, obj
            return None

    def load_object_by_full_key(self, txn: TXN, full_key: bytes) -> Optional[EntityStructure]:
        this_clazz_idx, idx = Serializer.full_key_to_clazz_idx(full_key)
        clazz = self.idx_clazz[this_clazz_idx]

        with txn.open_map(name=this_clazz_idx, flags=MDBXDBFlags.MDBX_DB_DEFAULTS) as db:
            value = db.get(txn, idx)
            if value:
                obj: EntityStructure = self.serializer.unmarshall(value, clazz)
                return obj

        return None

    def load_object(self, txn: TXN, clazz: type[Tid], key: bytes) -> Tid:
        this_clazz_idx = self.clazz_idx[clazz]
        with txn.open_map(name=this_clazz_idx, flags=MDBXDBFlags.MDBX_DB_DEFAULTS) as db:
            value = db.get(txn, key)
            assert value is not None
            # if value is None:
            #    print(clazz, key)

            obj = self.serializer.unmarshall(value, clazz)
            # idx = ((int.from_bytes(this_clazz_idx, 'little') << 32) | int.from_bytes(key, 'little')).to_bytes(8, 'little')
            return obj

    # TODO: It would be nice if we could do a caching layer here
    def load_object_by_reference(self, txn: TXN, ref: VersionOfObjectRefStructure) -> Optional[EntityStructure]:
        with txn.open_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS) as db_id_idx:
            # TODO: With our current schema, we always will have a name_of_ref_class filled in.
            if ref.name_of_ref_class is not None:
                # The optimal situation, we can search for the id class in the right place
                my_id = self.serializer.encode_ref(ref)
                full_key = db_id_idx.get(txn, my_id)
                if full_key is not None:
                    return self.load_object_by_full_key(txn, full_key)

            if True:
                # TODO: Fallback should not happen, because the references should already have been updated, but since we are here
                log_all(logging.WARNING, f"[load_object_by_reference] fallback prefix-scan for ref {ref.ref}")
                prefix = self.serializer.encode_prefix(str(ref.ref))
                cursor = txn.cursor(db_id_idx)
                for check_key, resolved_idx in cursor.iter(prefix):
                    if check_key.startswith(prefix):
                        referenced_clazz_idx, referenced_idx = Serializer.full_key_to_clazz_idx(resolved_idx)
                        # We now want to check if the referenced_clazz_idx actually matches what should be "possible"

                        return self.load_object(txn, self.idx_clazz[referenced_clazz_idx], referenced_idx)
                    else:
                        break

        # TODO means that a reference can't be resolved in the source data. Perhaps we want to generate dummy ones.
        raise Exception(f"Can't load element from key {ref.ref} via {my_id!r}.")
        return None

    def scan_objects(self, txn: TXN, clazz: type[Tid], start_key: bytes | None = None, limit: int | None = None) -> Generator[bytes, None, None]:
        with txn.open_map(name=self.clazz_idx[clazz], flags=MDBXDBFlags.MDBX_DB_DEFAULTS) as db:
            with txn.cursor(db) as cursor:
                count = 0

                # Iterate over keys only for maximum efficiency
                for key, _value in cursor.iter(start_key=start_key):  # TODO: MDBX_SET
                    yield key
                    if limit:
                        count += 1
                        if count >= limit:
                            break

    def count_objects(self, txn: TXN, clazz: type[EntityStructure]) -> int | c_uint64:
        try:
            db = txn.open_map(name=self.clazz_idx[clazz], flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
            return cast(c_uint64, db.get_stat(txn).ms_entries)
        except:  # noqa: E722  # TODO: Better catching by pymdbx proper exceptions
            return 0

    def iter_objects(self, txn: TXN, clazz: type[Tid], start_key: bytes | None = None, limit: int | None = None) -> Generator[tuple[bytes, Tid], None, None]:
        try:
            db = txn.open_map(name=self.clazz_idx[clazz], flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
            entries = db.get_stat(txn).ms_entries
        except:  # noqa: E722  # TODO: Better catching by pymdbx proper exceptions
            return

        with txn.cursor(db) as cursor:
            count = 0

            for key, value in cursor.iter(start_key=start_key):
                if count % 100 == 0:
                    log_all(logging.INFO, f"{clazz.__name__} processed: {count}/{entries}")

                yield key, self.serializer.unmarshall(value, clazz)
                count += 1
                if limit and count >= limit:
                    break
            log_all(logging.INFO, f"{clazz.__name__} processed: {count}/{entries}")

    def iter_only_objects(self, txn: TXN, clazz: type[Tid], start_key: bytes | None = None, limit: int | None = None) -> Generator[Tid, None, None]:
        for _key, obj in self.iter_objects(txn, clazz, start_key, limit):
            yield obj

    def copy_map(self, txn: TXN, remote_storage: "MdbxStorage", remote_txn: TXN, clazz: type[EntityStructure]) -> None:
        remote_storage.insert_any_object_on_queue(remote_txn, self.iter_only_objects(txn, clazz))
        """
        We missen hier de afhandeling van db_id's etc.
        with remote_txn.create_map(name=remote_storage.clazz_idx[clazz]) as db_destination:
            try:
                with txn.open_map(name=self.clazz_idx[clazz], flags=MDBXDBFlags.MDBX_DB_DEFAULTS) as db_source:
                    with txn.cursor(db_source) as cursor:
                        for key, value in cursor.iter():
                            db_destination.put(remote_txn, key, value)
            except MDBXErrorExc as e:
                if e.errno == -30798:
                    pass
                else:
                    raise
        """
