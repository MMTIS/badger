from pathlib import Path
from sys import exception
from types import TracebackType
from typing import Optional, Type, Literal, Iterable, Generator, Self, Any

from mdbx import Env, MDBXDBFlags
from mdbx.mdbx import TXN, MDBXErrorExc

from domain.netex.model import VersionOfObjectRefStructure, EntityStructure
from domain.netex.services.model_typing import Tid
from domain.netex.services.recursive_attributes import only_references
from domain.netex.services.utils import get_boring_classes
from domain.utils import get_object_name
from storage.mdbx.serialization.byteserializer import ByteSerializer

DB_CLASS_IDX = bytes(b'_class_idx')
DB_UNRESOLVED = bytes(b'_unresolved')
DB_ID_IDX = bytes(b'_id_idx')
DB_REFERENCE_OUTWARD = bytes(b'_reference_outward')


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
            with txn.create_map(name=DB_CLASS_IDX) as db_class_idx:
                for idx, clazz in enumerate(self.serializer.name_object.values()):
                    clazz_name = get_object_name(clazz)
                    db_class_idx.put(txn, idx.to_bytes(2, 'little'), clazz_name.encode('utf-8'))

            txn.create_map(name=DB_UNRESOLVED, flags=MDBXDBFlags.MDBX_INTEGERKEY | MDBXDBFlags.MDBX_DUPSORT)
            txn.create_map(name=DB_ID_IDX)
            txn.create_map(
                name=DB_REFERENCE_OUTWARD,
                flags=MDBXDBFlags.MDBX_INTEGERKEY | MDBXDBFlags.MDBX_DUPSORT | MDBXDBFlags.MDBX_DUPFIXED | MDBXDBFlags.MDBX_INTEGERDUP,
            )
            txn.commit()

    def _restore_class_idx(self) -> None:
        with self.env.ro_transaction() as txn:
            with txn.open_map(name=DB_CLASS_IDX) as db_class_idx:
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

    def db_names(self, txn: TXN = None) -> dict[bytes, type]:
        db_names: dict[bytes, type] = {}
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

    def clean(self) -> None:
        with self.env.rw_transaction() as txn:
            with txn.cursor() as cur:
                for db_name, _ in cur.iter():
                    dbi = txn.open_map(name=db_name)
                    if dbi:
                        dbi.drop(delete=True)
            txn.commit()
        self._populate_class_idx()

    def insert_any_object_on_queue(self, txn: TXN, objects: Iterable[Tid]) -> None:
        if self.readonly:
            raise

        db_unresolved = txn.open_map(name=DB_UNRESOLVED)
        db_id_idx = txn.open_map(name=DB_ID_IDX)
        db_reference_outward = txn.open_map(name=DB_REFERENCE_OUTWARD)

        for obj in objects:
            this_class_idx = self.class_idx[obj.__class__]
            db = txn.create_map(name=this_class_idx)

            my_id = self.serializer.encode_key(str(obj.id), obj.version if hasattr(obj, "version") else None, obj.__class__, include_clazz=True)

            # First: check if the id already exists, then we must overwrite.
            full_key = db_id_idx.get(txn, my_id)
            if full_key is not None:
                full_int = int.from_bytes(full_key, 'little')
                key = (full_int & 0xFFFFFFFF)
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
            db_unresolved = txn.open_map(name=DB_UNRESOLVED)
            db_id_idx = txn.open_map(name=DB_ID_IDX)
            db_reference_outward = txn.open_map(name=DB_REFERENCE_OUTWARD)

            if empty:
                db.drop(txn, delete=False)

            for obj in objects:
                my_id = self.serializer.encode_key(str(obj.id), obj.version if hasattr(obj, "version") else None, obj.__class__, include_clazz=True)

                # First: check if the id already exists, then we must overwrite.
                full_key = db_id_idx.get(txn, my_id)
                if full_key is not None:
                    full_int = int.from_bytes(full_key, 'little')
                    key = (full_int & 0xFFFFFFFF)
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

    def _load_references(self, txn: TXN, full_key: bytes) -> Generator[tuple[type[EntityStructure], bytes], None, None]:
        db = txn.open_map(DB_REFERENCE_OUTWARD)
        cursor = txn.cursor(db)
        for it in cursor.iter_dupsort_rows(start_key=full_key):
            for referencing_key, reference_key in it:
                if referencing_key != full_key:
                    break
                class_idx, reference_local_key = ByteSerializer.full_key_to_idx(reference_key)
                yield self.idx_class[class_idx], reference_local_key
            break

    def _load_references_inwards(self, txn: TXN, full_key: bytes) -> Generator[tuple[type, bytes], None, None]:
        db = txn.open_map(DB_REFERENCE_OUTWARD)
        cursor = txn.cursor(db)
        for it in cursor.iter_dupsort_rows():
            for referencing_key, reference_key in it:
                if reference_key == full_key:
                    class_idx, referencing_local_key = ByteSerializer.full_key_to_idx(referencing_key)
                    yield self.idx_class[class_idx], referencing_local_key

    def load_references_by_clazz_key(self, txn: TXN, clazz: type, key: bytes, inwards: bool) -> Generator[tuple[type, bytes], None, None]:
        this_class_idx = self.class_idx[clazz]
        full_key = ((int.from_bytes(this_class_idx, 'little') << 32) | int.from_bytes(key, 'little')).to_bytes(8, 'little')
        if inwards:
            yield from self._load_references_inwards(txn, full_key)
        else:
            yield from self._load_references(txn, full_key)

    def load_references_by_object(self, txn: TXN, obj: Tid, inwards: bool) -> Generator[tuple[type, bytes], None, None]:
        if hasattr(obj, 'idx'):
            full_key = obj.idx
            if inwards:
                yield from self._load_references_inwards(txn, full_key)
            else:
                yield from self._load_references(txn, full_key)
        else:
            with txn.open_map(name=DB_ID_IDX) as db_id_idx:
                key = self.serializer.encode_key(str(obj.id), obj.version if hasattr(obj, "version") else None, obj.__class__, include_clazz=True)
                full_key = db_id_idx.get(txn, key)
                if inwards:
                    yield from self._load_references_inwards(txn, full_key)
                else:
                    yield from self._load_references(txn, full_key)

    def load_object_by_full_key(self, txn: TXN, full_key: bytes) -> EntityStructure:
        this_clazz_idx, key = self.serializer.full_key_to_idx(full_key)
        clazz = self.idx_class[this_clazz_idx]
        with txn.open_map(name=this_clazz_idx) as db:
            value = db.get(txn, key)
            obj: EntityStructure = self.serializer.unmarshall(value, clazz)
            return obj

    def load_object(self, txn: TXN, clazz: type[Tid], key: bytes) -> EntityStructure:
        this_class_idx = self.class_idx[clazz]
        with txn.open_map(name=this_class_idx) as db:
            value = db.get(txn, key)
            assert value is not None
            # if value is None:
            #    print(clazz, key)

            obj = self.serializer.unmarshall(value, clazz)
            # idx = ((int.from_bytes(this_class_idx, 'little') << 32) | int.from_bytes(key, 'little')).to_bytes(8, 'little')
            return obj

    def load_object_by_reference(self, txn: TXN, ref: VersionOfObjectRefStructure) -> Optional[EntityStructure]:
        with txn.open_map(name=DB_ID_IDX) as db_id_idx:
            if ref.name_of_ref_class is not None:
                # The optimal situation, we can search for the id class in the right place
                name_of_ref_class = str(ref.name_of_ref_class.value if hasattr(ref.name_of_ref_class, 'value') else ref.name_of_ref_class)
                key = self.serializer.encode_key(str(ref.ref), ref.version if hasattr(ref, "version") else None, self.idx_class[self.class_name_idx[name_of_ref_class]], include_clazz=True)
                full_key = db_id_idx.get(txn, key)
                return self.load_object_by_full_key(txn, full_key)

            if True:
                print("Fallback...")
                prefix = self.serializer.encode_key(str(ref.ref), ref.version if hasattr(ref, "version") else None)
                cursor = txn.cursor(db=DB_ID_IDX)
                for check_key, resolved_idx in cursor.iter(prefix):
                    if check_key.startswith(prefix):
                        referenced_class_idx, referenced_key = self.serializer.full_key_to_idx(resolved_idx)
                        # We now want to check if the referenced_class_idx actually matches what should be "possible"

                        return self.load_object(txn, self.idx_class[referenced_class_idx], referenced_key)
                    else:
                        break
        return None

    def scan_objects(self, txn: TXN, clazz: type[Tid], start_key: bytes | None = None, limit: int | None = None) -> Generator[bytes, None, None]:
        with txn.open_map(name=self.class_idx[clazz]) as db:
            with txn.cursor(db) as cursor:
                count = 0

                # Iterate over keys only for maximum efficiency
                for key, _value in cursor.iter(start_key=start_key):  # TODO: MDBX_SET
                    yield key
                    if limit:
                        count += 1
                        if count >= limit:
                            break

    def iter_objects(self, txn: TXN, clazz: type[Tid], start_key: bytes | None = None, limit: int | None = None) -> Generator[tuple[bytes, Tid], None, None]:
        try:
            db = txn.open_map(name=self.class_idx[clazz])
        except: # TODO: Better catching by pymdbx proper exceptions
            return

        with txn.cursor(db) as cursor:
            count = 0

            for key, value in cursor.iter(start_key=start_key):
                yield key, self.serializer.unmarshall(value, clazz)
                if limit:
                    count += 1
                    if count >= limit:
                        break

    def copy_map(self, txn: TXN, remote_storage: "MdbxStorage", remote_txn: TXN, clazz: type[EntityStructure]) -> None:
        with remote_txn.create_map(name=remote_storage.class_idx[clazz]) as db_destination:
            try:
                with txn.open_map(name=self.class_idx[clazz]) as db_source:
                    with txn.cursor(db_source) as cursor:
                        for key, value in cursor.iter():
                            db_destination.put(remote_txn, key, value)
            except MDBXErrorExc as e:
                if e.errno == -30798:
                    pass
                else:
                    raise