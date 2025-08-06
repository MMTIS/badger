from __future__ import annotations

from typing import Optional, Any, Iterator
from PySide6.QtCore import QObject, Signal
from netex import MultilingualString
from netexio.database import Database, Tid
from utils.utils import get_object_name

class QDatabase(Database):
    def scan_objects(self, clazz: type[Tid], start_key: Optional[bytes] = None, limit: int = 100,
                     filter_text: str = "") -> Iterator[LMDBObject]:
        db = self.open_database(clazz)
        if not db:
            return
        with self.env.begin(db=db) as txn:
            cursor = txn.cursor()

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
            filter_text_lower = filter_text.lower()

            # Iterate over keys only for maximum efficiency
            for key in cursor.iternext(keys=True, values=False):
                key_str = key.decode('utf-8', 'ignore').lower()
                if not filter_text_lower or filter_text_lower in key_str:
                    # Yield a lightweight, lazy object
                    yield LMDBObject(clazz, key, self)
                    count += 1
                    if count >= limit:
                        break

    def get_object_by_key(self, clazz: type[Tid], key: bytes) -> Optional[LMDBObject]:
        # This method now only checks for existence and returns a lazy object
        # if the key is found. The actual data is not read here.
        if self.check_object_by_key(clazz, key):
            return LMDBObject(clazz, key, self)
        return None

    def get_relationships(self, obj: LMDBObject) -> tuple[list[LMDBObject], list[LMDBObject]]:
        if not obj or not obj.obj:
            return [], []
        outgoing = [self.get_object_by_key(ref['db_name'], ref['key'].encode()) for ref in getattr(obj.obj, 'outgoing_refs', [])]
        incoming = [self.get_object_by_key(ref['db_name'], ref['key'].encode()) for ref in getattr(obj.obj, 'incoming_refs', [])]
        # Filter Nones eruit voor het geval een referentie niet gevonden kon worden
        return [o for o in outgoing if o], [i for i in incoming if i]


class LMDBObject(QObject):
    """A lightweight, lazy-loading proxy for an object stored in LMDB.

    It inherits from QObject to signal when its data is loaded, allowing
    UI components to update automatically.
    """
    dataLoaded = Signal()

    def __init__(self, clazz: type[Tid], key: bytes, database: QDatabase, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.clazz = clazz
        self.key = key
        self._db = database
        self._obj: Optional[Tid] = None
        self._name: Optional[str] = None

    @property
    def obj(self) -> Optional[Tid]:
        """Lazily deserializes the object on first access."""
        if self._obj is None:  # Load only once
            raw_value = self._db.get_raw_value_by_key(self.clazz, self.key)
            if raw_value:
                self._obj = self._db.serializer.unmarshall(raw_value, self.clazz)
                if self._obj is not None:
                    self.dataLoaded.emit()
        return self._obj

    def get_name_or_id(self) -> tuple[str, str]:
        id, version = self.get_id_version()

        if self._obj is not None:
            name = getattr(self._obj, 'name', None)
            if name is not None:
                if isinstance(name, MultilingualString):
                    name = name.value

            if name is not None:
                return name, version

        return id, version

    def get_id_version(self) -> Optional[tuple[Any, Any]]:
        if self._obj is not None:
            return self._obj.id, self._obj.version

        # Optimization: parse id/version from key instead of loading the object.
        # This is much faster and avoids a DB read when just displaying the list.
        # Assuming the key format for data dbs is "id:version" or "id".
        key_str = self.key.decode('utf-8', 'ignore')
        parts = key_str.split('-', 1)
        parts[0] = parts[0].replace('#', get_object_name(self.clazz)).replace('*', ':')
        if len(parts) == 2:
            return parts[0], parts[1]
        elif len(parts) == 1 and parts[0]:
            # Handle case where there is no version in the key
            return parts[0], None

        return None, None


