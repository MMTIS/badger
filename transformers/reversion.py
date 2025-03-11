import functools

import netex
from netex import VersionOfObjectRef, VersionOfObjectRefStructure
from netexio.database import Database
from netexio.serializer import Serializer
from utils.utils import get_object_name


def simple_recursive_attributes(obj):
    for attr in obj.__dataclass_fields__.keys():
        v = getattr(obj, attr, None)
        if v is not None:
            if issubclass(v.__class__, VersionOfObjectRef) or issubclass(v.__class__, VersionOfObjectRefStructure):
                yield v

            else:
                if hasattr(v, '__dataclass_fields__') and v.__class__.__name__ in netex.set_all:
                    if hasattr(v, 'id'):
                        yield v
                    yield from simple_recursive_attributes(v)

                elif v.__class__ in (list, tuple):
                    for x in v:
                        if x is not None:
                            if issubclass(x.__class__, VersionOfObjectRef) or issubclass(x.__class__, VersionOfObjectRefStructure):
                                yield x
                            elif hasattr(x, '__dataclass_fields__') and x.__class__.__name__ in netex.set_all:
                                if hasattr(x, 'id'):
                                    yield x
                                yield from simple_recursive_attributes(x)


def reversion_object(deserialized, updated_version, any_too=False):
    for obj in simple_recursive_attributes(deserialized):
        if hasattr(obj, 'version') and (any_too or obj.version != 'any'):
            obj.version = updated_version

    if hasattr(object, 'version') and (any_too or object.version != 'any'):
        object.version = updated_version


def reversion_udf(serializer: Serializer, serialized: bytes, clazz: str, updated_version: str, any_too: bool = False) -> bytes:
    deserialized = serializer.unmarshall(serialized, clazz)
    reversion_object(deserialized, updated_version, any_too)
    return serializer.marshall(deserialized, clazz)


# TODO: Implement for lmdb
def reversion_all_objects(db: Database, updated_version: str, any_too: bool = False):
    con = db.con
    con.create_function('reversion', functools.partial(reversion_udf, db.serializer))

    for clazz in db.tables():
        objectname = get_object_name(clazz)
        if any_too:
            con.execute(f"UPDATE {objectname} SET version = ?, object = reversion(object, '{objectname}', ?, ?);",
                        (updated_version, updated_version, any_too))
        else:
            con.execute(f"UPDATE {objectname} SET version = CASE WHEN version == 'any' THEN version ELSE ? END, object = reversion(object, '{objectname}', ?, ?);",
                        (updated_version, updated_version, any_too))

    con.remove_function('reversion')

    if any_too:
        con.execute("UPDATE embedded SET parent_version = ?;", (updated_version,))
        con.execute("UPDATE embedded SET version = ?;", (updated_version,))
        con.execute("UPDATE referencing SET parent_version = ?;", (updated_version,))
        con.execute("UPDATE referencing SET version = ?;", (updated_version,))
    else:
        con.execute("UPDATE embedded SET parent_version = ? WHERE parent_version <> 'any';", (updated_version,))
        con.execute("UPDATE embedded SET version = ? WHERE version <> 'any';", (updated_version,))
        con.execute("UPDATE referencing SET parent_version = ? WHERE parent_version <> 'any';", (updated_version,))
        con.execute("UPDATE referencing SET version = ? WHERE version <> 'any';", (updated_version,))
