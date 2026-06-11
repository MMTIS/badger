"""Helpers for writing NeTEx-XML-driven transformer tests.

A test provides a few NeTEx frames as a string; :func:`load_netex` parses them into an
``MdbxStorage`` exactly the way the real importer does (``insert_database`` + ``resolve``),
so a test reads like a tiny NeTEx file plus an assertion on what a transformer makes of it.
"""

import io
from pathlib import Path
from typing import Any, Callable, Iterable, TypeVar

from domain.netex.model import Codespace
from storage.lxml.core.insert import get_interesting_classes, insert_database
from storage.lxml.serialization.xmlserializer import MyXmlSerializer
from storage.mdbx.core.implementation import MdbxStorage
from storage.mdbx.core.references import resolve, resolve_embeddings

T = TypeVar("T")

# Minimal generator_defaults accepted by every epip_* generator (avoids importing the real config).
GEN_DEFAULTS: dict[str, Any] = {"codespace": Codespace(id="cs", xmlns="test"), "version": "1"}

_SKELETON = (
    '<PublicationDelivery xmlns="http://www.netex.org.uk/netex"'
    ' xmlns:gml="http://www.opengis.net/gml/3.2" version="ntx:1.1">'
    "<PublicationTimestamp>2026-01-01T00:00:00</PublicationTimestamp>"
    "<ParticipantRef>test</ParticipantRef>"
    '<dataObjects><CompositeFrame id="cf" version="1">'
    "<frames>__FRAMES__</frames>"
    "</CompositeFrame></dataObjects></PublicationDelivery>"
)


def load_netex(db: MdbxStorage, frames_xml: str) -> None:
    """Parse ``frames_xml`` (the inner content of ``<frames>``) into ``db``.

    Wrap one or more frames, e.g. ``<ServiceFrame>…</ServiceFrame>``, and pass the string.
    """
    xml = _SKELETON.replace("__FRAMES__", frames_xml)
    insert_database(db, get_interesting_classes(), io.BytesIO(xml.encode("utf-8")))
    resolve(db)
    resolve_embeddings(db)


def run(generator_fn: Callable[..., Iterable[T]], db: MdbxStorage) -> list[T]:
    """Run an epip generator ``fn(db, txn, generator_defaults)`` and collect its output."""
    with db.env.ro_transaction() as txn:
        return list(generator_fn(db, txn, GEN_DEFAULTS))


def to_xml(obj: Any) -> str:
    """Serialise a NeTEx object to an XML string (handy for readable assertions)."""
    return MyXmlSerializer([]).marshall(obj, type(obj), True)


def to_xml_all(objs: Iterable[Any]) -> str:
    """Serialise a sequence of objects and join them, for full-output XML assertions.

    Each object is serialised with :func:`to_xml` (pretty-printed) and stripped, then joined
    with newlines, so a test can assert the exact output XML a transform produces against a
    readable expected block.
    """
    return "\n".join(to_xml(o).strip() for o in objs)


def make_db(path: Path, frames_xml: str) -> Path:
    """Create a fresh on-disk db at ``path``, load ``frames_xml`` into it, and close it.

    For the ``fix.*`` / ``conv.*`` steps, which open the database file themselves by path.
    """
    with MdbxStorage(path, readonly=False) as db:
        load_netex(db, frames_xml)
    return path


def read_objects(path: Path, clazz: type[T]) -> list[T]:
    """Open the db at ``path`` read-only and return all stored objects of ``clazz``."""
    with MdbxStorage(path, readonly=True) as db:
        with db.env.ro_transaction() as txn:
            return list(db.iter_only_objects(txn, clazz))
