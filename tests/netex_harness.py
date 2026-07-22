"""Helpers for writing NeTEx-XML-driven transformer tests.

A test provides a few NeTEx frames as a string; :func:`load_netex` parses them into an
``MdbxStorage`` exactly the way the real importer does (``insert_database`` + ``resolve``),
so a test reads like a tiny NeTEx file plus an assertion on what a transformer makes of it.
"""

import io
import unittest
from typing import Any, Callable, Iterable, TypeVar

from lxml import etree

from domain.netex.model import Codespace
from storage.lxml.core.insert import get_interesting_classes, insert_database
from storage.lxml.serialization.xmlserializer import MyXmlSerializer
from storage.mdbx.core.implementation import MdbxStorage
from storage.mdbx.core.references import resolve, resolve_embeddings_index

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
    resolve_embeddings_index(db)


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
    with newlines. Compare the result against an expected block with
    :meth:`XmlAssertions.assertXmlEqual` (which canonicalises both sides) rather than an exact
    ``==``, so the comparison is not sensitive to whitespace/indentation.
    """
    return "\n".join(to_xml(o).strip() for o in objs)


def canonical_xml(xml: str) -> str:
    """Canonical (C14N) form of one or more serialised NeTEx objects, with formatting
    (indentation between elements) stripped, so comparisons ignore layout.

    Parsing with ``remove_blank_text`` drops only *formatting* whitespace; significant
    leading/trailing whitespace inside text nodes is preserved, so a transform that mangles
    text content is still caught. C14N then normalises attribute order and namespace declarations.

    ``xml`` may hold several concatenated top-level objects (as produced by :func:`to_xml_all`);
    they are wrapped in a throwaway root so the fragment parses as one document. The wrapper is
    applied identically to both sides of a comparison, so it cancels out.
    """
    parser = etree.XMLParser(remove_blank_text=True)
    root = etree.fromstring(f"<_harness>{xml}</_harness>".encode("utf-8"), parser)
    canonical: bytes = etree.tostring(root, method="c14n2")
    return canonical.decode("utf-8")


class XmlAssertions(unittest.TestCase):
    """Mixin adding XML-aware assertions; combine with a test's base class."""

    def assertXmlEqual(self, actual: str, expected: str, msg: Any = None) -> None:
        """``assertEqual`` for XML that ignores formatting / attribute-order differences.

        Both sides are canonicalised via :func:`canonical_xml` first, so an ``expected`` block
        can be indented to fit the test file (or written compactly) without affecting the result.
        """
        self.assertEqual(canonical_xml(actual), canonical_xml(expected), msg)
