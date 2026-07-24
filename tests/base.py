import tempfile
import unittest
from pathlib import Path
from typing import TypeVar

from xsdata.models.datatype import XmlDateTime

from domain.netex.model import Line, LineRef, MultilingualString, Route, RouteRef, ServiceJourneyPattern, TextType
from domain.netex.services.model_typing import Tid

from storage.mdbx.core.implementation import MdbxStorage
from tests.netex_harness import load_netex, to_xml
from transformers.generalframe import export_to_general_frame

T = TypeVar("T")


class MdbxStorageTestCase(unittest.TestCase):
    """Base class providing a writable, empty MdbxStorage in a per-test temporary directory."""

    storage: MdbxStorage

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        cm = MdbxStorage(Path(tmp.name) / "test.mdbx", readonly=False)
        self.storage = cm.__enter__()
        self.addCleanup(cm.__exit__, None, None, None)

    def load_netex(self, frames_xml: str) -> None:
        """Parse ``frames_xml`` (the inner content of ``<frames>``).

        Wrap one or more frames, e.g. ``<ServiceFrame>…</ServiceFrame>``, and pass the string.
        """
        load_netex(self.storage, frames_xml)

    def read_objects(self, clazz: type[Tid]) -> list[Tid]:
        """Return all stored objects of ``clazz``."""
        with self.storage.env.ro_transaction() as txn:
            return list(self.storage.iter_only_objects(txn, clazz))

    def export_netex(self) -> str:
        with self.storage.env.ro_transaction() as txn:
            publication_delivery = export_to_general_frame(self.storage, txn)
            publication_delivery.publication_timestamp = XmlDateTime.from_string("2026-01-01T00:00:00")
            return to_xml(publication_delivery)

    def make_line_route_sjp(self) -> tuple[Line, Route, ServiceJourneyPattern]:
        """A small outward-reference chain: ServiceJourneyPattern -> Route -> Line."""
        line = Line(id="l1", version="1", name=MultilingualString(content=[TextType(value="Line l1")]))
        route = Route(id="r1", version="1", line_ref=LineRef(ref="l1", version="1"))
        sjp = ServiceJourneyPattern(id="sjp1", version="1", route_ref_or_route_view=RouteRef(ref="r1", version="1"))
        return line, route, sjp
