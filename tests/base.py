import tempfile
import unittest
from pathlib import Path

from domain.netex.model import Line, LineRef, MultilingualString, Route, RouteRef, ServiceJourneyPattern, TextType

from storage.mdbx.core.implementation import MdbxStorage


class MdbxStorageTestCase(unittest.TestCase):
    """Base class providing a writable, empty MdbxStorage in a per-test temporary directory."""

    storage: MdbxStorage

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        cm = MdbxStorage(Path(tmp.name) / "test.mdbx", readonly=False)
        self.storage = cm.__enter__()
        self.addCleanup(cm.__exit__, None, None, None)

    def make_line_route_sjp(self) -> tuple[Line, Route, ServiceJourneyPattern]:
        """A small outward-reference chain: ServiceJourneyPattern -> Route -> Line."""
        line = Line(id="l1", version="1", name=MultilingualString(content=[TextType(value="Line l1")]))
        route = Route(id="r1", version="1", line_ref=LineRef(ref="l1", version="1"))
        sjp = ServiceJourneyPattern(id="sjp1", version="1", route_ref_or_route_view=RouteRef(ref="r1", version="1"))
        return line, route, sjp
