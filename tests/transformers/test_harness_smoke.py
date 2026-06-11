from domain.netex.model import ScheduledStopPoint

from tests.base import MdbxStorageTestCase
from tests.netex_harness import load_netex, to_xml_all

EXPECTED = """\
<ScheduledStopPoint xmlns="http://www.netex.org.uk/netex" xmlns:gml="http://www.opengis.net/gml/3.2" id="ssp1" version="1">
  <Name>Test</Name>
</ScheduledStopPoint>"""


class TestHarnessSmoke(MdbxStorageTestCase):
    def test_load_netex_round_trips_an_entity(self) -> None:
        load_netex(
            self.storage,
            """
            <ServiceFrame id="sf" version="1">
              <scheduledStopPoints>
                <ScheduledStopPoint id="ssp1" version="1"><Name>Test</Name></ScheduledStopPoint>
              </scheduledStopPoints>
            </ServiceFrame>
            """,
        )
        with self.storage.env.ro_transaction() as txn:
            ssps = list(self.storage.iter_only_objects(txn, ScheduledStopPoint))

        self.assertEqual(to_xml_all(ssps), EXPECTED)
