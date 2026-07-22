from xsdata.models.datatype import XmlDateTime

from domain.netex.model import MultilingualString, ServiceJourney, TextType, TrainNumber

from fix.trenitalia.add_train_numbers import train_number_for_journey, add_train_numbers
from tests.base import MdbxStorageTestCase
from tests.netex_harness import XmlAssertions, to_xml
from transformers.generalframe import export_to_general_frame


class TestAddTrainNumbers(XmlAssertions, MdbxStorageTestCase):
    def test_add_train_numbers_creates_and_links_train_number(self) -> None:
        self.load_netex("""
            <TimetableFrame id="tf" version="1">
              <vehicleJourneys>
                <ServiceJourney id="sj1" version="1"><Name>25</Name></ServiceJourney>
              </vehicleJourneys>
            </TimetableFrame>
            """)

        add_train_numbers(self.storage)

        self.assertXmlEqual(
            self.export_netex(),
            """\
            <PublicationDelivery xmlns="http://www.netex.org.uk/netex" xmlns:gml="http://www.opengis.net/gml/3.2" version="ntx:1.1">
              <PublicationTimestamp>2026-01-01T00:00:00</PublicationTimestamp>
              <ParticipantRef>PyNeTExConv</ParticipantRef>
              <dataObjects>
                <GeneralFrame id="Database" version="1">
                  <members>
                    <TrainNumber id="IT:TrainNumber:25" version="1">
                      <ForAdvertisement>25</ForAdvertisement>
                    </TrainNumber>
                    <ServiceJourney id="sj1" version="1">
                      <Name>25</Name>
                      <trainNumbers>
                        <TrainNumberRef version="1" ref="IT:TrainNumber:25"/>
                      </trainNumbers>
                    </ServiceJourney>
                  </members>
                </GeneralFrame>
              </dataObjects>
            </PublicationDelivery>""",
        )

    def test_distinct_number_is_deduplicated_across_journeys(self) -> None:
        self.load_netex("""
            <TimetableFrame id="tf" version="1">
              <vehicleJourneys>
                <ServiceJourney id="sj1" version="1"><Name>25</Name></ServiceJourney>
                <ServiceJourney id="sj2" version="1"><Name>25</Name></ServiceJourney>
              </vehicleJourneys>
            </TimetableFrame>
            """)

        add_train_numbers(self.storage)

        # Two journeys share number 25 -> exactly one first-class TrainNumber.
        train_numbers = self.read_objects(TrainNumber)
        self.assertEqual(len(train_numbers), 1)
        self.assertEqual(train_numbers[0].id, "IT:TrainNumber:25")
        # ...and both journeys link to it.
        refs = sorted((ref.ref, ref.version) for sj in self.read_objects(ServiceJourney) if sj.train_numbers for ref in sj.train_numbers.train_number_ref)
        self.assertEqual(refs, [("IT:TrainNumber:25", "1"), ("IT:TrainNumber:25", "1")])

    def test_add_train_numbers_ignores_words(self) -> None:
        self.load_netex("""
            <TimetableFrame id="tf" version="1">
              <vehicleJourneys>
                <ServiceJourney id="sj1" version="1"><Name>Frecciarossa</Name></ServiceJourney>
              </vehicleJourneys>
            </TimetableFrame>
            """)

        add_train_numbers(self.storage)

        self.assertEqual(self.read_objects(TrainNumber), [], "No TrainNumber created for a name containing a word")

    def test_train_number_read_from_texttype_name(self) -> None:
        numbered = ServiceJourney(id="x", version="1", name=MultilingualString(content=[TextType(value="rj 25")]))
        self.assertEqual(train_number_for_journey(numbered), "25")

        named = ServiceJourney(id="y", version="1", name=MultilingualString(content=[TextType(value="Frecciarossa")]))
        self.assertIsNone(train_number_for_journey(named))
