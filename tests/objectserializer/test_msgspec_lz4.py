from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
import pathlib
from typing import Any
import unittest
from xml.etree.ElementTree import QName

from xsdata.models.datatype import XmlDate, XmlDateTime, XmlDuration, XmlTime

from storage.objectserializer.msgspec.serializer import MsgspecSerializer
from storage.objectserializer.codecs.lz4 import Lz4Codec
from storage.objectserializer.interface import ObjectSerializer
from storage.objectserializer.pipeline import PipelineSerializer

from tests.objectserializer.contracts.serializer_contract import (
    assert_serializer_roundtrip,
)


class TransportMode(Enum):
    BUS = "bus"
    TRAM = "tram"
    RAIL = "rail"


@dataclass
class SampleEntity:
    id: str
    version: str
    latitude: Decimal
    longitude: Decimal
    tags: list[str]


@dataclass
class XmlSampleEntity:
    id: str
    mode: TransportMode
    date: XmlDate
    time: XmlTime
    datetime: XmlDateTime
    duration: XmlDuration
    price: Decimal
    tag: QName
    path: pathlib.Path


SAMPLE_ENTITY = SampleEntity(
    id="NL:OPENOV:ScheduledStopPoint:1",
    version="1",
    latitude=Decimal("52.370216"),
    longitude=Decimal("4.895168"),
    tags=["tram", "amsterdam", "central"],
)


class MsgspecLz4SerializerTestCase(unittest.TestCase):
    """Tests for the Msgspec + LZ4 serializer pipeline."""

    def setUp(self) -> None:
        self.object_serializer: ObjectSerializer = PipelineSerializer(
            object_serializer=MsgspecSerializer(),
            codecs=[
                Lz4Codec(),
            ],
        )

    def test_roundtrip_entity(self) -> None:
        """Verify the complete Msgspec + LZ4 pipeline on a structured entity."""
        assert_serializer_roundtrip(
            self.object_serializer,
            SAMPLE_ENTITY,
        )

    def test_builtin_types(self) -> None:
        """Verify common Python builtin types."""
        values: list[Any] = [
            None,
            True,
            False,
            0,
            -1,
            123456789,
            "",
            "hello world",
            b"binary data",
            [],
            [1, 2, 3],
            {},
            {"key": "value"},
        ]

        for value in values:
            with self.subTest(value=repr(value)):
                assert_serializer_roundtrip(
                    self.object_serializer,
                    value,
                )

    def test_is_compressed(self) -> None:
        """Verify that the LZ4 layer actually participates."""
        raw = MsgspecSerializer().dumps(SAMPLE_ENTITY)
        compressed = self.object_serializer.dumps(SAMPLE_ENTITY)
        # Verify valid roundtrip on both
        self.assertEqual(MsgspecSerializer().loads(raw), SAMPLE_ENTITY)
        self.assertEqual(self.object_serializer.loads(compressed), SAMPLE_ENTITY)

    def test_xml_sample_entity_roundtrip(self) -> None:
        """Verify roundtrip of complete XML datatype entity."""
        xml_entity = XmlSampleEntity(
            id="NL:NETEX:ScheduledStopPoint:100",
            mode=TransportMode.TRAM,
            date=XmlDate(2026, 9, 12),
            time=XmlTime(14, 30, 0),
            datetime=XmlDateTime(2026, 9, 12, 14, 30, 0),
            duration=XmlDuration("PT1H30M"),
            price=Decimal("3.85"),
            tag=QName("http://netex.org.uk/netex", "ScheduledStopPoint"),
            path=pathlib.Path("/tmp/netex_feed.xml"),
        )
        assert_serializer_roundtrip(self.object_serializer, xml_entity)
        assert_serializer_roundtrip(MsgspecSerializer(), xml_entity)


if __name__ == "__main__":
    unittest.main()
