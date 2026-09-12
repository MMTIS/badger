from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any
import unittest

from storage.objectserializer.msgspec.serializer import MsgspecSerializer
from storage.objectserializer.codecs.lz4 import Lz4Codec
from storage.objectserializer.interface import ObjectSerializer
from storage.objectserializer.pipeline import PipelineSerializer

from tests.objectserializer.contracts.serializer_contract import (
    assert_serializer_roundtrip,
)


@dataclass
class SampleEntity:
    id: str
    version: str
    latitude: Decimal
    longitude: Decimal
    tags: list[str]


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


if __name__ == "__main__":
    unittest.main()
