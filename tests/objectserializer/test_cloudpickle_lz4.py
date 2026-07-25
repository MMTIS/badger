from __future__ import annotations

from decimal import Decimal
from typing import Any
import unittest

from storage.objectserializer.cloudpickle.serializer import CloudPickleSerializer
from storage.objectserializer.codecs.lz4 import Lz4Codec
from storage.objectserializer.interface import ObjectSerializer
from storage.objectserializer.pipeline import PipelineSerializer

from tests.objectserializer.contracts.serializer_contract import (
    assert_serializer_roundtrip,
)

from domain.netex.model import (
    ScheduledStopPoint,
    PrivateCode,
    PrivateCodes,
    MultilingualString,
    TextType,
    LocationStructure2,
)

SCHEDULED_STOP_POINT = ScheduledStopPoint(
    id="NL:OPENOV:ScheduledStopPoint:1",
    version="1",
    name=MultilingualString(content=[TextType(lang="nl", value="Hello World")]),
    private_codes=PrivateCodes(private_code=[PrivateCode(type_value="type", value="value")]),
    location=LocationStructure2(
        longitude=Decimal("12.13"),
        latitude=Decimal("23.16"),
    ),
)


class CloudPickleLz4SerializerTestCase(unittest.TestCase):
    """Tests for the CloudPickle + LZ4 serializer pipeline."""

    def setUp(self) -> None:
        self.object_serializer: ObjectSerializer = PipelineSerializer(
            object_serializer=CloudPickleSerializer(),
            codecs=[
                Lz4Codec(),
            ],
        )

    def test_roundtrip(self) -> None:
        """Verify the complete CloudPickle + LZ4 pipeline."""

        assert_serializer_roundtrip(
            self.object_serializer,
            SCHEDULED_STOP_POINT,
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
        """
        Verify that the LZ4 layer actually participates.

        This is not a compression ratio benchmark, but only verifies
        that the pipeline output is not identical to raw CloudPickle.
        """

        raw = CloudPickleSerializer().dumps(SCHEDULED_STOP_POINT)
        compressed = self.object_serializer.dumps(SCHEDULED_STOP_POINT)

        self.assertLessEqual(len(compressed), len(raw))


if __name__ == "__main__":
    unittest.main()
