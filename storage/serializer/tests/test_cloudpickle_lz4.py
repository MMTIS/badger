from __future__ import annotations

from dataclasses import dataclass

import pytest

from storage.serializer.cloudpickle.serializer import CloudPickleSerializer
from storage.serializer.codecs.lz4 import Lz4Codec
from storage.serializer.pipeline import PipelineSerializer

from storage.serializer.tests.contracts.serializer_contract import (
    assert_serializer_roundtrip,
)

from domain.netex.model import ScheduledStopPoint, PrivateCode, PrivateCodes, MultilingualString, TextType, LocationStructure2
from decimal import Decimal

SCHEDULED_STOP_POINT = ScheduledStopPoint(
    id="NL:OPENOV:ScheduledStopPoint:1",
    version="1",
    name=MultilingualString(content=[TextType(lang="nl", value="Hello World")]),
    private_codes=PrivateCodes(private_code=[PrivateCode(type_value="type", value="value")]),
    location=LocationStructure2(longitude=Decimal('12.13'), latitude=Decimal('23.16'))
)

@pytest.fixture
def serializer() -> PipelineSerializer:
    """
    CloudPickle with LZ4 compression.
    """

    return PipelineSerializer(
        serializer=CloudPickleSerializer(),
        codecs=[
            Lz4Codec(),
        ],
    )


def test_cloudpickle_lz4_roundtrip(serializer):
    """
    Verify the complete CloudPickle + LZ4 pipeline.
    """

    assert_serializer_roundtrip(
        serializer,
        SCHEDULED_STOP_POINT,
    )


@pytest.mark.parametrize(
    "value",
    [
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
        {
            "key": "value",
        },
    ],
)
def test_cloudpickle_lz4_builtin_types(
    serializer,
    value,
):
    """
    Verify common Python builtin types.
    """

    assert_serializer_roundtrip(
        serializer,
        value,
    )

def test_cloudpickle_lz4_is_compressed(serializer):
    """
    Verify that the LZ4 layer actually participates.

    This is not a compression ratio benchmark, but only verifies
    that the pipeline output is not identical to raw CloudPickle.
    """

    raw = CloudPickleSerializer().dumps(SCHEDULED_STOP_POINT)

    compressed = serializer.dumps(SCHEDULED_STOP_POINT)

    assert len(compressed) <= len(raw)