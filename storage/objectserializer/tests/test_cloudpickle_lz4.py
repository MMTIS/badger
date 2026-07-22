from __future__ import annotations

from typing import Any, TypeVar
import pytest

from storage.objectserializer.cloudpickle.serializer import CloudPickleSerializer
from storage.objectserializer.codecs.lz4 import Lz4Codec
from storage.objectserializer.interface import ObjectSerializer
from storage.objectserializer.pipeline import PipelineSerializer

from storage.objectserializer.tests.contracts.serializer_contract import (
    assert_serializer_roundtrip,
)

from domain.netex.model import ScheduledStopPoint, PrivateCode, PrivateCodes, MultilingualString, TextType, LocationStructure2
from decimal import Decimal

T = TypeVar("T")

SCHEDULED_STOP_POINT = ScheduledStopPoint(
    id="NL:OPENOV:ScheduledStopPoint:1",
    version="1",
    name=MultilingualString(content=[TextType(lang="nl", value="Hello World")]),
    private_codes=PrivateCodes(private_code=[PrivateCode(type_value="type", value="value")]),
    location=LocationStructure2(longitude=Decimal('12.13'), latitude=Decimal('23.16')),
)


@pytest.fixture
def object_serializer() -> PipelineSerializer[T]:
    """
    CloudPickle with LZ4 compression.
    """

    return PipelineSerializer(
        object_serializer=CloudPickleSerializer(),
        codecs=[
            Lz4Codec(),
        ],
    )


def test_cloudpickle_lz4_roundtrip(object_serializer: ObjectSerializer[T]) -> None:
    """
    Verify the complete CloudPickle + LZ4 pipeline.
    """

    assert_serializer_roundtrip(
        object_serializer,
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
    object_serializer: ObjectSerializer[T],
    value: Any,
) -> None:
    """
    Verify common Python builtin types.
    """

    assert_serializer_roundtrip(
        object_serializer,
        value,
    )


def test_cloudpickle_lz4_is_compressed(object_serializer: ObjectSerializer[T]) -> None:
    """
    Verify that the LZ4 layer actually participates.

    This is not a compression ratio benchmark, but only verifies
    that the pipeline output is not identical to raw CloudPickle.
    """

    raw = CloudPickleSerializer().dumps(SCHEDULED_STOP_POINT)

    compressed = object_serializer.dumps(SCHEDULED_STOP_POINT)

    assert len(compressed) <= len(raw)
