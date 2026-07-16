from __future__ import annotations

from dataclasses import dataclass

import pytest

from storage.keycodec.baseline import BaseLineKeyCodec

from storage.keycodec.tests.contracts.keycodec_contract import (
    assert_keycodec,
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
def baseline_keycodec() -> BaseLineKeyCodec:
    return BaseLineKeyCodec(
        class_byte={ScheduledStopPoint: b'\0'},
    )


def test_baseline_keycodec(baseline_keycodec):
    assert_keycodec(
        baseline_keycodec,
        SCHEDULED_STOP_POINT,
    )