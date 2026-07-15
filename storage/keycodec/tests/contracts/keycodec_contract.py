from __future__ import annotations

from typing import Any


def assert_keycodec(
    keycodec: Any,
    value: Any,
) -> None:
    encoded = keycodec.encode(value)

    assert isinstance(encoded, bytes), (
        "Keycodec must return bytes"
    )