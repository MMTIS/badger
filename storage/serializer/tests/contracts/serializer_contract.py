from __future__ import annotations

from typing import Any


def assert_serializer_roundtrip(
    serializer: Any,
    value: Any,
) -> None:
    """
    Generic serializer contract.

    Requirement:

        loads(dumps(value)) == value
    """

    encoded = serializer.dumps(value)

    assert isinstance(encoded, bytes), (
        "Serializer must return bytes"
    )

    decoded = serializer.loads(encoded)

    assert decoded == value, (
        "Deserialized value differs from original"
    )