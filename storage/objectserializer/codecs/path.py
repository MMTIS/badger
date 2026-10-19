from collections.abc import Sequence


def encode_path(path: Sequence[int]) -> bytes:
    """Encode a sequence of non-negative integers using unsigned LEB128."""
    result = bytearray()

    for value in path:
        if value < 0:
            raise ValueError("Path indices must be non-negative")

        while value >= 0x80:
            result.append((value & 0x7F) | 0x80)
            value >>= 7

        result.append(value)

    return bytes(result)


def decode_path(data: bytes) -> tuple[int, ...]:
    """Decode unsigned LEB128 values into a tuple of integers."""
    result: list[int] = []
    value = 0
    shift = 0

    for byte in data:
        value |= (byte & 0x7F) << shift

        if byte & 0x80:
            shift += 7
        else:
            result.append(value)
            value = 0
            shift = 0

    if shift:
        raise ValueError("Incomplete LEB128 encoding")

    return tuple(result)