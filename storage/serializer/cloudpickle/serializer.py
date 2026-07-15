from __future__ import annotations

from typing import Any

import cloudpickle

from storage.serializer.interface import Serializer


class CloudPickleSerializer(Serializer[Any]):
    """
    Serializer based on cloudpickle.
    """

    def __init__(self, protocol: int | None = None):
        self._protocol = protocol

    def dumps(self, obj: Any) -> bytes:
        return cloudpickle.dumps(
            obj,
            protocol=self._protocol,
        )

    def loads(self, data: bytes) -> Any:
        return cloudpickle.loads(data)