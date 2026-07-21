from __future__ import annotations

from typing import Any, cast

import cloudpickle

from storage.objectserializer.interface import ObjectSerializer


class CloudPickleSerializer(ObjectSerializer[Any]):
    """
    Serializer based on cloudpickle.
    """

    def __init__(self, protocol: int | None = None):
        self._protocol = protocol

    def dumps(self, obj: Any) -> bytes:
        return cast(
            bytes,
            cloudpickle.dumps(
                obj,
                protocol=self._protocol,
            ),
        )

    def loads(self, data: bytes) -> Any:
        return cloudpickle.loads(data)
