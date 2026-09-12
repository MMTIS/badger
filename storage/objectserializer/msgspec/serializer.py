from collections import UserString
from decimal import Decimal
from enum import Enum
import importlib
import pathlib
from typing import Any, cast
from dataclasses import is_dataclass
from xml.etree.ElementTree import QName

import msgspec

from storage.objectserializer.interface import ObjectSerializer


class MsgspecSerializer(ObjectSerializer):
    """
    High-performance MessagePack serializer based on msgspec.
    Supports Python builtins, dataclasses, and XML schema datatypes.
    """

    _class_cache: dict[str, type[Any]] = {}

    @staticmethod
    def _enc_hook(obj: Any) -> Any:
        if isinstance(obj, (Decimal, pathlib.Path)):
            return str(obj)
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, QName):
            return str(obj)
        if isinstance(obj, UserString):
            return str(obj)
        if hasattr(obj, "from_string"):
            return str(obj)
        raise NotImplementedError(f"Cannot serialize object of type {type(obj)}")

    @staticmethod
    def _dec_hook(target_type: type[Any], obj: Any) -> Any:
        if target_type is Decimal:
            return Decimal(str(obj))
        if target_type is pathlib.Path:
            return pathlib.Path(str(obj))
        if isinstance(target_type, type) and issubclass(target_type, Enum):
            return target_type(obj)
        if target_type is QName or (
            isinstance(target_type, type) and issubclass(target_type, QName)
        ):
            return QName(str(obj))
        if hasattr(target_type, "from_string"):
            return target_type.from_string(str(obj))
        if isinstance(target_type, type) and issubclass(target_type, UserString):
            return target_type(str(obj))
        raise NotImplementedError(f"Cannot deserialize object of type {target_type}")

    def __init__(self, target_type: type[Any] | None = None):
        self._target_type = target_type
        self._encoder = msgspec.msgpack.Encoder(enc_hook=self._enc_hook)

    @classmethod
    def _resolve_class(cls, class_identifier: str) -> type[Any] | None:
        if not class_identifier:
            return None
        if class_identifier in cls._class_cache:
            return cls._class_cache[class_identifier]

        module_name, class_name = class_identifier.rsplit(":", 1)
        mod = importlib.import_module(module_name)
        resolved = getattr(mod, class_name)
        cls._class_cache[class_identifier] = resolved
        return resolved

    def dumps(self, obj: Any) -> bytes:
        if self._target_type is not None:
            return cast(bytes, self._encoder.encode(obj))

        if is_dataclass(obj):
            cls = obj.__class__
            identifier = f"{cls.__module__}:{cls.__qualname__}"
            self._class_cache[identifier] = cls
            payload = self._encoder.encode(obj)
            return cast(bytes, msgspec.msgpack.encode((identifier, payload)))

        payload = self._encoder.encode(obj)
        return cast(bytes, msgspec.msgpack.encode(("", payload)))

    def loads(self, data: bytes, clazz: type[Any] | None = None) -> Any:
        target_cls = clazz or self._target_type
        if target_cls is not None:
            try:
                return msgspec.msgpack.decode(
                    data,
                    type=target_cls,
                    dec_hook=self._dec_hook,
                )
            except TypeError:
                raw_dict = msgspec.msgpack.decode(data, dec_hook=self._dec_hook)
                if isinstance(raw_dict, dict) and hasattr(target_cls, "__dataclass_fields__"):
                    return target_cls(**{k: v for k, v in raw_dict.items() if k in target_cls.__dataclass_fields__})
                return raw_dict

        tag, payload = msgspec.msgpack.decode(data)
        if tag:
            resolved_cls = self._resolve_class(tag)
            if resolved_cls is not None:
                try:
                    return msgspec.msgpack.decode(
                        payload,
                        type=resolved_cls,
                        dec_hook=self._dec_hook,
                    )
                except TypeError:
                    raw_dict = msgspec.msgpack.decode(payload, dec_hook=self._dec_hook)
                    if isinstance(raw_dict, dict) and hasattr(resolved_cls, "__dataclass_fields__"):
                        return resolved_cls(**{k: v for k, v in raw_dict.items() if k in resolved_cls.__dataclass_fields__})
                    return raw_dict
        return msgspec.msgpack.decode(payload, dec_hook=self._dec_hook)
