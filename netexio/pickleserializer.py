import inspect
import string

import netex
from netex import EntityStructure
from netexio.dbaccess import get_local_name
from netexio.serializer import Serializer
import lz4.frame
import cloudpickle
import re
from netexio.xmlserializer import MyXmlSerializer
from utils.utils import get_object_name
from typing import TypeVar, Any, cast

T = TypeVar("T")
Tid = TypeVar("Tid", bound=EntityStructure)


class MyPickleSerializer(Serializer):
    SEPARATOR = ord("-")
    SPECIAL_CHAR = ord("*")
    WORD_MASK = "#"
    xmlserializer: MyXmlSerializer = MyXmlSerializer()

    def __init__(self, compression: bool = True) -> None:
        Serializer.__init__(self)
        self.compression = compression

        # This should really be done generically...
        clsmembers = inspect.getmembers(netex, inspect.isclass)
        self.encoding_to_class = {
            MyPickleSerializer.encode_string(get_local_name(x[1]).upper(), None, mask=False): x[1] for x in clsmembers if hasattr(x[1], "id")
        }

    @staticmethod
    def encode_string(value: str, obj_name: str | None, mask: bool = True) -> bytes:
        """Encodes a string by replacing special characters and masking the object name."""
        if mask and obj_name is not None:
            value = re.sub(rf"\b{re.escape(obj_name)}\b", MyPickleSerializer.WORD_MASK, value.upper())
        return bytes(
            (ord(char) if char in string.ascii_uppercase or char in string.digits or char == MyPickleSerializer.WORD_MASK else MyPickleSerializer.SPECIAL_CHAR)
            for char in value
        )

    @staticmethod
    def encode_key(id: str | None, version: str | None, clazz: type[T], include_clazz: bool = False) -> bytes:
        obj_name = get_object_name(clazz).upper()
        encoded_bytes = bytearray()

        if include_clazz:
            encoded_bytes.extend(MyPickleSerializer.encode_string(obj_name, obj_name, False))
            encoded_bytes.append(MyPickleSerializer.SEPARATOR)

        if id is not None:
            encoded_bytes.extend(MyPickleSerializer.encode_string(id, obj_name))
            encoded_bytes.append(MyPickleSerializer.SEPARATOR)

        if version is not None and version != "any":
            encoded_bytes.extend(MyPickleSerializer.encode_string(version, obj_name))

        return bytes(encoded_bytes)

    # I want these two methods combined.
    @staticmethod
    def encode_key_by_key(key: bytes, clazz: type[T]) -> bytes:
        def encode_string(value: str, obj_name: str, mask: bool = True) -> bytes:
            """Encodes a string by replacing special characters and masking the object name."""
            if mask:
                value = re.sub(rf"\b{re.escape(obj_name)}\b", MyPickleSerializer.WORD_MASK, value.upper())
            return bytes(
                (
                    ord(char)
                    if char in string.ascii_uppercase or char in string.digits or char == MyPickleSerializer.WORD_MASK
                    else MyPickleSerializer.SPECIAL_CHAR
                )
                for char in value
            )

        obj_name = get_object_name(clazz).upper()
        encoded_bytes = bytearray()
        encoded_bytes.extend(encode_string(obj_name, obj_name, False))
        encoded_bytes.append(MyPickleSerializer.SEPARATOR)
        encoded_bytes.extend(key)
        return bytes(encoded_bytes)

    def decode_key(self, key: bytes) -> tuple[type[Tid], bytes] | None:
        parts = key.split(bytes((MyPickleSerializer.SEPARATOR,)))
        if len(parts) == 3:
            return self.encoding_to_class[parts[0]], bytes((MyPickleSerializer.SEPARATOR,)).join(parts[1:])

        return None

    def marshall(self, obj: Any, clazz: type[T]) -> bytes:
        if not getattr(obj, "__module__").startswith("netex."):  # TODO: can we just get the parent?
            obj = self.xmlserializer.unmarshall(obj, clazz)

        if self.compression:
            return cast(bytes, lz4.frame.compress(cloudpickle.dumps(obj)))
        else:
            return cast(bytes, cloudpickle.dumps(obj))

    def unmarshall(self, obj: bytes, clazz: type[T]) -> T:
        if self.compression:
            return cast(T, cloudpickle.loads(lz4.frame.decompress(obj)))
        else:
            return cast(T, cloudpickle.loads(obj))
