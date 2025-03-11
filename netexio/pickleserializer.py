import string
from netexio.serializer import Serializer
import lz4.frame
import cloudpickle
import re
from netexio.xmlserializer import MyXmlSerializer
from utils.utils import get_object_name
from typing import TypeVar

T = TypeVar("T")


class MyPickleSerializer(Serializer):
    xmlserializer: MyXmlSerializer = MyXmlSerializer()

    def __init__(self, compression: bool = True):
        Serializer.__init__(self)
        self.compression = compression

    @staticmethod
    def encode_key(id, version, clazz, include_clazz=False):
        SEPARATOR = ord('-')
        SPECIAL_CHAR = ord('*')
        WORD_MASK = '#'


        def encode_string(value, obj_name, mask=True):
            """Encodes a string by replacing special characters and masking the object name."""
            if mask:
                value = re.sub(rf'\b{re.escape(obj_name)}\b', WORD_MASK, value.upper())
            return bytes(
                ord(char) if char in string.ascii_uppercase or char in string.digits or char == WORD_MASK else SPECIAL_CHAR for
                char in value)

        obj_name = get_object_name(clazz).upper()
        encoded_bytes = bytearray()

        if include_clazz:
            encoded_bytes.extend(encode_string(obj_name, obj_name, False))
            encoded_bytes.append(SEPARATOR)

        if id is not None:
            encoded_bytes.extend(encode_string(id, obj_name))
            encoded_bytes.append(SEPARATOR)

        if version is not None and version != 'any':
            encoded_bytes.extend(encode_string(version, obj_name))

        return bytes(encoded_bytes)

    def marshall(self, obj, clazz: T):
        if not getattr(obj, '__module__', None).startswith('netex.'):  # TODO: can we just get the parent?
            obj = self.xmlserializer.unmarshall(obj, clazz)

        if self.compression:
            return lz4.frame.compress(cloudpickle.dumps(obj))
        else:
            return cloudpickle.dumps(obj)

    def unmarshall(self, obj, clazz: T) -> T:
        if self.compression:
            return cloudpickle.loads(lz4.frame.decompress(obj))
        else:
            return cloudpickle.loads(obj)
