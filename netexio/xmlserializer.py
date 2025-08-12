from netex import EntityStructure
from netexio.serializer import Serializer

from typing import TypeVar, Any, cast

from xsdata.formats.dataclass.context import XmlContext
from xsdata.formats.dataclass.parsers import XmlParser
from xsdata.formats.dataclass.parsers.config import ParserConfig
from xsdata.formats.dataclass.parsers.handlers import LxmlEventHandler
from xsdata.formats.dataclass.serializers import XmlSerializer
from xsdata.formats.dataclass.serializers.config import SerializerConfig

from lxml import etree

from utils.utils import get_object_name

T = TypeVar("T")
Tid = TypeVar("Tid", bound=EntityStructure)


class MyXmlSerializer(Serializer):
    serializer: XmlSerializer
    parser: XmlParser
    ns_map = {
        "": "http://www.netex.org.uk/netex",
        "gml": "http://www.opengis.net/gml/3.2",
    }
    sql_type = "TEXT"

    def __init__(self) -> None:
        Serializer.__init__(self)
        context = XmlContext()
        config = ParserConfig(fail_on_unknown_properties=False)
        self.parser = XmlParser(context=context, config=config, handler=LxmlEventHandler)

        serializer_config = SerializerConfig(encoding="utf-8", ignore_default_attributes=True, xml_declaration=False)
        serializer_config.indent = None
        serializer_config.ignore_default_attributes = True
        self.serializer = XmlSerializer(config=serializer_config)

    @staticmethod
    def encode_key(id: str | None, version: str | None, clazz: type[T], include_clazz: bool = False) -> bytes:
        return ((id or '') + "-" + (version or 'any')).encode("utf-8")

    @staticmethod
    def encode_key_by_key(key: bytes, clazz: type[T]) -> bytes:
        return get_object_name(clazz).encode('utf-8') + b'-' + key

    def marshall(self, obj: Any, clazz: type[T], pretty_print: bool = False) -> str:
        self.serializer.config.pretty_print = pretty_print
        if isinstance(obj, str):
            return obj
        elif isinstance(obj, etree._Element):
            return cast(str, etree.tostring(obj, encoding="unicode"))
        elif pretty_print:
            return self.serializer.render(obj, self.ns_map)
        else:
            return self.serializer.render(obj, self.ns_map).replace("\n", "")

    def unmarshall(self, obj: Any, clazz: type[T]) -> T:
        if isinstance(obj, etree._Element):
            return self.parser.parse(obj, clazz)

        if isinstance(obj, str):
            return self.parser.from_string(obj, clazz)

        else:
            return self.parser.from_bytes(obj, clazz)
