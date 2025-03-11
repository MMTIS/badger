from netexio.serializer import Serializer

from typing import TypeVar

from xsdata.formats.dataclass.context import XmlContext
from xsdata.formats.dataclass.parsers import XmlParser
from xsdata.formats.dataclass.parsers.config import ParserConfig
from xsdata.formats.dataclass.parsers.handlers import LxmlEventHandler
from xsdata.formats.dataclass.serializers import XmlSerializer
from xsdata.formats.dataclass.serializers.config import SerializerConfig

from lxml import etree

T = TypeVar("T")

class MyXmlSerializer(Serializer):
    serializer: XmlSerializer
    parser: XmlParser
    ns_map = {'': 'http://www.netex.org.uk/netex', 'gml': 'http://www.opengis.net/gml/3.2'}
    sql_type = 'TEXT'

    def __init__(self):
        context = XmlContext()
        config = ParserConfig(fail_on_unknown_properties=False)
        self.parser = XmlParser(context=context, config=config, handler=LxmlEventHandler)

        serializer_config = SerializerConfig(encoding='utf-8', ignore_default_attributes=True, xml_declaration=False)
        serializer_config.indent = None
        serializer_config.ignore_default_attributes = True
        self.serializer = XmlSerializer(config=serializer_config)
        self.serializer.encoding = 'utf-8'

    def marshall(self, obj, clazz):
        if isinstance(obj, str):
            return obj
        elif isinstance(obj, etree._Element):
            return etree.tostring(obj, encoding='unicode')
        else:
            return self.serializer.render(obj, self.ns_map).replace('\n', '')

    def unmarshall(self, obj, clazz: T) -> T:
        if isinstance(obj, etree._Element):
            return self.parser.parse(obj, clazz)

        if isinstance(obj, str):
            return self.parser.from_string(obj, clazz)

        else:
            return self.parser.from_bytes(obj, clazz)