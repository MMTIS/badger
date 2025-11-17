import inspect
import warnings
from typing import IO, Any
from zoneinfo import ZoneInfo

from xsdata.formats.dataclass.context import XmlContext
from xsdata.formats.dataclass.parsers import XmlParser
from xsdata.formats.dataclass.parsers.config import ParserConfig
from xsdata.formats.dataclass.parsers.handlers import LxmlEventHandler

from domain.netex import model as netex
from domain.netex.model import VersionFrameDefaultsStructure
from domain.netex.services.model_typing import Tid
from domain.utils import get_object_name
from storage.interface import Storage
from storage.lxml.core.implementation import XmlStorage
from storage.lxml.core.time import class_contains_xml_time, recursive_replace

from lxml import etree

from storage.lxml.serialization.xmlserializer import MyXmlSerializer
from storage.mdbx.core.implementation import MdbxStorage


def get_element_name_with_ns(clazz: type[Tid]) -> str:
    name = get_object_name(clazz)
    meta = getattr(clazz, "Meta", None)

    return "{" + (meta.namespace if meta is not None else "") + "}" + name

def get_interesting_classes(
    my_filter: set[type] | None = None,
) -> tuple[list[str], list[str], list[Any]]:
    # Get all classes from the generated NeTEx Python Dataclasses
    clsmembers: list[tuple[str, type[Any]]] = inspect.getmembers(netex, inspect.isclass)

    # The interesting class members certainly will have a "Meta class" with a namespace
    interesting_members: list[tuple[str, type[Any]]] = [x for x in clsmembers if hasattr(x[1], "Meta") and hasattr(x[1].Meta, "namespace")]

    # Specifically we are interested in classes that are derived from "EntityInVersion", to find them, we exclude embedded child objects called "VersionedChild"
    entitiesinversion: list[tuple[str, type[Any]]] = [
        x for x in interesting_members if netex.VersionedChildStructure in x[1].__mro__ or netex.EntityInVersionStructure in x[1].__mro__ or netex.EntityStructure in x[1].__mro__
    ]

    # Obviously we want to have the VersionedChild too
    # versionedchild = [x for x in interesting_members if netex.VersionedChildStructure in x[1].__mro__]

    # There is one particular container in NeTEx that should reflect almost the same our collection EntityInVersion namely the "GeneralFrame"
    # general_frame_members = netex.GeneralFrameMembersRelStructure.__dataclass_fields__['choice'].metadata['choices']

    # The interesting part here is where the difference between the two lie.
    # geme = [x['type'].Meta.getattr('name', x['type'].__name__) for x in general_frame_members]
    # envi = [x[0] for x in entitiesinversion]
    # set(geme) - set(envi)

    if my_filter is not None:
        clean_element_names = [x[0] for x in entitiesinversion if x[1] in my_filter]
        interesting_element_names = [get_element_name_with_ns(x[1]) for x in entitiesinversion if x[1] in my_filter]
        interesting_clazzes = [x[1] for x in entitiesinversion if x[1] in my_filter]
    else:
        clean_element_names = [x[0] for x in entitiesinversion if not x[0].endswith("Frame")]
        interesting_element_names = [get_element_name_with_ns(x[1]) for x in entitiesinversion if not x[0].endswith("Frame") and not x[0].endswith("Structure") and not x[0].endswith("Dummy")]
        interesting_clazzes = [x[1] for x in entitiesinversion if not x[0].endswith("Frame") and not x[0].endswith("Structure") and not x[0].endswith("Dummy")]

    return clean_element_names, interesting_element_names, interesting_clazzes

def get_local_name(element: type[Tid]) -> str:
    meta = getattr(element, "Meta", None)
    if meta:
        return getattr(meta, "name", element.__name__)

    return element.__name__



def insert_database(
    storage: MdbxStorage,
    classes: tuple[list[str], list[str], list[Any]],
    f: IO[Any] | None = None,
    type_of_frame_filter: list[str] | None = None) -> None:

    myserializer: MyXmlSerializer = MyXmlSerializer([])
    xml_context = XmlContext()
    parser_config = ParserConfig(fail_on_unknown_properties=False)
    parser = XmlParser(context=xml_context, config=parser_config, handler=LxmlEventHandler)

    # TODO: resolve this part in a more generic way
    clsmembers = inspect.getmembers(netex, inspect.isclass)
    all_frames = [
        get_local_name(x[1])
        for x in clsmembers
        if hasattr(x[1], "Meta") and hasattr(x[1].Meta, "namespace") and netex.VersionFrameVersionStructure in x[1].__mro__
    ]

    obj_clazz: type | None = None
    obj_list: list[Tid] = [] # We use this for buffering

    all_with_id = [get_local_name(x[1]) for x in clsmembers if hasattr(x[1], "id")]
    all_with_version = [get_local_name(x[1]) for x in clsmembers if hasattr(x[1], "version")]

    # See: https://github.com/NeTEx-CEN/NeTEx/issues/788
    # all_datasource_refs = [x[0] for x in clsmembers if hasattr(x[1], 'Meta') and hasattr(x[1].Meta, 'namespace') and hasattr(x[1], 'data_source_ref_attribute')]
    all_datasource_refs = [
        get_local_name(x[1])
        for x in clsmembers
        if hasattr(x[1], "Meta") and hasattr(x[1].Meta, "namespace") and netex.DataManagedObjectStructure in x[1].__mro__
    ]
    all_responsibility_set_refs = [
        get_local_name(x[1]) for x in clsmembers if hasattr(x[1], "Meta") and hasattr(x[1].Meta, "namespace") and netex.EntityInVersionStructure in x[1].__mro__
    ]
    all_srs_name = [get_local_name(x[1]) for x in clsmembers if hasattr(x[1], "Meta") and hasattr(x[1], "srs_name")]

    all_classes_with_xml_time = [get_local_name(x[1]) for x in clsmembers if hasattr(x[1], "Meta") and class_contains_xml_time(x[1])]

    frame_defaults_stack: list[netex.VersionFrameDefaultsStructure | None] = []
    if f is None:
        return

    clean_element_names, interesting_element_names, interesting_classes = classes
    clazz_by_name = {}

    for i in range(0, len(interesting_element_names)):
        clazz_by_name[interesting_element_names[i]] = interesting_classes[i]

    events = ("start", "end")
    context = etree.iterparse(f, events=events, remove_blank_text=True)
    _current_frame_id = None
    current_element_tag = None
    current_framedefaults = None
    current_datasource_ref = None
    current_responsibility_set_ref = None
    current_location_system = None
    current_zoneinfo: ZoneInfo | None = None
    last_id_stack = []
    last_version_stack = []
    skip_frame = False

    location_srsName = None
    for event, element in context:
        localname = element.tag.split("}")[-1]  # localname

        if event == "start":
            if current_element_tag is None and element.tag in interesting_element_names:
                current_element_tag = element.tag

            if localname in all_with_id:
                id = element.attrib.get("id", None)
                if id is None:
                    id = last_id_stack[-1][1].replace(last_id_stack[-1][0], localname)
                    element.attrib['id'] = id

                last_id_stack.append(
                    (
                        localname,
                        id,
                    )
                )

            if localname in all_with_version:
                version = element.attrib.get("version", None)
                if version is None:
                    if localname in all_with_id:
                        version = last_version_stack[-1]
                    else:  # This is a ref, and we cannot yet know if this reference exists
                        version = "any"

                element.attrib['version'] = version
                last_version_stack.append(version)

            elif localname == "TypeOfFrameRef":
                if type_of_frame_filter is not None and element.attrib["ref"] not in type_of_frame_filter:
                    # TODO: log a single warning that an unknown TypeOfFrame is found, and is not processed
                    print(f"{element.attrib['ref']} is not a known TypeOfFrame")
                    skip_frame = True

            if localname in all_frames:
                _current_frame_id = (element.attrib['id'], element.attrib['version'])
                frame_defaults_stack.append(None)

            elif localname == "Location":
                if "srsName" in element.attrib:
                    location_srsName = element.attrib["srsName"]

        elif event == "end":
            # current_element_tag = element.tag
            if localname == "FrameDefaults":
                xml = etree.tostring(element, encoding="unicode")
                frame_defaults: VersionFrameDefaultsStructure = parser.from_string(xml, VersionFrameDefaultsStructure)
                frame_defaults_stack[-1] = frame_defaults
                current_framedefaults = frame_defaults
                if current_framedefaults.default_data_source_ref is not None:
                    current_datasource_ref = current_framedefaults.default_data_source_ref.ref
                if current_framedefaults.default_responsibility_set_ref is not None:
                    current_responsibility_set_ref = current_framedefaults.default_responsibility_set_ref.ref
                if current_framedefaults.default_location_system is not None:
                    current_location_system = current_framedefaults.default_location_system
                if current_framedefaults.default_locale and current_framedefaults.default_locale.time_zone:
                    current_zoneinfo = ZoneInfo(current_framedefaults.default_locale.time_zone)

                # TODO: Metadata nog niet geimplementeerd
                # if current_frame_id is not None:
                #    db.insert_metadata_on_queue([(current_frame_id[0], current_frame_id[1], frame_defaults)])

                continue

            elif localname in all_frames:
                # This is the end of the frame, pop the frame_defaults stack
                frame_defaults_stack.pop()
                filtered = [fd for fd in frame_defaults_stack if fd is not None]
                current_framedefaults = filtered[-1] if len(filtered) > 0 else None

                current_datasource_ref = None
                current_responsibility_set_ref = None
                current_location_system = None
                current_zoneinfo = None
                for fd in reversed(filtered):
                    if current_datasource_ref is None:
                        if fd.default_data_source_ref is not None:
                            current_datasource_ref = fd.default_data_source_ref.ref
                    if current_responsibility_set_ref is None:
                        if fd.default_responsibility_set_ref is not None:
                            current_responsibility_set_ref = fd.default_responsibility_set_ref.ref
                    if current_location_system is None:
                        if fd.default_location_system is not None:
                            current_location_system = fd.default_location_system
                    if current_zoneinfo is None:
                        if fd.default_locale and fd.default_locale.time_zone:
                            current_zoneinfo = ZoneInfo(fd.default_locale.time_zone)

                if localname in all_with_id:  # Feels redundant
                    last_id_stack.pop()

                if localname in all_with_version:  # Feels redundant
                    last_version_stack.pop()

                skip_frame = False
                continue

            if skip_frame:
                continue

            if current_framedefaults is not None:
                if current_datasource_ref is not None and localname in all_datasource_refs:
                    if "dataSourceRef" not in element.attrib:
                        element.attrib["dataSourceRef"] = current_datasource_ref

                if current_responsibility_set_ref is not None and localname in all_responsibility_set_refs:
                    if "responsibilitySetRef" not in element.attrib:
                        element.attrib["responsibilitySetRef"] = current_responsibility_set_ref

                if current_location_system is not None:
                    if localname in all_srs_name:
                        if "srsName" not in element.attrib:
                            element.attrib["srsName"] = location_srsName if location_srsName is not None else current_location_system

                    if localname == "Location":
                        if "srsName" not in element.attrib:
                            element.attrib["srsName"] = current_location_system

                        location_srsName = None

            if (
                current_element_tag == element.tag
            ):  # https://stackoverflow.com/questions/65935392/why-does-elementtree-iterparse-sometimes-retrieve-xml-elements-incompletely
                if "id" not in element.attrib:
                    if localname in all_with_id:  # Feels redundant
                        last_id_stack.pop()

                    if localname in all_with_version:  # Feels redundant
                        last_version_stack.pop()

                    current_element_tag = None
                    # print(xml)
                    continue

                clazz = clazz_by_name[element.tag]

                id = element.attrib["id"]
                order = element.attrib.get("order", None)
                object = myserializer.unmarshall(element, clazz)

                if False and current_zoneinfo is not None:  # TODO: Fix this after we can do this in xsData
                    if localname in all_classes_with_xml_time:
                        recursive_replace(object, current_zoneinfo)

                if hasattr(clazz, "order"):
                    if order is None:
                        warnings.warn(f"{localname} {id} does not have a required order, setting it to 1.")
                        order = 1
                        object.order = order

                # TODO: Als we deze nu eens vervangen voor een lijst van objecten, tot het object type wijzigt.
                if clazz != obj_clazz or len(obj_list) > 10000:
                    if obj_clazz is not None and len(obj_list) > 0:
                        storage.insert_objects_on_queue(obj_clazz, obj_list, False)
                        obj_list = []
                    obj_clazz = clazz

                obj_list.append(object)
                current_element_tag = None

            elif current_element_tag is None:
                pass

            if localname in all_with_id:
                last_id_stack.pop()

            if localname in all_with_version:
                last_version_stack.pop()

    if obj_clazz is not None and len(obj_list) > 0:
        storage.insert_objects_on_queue(obj_clazz, obj_list, False)
        obj_list = []