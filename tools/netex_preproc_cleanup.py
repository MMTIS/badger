from io import BytesIO
from pathlib import Path

from utils.aux_logging import *
from storage.lxml.core.implementation import XmlStorage

from isal import igzip_threaded
import os
import zipfile
from typing import Set, List
import re
import xml.etree.ElementTree as ET
from collections import Counter
from typing import Iterable


_id_starts_with_digit = re.compile(r'^\d')



def _local_name(tag: str) -> str:
    if tag.startswith('{'):
        return tag.split('}', 1)[1]
    return tag

def simplify_version(root: ET.Element,
                     elements_to_ignore: Iterable[str] = ("PublicationDelivery", "ResourceFrame","SiteFrame", "CalendarFrame",
                                                          "ServiceFrame","TimetableFrame","COMMON","TypeOfFrame"),
                     consider_namespaces: bool = False) -> None:
    """
    Normalize version attribute values in-place.

    Additional checks:
      - Prefer the most used version that is not "any" as the canonical value.
      - Compute the count of "any".
      - If there are other version values (excluding canonical and "any") that appear
        more than 5 times, emit a warning.
      - If all values are "any", canonical will be "any".

    Parameters:
      root: Element — root element to traverse
      elements_to_ignore: iterable of element names to skip
      consider_namespaces: if False, compare element local-names; if True, compare elem.tag exactly.

    Returns:
      None (modifies tree in place)
    """
    versions = []

    for elem in root.iter():
        if consider_namespaces:
            if elem.tag in elements_to_ignore:
                continue
        else:
            if _local_name(elem.tag) in elements_to_ignore:
                continue

        v = elem.get('version')
        if v is not None:
            versions.append(v)

    if not versions:
        return

    counts = Counter(versions)

    any_count = counts.get('any', 0)

    # Find the most common value that is not "any"
    non_any_items = [(val, cnt) for val, cnt in counts.items() if val != 'any']
    if non_any_items:
        # sort by count desc, then by value for deterministic tie-break
        non_any_items.sort(key=lambda x: (-x[1], x[0]))
        most_common_non_any, most_common_non_any_count = non_any_items[0]
        canonical = most_common_non_any
    else:
        # only "any" exists
        canonical = 'any'
        most_common_non_any = None
        most_common_non_any_count = 0

    # Check for other values (excluding canonical and 'any') appearing more than 5 times
    others_over_5 = [(val, cnt) for val, cnt in counts.items()
                     if val not in (canonical, 'any') and cnt > 5]

    # Issue warnings if needed
    if any_count > 0:
        log_print(f"Found 'any' {any_count} time(s). Using canonical version '{canonical}'.")

    if others_over_5:
        # list the problematic values
        details = ", ".join(f"'{v}':{c}" for v, c in others_over_5)
        log_print(f"Found other version values occurring more than 5 times: {details}. Aborting this function")
        return

    # Finally overwrite all version attributes (except ignored elements)
    for elem in root.iter():
        if consider_namespaces:
            if elem.tag in elements_to_ignore:
                continue
        else:
            if _local_name(elem.tag) in elements_to_ignore:
                continue

        if elem.get('version') is not None:
            elem.set('version', canonical)

def fix_linestring_ids(root: ET.Element,
                       consider_namespaces: bool = False) -> None:
    """
    Ensure all LineString elements have an id attribute that starts with a letter.
    If an id starts with a digit, prefix it with "fix-".
    Modifies the tree in place.

    Parameters:
    - root: ET.Element — the root element to search under
    - consider_namespaces: bool — if False (default), match elements by local name
                                    (ignores namespaces). If True, match only when
                                    the tag exactly equals 'LineString' or a namespaced
                                    tag that includes the namespace braces.
    Returns:
    - None
    """
    def local_name(tag: str) -> str:
        if tag.startswith('{'):
            return tag.split('}', 1)[1]
        return tag

    for elem in root.iter():
        if consider_namespaces:
            # match only when the full tag equals 'LineString' or any namespaced variant
            # (i.e. exact tag including namespace) — this means only tags that end with
            # 'LineString' but keep their namespace are matched as well.
            # To be strict: require the local name to be exactly 'LineString' but keep namespace considered
            match = (elem.tag == 'LineString') or (elem.tag.startswith('{') and local_name(elem.tag) == 'LineString')
            if not match:
                continue
        else:
            # ignore namespace, match solely by local name
            if local_name(elem.tag) != 'LineString':
                continue

        id_val = elem.get('id')
        if id_val and _id_starts_with_digit.match(id_val):
            elem.set('id', 'fix-' + id_val)


def remove_id_and_version_from_tags(root: ET.Element,
                                    target_tags: Iterable[str] = ("Location", "Centroid"),
                                    consider_namespaces: bool = False) -> None:
    """
    Remove attributes 'id' and 'version' from elements whose tag is in target_tags.
    Operates in-place on the tree rooted at `root`.

    Parameters:
    - root: xml.etree.ElementTree.Element - root element (or any element) to process.
    - target_tags: iterable of tag names to target (default: ("Location", "Centroid")).
      If your XML uses namespaces, these should match either the raw tag values
      (e.g. "{http://...}Location") when consider_namespaces is False,
      or the local names (e.g. "Location") when consider_namespaces is True.
    - consider_namespaces: if True, compare the localname (strip namespace) when checking tags.
    """
    targets: List[str] = set(target_tags)

    def localname(tag: str) -> str:
        if tag.startswith("{"):
            return tag.split("}", 1)[1]
        return tag

    for elem in root.iter():
        tag_to_check = localname(elem.tag) if consider_namespaces else elem.tag
        if tag_to_check in targets:
            # remove attributes if present
            elem.attrib.pop("id", None)
            elem.attrib.pop("version", None)

def replace_versionref_with_version(root: ET.Element,
                                     exclude_tags: Iterable[str] = ("TypeOfFrameRef",),
                                     consider_namespaces: bool = False) -> None:
    """
    Replace attributes named 'versionRef' with 'version' (same value) for all elements
    in the tree rooted at `root`, except for elements whose tag is in `exclude_tags`.

    This function modifies the tree in place.

    Parameters:
    - root: xml.etree.ElementTree.Element - root element (or any element) to process.
    - exclude_tags: iterable of tag names to exclude (default: ("TypeOfFrameRef",)).
      If your XML uses namespaces, these should match the raw tag values (including namespace),
      unless consider_namespaces=True (see below).
    - consider_namespaces: if True, the function compares the localname of the tag
      (stripping any namespace) when checking the exclude list.
    """
    exclude_set: List[str] = set(exclude_tags)

    def localname(tag: str) -> str:
        # strip namespace if present: "{ns}local" -> "local"
        if tag.startswith("{"):
            return tag.split("}", 1)[1]
        return tag

    for elem in root.iter():
        tag_to_check = localname(elem.tag) if consider_namespaces else elem.tag
        if tag_to_check in exclude_set:
            continue

        if "versionRef" in elem.attrib:
            # keep value, set new attribute, remove old
            val = elem.attrib.pop("versionRef")
            # if "version" exists it will be overwritten with the same value (or you can choose to keep)
            elem.set("version", val)



def process_file(file_path, output_filename, actions: Iterable[str] | None = None):
    # normalize actions to a set for fast membership checks
    if actions is None:
        actions_set = set()
    else:
        actions_set = set(actions)

    xml_storage = XmlStorage(file_path)
    filecounter = 0
    for f, real_filename in xml_storage.open_netex_file():
        et = ET.parse(f)

        # replaces versionRef with version for most elements
        if "VERSIONREF" in actions_set or not actions_set:
            log_print("Replaces versionRef with version for most elements.")
            replace_versionref_with_version(et.getroot())

        # removes id and version from elements like Centroid and Location
        if "REMOVEUNNECESSARYIDTAGS" in actions_set or not actions_set:
            log_print("Removes id and version from elements like Centroid and Location")
            remove_id_and_version_from_tags(et.getroot())

        # Fixes the line string id to become valid
        if "FIXLINESTRINGID" in actions_set or not actions_set:
            log_print("Fixes the line string id to become valid")
            fix_linestring_ids(et.getroot())

        # simplify versions if possible: especially if there are only any and one other
        if "REMOVEAMBIGUOUSANY" in actions_set or not actions_set:
            log_print(" simplify versions if possible: especially if there are only any and one other")
            # call corresponding function...
    filecounter = filecounter + 1
    # Comes from xml.py
    if output_filename.endswith(".gz"):
        with igzip_threaded.open(  # type: ignore
                output_filename,
                "wb",
                compresslevel=3,
                threads=3,
                block_size=2 * 10 ** 8,
        ) as out:
            et.write(out)
    elif output_filename.endswith(".zip"):
        with zipfile.ZipFile(output_filename, "a", zipfile.ZIP_DEFLATED) as zf:
            buffer = BytesIO()
            et.write(buffer, encoding='utf-8', xml_declaration=True)
            xml_bytes = buffer.getvalue()
            if "<ZipInfo" in real_filename:
                zf.writestr(f"file_{filecounter}.xml", xml_bytes)
            else:
                zf.writestr(real_filename, xml_bytes)
    else:
        with open(output_filename, "wb") as out:
            et.write(out)



def ensure_same_extension(input_path: str, output_path: str) -> None:
    if Path(input_path).suffix != Path(output_path).suffix:
        raise ValueError(f"File extensions differ: {input_path} ({Path(input_path).suffix!r}) != "
                         f"{output_path} ({Path(output_path).suffix!r})")

def netex_processing(infile: Path, outfile: Path, actions : Iterable[str] | None = None):
    # we need to have the same extension for this step to work
    ensure_same_extension(str(infile),str(outfile))

    try:
        os.remove(outfile)
    except FileNotFoundError:
        pass
    process_file(infile, str(outfile),actions)


def main(infile: str, outfile: str, actions : Iterable[str] | None = None) -> None:
    # checks the input
    inpath = Path(infile)
    outpath = Path(outfile)
    # calling correction
    netex_processing(inpath, outpath,actions=actions)


if __name__ == '__main__':
    import argparse

    log_all(logging.INFO, f"Some French files contain versionRef instead of version in many places. This removes them. Also id/version are removed from Centroid/Location")

    argument_parser = argparse.ArgumentParser(description='Removing unnecessary versionRef and replacing them with version')
    argument_parser.add_argument('input', help='NeTEx file with problematic versionRef')
    argument_parser.add_argument('actions', nargs='+', default=set(), help='actions to take')
    argument_parser.add_argument('output', help='NeTEx outputfile')

    args = argument_parser.parse_args()

    main(args.input, args.output, args.actions)
