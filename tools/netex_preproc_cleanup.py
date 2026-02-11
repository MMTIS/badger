import uuid
from io import BytesIO
from pathlib import Path

from utils.aux_logging import *
from storage.lxml.core.implementation import XmlStorage

from isal import igzip_threaded
import os
import zipfile
from typing import Set, List, Dict, Tuple, Optional
import re
import xml.etree.ElementTree as ET
from collections import Counter
from typing import Iterable

from urllib.parse import urlparse

FALLBACK_URL = "https://opentransportdata.swiss"

_id_starts_with_digit = re.compile(r'^\d')



def _local_name(tag: str) -> str:
    if tag.startswith('{'):
        return tag.split('}', 1)[1]
    return tag

def simplify_version(root: ET.Element,
                     elements_to_exclude: Iterable[str] = (),
                     consider_namespaces: bool = False
                     ) -> None:
    """
    Count concrete versions globally (excluding elements_to_exclude), select the
    most frequent concrete version if it appears at least 10 times, and replace
    all version="any" with that chosen version.

    Returns the applied version string if replacement was done, otherwise None.
    """
    excluded = set(elements_to_exclude)

    # Count all version values globally (including None and "any")
    counts = Counter()
    for elem in root.iter():
        tag_key = elem.tag if consider_namespaces else _local_name(elem.tag)
        if not tag_key or tag_key in excluded:
            continue
        counts[elem.get("version")] += 1

    # Filter to concrete versions (not None and not "any") and pick the most common
    concrete_counts = {v: n for v, n in counts.items() if v is not None and v != "any"}
    if not concrete_counts:
        return None

    # pick the version with highest count; tie-break deterministically by lexical order
    chosen_version = max(concrete_counts.items(), key=lambda item: (item[1], item[0]))[0]
    chosen_count = concrete_counts[chosen_version]

    # apply only if chosen_count >= 10
    if chosen_count < 10:
        return None

    # replace all "any" with chosen_version (excluding excluded tags)
    for elem in root.iter():
        tag_key = elem.tag if consider_namespaces else _local_name(elem.tag)
        if not tag_key or tag_key in excluded:
            continue
        if elem.get("version") == "any":
            elem.set("version", chosen_version)

    return None

def local_name_from_attr(attr_name):
    # Clark notation "{ns}local" -> "local"
    if attr_name.startswith('{'):
        end = attr_name.find('}')
        if end != -1:
            return attr_name[end+1:]
    # Prefixed form "ns:local" -> "local"
    return attr_name.split(':', 1)[-1]

def set_emails(root: ET.Element, consider_namespaces: bool = False) -> None:
    """
    Walk the XML tree under root and set all <Email> elements that have no meaningful
    text to 'opendata@sbb.ch'.

    An Email element is considered empty if:
    - elem.text is None
    - elem.text is empty or only whitespace
    - elem.text (after stripping) equals the literal 'none' (case-insensitive)

    If consider_namespaces is True, elements are matched by local-name only,
    so namespaced <ns:Email> elements are also found. Otherwise only tag == 'Email'.
    """
    target = "Email"
    replacement = "opendata@sbb.ch"

    for elem in root.iter():
        tag_matches = (
            (not consider_namespaces and _local_name(elem.tag) == target)
            or (consider_namespaces and elem.tag == target)
        )
        if not tag_matches:
            continue

        text: Optional[str] = elem.text
        if text is None:
            elem.text = replacement
            continue

        # Normalize text for checks
        stripped = text.strip()
        if stripped == "" or stripped.lower() == "none":
            elem.text = replacement

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

        for attr_name, attr_val in list(elem.attrib.items()):
            local_attr_name = local_name_from_attr(attr_name)
            if local_attr_name == 'id':
                id_val = attr_val
                if id_val and _id_starts_with_digit.match(id_val):
                    # preserve original attribute key (including namespace) when setting
                    elem.set(attr_name, 'fix-' + id_val)
                break


def remove_id_and_version_from_tags(root: ET.Element,
                                    target_tags: Iterable[str] = ("Location", "Centroid", "responsibilitySets"),
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


def include_order_in_id(root: ET.Element,
                        elements_to_process: Iterable[str] = ("NoticeAssignment", "PassengerStopAssignment","AlternativeName"),
                        consider_namespaces: bool = False) -> None:
    """
    Walk the element tree rooted at `root` and for each element whose tag matches one of
    `elements_to_process`, take its "order" attribute and add it to its "id" attribute
    separated by a hyphen ("-").

    The function modifies the tree in place and returns None.

    Parameters:
    - root: the root Element to start the search from.
    - elements_to_process: iterable of tag names to process. If consider_namespaces is False,
      these should be local names (without namespace). If consider_namespaces is True,
      these should match the element.tag value (including namespace braces).
    - consider_namespaces: whether to treat the provided names as namespace-aware (True)
      or to match only the local name part of element.tag (False).
    """
    # Normalize set for faster membership tests
    targets = set(elements_to_process)

    for elem in root.iter():
        tag_to_check = elem.tag if consider_namespaces else (elem.tag.rsplit('}', 1)[-1] if isinstance(elem.tag, str) and elem.tag.startswith('{') else elem.tag)
        if tag_to_check in targets:
            order = elem.get("order")
            id_val = elem.get("id")
            if not order:
                # nothing to append
                continue
            if not id_val:
                # if there is no id, we can either skip or set id to order; here we set id to order
                elem.set("id", order)
                continue
            # Avoid duplicating the suffix if already present
            suffix = f"-{order}"
            if id_val.endswith(suffix):
                continue
            elem.set("id", f"{id_val}{suffix}")

def change_order_0(root: ET.Element,
                   elements_to_process: Iterable[str] = ("PassengerStopAssignment","AlternativeName"),
                   consider_namespaces: bool = False) -> None:
    """
    Traverse the XML tree rooted at `root` and for each element whose tag matches one of
    `elements_to_process`, if it has an attribute 'order' with value "0", replace it with "1".
    The tree is modified in place; the function returns None.

    Parameters
    - root: ElementTree Element root to process.
    - elements_to_process: iterable of tag names to process. By default ("PassengerStopAssignment",).
      If consider_namespaces is False (default) the comparison uses the local tag name (namespace stripped).
      If consider_namespaces is True the comparison uses the full tag (including namespace in "{uri}local" form).
    - consider_namespaces: whether to compare tags including namespace part.
    """
    # normalize elements_to_process to a set for fast membership tests
    names = set(elements_to_process)

    for elem in root.iter():
        tag_to_check = elem.tag if consider_namespaces else _local_name(elem.tag)
        if tag_to_check in names:
            # Check for 'order' attribute exactly equal to the string "0"
            if elem.get("order") == "0":
                elem.set("order", "1")



def add_id_version(root: ET.Element,
                   include_tags: Iterable[str] = ("AlternativeName","AlternativeText", "OperatorRef","DayTypeRef","LineRef",
                                                "ScheduledStopPointRef", "ServiceJourneyPatternRef", "PassingTime","StopPointInJourneyPatternRef","TimetabledPassingTime"),
                   consider_namespaces: bool = False) -> None:
    """
    Add a unique id attribute (if missing) and version="any" (if missing)11
    to elements under root whose tag matches include_tags.

    Parameters:
    - root: xml.etree.ElementTree.Element -- root element to search.
    - include_tags: iterable of tag names to include (local names). If
      consider_namespaces is True, the items should match the full tag
      (including namespace braces) or you can still pass local names and
      they'll match local part only (behavior below).
    - consider_namespaces: if False (default), matching is done against the
      local part of the tag (namespace is ignored). If True, full tag string
      (including namespace) is used for matching; however local names in
      include_tags will still match as convenience.
    """
    # Normalize include_tags into a set for efficient membership tests
    include_set = set(include_tags)

    for elem in root.iter():
        tag_for_match = elem.tag if consider_namespaces else _local_name(elem.tag)

        # allow include_tags to contain either full tag or local name when consider_namespaces True:
        matches = False
        if consider_namespaces:
            if tag_for_match in include_set:
                matches = True
            else:
                # also accept local names in include_tags for convenience
                if _local_name(elem.tag) in include_set:
                    matches = True
        else:
            if tag_for_match in include_set:
                matches = True

        if not matches:
            continue

        # Add id if missing
        if "id" not in elem.attrib:
            # generate unique id; prefix 'id-' so it's a valid name and more readable
            new_id = "id-" + uuid.uuid4().hex
            elem.set("id", new_id)

        # Add version if missing
        if "version" not in elem.attrib:
            elem.set("version", "any")


def _is_valid_url(url: str) -> bool:
    """
    Consider a URL valid if it has a scheme (http/https) and a netloc.
    """
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    # netloc is required (domain or host)
    if not parsed.netloc:
        return False
    return True

def remove_attrs(
    root: ET.Element,
    include_attrs: Iterable[str] = ("dataSourceRef",),
    consider_namespaces: bool = False,
) -> None:
    """
    Remove attributes from all elements in the tree rooted at `root`.

    - include_attrs: iterable of attribute names. If consider_namespaces is False,
      these are compared against the local name (without namespace). If True,
      they must match the full attribute key as stored in ElementTree (e.g. '{ns}local' or 'local').
    - consider_namespaces: when False (default), strip namespace before comparing.

    The function modifies the tree in place and returns None.
    """
    # Normalize include_attrs to a set for faster membership tests
    include_set = set(include_attrs)

    # Iterate root and all descendants
    for elem in root.iter():
        if not elem.attrib:
            continue

        # Build list of keys to remove to avoid changing dict during iteration
        to_remove = []
        if consider_namespaces:
            # Match full attribute keys
            for key in elem.attrib.keys():
                if key in include_set:
                    to_remove.append(key)
        else:
            # Match local name only (strip '{ns}' if present)
            for key in elem.attrib.keys():
                if _local_name(key) in include_set:
                    to_remove.append(key)

        for key in to_remove:
            elem.attrib.pop(key, None)

def remove_refs(root: ET.Element,
                include_tags: Iterable[str] = ("SupplyContactRef","TopographicPlaceRef", "ParentSiteRef","TypeOfPlaceRef","BrandingRef"),
                consider_namespaces: bool = False) -> None:
    """
    Remove elements whose tag is in include_tags from the tree rooted at root.

    Parameters:
    - root: ElementTree Element to process (modified in-place).
    - include_tags: iterable of tag names (local names or full tags) to remove.
      Default removes 'SupplyContactRef'.
    - consider_namespaces: if False (default), matching is done on local names
      (namespace is ignored). If True, include_tags may contain either full
      tag strings (with {namespace}) or local names; both will be accepted.
    """
    include_set = set(include_tags)

    # We need parent access to remove children. Element.iter() does not provide parent,
    # so iterate over all parents and examine their direct children.
    for parent in root.iter():
        # create a list of children to remove to avoid modifying the list while iterating
        to_remove = []
        for child in list(parent):
            tag_for_match = child.tag if consider_namespaces else _local_name(child.tag)

            matches = False
            if consider_namespaces:
                # accept full tag or local name in include_set
                if child.tag in include_set or _local_name(child.tag) in include_set:
                    matches = True
            else:
                if tag_for_match in include_set:
                    matches = True

            if matches:
                to_remove.append(child)

        for child in to_remove:
            parent.remove(child)

def make_url_useful(root: ET.Element, consider_namespaces: bool = False) -> None:
    """
    Normalize and validate text content of elements named 'Url'.

    For each matching element:
    - Trim whitespace from text.
    - If the text already looks like a valid URL with http/https, keep it.
    - Else, try prepending 'https://' and validate again.
    - If still invalid, set text to FALLBACK_URL.

    The function mutates elements in-place and returns None.
    """
    for elem in root.iter():
        tag_for_match = elem.tag if consider_namespaces else _local_name(elem.tag)
        if tag_for_match != "Url":
            continue

        text = elem.text or ""
        text = text.strip()

        if not text:
            elem.text = FALLBACK_URL
            continue

        # If already valid (has scheme and netloc), keep it
        if _is_valid_url(text):
            elem.text = text
            continue

        # If missing scheme, try prepending https:// and validate
        # But avoid double-adding if it already starts with something like "//host"
        candidate = text
        parsed = urlparse(candidate)
        if not parsed.scheme:
            # handle protocol-relative URLs like //example.com -> add https:
            if candidate.startswith("//"):
                candidate = "https:" + candidate
            else:
                candidate = "https://" + candidate

        if _is_valid_url(candidate):
            elem.text = candidate
        else:
            # final fallback
            elem.text = FALLBACK_URL


def remove_lux_problems(root: ET.Element, consider_namespaces: bool = True) -> None:
    """
    Modify the XML tree in-place. For every element whose local name is
    'FromJourney' or 'ToJourney', replace its attribute 'nameOfRefClass'
    with the value 'ServiceJourney'.

    Args:
      root: the ElementTree Element to process (typically the document root).
      consider_namespaces: if True, elements named '{namespace}FromJourney'
        or '{namespace}ToJourney' will also be matched. If False, only matches
        based on local (non-namespaced) tag names.

    Note: This changes attributes in-place and does not return anything.
    """
    target_names = {"FromJourneyRef", "ToJourneyRef"}

    if consider_namespaces:
        # Match by local name regardless of namespace URI.
        for elem in root.iter():
            if _local_name(elem.tag) in target_names:
                # Set/replace the attribute
                elem.set("nameOfRefClass", "ServiceJourney")
    else:
        # Match only when the tag equals the plain name (no namespace).
        for elem in root.iter():
            # If elem.tag contains a namespace it will be like '{ns}Local'
            if not elem.tag.startswith("{"):
                if elem.tag in target_names:
                    elem.set("nameOfRefClass", "ServiceJourney")
            else:
                # skip namespaced tags when not considering namespaces
                continue



def remove_sncf_problems(root: ET.Element, consider_namespaces: bool = False  ) -> None:
    """
            Fix SNCF-specific issues in-place.

            - Remove elements DestinationDisplayRef and OperatorRef when they have attribute ref == "".
            - Remove elements TypeOfLineRef and routes unconditionally.
            - Remove attributes responsibilitySetRef when value == "".

            If consider_namespaces is False, match by local name. If True, match full QName.
            """
    # target names
    dest_name = "DestinationDisplayRef"
    op_name = "OperatorRef"
    type_of_line_name = "TypeOfLineRef"
    routes_name = "routes"
    ref_attr_local = "ref"
    resp_attr_local = "responsibilitySetRef"

    # 1) Remove child elements based on parent iteration (safe removal)
    for parent in root.iter():
        children = list(parent)
        for child in children:
            child_tag = child.tag if consider_namespaces else _local_name(child.tag)

            # Remove unconditional elements
            if child_tag == (type_of_line_name if consider_namespaces else type_of_line_name) \
                    or child_tag == (routes_name if consider_namespaces else routes_name):
                parent.remove(child)
                continue

            # Remove DestinationDisplayRef or OperatorRef only if ref == ""
            if child_tag == (dest_name if consider_namespaces else dest_name) \
                    or child_tag == (op_name if consider_namespaces else op_name):
                # Find ref attribute value (consider namespace variants)
                ref_val = None
                # prefer exact 'ref' key, otherwise search attributes by local name
                if "ref" in child.attrib:
                    ref_val = child.attrib.get("ref")
                else:
                    for akey, aval in child.attrib.items():
                        if _local_name(akey) == ref_attr_local:
                            ref_val = aval
                            break
                if ref_val == "":
                    parent.remove(child)
                    continue

    # 2) Remove responsibilitySetRef attributes when value == ""
    for elem in root.iter():
        if not elem.attrib:
            continue
        to_remove = []
        for key, val in elem.attrib.items():
            if val != "":
                continue
            key_local = _local_name(key)
            if key_local == resp_attr_local:
                # if consider_namespaces==True and you want to restrict to exact QName
                # you could check `if consider_namespaces and key != resp_attr_local: skip`
                to_remove.append(key)
        for key in to_remove:
            elem.attrib.pop(key, None)

    # Remove attributes responsibilitySetRef with value == ""
    # Walk all elements and remove matching attributes
    for elem in root.iter():
        if not elem.attrib:
            continue
        to_remove = []
        if consider_namespaces:
            # Remove attributes whose full key equals the exact name (unlikely) or whose local name matches
            for key, val in elem.attrib.items():
                if key == "responsibilitySetRef" and val == "":
                    to_remove.append(key)
                elif _local_name(key) == "responsibilitySetRef" and val == "":
                    to_remove.append(key)
        else:
            for key, val in elem.attrib.items():
                if _local_name(key) == "responsibilitySetRef" and val == "":
                    to_remove.append(key)
        for key in to_remove:
            elem.attrib.pop(key, None)





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
            log_print("Fixes the line string id to become valid as it is not allowed to start with a number.")
            fix_linestring_ids(et.getroot())

        if "ADDIDVERSION" in actions_set or not actions_set:
            log_print("Adds id and version to a a set of Tags")
            add_id_version(et.getroot())

        if "FIXORDER0" in actions_set or not actions_set:
            log_print("some files contain order='0'. We replace it with order='1'")
            change_order_0(et.getroot())

        if "INCLUDEORDERINID" in actions_set or not actions_set:
            log_print("include order in the id of some elements")
            include_order_in_id(et.getroot())

        if "SIMPLIFYVERSION" in actions_set or not actions_set:
            log_print("use only any for some elements")
            simplify_version(et.getroot())

        if "FIXEMAILNONE" in actions_set or not actions_set:
            log_print("Remove a 'None' in the eMail.")
            set_emails(et.getroot())

        if "ADDHTTPSURL" in actions_set or not actions_set:
            log_print("GTFS demands real URL so, we need to add them before")
            make_url_useful(et.getroot())

        if "REMOVESOMEREFS" in actions_set or not actions_set:
            log_print("Removing some Refs that are not present as elements and not relevant.")
            remove_refs(et.getroot())
        if "REMOVESOMEATTRS" in actions_set or not actions_set:
            log_print("Removing some attributs that cause problems (France dataSourceRef)")
            remove_attrs(et.getroot())

        if "UICOPERATINGPERIODCORRECTION" in actions_set or not actions_set:
            log_print("Correction UIC Operating period.")
            #TODO currently separate file as it does a bit more....netex_uicoperatingperiod_correction.py

        if "REMOVESNCFPROBLEMS" in actions_set or not actions_set:
            log_print("Fix some additional SNCF problems. Remove empty DestinationDisplayRefs, OperatorRefs and attribute responsibilitySets")
            remove_sncf_problems(et.getroot())

        if "REMOVELUXPROBLEMS" in actions_set or not actions_set:
            log_print("Fix some additional LUX problems.  It replaces the nameOfClassRef in FromJourney and ToJourney with 'ServiceJourney'")
            remove_lux_problems(et.getroot())
        if "NONE" in actions_set or not actions_set:
            log_print("No action. But processes.")


        #Saving file
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
