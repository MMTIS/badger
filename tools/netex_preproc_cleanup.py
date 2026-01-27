from io import BytesIO
from pathlib import Path

from utils.aux_logging import *
from storage.lxml.core.implementation import XmlStorage

from isal import igzip_threaded
import os
import zipfile

import xml.etree.ElementTree as ET
from typing import Iterable, Set


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
    targets: Set[str] = set(target_tags)

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
    exclude_set: Set[str] = set(exclude_tags)

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



def process_file(file_path, output_filename):
    xml_storage = XmlStorage(file_path)
    filecounter=0
    for f, real_filename in xml_storage.open_netex_file():
        et = ET.parse(f)
        # replaces versionRef with version for most elements
        replace_versionref_with_version(et.getroot())
        #removes id and version from elements like Centroid and Location
        remove_id_and_version_from_tags(et.getroot())
        filecounter =filecounter+1
        artifical_file_name="file"
        # Comes from xml.py
        if output_filename.endswith(".gz"):
            with igzip_threaded.open(  # type: ignore
                output_filename,
                "wb",
                compresslevel=3,
                threads=3,
                block_size=2 * 10**8,
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



def netex_processing(infile: Path, outfile: Path):
    try:
        os.remove(outfile)
    except FileNotFoundError:
        pass
    process_file(infile, str(outfile))


def main(infile: str, outfile: str) -> None:
    # checks the input
    inpath = Path(infile)
    outpath = Path(outfile)
    # calling correction
    netex_processing(inpath, outpath)


if __name__ == '__main__':
    import argparse

    log_all(logging.INFO, f"Some French files contain versionRef instead of version in many places. This removes them. Also id/version are removed from Centroid/Location")

    argument_parser = argparse.ArgumentParser(description='Removing unnecessary versionRef and replacing them with version')
    argument_parser.add_argument('input', help='NeTEx file with problematic versionRef')
    argument_parser.add_argument('output', help='NeTEx outputfile')

    args = argument_parser.parse_args()

    main(args.input, args.output)
