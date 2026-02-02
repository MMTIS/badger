from io import BytesIO
from pathlib import Path

from utils.aux_logging import *
from storage.lxml.core.implementation import XmlStorage

from isal import igzip_threaded
import os
import xml.etree.ElementTree as ET
import zipfile


def modify_xml_content(root, file_path, outfile=None):
    # Define the namespace
    namespaces = {'': 'http://www.netex.org.uk/netex'}  # Empty prefix for default namespace
    ET.register_namespace('', 'http://www.netex.org.uk/netex')
    # Find all OperatingPeriodRef elements using the default namespace

    for ref in root.iterfind(".//OperatingPeriodRef", namespaces):
        if 'nameOfRefClass' in ref.attrib:
            continue

        else:
            target_id = ref.attrib.get('ref')
            match = root.find(f".//UicOperatingPeriod[@id='{target_id}']", namespaces)

            if match is not None:
                ref.attrib['nameOfRefClass'] = 'UicOperatingPeriod'

def _serialize_etree_to_bytes(etree_element_or_tree):
    """
    Return bytes for an ElementTree (or element). Produces utf-8 bytes
    with XML declaration.
    """
    # If user passed an ElementTree, get its root for tostring but include declaration.
    # ET.tostring for Element does not include XML declaration; ET.write to BytesIO does.
    buf = BytesIO()
    # If etree_element_or_tree is an ElementTree, it has write(); if it's Element, wrap it.
    if hasattr(etree_element_or_tree, "write"):
        etree_element_or_tree.write(buf, encoding="utf-8", xml_declaration=True)
    else:
        # It's an Element
        tree = ET.ElementTree(etree_element_or_tree)
        tree.write(buf, encoding="utf-8", xml_declaration=True)
    return buf.getvalue()

def modify_xml_file(file_path, output_filename):
    """
    Modify XML files contained in file_path (could be archive or plain xml) and
    write results to output_filename (same type). Preserve zip internal paths.
    """
    file_path = Path(file_path)
    output_filename = Path(output_filename)

    xml_storage = XmlStorage(file_path)

    # Determine output type once
    out_is_zip = output_filename.suffix.lower() == ".zip"
    out_is_gz = output_filename.suffix.lower() == ".gz"
    out_is_plain = not (out_is_zip or out_is_gz)

    if out_is_zip:
        # Open once in append mode (creates file if not exists). Using 'a' preserves
        # existing entries; 'w' would recreate. Choose based on desired behaviour.
        # If you want to overwrite, use mode='w' instead.
        with zipfile.ZipFile(output_filename, mode="a", compression=zipfile.ZIP_DEFLATED) as zf:
            for f, real_filename in xml_storage.open_netex_file():
                # parse & modify
                et = ET.parse(f)
                modify_xml_content(et.getroot(), file_path.name)

                # Ensure zip internal path uses forward slashes and does not start with a leading slash
                # real_filename can be something like "dir/sub/file.xml" or "file.xml"
                # Normalize using pathlib and then convert to posix (forward slashes)
                # If real_filename is already a str path inside the archive, normalize simply:
                try:
                    real_path = Path(real_filename)
                    zip_internal_name = real_path.as_posix().lstrip("/")
                except Exception:
                    zip_internal_name = str(real_filename).lstrip("/")

                xml_bytes = _serialize_etree_to_bytes(et)
                # write into zip preserving directories
                zf.writestr(zip_internal_name, xml_bytes)
    elif out_is_gz:
        # If input contains multiple files and output is a single .gz, you'll need to
        # decide how to combine them. Assuming input is single-file .gz <-> .gz
        # Open output once and write the single modified file.
        # If xml_storage yields multiple files, you may want to raise.
        with igzip_threaded.open(  # type: ignore
            str(output_filename),
            "wb",
            compresslevel=3,
            threads=3,
            block_size=2 * 10**8,
        ) as out:
            # Expect exactly one file yielded if input was gz -> gz
            first = True
            for f, real_filename in xml_storage.open_netex_file():
                if not first:
                    raise RuntimeError("Multiple files in input but output is a single .gz")
                et = ET.parse(f)
                modify_xml_content(et.getroot(), file_path.name)
                # write directly to gz file-like object to avoid extra copy
                et.write(out, encoding="utf-8", xml_declaration=True)
                first = False
    else:
        # plain output file
        # Expect exactly one file in storage if output is single plain file
        first = True
        with open(output_filename, "wb") as out:
            for f, real_filename in xml_storage.open_netex_file():
                if not first:
                    raise RuntimeError("Multiple files in input but output is a single plain file")
                et = ET.parse(f)
                modify_xml_content(et.getroot(), file_path.name)
                et.write(out, encoding="utf-8", xml_declaration=True)
                first = False


def netex_uicoperatingperiod_correction(infile: Path, outfile: Path):
    try:
        os.remove(outfile)
    except FileNotFoundError:
        pass
    modify_xml_file(infile, str(outfile))


def main(infile: str, outfile: str) -> None:
    # checks the input
    inpath = Path(infile)
    outpath = Path(outfile)
    # calling correction
    netex_uicoperatingperiod_correction(inpath, outpath)


if __name__ == '__main__':
    import argparse

    log_all(logging.INFO, f"Start processing UicOperatingPeriod problem. This code adds NameOfClass for the usage of UicOperatingPeriod in OperatingPeriods.")

    argument_parser = argparse.ArgumentParser(description='Processing UicOperatingPeriodRef')
    argument_parser.add_argument('input', help='NeTEx file with problematic UicOperatingPeriodRef')
    argument_parser.add_argument('output', help='NeTEx outputfile')

    args = argument_parser.parse_args()

    main(args.input, args.output)
