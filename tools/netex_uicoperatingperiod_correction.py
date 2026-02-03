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

def _serialize_etree_to_bytes(tree):
    buf = BytesIO()
    tree.write(buf, encoding="utf-8", xml_declaration=True)
    return buf.getvalue()

def modify_xml_file(file_path, output_filename):
    xml_storage = XmlStorage(file_path)
    filecounter = 0
    for f, real_filename in xml_storage.open_netex_file():
        et = ET.parse(f)
        modify_xml_content(et.getroot(), file_path.name)
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


def netex_uicoperatingperiod_correction(infile: Path, outfile: Path):
    ensure_same_extension(str(infile), str(outfile))

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
