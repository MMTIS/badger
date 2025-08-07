import logging
import shutil
from io import BytesIO

from utils.aux_logging import *
from netexio.dbaccess import open_netex_file
from isal import igzip_threaded
import os
import xml.etree.ElementTree as ET
import gzip
import zipfile
import tempfile
from configuration import processing_data


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

def modify_xml_file(file_name, output_filename):
    print(file_name)
    for f, real_filename in open_netex_file(file_name):
        et = ET.parse(f)
        modify_xml_content(et.getroot(), file_name)

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
                zf.writestr(real_filename, xml_bytes)
        else:
            with open(output_filename, "wb") as out:
                et.write(out)


def main(infile: str, outfile: str) -> None:
    modify_xml_file(infile, outfile)


if __name__ == '__main__':
    import argparse

    log_all(logging.INFO, f"Start processing UicOperatingPeriod problem")

    argument_parser = argparse.ArgumentParser(description='Processing UicOperatingPeriodRef')
    argument_parser.add_argument('input', help='NeTEx file with problematic UicOperatingPeriodRef')
    argument_parser.add_argument('output', help='NeTEx outputfile')

    args = argument_parser.parse_args()

    main(args.input, args.output)
