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
    file_path = Path(file_path)
    output_filename = Path(output_filename)

    xml_storage = XmlStorage(file_path)

    out_sfx = output_filename.suffix.lower()
    out_is_zip = out_sfx == ".zip"
    out_is_gz = out_sfx == ".gz"

    if out_is_zip:
        # Open once; use 'w' to overwrite or 'a' to append existing output
        with zipfile.ZipFile(output_filename, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for f, real_filename in xml_storage.open_netex_file():
                et = ET.parse(f)
                modify_xml_content(et.getroot(), file_path.name)

                # Preserve directory structure using forward slashes, no leading slash.
                # Example: 'AVL_AVL/NX-PI-01_...xml' -> 'AVL_AVL/NX-PI-01_...xml'
                try:
                    arcname = Path(real_filename).as_posix().lstrip("/")
                except Exception:
                    arcname = str(real_filename).replace("\\", "/").lstrip("/")

                xml_bytes = _serialize_etree_to_bytes(et)
                zf.writestr(arcname, xml_bytes)

    elif out_is_gz:
        # Expect single-file mapping for .gz -> .gz
        with igzip_threaded.open(str(output_filename), "wb", compresslevel=3, threads=3, block_size=2 * 10 ** 8) as out:
            it = xml_storage.open_netex_file()
            f, real_filename = next(it, (None, None))
            if f is None:
                return
            et = ET.parse(f)
            modify_xml_content(et.getroot(), file_path.name)
            et.write(out, encoding="utf-8", xml_declaration=True)
            if next(it, None) is not None:
                raise RuntimeError("Input archive contains multiple files but output is single .gz")

    else:
        # plain output: expect single file
        it = xml_storage.open_netex_file()
        f, real_filename = next(it, (None, None))
        if f is None:
            return
        et = ET.parse(f)
        modify_xml_content(et.getroot(), file_path.name)
        with open(output_filename, "wb") as out:
            et.write(out, encoding="utf-8", xml_declaration=True)
        if next(it, None) is not None:
            raise RuntimeError("Input archive contains multiple files but output is single plain file")

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
