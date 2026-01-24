from io import BytesIO, TextIOWrapper
from pathlib import Path
import logging
import os
import xml.etree.ElementTree as ET
import zipfile

from utils.aux_logging import *
from storage.lxml.core.implementation import XmlStorage

from isal import igzip_threaded
import codecs

def my_replace_error(exc):
    # exc is a UnicodeDecodeError instance
    # return a tuple (replacement_string, position_to_resume)
    return ('?', exc.end)




# Some Italian files have illegal characters in their stops. We asked them to improve on that, but they didn't so we fix it here.


def _read_stream_as_text(stream) -> str:
    """
    Read bytes or text from a stream and return a Unicode string.
    Replace any invalid UTF-8 sequences with the replacement character.
    """
    codecs.register_error('myreplace', my_replace_error)

    # Try to read bytes first
    try:
        # Some XmlStorage file handles may be binary; read bytes
        data = stream.read()
        # If stream.read() returns str, keep it. If bytes, decode with replacement.
        if isinstance(data, bytes):
            text = data.decode('utf-8', errors='myreplace')
            print("ping")
        else:
            # It's already str: ensure that any undecodable parts are replaced
            # (str in Python are already Unicode; nothing to do).
            text = data
    except Exception:
        # As a fallback, try reading text mode (some handles behave differently)
        try:
            stream.seek(0)
        except Exception:
            pass
        wrapper = TextIOWrapper(stream, encoding="utf-8", errors="ignore")
        text = wrapper.read()
        try:
            wrapper.detach()
        except Exception:
            pass
    finally:
        try:
            stream.seek(0)
        except Exception:
            pass
    return text


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
            if not target_id:
                continue
            match = root.find(f".//UicOperatingPeriod[@id='{target_id}']", namespaces)
            if match is not None:
                ref.attrib['nameOfRefClass'] = 'UicOperatingPeriod'


def modify_xml_file(file_path: Path, output_filename: str):
    """
    Open Netex file using XmlStorage, read file(s), sanitize invalid UTF-8 sequences,
    parse, modify, and write to output (supports .gz, .zip or plain file).
    """
    xml_storage = XmlStorage(file_path)

    # If output is a zip, open it once for append (we create if not exists)
    if output_filename.endswith(".zip"):
        zf = zipfile.ZipFile(output_filename, "a", zipfile.ZIP_DEFLATED)
    else:
        zf = None

    try:
        for f, real_filename in xml_storage.open_netex_file():
            # Read the raw file contents and ensure UTF-8 with replacement for invalid bytes
            # Many XmlStorage file objects implement a binary file-like interface.
            text = _read_stream_as_text(f)

            # Parse from string (ElementTree.fromstring expects str)
            try:
                et = ET.ElementTree(ET.fromstring(text))
            except ET.ParseError as e:
                # If parsing still fails, write a helpful error and re-raise
                logging.error("XML parse error in %s: %s", real_filename, e)
                raise

            # Apply your modification
            modify_xml_content(et.getroot(), file_path.name)

            # Write output depending on extension
            if output_filename.endswith(".gz"):
                with igzip_threaded.open(  # type: ignore
                    output_filename,
                    "wb",
                    compresslevel=3,
                    threads=3,
                    block_size=2 * 10**8,
                ) as out:
                    # ElementTree.write will write bytes if file is binary
                    et.write(out, encoding="utf-8", xml_declaration=True)
            elif output_filename.endswith(".zip"):
                buffer = BytesIO()
                et.write(buffer, encoding='utf-8', xml_declaration=True)
                xml_bytes = buffer.getvalue()
                zf.writestr(real_filename, xml_bytes)
            else:
                # plain file - write the single XML content (if XmlStorage had multiple files
                # and output is non-zip, this will overwrite repeatedly; this mirrors your template)
                with open(output_filename, "wb") as out:
                    et.write(out, encoding="utf-8", xml_declaration=True)
    finally:
        if zf is not None:
            zf.close()


def main(infile: str, outfile: str) -> None:
    # checks the input
    inpath = Path(infile)
    outpath = Path(outfile)
    # calling correction
    try:
        os.remove(outfile)
    except FileNotFoundError:
        pass
    modify_xml_file(inpath, outfile)



if __name__ == '__main__':
    import argparse

    log_all(logging.INFO, "Start processing XML files and make sure that everything is UTF-8")

    argument_parser = argparse.ArgumentParser(description='Processing XML files for illegal characters')
    argument_parser.add_argument('input', help='NeTEx file with problematic characters')
    argument_parser.add_argument('output', help='NeTEx outputfile')

    args = argument_parser.parse_args()

    main(args.input, args.output)
