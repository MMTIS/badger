# @https://github.com/ue71603, 2024
import logging
import os
from utils.aux_logging import (
    prepare_logger,
    log_all,
    log_flush,
    log_write_counts,
    log_print,
)
import traceback
from lxml import etree
from netexio.dbaccess import open_netex_file


def check_xsd_validity(xsd_file):
    try:
        etree.parse(xsd_file)
        log_all(logging.INFO,f"{xsd_file} is a valid XSD file")
    except etree.XMLSyntaxError as e:
        log_all(logging.ERROR,f"{xsd_file} is an invalid XSD file: {str(e)}")
    except Exception as e:
        log_all(logging.ERROR,f"An error occurred while checking {xsd_file}: {str(e)}")
    return True

def validate_xml(xml_file, xmlschema):
    try:

        # Validate the XML file against the schema
        xmlschema.assertValid(etree.parse(xml_file))
        # Check for any errors captured by the handler
        log_all(logging.INFO, f"{xml_file} is valid against the schema")
    except etree.XMLSchemaError as e:
        log_all(logging.ERROR, f"{xml_file} is invalid: {str(e)}")
        raise e
    except Exception as e:
        log_all(logging.ERROR, f"An error occurred while validating {xml_file}: {str(e)}")
        raise e

    return True

def main(folder, xsd_schema):
    xmlschema = etree.XMLSchema(etree.parse(xsd_schema))
    for root, dirs, files in os.walk(folder):
        for filename in files:
            log_all(logging.INFO,f"Processing file: {filename}")
            file_full_path = os.path.join(root, filename)
            if filename.endswith(".xml") or filename.endswith(".xml.gz"):  #TODO zips are not processed (because might be GTFS)
                for sub_file in open_netex_file(file_full_path):
                    validate_xml(sub_file, xmlschema)

            if filename.endswith(".xsd"):
                xsd_file = os.path.join(root, filename)
                check_xsd_validity(xsd_file)


if __name__ == '__main__':
    import argparse

    argument_parser = argparse.ArgumentParser(description='Validates each file in a folder (xsd and xml)')
    argument_parser.add_argument('folder', type=str, help='The folder containing the xml files to validate')
    # argument_parser.add_argument('xmlschema', type=str, help='The schema file to use as a basis for validation')
    xmlschema="./netex-xsd/xsd/NeTEx_publication.xsd"
    argument_parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = argument_parser.parse_args()
    # Fetching the data based on command-line arguments
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.folder, xmlschema)
    except Exception as e:
        log_all(logging.ERROR, f'{e} {traceback.format_exc()}')
        raise e
