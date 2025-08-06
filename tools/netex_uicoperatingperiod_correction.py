import logging
import shutil

from utils.aux_logging import *
from netexio.dbaccess import open_netex_file
import os
import xml.etree.ElementTree as ET
import gzip
import zipfile
import tempfile
from configuration import processing_data

def addParentInfo(et):
    for child in et:
        child.attrib['__my_parent__'] = et
        addParentInfo(child)

def stripParentInfo(et):
    for child in et:
        child.attrib.pop('__my_parent__', 'None')
        stripParentInfo(child)

def getParent(et):
    if '__my_parent__' in et.attrib:
        return et.attrib['__my_parent__']
    else:
        return None

def modify_xml_content(xml_content, file_path,outfile=None):
    # Parse the XML content
    root = ET.fromstring(xml_content)
    addParentInfo(root)
    # Define the namespace
    namespaces = {'': 'http://www.netex.org.uk/netex'}  # Empty prefix for default namespace
    ET.register_namespace('','http://www.netex.org.uk/netex')
    #create parent map
    # Find all OperatingPeriodRef elements using the default namespace
    operating_period_refs = root.findall('.//OperatingPeriodRef', namespaces)

    # Check if any OperatingPeriodRef elements exist
    if operating_period_refs:
        for ref in operating_period_refs:
            # Create a new element with the desired name
            new_elem = ET.Element('UicOperatingPeriodRef')

            # Copy attributes from the original element
            new_elem.attrib = ref.attrib

            # Copy all child elements from the original element to the new element
            for child in ref:
                new_elem.append(child)

            # Set the additional attribute
            new_elem.set('nameOfRefClass', 'UicOperatingPeriod')
            # Replace the original element with the new element
            parent = getParent(ref)
            parent.remove(ref)
            parent.append(new_elem)

        stripParentInfo(root)
        # Convert the modified XML back to a string
        modified_xml = ET.tostring(root, encoding='utf-8',xml_declaration=True)

        # Write the modified XML back to the same file if it is an xml outfile is set and we write directly to it
        if outfile==None:
            outfile=file_path
        with open(outfile, 'wb') as f:
            f.write(modified_xml)
        print(f'Modified {outfile}')
    else:
        print(f'No OperatingPeriodRef found in {outfile}')


def modify_xml_file(file_like, output_file,file_type):
    if file_type == 'xml':
        # Read the content from the BufferedReader
        xml_content = file_like.read()
        modify_xml_content(xml_content, file_like.name)

    elif file_type == 'gz':
        # Decompress the gzipped content
        #xml_content = gzip.decompress(file_like.read()).decode('utf-8')
        xml_content = file_like.read().decode('utf-8')
        temp_file_path =processing_data+"/"+'temp.xml'  # Temporary file path for the modified XML
        modify_xml_content(xml_content, temp_file_path)

        # Re-compress the modified XML back to .gz
        with open(temp_file_path, 'rb') as temp_file:
            with gzip.open(output_file, 'wb') as gz_file:
                gz_file.writelines(temp_file)

        os.remove(temp_file_path)  # Clean up temporary file

    elif file_type == 'zip':
        # Create a temporary directory to extract files
        temp_dir = processing_data+"/"+"tmp_folder"
        with zipfile.ZipFile(file_like, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        # Process each XML file in the extracted directory
        for extracted_file in os.listdir(temp_dir):
            if extracted_file.endswith('.xml'):
                extracted_file_path = os.path.join(temp_dir, extracted_file)
                with open(extracted_file_path, 'rb') as f:
                    xml_content = f.read()
                modify_xml_content(xml_content, extracted_file_path)

        # Re-zip the modified XML files
        with zipfile.ZipFile(output_file, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
            for modified_file in os.listdir(temp_dir):
                zip_ref.write(os.path.join(temp_dir, modified_file), modified_file)

        # Clean up the temporary directory
        shutil.rmtree(temp_dir)
        print(f'Modified files in {file_like}')

def main(infile: str, outfile: str) -> None:
    # if filenames is not a list of str  => error
    #Start processing
    if infile.endswith('.gz'):
        file_type = "gz"
    elif infile.endswith('.zip'):
        file_type = "zip"
    elif infile.endswith('.xml'):
        file_type = 'xml'
    else:
        return
    modify_xml_file(infile, outfile, file_type)


if __name__ == '__main__':
    import argparse

    log_all(logging.INFO, f"Start processing UicOperatingPeriod problem")

    argument_parser = argparse.ArgumentParser(description='Processing UicOperatingPeriodRef')
    argument_parser.add_argument('input', nargs='+',  help='NeTEx file with problematic UicOperatingPeriodRef')
    argument_parser.add_argument('output', nargs='+',  help='NeTEx outputfile')

    args = argument_parser.parse_args()

    main(args.input)
