"""
Fixes GTFS CSV files in a zip archive by escaping unescaped quotes within quoted fields.
In GTFS CSV files, fields may contain quotes that need to be escaped by doubling them.
This script processes each .txt file in the input zip, finds quotes within quoted fields
that are not properly escaped, and doubles them (e.g., "GUITRAD CENTRE "TERMI" MESCHEDE"
becomes "GUITRAD CENTRE ""TERMI"" MESCHEDE").
"""

import argparse
import zipfile


def fix_line(line):
    """
    Fixes a CSV line by escaping unescaped quotes within quoted fields.
    Any quote inside a quoted field (not at the boundary) is doubled.
    """
    result = []
    in_quotes = False
    i = 0
    while i < len(line):
        c = line[i]
        if c == '"':
            if in_quotes:
                # Check if this is a closing quote by looking ahead
                j = i + 1
                while j < len(line) and line[j] == ' ':
                    j += 1
                if j < len(line) and line[j] == ',':
                    # This is a closing quote
                    result.append('"')
                    in_quotes = False
                    i += 1
                elif j == len(line):
                    # This is a closing quote at end of line
                    result.append('"')
                    in_quotes = False
                    i += 1
                else:
                    # This is an internal quote, escape it
                    result.append('""')
                    i += 1
            else:
                # Opening quote
                result.append('"')
                in_quotes = True
                i += 1
        else:
            result.append(c)
            i += 1
    return ''.join(result)


def main(input_zip:str, output_zip: str):

    with zipfile.ZipFile(input_zip, 'r') as zin:
        with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                if item.filename.endswith('.txt'):
                    # Read and fix the file
                    with zin.open(item) as f:
                        content = f.read().decode('utf-8-sig')
                    lines = content.splitlines(True)
                    fixed_lines = [fix_line(line) for line in lines]
                    fixed_content = ''.join(fixed_lines)
                    zout.writestr(item, fixed_content.encode('utf-8'))
                else:
                    # Copy non-txt files as is
                    zout.writestr(item, zin.read(item.filename))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fix GTFS CSV files in a zip by escaping unescaped quotes')
    parser.add_argument('input_zip', help='Input GTFS zip file')
    parser.add_argument('output_zip', help='Output fixed GTFS zip file')
    args = parser.parse_args()
    main(args.input_zip, args.output_zip)
