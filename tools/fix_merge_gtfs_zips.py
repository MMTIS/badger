#!/usr/bin/env python3
import argparse
import os
import zipfile
import tempfile
import shutil
from collections import defaultdict

# GTFS files and their primary keys
GTFS_FILES = {
    'agency.txt': 'agency_id',
    'stops.txt': 'stop_id',
    'routes.txt': 'route_id',
    'trips.txt': 'trip_id',
    'stop_times.txt': ('trip_id', 'stop_sequence'),
    'calendar.txt': 'service_id',
    'calendar_dates.txt': ('service_id', 'date'),
    'fare_attributes.txt': 'fare_id',
    'fare_rules.txt': ('fare_id', 'route_id'),
    'shapes.txt': ('shape_id', 'shape_pt_sequence'),
    'frequencies.txt': 'trip_id',
    'transfers.txt': ('from_stop_id', 'to_stop_id', 'from_route_id', 'to_route_id'),
    'pathways.txt': 'pathway_id',
    'levels.txt': 'level_id',
    'feed_info.txt': 'feed_publisher_url'
}

def parse_args():
    parser = argparse.ArgumentParser(description='Merge multiple GTFS zip files into one')
    parser.add_argument('input_zip', help='Input zip file containing GTFS zip files')
    parser.add_argument('output_gtfs', help='Output GTFS zip file')
    return parser.parse_args()

def merge_files(file_path, primary_keys, tmp_dir):
    # Use a dictionary to store unique entries
    unique_entries = defaultdict(dict)

    # Find all instances of this file in the input zip
    for root, _, files in os.walk(tmp_dir):
        if file_path in files:
            full_path = os.path.join(root, file_path)
            with open(full_path, 'r', encoding='utf-8-sig') as f:
                header = f.readline().strip()
                if not header:
                    continue

                # Process each line
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    # Split into fields
                    fields = line.split(',')
                    if len(fields) < 1:
                        continue

                    # Create a key based on primary keys
                    if isinstance(primary_keys, tuple):
                        key = tuple(fields[i] for i, col in enumerate(header.split(',')) if col in primary_keys)
                    else:
                        key = (fields[header.split(',').index(primary_keys)],)

                    # Only keep the first occurrence of each key
                    if key not in unique_entries:
                        unique_entries[key] = line

    # Write the merged file
    if unique_entries:
        output_path = os.path.join(tmp_dir, file_path)
        with open(output_path, 'w', encoding='utf-8') as out_file:
            out_file.write(header + '\n')
            for entry in unique_entries.values():
                out_file.write(entry + '\n')

def main(a_input_zip,a_output_gtfs):

    with tempfile.TemporaryDirectory() as tmp_dir:
        # First, extract the input zip to a temporary directory
        with zipfile.ZipFile(a_input_zip, 'r') as input_zip:
            input_zip.extractall(tmp_dir)

        # Find all zip files in the extracted directory
        zip_files = []
        for root, _, files in os.walk(tmp_dir):
            for file in files:
                if file.endswith('.zip'):
                    zip_files.append(os.path.join(root, file))

        # Extract each GTFS zip to the same temporary directory
        for zip_file in zip_files:
            with zipfile.ZipFile(zip_file, 'r') as gtfs_zip:
                gtfs_zip.extractall(tmp_dir)

        # Merge each GTFS file
        for file_path, primary_keys in GTFS_FILES.items():
            merge_files(file_path, primary_keys, tmp_dir)

        # Create the output GTFS zip
        with zipfile.ZipFile(a_output_gtfs, 'w', zipfile.ZIP_DEFLATED) as output_zip:
            for root, _, files in os.walk(tmp_dir):
                for file in files:
                    if file.endswith('.txt'):
                        file_path = os.path.join(root, file)
                        arcname = os.path.basename(file_path)
                        output_zip.write(file_path, arcname)

if __name__ == '__main__':
    args = parse_args()

    main(args.input_zip,args.output_gtfs)