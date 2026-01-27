import csv
import io
import json
import logging
import os
import zipfile
from pathlib import Path

import duckdb
from chardet import UniversalDetector

from domain.gtfs.model.tables import (
    feed_info_txt,
    agency_txt,
    calendar_dates_txt,
    calendar_txt,
    routes_txt,
    levels_txt,
    stops_txt,
    shapes_txt,
    trips_txt,
    transfers_txt,
    stop_times_txt,
    frequencies_txt,
    pathways_txt,
)
from domain.gtfs.services.gtfs_inference import create_feed_info, handle_single_agency, update_empty_enumerations
from utils.aux_logging import log_all


def _handle_file(con: duckdb.DuckDBPyConnection, zip_file: zipfile.ZipFile, filename: str, column_mapping: dict[str, str]) -> None:
    print(filename)
    table = filename.split('/')[-1].replace('.txt', '')
    with con.cursor() as cur:
        sql_drop_table = f"""DROP TABLE IF EXISTS {table};"""
        # print(sql_drop_table)
        cur.execute(sql_drop_table)

        if filename in [x.filename for x in zip_file.filelist]:
            if filename not in {'shapes.txt'}:
                detector = UniversalDetector()
                for line in zip_file.open(filename, 'r'):
                    detector.feed(line)
                    if detector.done:
                        break
                detector.close()

                assert detector.result is not None, "Detector must have a result"

                with zip_file.open(filename, mode='r') as f:
                    g = io.TextIOWrapper(f, detector.result['encoding'])
                    reader = csv.reader(g)
                    header = next(reader)

                if (detector.result['encoding'] or '').lower() not in ('utf-8', 'utf-8-sig', 'ascii'):
                    with zip_file.open(filename, 'r') as f_in:
                        g = io.TextIOWrapper(f_in, detector.result['encoding'])
                        with open("_tmp", 'w', encoding='UTF-8') as f_out:
                            f_out.writelines(g)
                else:
                    zip_file.extract(filename)
                    os.rename(filename, '_tmp')
            else:
                with zip_file.open(filename, mode='r') as f:
                    g = io.TextIOWrapper(f, 'utf-8')
                    reader = csv.reader(g)
                    header = next(reader)

                zip_file.extract(filename)
                os.rename(filename, '_tmp')

            filename = '_tmp'

            this_mapping = {}
            for column in header:
                this_mapping[column] = column_mapping.get(column, 'VARCHAR')

            missing_mapping = {}
            for column in column_mapping.keys() - this_mapping.keys():
                missing_mapping[column] = column_mapping.get(column, 'VARCHAR')

            this_mapping_str = json.dumps(this_mapping)

            sql_create_table = f"""CREATE TABLE {table} AS SELECT * FROM read_csv('{filename}', delim=',', quote='"', escape='"',header=true, auto_detect=true, columns = {this_mapping_str});"""
            # print(sql_create_table)
            cur.execute(sql_create_table)

            if filename == '_tmp':
                os.remove('_tmp')

            for column in column_mapping.keys() - this_mapping.keys():
                datatype = column_mapping.get(column, 'VARCHAR')
                cur.execute(f"""ALTER TABLE {table} ADD COLUMN {column} {datatype};""")

        else:
            data_types = []
            for column in column_mapping.keys():
                datatype = column_mapping.get(column, 'VARCHAR')
                data_types.append(f"{column} {datatype}")

            data_types_str = ', '.join(data_types)

            sql_create_table = f"""CREATE TABLE {table} ({data_types_str});"""
            cur.execute(sql_create_table)


def load_gtfs_to_duckdb(zip_file: Path, database_file: Path) -> None:
    con: duckdb.DuckDBPyConnection = duckdb.connect(database=database_file)

    zf = zipfile.ZipFile(zip_file.resolve())

    # check if this is a GTFS file
    if len(set(zf.namelist()) & {'agency.txt', 'routes.txt', 'trips.txt', 'stop_times.txt'}) == 0:
        log_all(logging.ERROR, 'This is not a GTFS file')
        return

    _handle_file(con, zf, 'feed_info.txt', feed_info_txt)
    _handle_file(con, zf, 'agency.txt', agency_txt)
    _handle_file(con, zf, 'calendar_dates.txt', calendar_dates_txt)
    _handle_file(con, zf, 'calendar.txt', calendar_txt)
    _handle_file(con, zf, 'routes.txt', routes_txt)
    _handle_file(con, zf, 'levels.txt', levels_txt)
    _handle_file(con, zf, 'stops.txt', stops_txt)
    _handle_file(con, zf, 'shapes.txt', shapes_txt)
    _handle_file(con, zf, 'trips.txt', trips_txt)
    _handle_file(con, zf, 'transfers.txt', transfers_txt)
    _handle_file(con, zf, 'stop_times.txt', stop_times_txt)
    _handle_file(con, zf, 'frequencies.txt', frequencies_txt)
    _handle_file(con, zf, 'pathways.txt', pathways_txt)

    create_feed_info(con)
    handle_single_agency(con)
    update_empty_enumerations(con)
