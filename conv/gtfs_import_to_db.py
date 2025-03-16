import io
import warnings
import logging
import zipfile

import duckdb
import csv
from utils.aux_logging import log_all, prepare_logger

import os
import json
from chardet.universaldetector import UniversalDetector

agency_txt = {'agency_id': 'VARCHAR', 'agency_name': 'VARCHAR', 'agency_url': 'VARCHAR', 'agency_timezone': 'VARCHAR', 'agency_lang': 'VARCHAR', 'agency_phone': 'VARCHAR', 'agency_fare_url': 'VARCHAR', 'agency_email': 'VARCHAR'}
stops_txt = {
    'stop_id': 'VARCHAR',
    'stop_code': 'VARCHAR',
    'stop_name': 'VARCHAR',
    'tts_stop_name': 'VARCHAR',
    'stop_desc': 'VARCHAR',
    'stop_lat': 'FLOAT',
    'stop_lon': 'FLOAT',
    'zone_id': 'VARCHAR',
    'stop_url': 'VARCHAR',
    'location_type': 'INTEGER',
    'parent_station': 'VARCHAR',
    'stop_timezone': 'VARCHAR',
    'wheelchair_boarding': 'INTEGER',
    'level_id': 'VARCHAR',
    'platform_code': 'VARCHAR',
}
routes_txt = {
    'route_id': 'VARCHAR',
    'agency_id': 'VARCHAR',
    'route_long_name': 'VARCHAR',
    'route_type': 'INTEGER',
    'route_url': 'VARCHAR',
    'route_color': 'CHAR(6)',
    'route_text_color': 'CHAR(6)',
    'route_sort_order': 'INTEGER',
    'continuous_pickup': 'INTEGER',
    'continuous_drop_off': 'INTEGER',
    'network_id': 'VARCHAR',
}
trips_txt = {'route_id': 'VARCHAR', 'service_id': 'VARCHAR', 'trip_id': 'VARCHAR', 'trip_headsign': 'VARCHAR', 'trip_short_name': 'VARCHAR', 'direction_id': 'INTEGER', 'block_id': 'VARCHAR', 'shape_id': 'VARCHAR', 'wheelchair_accessible': 'INTEGER', 'bikes_allowed': 'INTEGER'}
stop_times_txt = {
    'trip_id': 'VARCHAR',
    'arrival_time': 'VARCHAR',
    'departure_time': 'VARCHAR',
    'stop_id': 'VARCHAR',
    'location_group_id': 'VARCHAR',
    'location_id': 'VARCHAR',
    'stop_sequence': 'INTEGER',
    'stop_headsign': 'VARCHAR',
    'start_pickup_drop_off_window': 'VARCHAR',
    'end_pickup_drop_off_window': 'VARCHAR',
    'pickup_type': 'INTEGER',
    'drop_off_type': 'INTEGER',
    'continuous_pickup': 'INTEGER',
    'continuous_drop_off': 'INTEGER',
    'shape_dist_traveled': 'FLOAT',
    'timepoint': 'INTEGER',
    'drop_off_booking_rule_id': 'VARCHAR',
}
calendar_txt = {'service_id': 'VARCHAR', 'monday': 'INTEGER', 'tuesday': 'INTEGER', 'wednesday': 'INTEGER', 'thursday': 'INTEGER', 'friday': 'INTEGER', 'saturday': 'INTEGER', 'sunday': 'INTEGER', 'start_date': 'CHAR(8)', 'end_date': 'CHAR(8)'}
calendar_dates_txt = {'service_id': 'VARCHAR', 'date': 'CHAR(8)', 'exception_type': 'INTEGER'}
feed_info_txt = {'feed_publisher_name': 'VARCHAR', 'feed_publisher_url': 'VARCHAR', 'feed_lang': 'VARCHAR', 'default_lang': 'VARCHAR', 'feed_start_date': 'CHAR(8)', 'feed_end_date': 'CHAR(8)', 'feed_version': 'VARCHAR', 'feed_contact_email': 'VARCHAR', 'feed_contact_url': 'VARCHAR'}
shapes_txt = {'shape_id': 'VARCHAR', 'shape_pt_lat': 'FLOAT', 'shape_pt_lon': 'FLOAT', 'shape_pt_sequence': 'INTEGER', 'shape_dist_traveled': 'FLOAT'}
transfers_txt = {'from_stop_id': 'VARCHAR', 'to_stop_id': 'VARCHAR', 'from_route_id': 'VARCHAR', 'to_route_id': 'VARCHAR', 'from_trip_id': 'VARCHAR', 'to_trip_id': 'VARCHAR', 'transfer_type': 'INTEGER', 'min_transfer_time': 'INTEGER'}
levels_txt = {'level_id': 'VARCHAR', 'level_index': 'FLOAT', 'level_name': 'VARCHAR'}
frequencies_txt = {'trip_id': 'VARCHAR', 'start_time': 'VARCHAR', 'end_time': 'VARCHAR', 'headway_secs': 'INTEGER', 'exact_times': 'INTEGER'}
pathways_txt = {
    'pathway_id': 'VARCHAR',
    'from_stop_id': 'VARCHAR',
    'to_stop_id': 'VARCHAR',
    'pathway_mode': 'INTEGER',
    'is_bidirectional': 'INTEGER',
    'length': 'FLOAT',
    'traversal_time': 'INTEGER',
    'stair_count': 'INTEGER',
    'max_slope': 'FLOAT',
    'min_width': 'FLOAT',
    'signposted_as': 'VARCHAR',
    'reversed_signposted_as': 'VARCHAR',
}

# Example usage
column_mapping = {
    'stop_lat': 'FLOAT',
    'stop_lon': 'FLOAT',
    'location_type': 'INTEGER',
    'wheelchair_boarding': 'INTEGER',
    'route_type': 'INTEGER',
    'continuous_pickup': 'INTEGER',
    'continuous_drop_off': 'INTEGER',
    'direction_id': 'INTEGER',
    'bikes_allowed': 'INTEGER',
    'arrival_time': 'VARCHAR(8)',
    'departure_time': 'VARCHAR(8)',
    'start_pickup_drop_off_window': 'VARCHAR(8)',
    'end_pickup_drop_off_window': 'VARCHAR(8)',
    'start_time': 'VARCHAR(8)',
    'end_time': 'VARCHAR(8)',
    'prior_notice_last_time': 'VARCHAR(8)',
    'prior_notice_start_time': 'VARCHAR(8)',
    'pickup_type': 'INTEGER',
    'drop_off_type': 'INTEGER',
    'shape_dist_traveled': 'FLOAT',
    'timepoint': 'INTEGER',
    'monday': 'INTEGER',
    'tuesday': 'INTEGER',
    'wednesday': 'INTEGER',
    'thursday': 'INTEGER',
    'friday': 'INTEGER',
    'saturday': 'INTEGER',
    'sunday': 'INTEGER',
    'start_date': 'VARCHAR(8)',
    'end_date': 'VARCHAR(8)',
    'date': 'VARCHAR(8)',
    'exception_type': 'INTEGER',
    'price': 'FLOAT',
    'payment_method': 'INTEGER',
    'transfers': 'INTEGER',
    'transfer_duration': 'INTEGER',
    'fare_media_type': 'INTEGER',
    'amount': 'FLOAT',
    'transfer_count': 'INTEGER',
    'duration_limit': 'INTEGER',
    'duration_limit_type': 'INTEGER',
    'fare_transfer_type': 'INTEGER',
    'shape_pt_lat': 'FLOAT',
    'shape_pt_lon': 'FLOAT',
    'shape_pt_sequence': 'INTEGER',
    'headway_secs': 'INTEGER',
    'exact_times': 'INTEGER',
    'transfer_type': 'INTEGER',
    'min_transfer_time': 'INTEGER',
    'pathway_mode': 'INTEGER',
    'is_bidirectional': 'INTEGER',
    'length': 'FLOAT',
    'traversal_time': 'INTEGER',
    'stair_count': 'INTEGER',
    'max_slope': 'FLOAT',
    'min_width': 'FLOAT',
    'level_index': 'FLOAT',
    'booking_type': 'INTEGER',
    'prior_notice_duration_min': 'INTEGER',
    'prior_notice_duration_max': 'INTEGER',
    'prior_notice_last_day': 'INTEGER',
    'prior_notice_start_day': 'INTEGER',
    'feed_start_date': 'VARCHAR(8)',
    'feed_end_date': 'VARCHAR(8)',
    'is_producer': 'INTEGER',
    'is_operator': 'INTEGER',
    'is_authority': 'INTEGER',
}


def handle_file(con: duckdb.DuckDBPyConnection, zip: zipfile.ZipFile, filename: str, column_mapping: dict[str, str]) -> None:
    table = filename.split('/')[-1].replace('.txt', '')
    with con.cursor() as cur:
        sql_drop_table = f"""DROP TABLE IF EXISTS {table};"""
        # print(sql_drop_table)
        cur.execute(sql_drop_table)

        if filename in [x.filename for x in zip.filelist]:
            detector = UniversalDetector()
            for line in zip.open(filename, 'r'):
                detector.feed(line)
                if detector.done:
                    break
            detector.close()

            with zip.open(filename, mode='r') as f:
                g = io.TextIOWrapper(f, detector.result['encoding'])
                reader = csv.reader(g)
                header = next(reader)

            assert detector.result is not None

            if (detector.result['encoding'] or '').lower() not in ('utf-8', 'utf-8-sig', 'ascii'):
                with zip.open(filename, 'r') as f_in:
                    g = io.TextIOWrapper(f_in, detector.result['encoding'])
                    with open("_tmp", 'w', encoding='UTF-8') as f_out:
                        f_out.writelines(g)
            else:
                zip.extract(filename)
                os.rename(filename, '_tmp')

            filename = '_tmp'

            this_mapping = {}
            for column in header:
                this_mapping[column] = column_mapping.get(column, 'VARCHAR')

            missing_mapping = {}
            for column in column_mapping.keys() - this_mapping.keys():
                missing_mapping[column] = column_mapping.get(column, 'VARCHAR')

            this_mapping_str = json.dumps(this_mapping)

            sql_create_table = f"""CREATE TABLE {table} AS SELECT * FROM read_csv('{filename}', delim=',', header=true, auto_detect=true, columns = {this_mapping_str});"""
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


def create_feed_info(con: duckdb.DuckDBPyConnection) -> None:
    with con.cursor() as cur:
        cur.execute("""SELECT feed_start_date, feed_end_date, feed_version FROM feed_info;""")
        data = cur.fetchall()

        if len(data) == 0:
            cur.execute(
                """INSERT INTO feed_info (SELECT X.*, Y.*, REPLACE(CAST(today() AS TEXT), '-', '') AS feed_version, '' AS feed_contact_email, '' AS feed_contact_url  FROM (SELECT agency_name AS feed_publisher_name, agency_url AS feed_publisher_url, agency_lang AS feed_lang, agency_lang AS default_lang FROM agency LIMIT 1) AS X, (SELECT MIN(start_date) AS feed_start_date, MAX(end_date) AS feed_end_date FROM (SELECT MIN(start_date) AS start_date, MAX(end_date) AS end_date FROM calendar UNION ALL SELECT MIN(date) AS start_date, MAX(date) AS end_date FROM calendar_dates) WHERE start_date <> '' and end_date <> '') AS Y);"""
            )

        else:
            if data[0][0] is None or data[0][1] is None or len(data[0][0]) == 0 or len(data[0][1]) == 0:
                cur.execute(
                    """UPDATE feed_info SET feed_start_date = start_date, feed_end_date = end_date FROM (SELECT start_date, end_date FROM (SELECT MIN(start_date) AS start_date, MAX(end_date) AS end_date FROM calendar UNION ALL SELECT MIN(date) AS start_date, MAX(date) AS end_date FROM calendar_dates) WHERE start_date <> '' and end_date <> '') AS Z;"""
                )
            if data[0][2] is None or len(data[0][2]) == 0:
                cur.execute("""UPDATE feed_info SET feed_version = REPLACE(CAST(today() AS TEXT), '-', '');""")


def handle_single_agency(con: duckdb.DuckDBPyConnection) -> None:
    with con.cursor() as cur:
        agency_id = None

        cur.execute("""SELECT DISTINCT agency_id FROM agency;""")
        data = cur.fetchall()
        if len(data) > 1:
            return
        else:
            agency_id = data[0][0]

        cur.execute("""SELECT agency_id FROM routes WHERE agency_id <> '' GROUP BY agency_id;""")
        data = cur.fetchall()
        if len(data) < 1:
            cur.execute("""UPDATE routes SET agency_id = ?;""", (agency_id,))
        elif len(data) > 1:
            warnings.warn("Multi values from agency_id are found, but only one was defined!")


def update_empty_enumerations(con: duckdb.DuckDBPyConnection) -> None:
    with con.cursor() as cur:
        cur.execute("""UPDATE stops SET location_type = 0 WHERE location_type IS NULL;""")


def main(gtfs: str, database: str) -> None:
    # Workaround for https://github.com/duckdb/duckdb/issues/8261
    try:
        os.remove(database)
    except OSError:
        pass

    import zipfile

    con: duckdb.DuckDBPyConnection = duckdb.connect(database=database)

    zf = zipfile.ZipFile(gtfs)

    # check if this is a GTFS file
    if len(set(zf.namelist()) & {'agency.txt', 'routes.txt', 'trips.txt', 'stop_times.txt'}) == 0:
        log_all(logging.ERROR, 'This is not a GTFS file')
        return

    handle_file(con, zf, 'feed_info.txt', feed_info_txt)
    handle_file(con, zf, 'agency.txt', agency_txt)
    handle_file(con, zf, 'calendar_dates.txt', calendar_dates_txt)
    handle_file(con, zf, 'calendar.txt', calendar_txt)
    handle_file(con, zf, 'routes.txt', routes_txt)
    handle_file(con, zf, 'levels.txt', levels_txt)
    handle_file(con, zf, 'stops.txt', stops_txt)
    handle_file(con, zf, 'shapes.txt', shapes_txt)
    handle_file(con, zf, 'trips.txt', trips_txt)
    handle_file(con, zf, 'transfers.txt', transfers_txt)
    handle_file(con, zf, 'stop_times.txt', stop_times_txt)
    handle_file(con, zf, 'frequencies.txt', frequencies_txt)
    handle_file(con, zf, 'pathways.txt', pathways_txt)

    create_feed_info(con)
    handle_single_agency(con)
    update_empty_enumerations(con)


if __name__ == "__main__":
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description='GTFS import into DuckDB')
    parser.add_argument('gtfs', type=str, help='GTFS file to import, for example: gtfs.zip')
    parser.add_argument('database', type=str, help='DuckDB file to overwrite and store contents of the import.')
    parser.add_argument('--log_file', type=str, required=False, help='the logfile')
    args = parser.parse_args()
    mylogger = prepare_logger(logging.INFO, args.log_file)
    try:
        main(args.gtfs, args.database)
    except Exception as e:
        log_all(logging.ERROR, f'{e}  {traceback.format_exc()}')
        raise e
