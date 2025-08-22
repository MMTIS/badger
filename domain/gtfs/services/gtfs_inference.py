import warnings

import duckdb


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
    agency_id: str

    with con.cursor() as cur:
        cur.execute("""SELECT DISTINCT agency_id FROM agency;""")
        data = cur.fetchall()
        if len(data) > 1:
            # warnings.warn("This feed has multiple agencies.")
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
