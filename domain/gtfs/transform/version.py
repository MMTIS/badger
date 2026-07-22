import duckdb


def getVersion(con: duckdb.DuckDBPyConnection) -> str | None:
    feed_info_sql = """SELECT feed_version FROM feed_info LIMIT 1;"""

    with con.cursor() as cur:
        row = cur.execute(feed_info_sql).fetchone()
        if row is None:
            return None

        (feed_version,) = row
        return str(feed_version)
