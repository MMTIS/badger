from datetime import datetime

import duckdb
from xsdata.models.datatype import XmlDateTime

from domain.gtfs.transform.datetime import gtfs_date
from domain.netex.model import ValidBetween


def getValidBetween(con: duckdb.DuckDBPyConnection) -> ValidBetween | None:
    feed_info_sql = """SELECT feed_start_date, feed_end_date FROM feed_info LIMIT 1;"""

    with con.cursor() as cur:
        cur.execute(feed_info_sql)

        row = cur.fetchone()
        if row is None:
            return None

        (
            feed_start_date,
            feed_end_date,
        ) = row

        from_date = XmlDateTime.from_datetime(datetime.combine(gtfs_date(feed_start_date), datetime.min.time()))
        to_date = XmlDateTime.from_datetime(datetime.combine(gtfs_date(feed_end_date), datetime.min.time()))

        return ValidBetween(from_date=from_date, to_date=to_date)
