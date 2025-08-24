import duckdb

from domain.gtfs.transform.string import getShortName
from domain.netex.model import Codespace


def getCodespace(con: duckdb.DuckDBPyConnection) -> Codespace | None:
    feed_info_sql = """SELECT feed_publisher_name, feed_publisher_name FROM feed_info LIMIT 1;"""

    with con.cursor() as cur:
        cur.execute(feed_info_sql)

        row = cur.fetchone()
        if row is None:
            return None

        (
            feed_publisher_name,
            feed_publisher_url,
        ) = row
        short_name = getShortName(feed_publisher_name)
        codespace_name = short_name.replace(' ', '')

        codespace = Codespace(
            id="{}:Codespace:{}".format(codespace_name, codespace_name),
            xmlns=codespace_name,
            xmlns_url=feed_publisher_url,
            description=feed_publisher_name,
        )

        return codespace
