import duckdb

from domain.gtfs.transform.string import getShortName
from domain.netex.model import DataSource, MultilingualString, TextType, Codespace
from domain.netex.services.ids import getId


def getDataSource(con: duckdb.DuckDBPyConnection, codespace: Codespace, version: str) -> DataSource | None:
    feed_info_sql = """SELECT feed_publisher_name, feed_publisher_url FROM feed_info LIMIT 1;"""

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

        data_source = DataSource(
            id=getId(codespace, DataSource, codespace_name),
            version=version,
            name=MultilingualString(content=[TextType(value=feed_publisher_name)]),
            short_name=MultilingualString(content=[TextType(value=short_name)]),
            description=MultilingualString(content=[TextType(value=feed_publisher_name)]),
            url=feed_publisher_url,
        )

        return data_source
