from decimal import Decimal
from typing import Generator

import duckdb

from domain.gtfs.transform.string import getRequiredString, getOptionalString
from domain.netex.model import StopArea, Codespace, PublicCodeStructure, PrivateCodes, PrivateCode, SimplePointVersionStructure, LocationStructure2
from domain.netex.services.ids import getId


def get_stop_id_sa(codespace: Codespace, stop_id: str) -> str:
    if ':StopArea:' in stop_id:
        return stop_id
    elif ':StopPlace:' in stop_id:
        return stop_id.replace(':StopPlace:', ':StopArea:')
    else:
        return getId(codespace, StopArea, stop_id)


def getStopAreas(con: duckdb.DuckDBPyConnection, codespace: Codespace, version: str) -> Generator[StopArea, None, None]:
    stoparea_sql = "SELECT DISTINCT stop_id, stop_name, stop_lat, stop_lon, stop_code, stop_desc, zone_id, stop_url, location_type, parent_station, wheelchair_boarding, stop_timezone, platform_code FROM stops WHERE location_type = 1 ORDER BY stop_id;"

    with con.cursor() as cur:
        cur.execute(stoparea_sql)

        while True:
            row = cur.fetchone()
            if row is None:
                break

            (
                stop_id,
                stop_name,
                stop_lat,
                stop_lon,
                stop_code,
                stop_desc,
                zone_id,
                stop_url,
                location_type,
                parent_station,
                wheelchair_boarding,
                stop_timezone,
                platform_code,
            ) = row

            stop_area = StopArea(
                id=get_stop_id_sa(codespace, stop_id),
                version=version,
                name=getRequiredString(stop_name, None),
                public_code=PublicCodeStructure(value=stop_code) if stop_code is not None else None,
                description=getOptionalString(stop_desc) if stop_desc is not None else None,
                private_codes=PrivateCodes(private_code=[PrivateCode(value=stop_id, type_value="stop_id")]),
                centroid=SimplePointVersionStructure(
                    location=LocationStructure2(latitude=Decimal(str(stop_lat)), longitude=Decimal(str(stop_lon)), srs_name="urn:ogc:def:crs:EPSG::4326")
                ),
            )

            yield stop_area
