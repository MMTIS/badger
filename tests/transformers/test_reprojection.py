import pytest
from pyproj import Transformer

from domain.netex.model import LocationStructure2, Pos, ScheduledStopPoint
from domain.netex.services.recursive_attributes import get_all_geo_elements

from storage.mdbx.core.implementation import MdbxStorage
from transformers.projection import reprojection, reprojection_update

WGS84 = "urn:ogc:def:crs:EPSG::4326"
UTM32N = "EPSG:32632"

GEO_DISCOVERY_BROKEN = pytest.mark.xfail(
    reason="get_all_geo_elements() returns nothing: utils/mro_attributes.py cannot resolve "
    "the generated 'None | X' union annotations (on Python >= 3.14 dataclass field.type is "
    "even a plain string), so reprojection_update() never finds geo-carrying classes",
    strict=True,
)


EASTING, NORTHING = 680000.0, 5140000.0  # roughly Bolzano, UTM zone 32N


def _stop_point_in_utm32n() -> ScheduledStopPoint:
    return ScheduledStopPoint(
        id="ssp1", version="1", location=LocationStructure2(srs_name=UTM32N, pos=Pos(value=[EASTING, NORTHING], srs_name=UTM32N, srs_dimension=2))
    )


def _assert_location_is_wgs84(location: LocationStructure2) -> None:
    assert location.srs_name == WGS84
    assert location.pos is not None

    expected = Transformer.from_crs(UTM32N, WGS84).transform(EASTING, NORTHING)
    stored = [float(value) for value in location.pos.value]
    assert stored == pytest.approx(expected, abs=1e-5)
    # Coordinates must now be valid latitude/longitude.
    assert -90 <= stored[0] <= 90
    assert -180 <= stored[1] <= 180


def test_reprojection_converts_location_to_wgs84() -> None:
    ssp = reprojection(_stop_point_in_utm32n(), WGS84)

    assert ssp.location is not None
    _assert_location_is_wgs84(ssp.location)


@GEO_DISCOVERY_BROKEN
def test_get_all_geo_elements_is_not_empty() -> None:
    assert len(list(get_all_geo_elements())) > 0


@GEO_DISCOVERY_BROKEN
def test_reprojection_update_converts_stored_locations_to_wgs84(mdbx_storage: MdbxStorage) -> None:
    with mdbx_storage.env.rw_transaction() as txn_write:
        mdbx_storage.insert_any_object_on_queue(txn_write, [_stop_point_in_utm32n()])
        txn_write.commit()

    with mdbx_storage.env.rw_transaction() as txn_write:
        mdbx_storage.insert_any_object_on_queue(txn_write, reprojection_update(mdbx_storage, txn_write, WGS84))
        txn_write.commit()

    with mdbx_storage.env.ro_transaction() as txn_read:
        result = mdbx_storage.load_object_by_id_version(txn_read, "ssp1", ScheduledStopPoint, "1")
        assert result is not None
        _full_key, reprojected = result

    assert isinstance(reprojected, ScheduledStopPoint)
    assert reprojected.location is not None
    _assert_location_is_wgs84(reprojected.location)
