import unittest

from pyproj import Transformer

from domain.netex.model import LocationStructure2, Pos, ScheduledStopPoint
from domain.netex.services.recursive_attributes import get_all_geo_elements

from transformers.projection import reprojection, reprojection_update

from tests.base import MdbxStorageTestCase

WGS84 = "urn:ogc:def:crs:EPSG::4326"
UTM32N = "EPSG:32632"

EASTING, NORTHING = 680000.0, 5140000.0  # roughly Bolzano, UTM zone 32N


def _stop_point_in_utm32n() -> ScheduledStopPoint:
    return ScheduledStopPoint(
        id="ssp1", version="1", location=LocationStructure2(srs_name=UTM32N, pos=Pos(value=[EASTING, NORTHING], srs_name=UTM32N, srs_dimension=2))
    )


def _assert_location_is_wgs84(test: unittest.TestCase, location: LocationStructure2) -> None:
    test.assertEqual(location.srs_name, WGS84)
    test.assertIsNotNone(location.pos)
    assert location.pos is not None

    expected = Transformer.from_crs(UTM32N, WGS84).transform(EASTING, NORTHING)
    stored = [float(value) for value in location.pos.value]
    for got, exp in zip(stored, expected):
        test.assertAlmostEqual(got, exp, delta=1e-5)
    # Coordinates must now be valid latitude/longitude.
    test.assertTrue(-90 <= stored[0] <= 90)
    test.assertTrue(-180 <= stored[1] <= 180)


class TestReprojection(unittest.TestCase):
    def test_reprojection_converts_location_to_wgs84(self) -> None:
        ssp = reprojection(_stop_point_in_utm32n(), WGS84)

        self.assertIsNotNone(ssp.location)
        assert ssp.location is not None
        _assert_location_is_wgs84(self, ssp.location)

    def test_get_all_geo_elements_is_not_empty(self) -> None:
        geo_classes = list(get_all_geo_elements())

        self.assertGreater(len(geo_classes), 0)
        self.assertIn(ScheduledStopPoint, geo_classes)


class TestReprojectionUpdate(MdbxStorageTestCase):
    def test_reprojection_update_converts_stored_locations_to_wgs84(self) -> None:
        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, [_stop_point_in_utm32n()])
            txn_write.commit()

        with self.storage.env.rw_transaction() as txn_write:
            self.storage.insert_any_object_on_queue(txn_write, reprojection_update(self.storage, txn_write, WGS84))
            txn_write.commit()

        with self.storage.env.ro_transaction() as txn_read:
            result = self.storage.load_object_by_id_version(txn_read, "ssp1", ScheduledStopPoint, "1")
            self.assertIsNotNone(result)
            assert result is not None
            _full_key, reprojected = result

        self.assertIsInstance(reprojected, ScheduledStopPoint)
        assert isinstance(reprojected, ScheduledStopPoint)
        self.assertIsNotNone(reprojected.location)
        assert reprojected.location is not None
        _assert_location_is_wgs84(self, reprojected.location)
