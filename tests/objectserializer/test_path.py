import unittest

from domain.netex.model import ScheduledStopPoint, ScheduledStopPointRef
from domain.netex.services.utils import get_boring_classes
from storage.mdbx.serialization.combinedserializer import CombinedSerializer
from storage.objectserializer.codecs.path import encode_path, decode_path

class TestPathCodec(unittest.TestCase):
    def test_encode_known_values(self) -> None:
        self.assertEqual(encode_path((41, 2, 0)), bytes.fromhex("290200"))
        self.assertEqual(encode_path((0,)), bytes.fromhex("00"))
        self.assertEqual(encode_path((127,)), bytes.fromhex("7f"))
        self.assertEqual(encode_path((128,)), bytes.fromhex("8001"))
        self.assertEqual(encode_path((300,)), bytes.fromhex("ac02"))

    def test_decode_known_values(self) -> None:
        self.assertEqual(decode_path(bytes.fromhex("290200")), (41, 2, 0))
        self.assertEqual(decode_path(bytes.fromhex("00")), (0,))
        self.assertEqual(decode_path(bytes.fromhex("7f")), (127,))
        self.assertEqual(decode_path(bytes.fromhex("8001")), (128,))
        self.assertEqual(decode_path(bytes.fromhex("ac02")), (300,))

    def test_roundtrip(self) -> None:
        paths = [
            (),
            (0,),
            (41, 2, 0),
            (127, 128, 129),
            (16383, 16384, 16385),
            (0, 0, 0, 0),
            (2**32, 2**63, 2**100),
        ]

        for path in paths:
            with self.subTest(path=path):
                self.assertEqual(decode_path(encode_path(path)), path)

    def test_encode_accepts_list(self) -> None:
        self.assertEqual(encode_path([41, 2, 0]), bytes.fromhex("290200"))

    def test_encode_rejects_negative_integer(self) -> None:
        with self.assertRaises(ValueError):
            encode_path((-1,))

    def test_decode_rejects_incomplete_encoding(self) -> None:
        with self.assertRaises(ValueError):
            decode_path(bytes.fromhex("80"))

    def test_decode_rejects_incomplete_multibyte_encoding(self) -> None:
        with self.assertRaises(ValueError):
            decode_path(bytes.fromhex("ac"))

    def test_combined_ref_path(self) -> None:
        serializer = CombinedSerializer(get_boring_classes())
        serializer.set_clazz_idx({ScheduledStopPoint: b'0', ScheduledStopPointRef: b'1'}, {b'0': ScheduledStopPoint, b'1': ScheduledStopPointRef})
        in_id = "test"
        in_version = "1"
        in_clazz = ScheduledStopPoint
        in_path = (10,1,33)
        in_ref_clazz = ScheduledStopPointRef

        ref_path_bytes = serializer.encode_ref_path(in_id, in_version, in_clazz, encode_path(in_path), in_ref_clazz)
        out_id, out_version, out_clazz, out_path, out_ref_clazz = serializer.decode_ref_path(ref_path_bytes)

        self.assertEqual(in_id, out_id)
        self.assertEqual(in_version, out_version)
        self.assertEqual(in_clazz, out_clazz)
        self.assertEqual(in_path, out_path)
        self.assertEqual(in_ref_clazz, out_ref_clazz)


if __name__ == "__main__":
    unittest.main()