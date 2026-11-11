from domain.netex.services.utils import get_boring_classes
from domain.netex.model import StopAreaRefStructure
from unittest import TestCase

from storage.mdbx.serialization.combinedserializer import CombinedSerializer


class TestClassSelection(TestCase):
    def test_get_boring_classes(self):
        container = get_boring_classes()
        self.assertIn(StopAreaRefStructure, container)

        cs = CombinedSerializer(get_boring_classes())
        result = 'StopAreaRefStructure' in cs.name_object
        self.assertTrue(result)