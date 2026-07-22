import unittest

from fix.rewrite_sta_ssp_ids import main

class FixSSPTestCase(unittest.TestCase):
    def test(self):
        main("sta.mdbx")

if __name__ == '__main__':
    unittest.main()