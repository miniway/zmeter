import sys
import unittest

from zmeter import ZMeter

class test_System(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFetchSysinfo(self):

        zm = ZMeter()
        info = zm.fetch('system')

        print info

        self.assertTrue(info.has_key('meta.host'))
        self.assertTrue(info.has_key('meta.dist'))
