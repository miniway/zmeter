import sys
import time
import unittest

from zmeter import ZMeter

class test_NetInfo(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFetchNetInfo(self):

        zm = ZMeter()
        info = zm.fetch('net')
        self.assertTrue(info.has_key('meta.ifs'))

        time.sleep(5)
        info = zm.fetch('net')

        self.assertTrue(info.has_key('0.in.bps'))
        self.assertTrue(info.has_key('0.out.bps'))

