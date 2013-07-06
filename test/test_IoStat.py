import sys
import unittest

from zmeter import ZMeter

class test_IoStat(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFetchIoStat(self):

        zm = ZMeter()
        info = zm.fetch('iostat')

        print info
        
        self.assertTrue(info.has_key('meta.devs'))

