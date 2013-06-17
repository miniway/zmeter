import sys
import unittest

from zmeter import ZMeter

class test_LoadInfo(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFetchLoadInfo(self):

        zm = ZMeter()
        info = zm.fetch('load')

        print info
        
        self.assertTrue(info.has_key('avg15'))

