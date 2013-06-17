import sys
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

        print info
        
        self.assertTrue(info.has_key('1.in.bytes'))
        self.assertTrue(info.has_key('0.out.bytes'))

