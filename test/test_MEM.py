import sys
import unittest

from zmeter import ZMeter

class test_MemInfo(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFetchMemInfo(self):

        zm = ZMeter()
        info = zm.fetch('mem')
        
        self.assertTrue(info.has_key('total'))
        self.assertEquals(info.get('used'), info.get('total') - info.get('free'))

