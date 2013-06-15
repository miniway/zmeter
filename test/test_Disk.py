import sys
import unittest

from zmeter import ZMeter

class test_DiskInfo(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFetchDiskInfo(self):

        zm = ZMeter()
        info = zm.fetch('disk')

        print info
        
        self.assertTrue(info.has_key('meta.mounts'))
        self.assertTrue(info.has_key('0.total'))
        self.assertEquals(info.get('0.pfree'), 100 - info.get('0.pused'))

