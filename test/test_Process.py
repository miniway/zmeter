import sys
import time
import unittest

from zmeter import ZMeter

class test_Process(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFetchProcessinfo(self):

        zm = ZMeter()
        info = zm.fetch('process')
        time.sleep(5)
        info = zm.fetch('process')

        self.assertTrue(info.has_key('snapshot.top10'))

    def testWatchProcessinfo(self):

        zm = ZMeter(config = {'watch': ['python', 'xyz']})
        info = zm.fetch('process')
        self.assertEquals(info['meta.watches'], 'python,xyz')
        time.sleep(5)
        info = zm.fetch('process')

        self.assertTrue(info['watch.0.count']> 0)
        self.assertEquals(info['watch.1.count'], 0)
