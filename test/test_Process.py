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

        zm = ZMeter(config = {'watch': {'x': 'python', 'y':'xyz'}})
        info = zm.fetch('process')
        self.assertEquals(info['meta.watches'], 'y:xyz,x:python')
        time.sleep(5)
        info = zm.fetch('process')

        self.assertTrue(info['watch.x.count']> 0)
        self.assertEquals(info['watch.y.count'], 0)
