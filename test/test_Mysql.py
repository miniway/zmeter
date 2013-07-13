import sys
import unittest

from zmeter import ZMeter

class test_Mysql(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFetchMysql(self):

        zm = ZMeter(config = {'mysql': {'port' : 3306}})
        info = zm.fetch('mysql')

        self.assertTrue(info.has_key('slow_queries'))

