import sys
import platform
import unittest

from zmeter import ZMeter

class test_Mysql(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFetchMssql(self):

        if platform.system() == 'Linux':
            return

        zm = ZMeter(config = {'mssql': {'name' : 'SQLEXPRESS'}})
        info = zm.fetch('mssql')

        self.assertTrue(info.has_key('lock_waited'))

