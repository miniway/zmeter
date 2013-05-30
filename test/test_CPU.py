import sys
import unittest

from zmeter import ZMeter

class test_CpuInfo(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFetchCpuInfo(self):

        zm = ZMeter()
        info = zm.fetch('cpu')
        
        self.assertTrue(info.has_key('all.usr'))
        self.assertEquals(info.get('all.idle') , 100.0 - info.get('all.used'))

    def testSendCpuInfo(self):

        import zmq
        import json

        ctx = zmq.Context()
        sock = ctx.socket(zmq.PULL)
        sock.bind('tcp://*:5555')

        zm = ZMeter()
        zm.send('cpu')

        header = sock.recv(0)
        body = sock.recv(0)

        header = json.loads(header)
        info = json.loads(body)
        
        self.assertEquals(header.get('kind'),'cpu')
        self.assertTrue(info.has_key('all.usr'))
        self.assertEquals(info.get('all.idle') , 100.0 - info.get('all.used'))

        sock.close()
        ctx.term()