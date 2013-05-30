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
        
        self.assertTrue(info.has_key('cpu.all.usr'))
        self.assertTrue(info.get('cpu.all.idle') > 0)

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

        info = json.loads(body)
        print info
        
        self.assertTrue(info.has_key('cpu.all.usr'))
        self.assertTrue(info.get('cpu.all.idle') > 0)

        sock.close()
        ctx.term()
