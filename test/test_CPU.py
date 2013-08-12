import sys
import unittest
import platform

from zmeter import ZMeter

class test_CpuInfo(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFetchCpuInfo(self):

        zm = ZMeter()
        info = zm.fetch('cpu')

        print info
        
        self.assertTrue(info.has_key('all.usr'))
        self.assertEquals(info.get('all.idle') , 100.0 - info.get('all.used'))

    def testSendCpuInfo(self):

        if platform.system() == 'Windows':
            return
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

        self.assertTrue(info.has_key('cpu'))

        data = info['cpu']
        self.assertTrue(data.has_key('all.usr'))
        self.assertEquals(data.get('all.idle') , 100.0 - data.get('all.used'))

        sock.close()
        ctx.term()

