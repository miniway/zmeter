import os
import re
import zmeter
import subprocess

class Mem(zmeter.Metric):

    def __init__(self):
        super(Mem, self).__init__()

        self.__mem = {}

    def fetch(self):
        if self._platform['system'] == 'Linux':
            return self.__fetchLinuxStat()

        self._logger.error("Not Supported Platform " + self._platform['system'])
        return None

    def __parseMemInfo(self):
        for line in open('/proc/meminfo').readlines():
            kv = line.split(':', 1)
            value = kv[1].strip().split()[0]
            self.__mem[kv[0]] = long(value) * 1024

    def __fetchLinuxStat(self):
        
        self.__parseMemInfo()
        data = {
            'total'     : self.__mem['MemTotal'],
            'free'      : self.__mem['MemFree'],
            'cached'    : self.__mem['Cached'],
            'buffers'   : self.__mem['Buffers'],
            'shared'    : self.__mem['Shmem'],
            'swap.total': self.__mem['SwapTotal'],
            'swap.free' : self.__mem['SwapFree'],
        }

        data['used'] = data['total'] - data['free']
        data['pused'] = round(data['used'] * 100.0 / data['total'], 2)
        data['swap.used'] = data['swap.total'] - data['swap.free']
        data['swap.pused'] = round(data['swap.used'] * 100.0 / data['total'], 2)

        return data
