import os
import re
import zmeter
import subprocess

class Mem(zmeter.Metric):

    def __init__(self):
        super(Mem, self).__init__()

        self.__mem = {}

    def __parseMemInfo(self):
        for line in open('/proc/meminfo').readlines():
            kv = line.split(':', 1)
            value = kv[1].strip().split()[0]
            self.__mem[kv[0]] = long(value) * 1024

    def fetchLinux(self):
        
        self.__parseMemInfo()
        data = {
            'total'     : self.__mem['MemTotal'],
            'free'      : self.__mem['MemFree'],
            'cached'    : self.__mem['Cached'],
            'swap.total': self.__mem['SwapTotal'],
            'swap.free' : self.__mem['SwapFree'],
            'used'      : self.__mem['Active'] + self.__mem['Unevictable']
        }

        data['pused'] = round(data['used'] * 100.0 / data['total'], 2)
        data['swap.used'] = data['swap.total'] - data['swap.free']
        data['swap.pused'] = round(data['swap.used'] * 100.0 / data['total'], 2)

        data = self._updateMeta(data)

        return data

    def fetchWindows(self):
        stats = {}
        if not self.__mem.has_key('MemTotal'):
            data = self._wmi.InstancesOf("Win32_OperatingSystem")[0]
            self.__mem['MemTotal'] = long(data.TotalVisibleMemorySize) * 1024
            
        data = self._wmi.InstancesOf("Win32_PerfFormattedData_PerfOS_Memory")[0]

        stats = {
            'total'     : self.__mem['MemTotal'],
            'free'      : long(data.AvailableBytes),
            'cached'    : long(data.CacheBytes),
            'swap.total': long(data.CommitLimit),
            'swap.pused' : int(data.PercentCommittedBytesInUse),
        }
        stats['used'] = stats['total'] - stats['free']
        stats['pused'] = round(stats['used'] * 100.0 / stats['total'], 2)
        stats['swap.free'] = stats['swap.total'] - int(data.CommittedBytes)
        stats['swap.used'] = stats['swap.total'] - stats['swap.free']

        stats = self._updateMeta(stats)

        return stats

    def _updateMeta(self, data):
        if self.__mem.get('meta.total') != data['total']: 
            data['meta.total'] = data['total']
            self.__mem['meta.total'] = data['total']

        return data
