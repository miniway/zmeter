import os
import re
import zmeter
import subprocess

class IoStat(zmeter.Metric):

    def __init__(self):
        super(IoStat, self).__init__()

        self.__devices = []

    def fetch(self):
        if self._platform['system'] == 'Linux':
            return self.__fetchLinuxStat()

        self._logger.error("Not Supported Platform " + self._platform['system'])
        return None

    def __names(self, name):
        name = name.lower()
        name = name.replace('/s','')
        name = name.replace('%%','')

        if name == 'r':
            name = 'read.req'
        elif name == 'w':
            name = 'write.req'
        elif name == 'rkb':
            name = 'read'
        elif name == 'wkb':
            name = 'write'

        return name

    def __fetchLinuxStat(self):

        stats = {}
        result = self.execute('iostat', '-d', '1', '2', '-x', '-k')
        if not result:
            return None

        found = 0
        idx = 0
        devices = []
        header = None
        for i, line in enumerate(result.split('\n')):
            if not line: continue
            if line.startswith('Device:'):
               found += 1
            if found < 2: continue

            cols = line.split()
            if header and len(cols) != len(header) + 1:
                continue

            device, data = cols[0], cols[1:]
            if not header:
                header = data
                continue
            devices.append(device)
            for j in range(len(data)):
                name = '%d.%s' % (idx, self.__names(header[j]))
                value = float(data[j])
                if name in ['read', 'write']:
                    value = value * 1024 
                stats[name] = round(value,2)
            idx += 1

        if self.__devices != devices:
            stats['meta.devs'] = ','.join(devices)
            self.__devices = devices

        return stats