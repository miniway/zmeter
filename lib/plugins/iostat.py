import os
import re
import zmeter
import subprocess

class IoStat(zmeter.Metric):

    def __init__(self):
        super(IoStat, self).__init__()

        self.__devices = []

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

    def fetchLinux(self):

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

        stats = self._updateMeta(stats, devices)

        return stats
    
    def fetchWindows(self):
        stats = {}
        devices = []
        for data in self._wmi.InstancesOf("Win32_PerfFormattedData_PerfDisk_PhysicalDisk"):
            if data.Name == '_Total':
                continue
            try:
                idx, device = data.Name.split(" ")
            except ValueError:
                # unformatted disk
                continue
            devices.append(device)
            stat = {
                '%s.%%util' % idx : int(data.PercentDiskTime),
                '%s.await' % idx : long(data.AvgDiskSecPerTransfer) * 1000,
                '%s.avgrq-sz' % idx : int(data.AvgDiskBytesPerTransfer),
                '%s.avgqu-sz' % idx : int(data.AvgDiskQueueLength),
                '%s.svctm' % idx : long(data.AvgDiskSecPerTransfer) * 1000, # milli seconds
                '%s.read.req' % idx : int(data.DiskReadsPerSec),
                '%s.write.req' % idx : int(data.DiskWritesPerSec),
                '%s.rrpm' % idx : int(data.DiskReadsPerSec),
                '%s.wrpm' % idx : int(data.DiskWritesPerSec),
                '%s.read' % idx : round(float(data.DiskReadBytesPerSec) / 1024.0),
                '%s.write' % idx : round(float(data.DiskWriteBytesPerSec) / 1024.0),

            }
            stats.update(stat)

        stats = self._updateMeta(stats, devices)
            
        return stats

    def _updateMeta(self, stats, devices):
        if self.checkLast() or self.__devices != devices:
            stats['meta.devs'] = ','.join(devices)
            self.__devices = devices

        return stats
