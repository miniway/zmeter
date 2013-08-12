import os
import re
import zmeter
import subprocess

class Disk(zmeter.Metric):

    def __init__(self):
        super(Disk, self).__init__()

        self.__mounts = []

    def fetchLinux(self):
        
        result = self.execute('df', '-k')
        stats = {}
        long_fs = None
        mounts = []
        idx = 0
        for i, line in enumerate(result.split('\n')):
            if i == 0 or not line: # header
                continue

            cols = line.split(None, 5)
            if len(cols) == 1:
                long_fs = cols[0]
                continue

            if long_fs:
                cols.insert(0, long_fs)
                long_fs = None

            fs, total, used, available, pused, mount = cols
            if not fs.startswith('/'):
                continue

            try:
                data = {
                    'total'     : long(total) * 1024,
                    'used'      : long(used) * 1024,
                    'free'      : long(available) * 1024,
                    'pused'     : int(pused.replace('%',''))
                }
                data['pfree'] = 100 - data['pused']
                stat = dict(map(lambda (k, v): ('%s.%s' % (idx, k), v), data.items()))

                mounts.append(mount)
                idx += 1

                stats.update(stat)

            except ValueError:
                continue

        # end for

        if self.__mounts != mounts:
            stats['meta.mounts'] = ','.join(mounts)
            self.__mounts = mounts


        return stats

    def fetchWindows(self):
        stats = {}
        mounts = []
        idx = 0
        for data in self._wmi.Win32_LogicalDisk (DriveType=3):
            mount = data.Name;
            data = {
                'total'     : long(data.Size),
                'free'      : long(data.FreeSpace),
            }
            data['used'] = data['total'] - data['free']
            data['pfree'] = round(data['free'] * 100.0 / data['total'], 2)
            data['pused'] = round(100.0 - data['pfree'],2)
            stat = dict(map(lambda (k, v): ('%s.%s' % (idx, k), v), data.items()))

            mounts.append(mount)
            idx += 1

            stats.update(stat)

        if self.__mounts != mounts:
            stats['meta.mounts'] = ','.join(mounts)
            self.__mounts = mounts

        return stats

            
