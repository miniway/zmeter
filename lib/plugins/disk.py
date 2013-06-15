import os
import re
import zmeter
import subprocess

class Disk(zmeter.Metric):

    def __init__(self):
        super(Disk, self).__init__()

        self.__mounts = []

    def fetch(self):
        if self._platform['system'] == 'Linux':
            return self.__fetchLinuxStat()

        self._logger.error("Not Supported Platform " + self._platform['system'])
        return None

    def __fetchLinuxStat(self):
        
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
                    'total'     : int(total) * 1024,
                    'used'      : int(used) * 1024,
                    'free'      : int(available) * 1024,
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
