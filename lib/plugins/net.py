import os
import re
import zmeter
import subprocess

class Net(zmeter.Metric):

    ACCEPTS = [
        'in.bytes',
        'in.packets',
        'in.errs',
        'out.bytes',
        'out.packets',
        'out.errs',
    ]
    def __init__(self):
        super(Net, self).__init__()

        self.__ifs = []

    def fetch(self):
        if self._platform['system'] == 'Linux':
            return self.__fetchLinuxStat()

        self._logger.error("Not Supported Platform " + self._platform['system'])
        return None

    def __fetchLinuxStat(self):

        ifs = []
        idx = 0
        stats = {}

        for i, line in enumerate(open('/proc/net/dev').readlines()):
            line = line.strip()
            if i == 0: continue
            if i == 1:
                _, recv_cols, trans_cols = line.split('|')
                recv_cols = map(lambda c:'in.' + c, recv_cols.split())
                trans_cols = map(lambda c:'out.' + c, trans_cols.split())
                                        
                cols = recv_cols + trans_cols
                continue

            if line.find(':') < 0: continue

            face, data = line.split(':')
            data = data.split()
            ifs.append(face)

            for j in range(len(cols)):
                if cols[j] not in Net.ACCEPTS:
                    continue
                stats['%d.%s' % (idx, cols[j])] = int(data[j])
            idx += 1

        if self.__ifs != ifs:
            stats['meta.ifs'] = ','.join(ifs)
            self.__ifs = ifs

        return stats
