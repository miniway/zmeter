import os
import re
import zmeter
import subprocess

class Net(zmeter.Metric):

    ACCEPTS = {
        'in.bytes'      : 'in.bps',
        'in.packets'    : 'in.pps',
        'in.errs'       : 'in.errs',
        'out.bytes'     : 'out.bps',
        'out.packets'   : 'out.pps',
        'out.errs'      : 'out.errs',
    }
    def __init__(self):
        super(Net, self).__init__()

        self.__ifs = []
        self.__prev = {}

    def fetchLinux(self):

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
                col = Net.ACCEPTS[cols[j]]
                key = '%d.%s' % (idx, col)
                value = long(data[j]) 
                prev = self.__prev.get(key)
                if prev is not None:
                    if col in [ 'in.errs', 'out.errs']:
                        stats[key] = value - prev
                    else:
                        stats[key] = round((value - prev) / self._elapsed, 2)
                self.__prev[key] = value
            idx += 1

        if self.__ifs != ifs:
            stats['meta.ifs'] = ','.join(ifs)
            self.__ifs = ifs

        return stats
