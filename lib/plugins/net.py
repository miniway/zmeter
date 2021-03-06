import os
import re
import zmeter
import subprocess

class Net(zmeter.Metric):

    ACCEPTS = {
        'in.bytes'      : 'in.bps',   # Bytes Per Sec
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
            if face == 'lo': continue
            
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

        stats = self._updateMeta(stats, ifs)

        return stats

    def fetchWindows(self):

        stats = {}
        ifs = []
        for data in self._wmi.InstancesOf("Win32_PerfFormattedData_Tcpip_NetworkInterface"):
            ifname = re.sub('_(\d+)', r'#\1', data.Name)
            try:
                idx = self._nic.index(ifname)
            except ValueError:
                continue

            ifs.append(ifname)
            stat = {
                '%d.in.bps'  % idx : long(data.BytesReceivedPerSec),
                '%d.out.bps' % idx : long(data.BytesSentPerSec),
                '%d.in.pps'  % idx : long(data.PacketsReceivedPersec),
                '%d.out.pps' % idx : long(data.PacketsSentPersec),
                '%d.in.errs' % idx : long(data.PacketsReceivedErrors),
                '%d.out.errs'% idx : long(data.PacketsOutboundErrors),
            }
            stats.update(stat)

        stats = self._updateMeta(stats, ifs)

        return stats

    def _updateMeta(self, stats, ifs):
        if self.checkLast() or self.__ifs != ifs:
            stats['meta.ifs'] = ','.join(ifs)
            self.__ifs = ifs

        return stats
