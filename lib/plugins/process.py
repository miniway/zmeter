import os
import re
import zmeter
try:
    import simplejson as json
except ImportError:
    import json

try:
    json.encoder.FLOAT_REPR = lambda f: ("%.2f" % f)
    json.encoder.c_make_encoder = None
except:
    pass

class Process(zmeter.Metric):

    COMM    = 1
    UTIME   = 13
    STIME   = 14
    VSIZE   = 22
    RSS     = 23

    def __init__(self):
        super(Process, self).__init__()

        self.__prev = {}
        self.__cmdlines = {}
        self.__watches = []

    def fetchLinux(self):

        watches = self._config.get('watch', [])
    
        cpu_data = {}
        mem_data = {}
        stats = {}
        for i in range(len(watches)):
            stats['watch.%d.cpu' % i] = 0
            stats['watch.%d.mem' % i] = 0
            stats['watch.%d.count' % i] = 0

        for pid in os.listdir('/proc/'):
            if not pid.isdigit():
                continue

            try:
                values = open('/proc/%s/stat' % pid).read().split()
            except IOError:
                continue

            if len(values) < 15: 
                continue

            current = int(values[Process.UTIME]) + int(values[Process.STIME])
            previous = self.__prev.get(pid)
            name = values[Process.COMM][1:-1]
            cmdline = self.__getCmdline(pid, name)
            value = (current, cmdline, self._now)


            if not previous:
                self.__prev[pid] = value
                continue

            self.__prev[pid] = value

            usage = current - previous[0]
            cpu_data[pid] = round(usage * 100.0 / self._clock_ticks / self._elapsed, 2)
            mem_data[pid] = int(values[Process.RSS]) * self._page_size

            for i, watch in enumerate(watches):
                if cmdline.find(watch) >= 0:
                    stats['watch.%d.cpu' % i] += cpu_data[pid]
                    stats['watch.%d.mem' % i] += mem_data[pid]
                    stats['watch.%d.count' % i] += 1

        values = sorted(cpu_data, key=cpu_data.get, reverse=True)
        top = []
        for pid in values[:10]:
            cmdline = self.__prev[pid][1]
            top.append({ "pid"  : pid, 
                         "cpu"  : cpu_data[pid],
                         "mem"  : mem_data[pid],
                         "cmd" : cmdline})
        
        expire = self._now - 60  # 1 min
        for k, v in self.__prev.items():
            if v[2] < expire:
                del self.__prev[k]
                try:
                    del self.__cmdlines[k]
                except KeyError:
                    pass

        stats['snapshot.top10'] = json.dumps(top)

        if self.__watches != watches:
            stats['meta.watches'] = ','.join(watches)
            self.__watches = watches

        return stats
        
    def __getCmdline(self, pid, name):
        cmdline = self.__cmdlines.get(pid)
        if cmdline:
            return cmdline

        try:
            cmdline = open('/proc/%s/cmdline' % pid).read().split('\0')
            cmdline = ' '.join(cmdline).strip()
            if not cmdline:
                raise
        except:
            cmdline = name

        self.__cmdlines[pid] = cmdline
        return cmdline
