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
        self.__watches = {}

    def __initWatch(self, stats):
        watches = self._config.get('watch', {})
    
        for k in watches.keys():
            stats['watch.%s.cpu' % k] = 0
            stats['watch.%s.mem' % k] = 0
            stats['watch.%s.count' % k] = 0

        if self.__watches != watches:
            stats['meta.watches'] = ','.join(map(lambda o: '%s:%s' % o, watches.items()))
            self.__watches = watches
            
        return watches;

    def __updateWatch(self, pid, cmdline, cpu_data, mem_data, stats):
        for k, watch in self.__watches.items():
            if cmdline.find(watch) >= 0:
                stats['watch.%s.cpu' % k] += cpu_data[pid]
                stats['watch.%s.mem' % k] += mem_data[pid]
                stats['watch.%s.count' % k] += 1
            
    def fetchLinux(self):
        cpu_data = {}
        mem_data = {}
        stats = {}
        watches = self.__initWatch(stats);
        count = 0
    
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
            count += 1


            if not previous:
                self.__prev[pid] = value
                continue

            self.__prev[pid] = value

            usage = current - previous[0]
            cpu_data[pid] = round(usage * 100.0 / self._clock_ticks / self._elapsed / self._cores, 2)
            mem_data[pid] = int(values[Process.RSS]) * self._page_size

            self.__updateWatch(pid, cmdline, cpu_data, mem_data, stats)

        self.__sortStats(cpu_data, mem_data, stats)
        self._shared['process'] = self.__prev
        stats['all.count'] = count

        return stats
    
    def __sortStats(self, cpu_data, mem_data, stats):
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
    
    def fetchWindows(self):
        self.__cmdlines.clear()
        #self.__cmdlines[0] = 'Idle'
        cpu_data = {}
        mem_data = {}
        stats = {}
        watches = self.__initWatch(stats);
        count = 0

        for data in self._wmi.Win32_Process():
            pid = data.ProcessId
            cmdline = data.CommandLine or 'Idle'
            self.__cmdlines[pid] = cmdline
            current = long(getattr(data,'UserModeTime',0)) + long(getattr(data,'KernelModeTime',0)) # 100 nanos
            previous = self.__prev.get(pid)

            value = (current, cmdline, self._now)
            count += 1

            if not previous:
                self.__prev[pid] = value
                continue

            self.__prev[pid] = value

            usage = current - previous[0]
            cpu_data[pid] = round(usage * 100 / 10000000 / self._cores /self._elapsed, 2)
            mem_data[pid] = long(data.WorkingSetSize)
        
        self.__sortStats(cpu_data, mem_data, stats)
        self._shared['process'] = self.__prev
        stats['all.count'] = count
        return stats
