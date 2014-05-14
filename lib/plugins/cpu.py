import os
import re
import zmeter
import subprocess

class Cpu(zmeter.Metric):

    def __init__(self):
        super(Cpu, self).__init__()
        self.__re_header = re.compile(r'%(\w+)')
        self.__re_value = re.compile(r'\d+\.\d+')

    def _fixName(self, name):
        if name == 'user':
            return 'usr'
        return name

    def fetchLinux(self):
        result = self.execute('mpstat', '-P', 'ALL', '3', '1')

        if not result:
            return None

        base = 2
        pos = -1
        stats = {}
        for i, line in enumerate(result.split('\n')):
            if i < base: 
                continue
            if not line: # Average
                base = i+1
                break

            if i == base:  # Header
                pos = line.find('CPU')
                names = re.findall(self.__re_header, line)
            else:
                cpu = line[pos: line.find(' ', pos+3)].strip()
                values = re.findall(self.__re_value, line)
                if len(values) > len(names): # intrs/s
                    values = values[:len(names)]
                stat = dict(map(lambda k,v: ('%s.%s' % (cpu, self._fixName(k)), round(float(v),2)), names, values))
                stat['%s.used' % cpu ] = round(100.0 - stat['%s.idle' % cpu], 2)
                stats.update(stat)

        return stats
    
    def fetchWindows(self):
        stats = {}
        for data in self._wmi.InstancesOf("Win32_PerfFormattedData_Counters_ProcessorInformation"):
            if data.Name == '_Total':
                cpu = 'all'
            else:
                cpu = data.Name.split(",")[0]
            stat = {
                '%s.used' % cpu : int(data.PercentProcessorTime),
                '%s.idle' % cpu : int(data.PercentIdleTime),
                '%s.usr' % cpu : int(data.PercentUserTime),
                '%s.sys' % cpu : int(data.PercentPrivilegedTime),
                '%s.irq' % cpu : int(data.PercentInterruptTime),
            }
            stats.update(stat)
            
        return stats
