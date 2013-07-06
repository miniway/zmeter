import os
import re
import zmeter
import subprocess

class Cpu(zmeter.Metric):

    def __init__(self):
        super(Cpu, self).__init__()
        self.__re_header = re.compile(r'%(\w+)')
        self.__re_value = re.compile(r'\d+\.\d+')

    def fetchLinux(self):
        result = self.execute('mpstat', '-P', 'ALL', '1', '1')

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
                stat = dict(map(lambda k,v: ('%s.%s' % (cpu, k), round(float(v),2)), names, values))
                stat['%s.used' % cpu ] = 100 - stat['%s.idle' % cpu]
                stats.update(stat)

        return stats

