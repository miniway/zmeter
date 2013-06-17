import os
import re
import zmeter
import subprocess

class Load(zmeter.Metric):

    def __init__(self):
        super(Load, self).__init__()

    def fetch(self):
        if self._platform['system'] == 'Linux':
            return self.__fetchLinuxStat()

        self._logger.error("Not Supported Platform " + self._platform['system'])
        return None

    def __fetchLinuxStat(self):
        line = open('/proc/loadavg').readlines()[0].strip()
        cols = line.split(' ')
        if len(cols) != 5:
            self._logger.error("Invalid loadavg format " + line)
            return

        running, threads = cols[3].split('/')
        stats = {
            'avg1' : round(float(cols[0]),2),
            'avg5' : round(float(cols[1]),2),
            'avg15': round(float(cols[2]),2),
            'running': int(running),
            'threads': int(threads),
        }

        return stats
