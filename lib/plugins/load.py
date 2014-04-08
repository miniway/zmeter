import os
import re
import zmeter
import subprocess

class Load(zmeter.Metric):

    def __init__(self):
        super(Load, self).__init__()

    def fetchLinux(self):
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

    def fetchWindows(self):

        data = self._wmi.InstancesOf("Win32_PerfFormattedData_PerfOS_System")[0]
        stats = {
            'load'      : int(data.ProcessorQueueLength),
            'threads'   : int(data.Threads)
        }

        return stats
