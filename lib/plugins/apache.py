import os
import re
import time
import platform
import urllib2
import zmeter

class Apache(zmeter.Metric):

    ACCEPTS = {
        'Total Accesses'    : ('reqs', long),
        'Total kBytes'      : ('bytes', lambda v: 1024*long(v)),
        'CPULoad'           : ('load', lambda v: round(float(v), 2)),
        'Uptime'            : ('uptime', long),
        'ReqPerSec'         : ('rps', lambda v: round(float(v), 2)),
        'BytesPerSec'       : ('bps', lambda v: round(float(v), 2)),
        'BytesPerReq'       : ('bpr', lambda v: round(float(v), 2)),
        'BusyWorkers'       : ('busy', int),
        'IdleWorkers'       : ('idle', int)
    }
    def __init__(self):
        super(Apache, self).__init__()

        self.__prev = {}

    def fetch(self):

        conf = self._config.get('apache', {})
        url = 'http://localhost/server-status/?auto'
        if conf.has_key('status_url'):
            url = conf['status_url']

        response = self.urlget(url, conf.get('user'), conf.get('pass'))

        stat = {}
        diffs = {}

        for i, line in enumerate(response.split('\n')):
            cols = line.split(': ')
            if cols[0] not in Apache.ACCEPTS:
                continue
            key, func = Apache.ACCEPTS[cols[0]]
            prev = self.__prev.get(key)
            value = func(cols[1]) 
            stat[key] = value
            self.__prev[key] = value

            if prev:
                diffs[key] = value - prev
        if diffs:
            stat['rps'] = round((diffs['reqs'] -1) / self._elapsed, 2)

        return stat

