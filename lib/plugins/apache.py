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
        self.__urls = {}

    def __initUrls(self):
        conf = self._config.get('apache', {})

        if self.__urls:
            return False

        if not conf:
            conf['port.apache'] = 80

        url_base = 'http://localhost:%d/server-status/?auto'
        for k, v in conf.items():
            if k.startswith('status_url.'):
                self.__urls[k.split('.', 1)[1]] = v

            if k.startswith('port.'):
                self.__urls[k.split('.', 1)[1]] = url_base % int(v)

        return True

    def fetch(self):

        conf = self._config.get('apache', {})

        stat = {}
        if self.__initUrls():
            stat['meta.urls'] = ','.join(map(lambda o: '%s:%s' % o, self.__urls.items()))

        for name, url in self.__urls.items():
            response = self.urlget(url, conf.get('user.%s' % name), conf.get('pass.%s' % name))
            if not response:
                continue 

            diffs = {}

            for i, line in enumerate(response.split('\n')):
                cols = line.split(': ')
                if cols[0] not in Apache.ACCEPTS:
                    continue
                key, func = Apache.ACCEPTS[cols[0]]
                value = func(cols[1]) 
                key = '%s.%s' % (name, key)
                prev = self.__prev.get(key)
                stat[key] = value
                self.__prev[key] = value

                if prev:
                    diffs[key] = value - prev
            if diffs:
                stat['%s.rps' % name] = round((diffs['%s.reqs' % name] -1) / self._elapsed, 2)

        return stat

