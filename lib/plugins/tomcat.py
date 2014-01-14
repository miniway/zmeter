import os
import re
import time
import platform
import urllib
import urllib2
import zmeter

class Tomcat(zmeter.Metric):

    ACCEPTS = {
        'requestCount'      : ('reqs', long),
        'bytesSent'         : ('bytes', long),
        'errorCount'        : ('errs', long),
        'processingTime'    : ('time', long),  # milli seconds
        'currentThreadsBusy'       : ('busy', int),
        'currentThreadCount'       : ('thread.cnt', int)
    }
    def __init__(self):
        super(Tomcat, self).__init__()

        self.__prev = {}
        self.__urls = {}

    def __initUrls(self):
        conf = self._config.get('tomcat', {})

        if self.__urls:
            return False

        if not conf:
            conf['port.tomcat'] = 80

        url_base = 'http://localhost:%d/manager/jmxproxy?qry='
        for k, v in conf.items():
            if k.startswith('jmxproxy_url.'):
                self.__urls[k.split('.', 1)[1]] = v

            if k.startswith('port.'):
                self.__urls[k.split('.', 1)[1]] = url_base % int(v)

        return True

    def fetch(self):

        conf = self._config.get('tomcat', {})
        stat = {}
        if self.__initUrls():
            stat['meta.urls'] = ','.join(map(lambda o: '%s:%s' % o, self.__urls.items()))

        items = [
            'Catalina:type=GlobalRequestProcessor,name=http-*',
            'Catalina:type=ThreadPool,name=http-*',
        ]

        diffs = {}

        for name, url_base in self.__urls.items():
            for item in items:
                url = url_base + urllib.quote(item, '*')
                response = self.urlget(url, conf.get('user.%s' % name), conf.get('pass.%s' % name))

                for i, line in enumerate(response.split('\n')):
                    cols = line.split(': ')
                    if cols[0] not in Tomcat.ACCEPTS:
                        continue
                    key, func = Tomcat.ACCEPTS[cols[0]]
                    value = func(cols[1]) 
                    key = '%s.%s' % (name, key)
                    prev = self.__prev.get(key)
                    stat[key] = value
                    self.__prev[key] = value

                    if prev:
                        diffs[key] = value - prev

            if diffs:
                stat['%s.rps' % name] = round((diffs['%s.reqs' % name] - len(items)) / self._elapsed, 2)
                stat['%s.bps' % name] = long(diffs['%s.bytes' % name] / self._elapsed)
                stat['%s.bpr' % name] = long(diffs['%s.bytes' % name] / diffs['%s.reqs' % name])

            stat['%s.idle' % name] = stat.pop('%s.thread.cnt' % name) - stat['%s.busy' % name]
        return stat

