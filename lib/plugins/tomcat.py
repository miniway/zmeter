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

    def fetch(self):

        conf = self._config.get('tomcat', {})
        url_base = 'http://localhost:8080/manager/jmxproxy?qry='
        if conf.has_key('jmxproxy_url'):
            url_base = conf['jmxproxy_url']

        items = [
            'Catalina:type=GlobalRequestProcessor,name=http-*',
            'Catalina:type=ThreadPool,name=http-*',
        ]

        stat = {}
        diffs = {}

        for item in items:
            url = url_base + urllib.quote(item, '*')
            response = self.urlget(url, conf.get('user'), conf.get('pass'))

            for i, line in enumerate(response.split('\n')):
                cols = line.split(': ')
                if cols[0] not in Tomcat.ACCEPTS:
                    continue
                key, func = Tomcat.ACCEPTS[cols[0]]
                prev = self.__prev.get(key)
                value = func(cols[1]) 
                stat[key] = value
                self.__prev[key] = value

                if prev:
                    diffs[key] = value - prev

        if diffs:
            stat['rps'] = round((diffs['reqs'] - len(items)) / self._elapsed, 2)
            stat['bps'] = long(diffs['bytes'] / self._elapsed)
            stat['bpr'] = long(diffs['bytes'] / diffs['reqs'])

        stat['idle'] = stat.pop('thread.cnt') - stat['busy']
        return stat

