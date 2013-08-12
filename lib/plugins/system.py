import os
import re
import time
import platform
import zmeter

class System(zmeter.Metric):

    def __init__(self):
        super(System, self).__init__()

    def __checkLast(self):
        if self._last_updated and self._last_updated > self._now - 86400:
            return False
        return True
        
    def fetch(self):

        if not self.__checkLast():
            return

        data = {
            'host'        : platform.node(),
            'machine'     : platform.machine(), 
            'processor'   : platform.processor(), 
            'system'      : platform.system(),
            'release'     : platform.release(),
            'cores'       : self._cores
        }

        if data['system'] == 'Linux':
            data['dist'] = ','.join(platform.dist())
        elif data['system'] == 'Windows':
            data['dist'] = ','.join(platform.win32_ver())

        stat = {}
        for k, v in data.items():
            stat['meta.%s' % k] = v

        return stat

        

