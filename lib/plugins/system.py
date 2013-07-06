import os
import re
import time
import platform
import zmeter

class System(zmeter.Metric):

    def __init__(self):
        super(System, self).__init__()

    def fetchLinux(self):

        if self._last_updated and self._last_updated > self._now - 3600:
            return

        data = {
            'host'        : platform.node(),
            'machine'     : platform.machine(), 
            'processor'   : platform.processor(), 
            'system'      : platform.system(),
            'release'     : platform.release(),
            'dist'        : ','.join(platform.dist()),
            'cores'       : self._cores
        }

        stat = {}
        for k, v in data.items():
            stat['meta.%s' % k] = v

        return stat
