import os
import re
import zmeter
import subprocess

class Mysql(zmeter.Metric):

    ACCEPTS = {
        'Created_tmp_disk_tables'   : 'tmp_disk_tables',
        'Max_used_connections'      : 'max_connections',
        'Open_files'                : 'open_files',
        'Slow_queries'              : 'slow_queries',
        'Table_locks_waited'        : 'lock_waited',
        'Threads_connected'         : 'threads_connected',
    }
    DELTA_ACCEPTS = {
        'Connections'               : 'connections',
    }

    def __init__(self):
        super(Mysql, self).__init__()

        self.__prev = {}

    def fetchLinux(self):

        conf = self._config.get('mysql', {})

        args = ['mysql', '-e', 'show status']
        if conf.has_key('port'):
            args.extend(['-P', str(conf['port'])])
        if conf.has_key('user'):
            args.extend(['-u', str(conf['user'])])
        if conf.has_key('password'):
            args.extend(['-p', str(conf['password'])])

        result = self.execute(*args)
        if not result:
            return None

        stats = {}
        for i, line in enumerate(result.split('\n')):
            line = line.strip()
            if not line:
                continue
            kv = line.strip().split()
            if len(kv) == 1:
                continue
            col, value = kv
            if col in Mysql.ACCEPTS:
                col = Mysql.ACCEPTS[col]
                stats[col] = int(value)
            elif col in Mysql.DELTA_ACCEPTS:
                col = Mysql.DELTA_ACCEPTS[col]
                prev = self.__prev.get(col)
                self.__prev[col] = int(value)
                if prev is None:
                    continue
                stats[col] = int(value) - prev -1
            
        return stats
