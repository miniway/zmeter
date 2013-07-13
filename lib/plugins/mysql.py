import os
import re
import zmeter
import subprocess

class Mysql(zmeter.Metric):

    ACCEPTS = {
        'Connections'               : 'connections',
        'Created_tmp_disk_tables'   : 'tmp_disk_tables',
        'Max_used_connections'      : 'max_connections',
        'Open_files'                : 'open_files',
        'Slow_queries'              : 'slow_queries',
        'Table_locks_waited'        : 'lock_waited',
        'Threads_connected'         : 'threads_connected',
    }

    def __init__(self):
        super(Mysql, self).__init__()

    def fetchLinux(self):

        conf = self._config.get('mysql', {})

        args = ['mysql', '-e', 'show status']
        if conf.has_key('port'):
            args.extend(['-P', str(conf['port'])])

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
            if col not in Mysql.ACCEPTS:
                continue
            col = Mysql.ACCEPTS[col]
            stats[col] = int(value)
            

        return stats
