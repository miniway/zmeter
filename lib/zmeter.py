import os
import sys
import time
import platform
import subprocess
import tempfile
import logging
import logging.handlers
import szmq as zmq
import signal
import socket
import struct
import array
import urllib2
import base64
import string
import threading

class ZMeter(object):

    DEFAULT_PLUGINS = ["cpu","disk","iostat",
                                  "load","mem","net",
                                  "process","system"]

    def __init__(self,  endpoints = 'tcp://127.0.0.1:5555',
                        serializer = None,
                        logger = None,
                        config = {},
                        identity = None,
                        hwm = 10000):
        self.__serializer = serializer or JsonSerializer()
        self.__config = config

        self.__plugins = {}
        self.__closed = False

        self.loadLogger(logger)
        self.loadPlugins()
        self.loadZMQ(endpoints, identity, hwm)

    def plugins(self):
        return self.__plugins.keys()

    def loadLogger(self, logger):
        if logger:
            self.__logger = logger
            return

        self.__logger = StdoutLogger()

    def loadPlugins(self):
        self._platform = self.platform()
        plugin_dir = []
        plugin_dir.append(os.path.join(os.path.dirname(__file__), 'plugins'))
        names = []
        for path in plugin_dir:
            sys.path.append(path)
            try:
                for plugin in os.listdir(path):
                    if not plugin.endswith('.py') \
                        or plugin.startswith('__init__'):
                        continue
                    name, ext = os.path.splitext(plugin)
                    names.append(name)
            except:
                if self._platform['system'] == 'Windows':
                    # py2exe
                    names.extend(ZMeter.DEFAULT_PLUGINS)
                    names.extend(('mssql', 'apache', 'tomcat'))
                    break
                else:
                    raise
            
        for name in names:
            if name not in ZMeter.DEFAULT_PLUGINS and \
                            not self.__config.has_key(name):
                continue
            loaded_module = __import__( name ) 
                
            for comp in dir(loaded_module):
                mod = getattr(loaded_module, comp) 
                if type(mod) == type and issubclass(mod, Metric):
                    inst = mod()
                    inst.init(self._platform, self.__config, self.__logger)
                    self.__plugins[name] = inst

        self.__logger.info("Plantform: " + str(self._platform))
        self.__logger.info("Loaded Plugins: " + str(self.__plugins.keys()))

    def loadZMQ(self, endpoints, identity, hwm):
        endpoints = endpoints.split(',')
        self.__ctx = zmq.Context()
        self.__inbox = self.__ctx.socket(zmq.PUSH)
        self.__inbox.linger = 5000
        if identity:
            self.__inbox.identity = identity
        self.__inbox.hwm = hwm
        for endpoint in endpoints:
            self.__inbox.connect(endpoint.strip())

    def asList(self, val):
        import types
        return isinstance(val, types.StringTypes) and [val] or val

    def fetch(self, name):
        inst = self.__plugins.get(name)
        if not inst:
            return None

        system = self._platform['system']
        method = getattr(inst, 'fetch%s' % system, getattr(inst, 'fetch'))
        if not method:
            self._logger.error("Not Supported Platform %s" % system)
            return None
        inst.beforeFetch()
        try:
            result = method()
        except Exception, e:
            self.__logger.exception("Exception at %s 'fetch'" % name)
            return
        inst.afterFetch(result)

        return result

    def send(self, name, params = {}, value = {}):
        try:
            if not value:
                data = { name : self.fetch(name)}
            else:
                data = { name : value }
            frames = self.__serializer.feed(data, params)
        except Exception, e:
            self.__logger.exception("Exception at 'send'")
            return

        for frame in frames[:-1]:
            self.__inbox.send(frame, zmq.SNDMORE)
        self.__inbox.send(frames[-1])

    def sendall(self, params = {}):
        try:
            data = {}
            for name in self.__plugins.keys():
                stat = self.fetch(name)
                if stat:
                    data[name] = stat
            frames = self.__serializer.feed(data, params)
        except Exception, e:
            self.__logger.exception("Exception at 'sendall'")
            return

        for frame in frames[:-1]:
            self.__inbox.send(frame, zmq.SNDMORE)
        self.__inbox.send(frames[-1])

    def close(self):
        # Synchronize
        if self.__closed:
            return
        self.__closed = True
        self.__inbox.close()
        self.__ctx.term()

    def __del__(self):
        self.close()
        
    def platform(self):
        cores = self.__parseCpuInfo()
        system = platform.system()
        pf = {
            'ip'            : get_ips()[0],
            'system'        : system,
            'cores'         : len(cores)
        }
        if system == 'Linux':
            pf.update({
                'clock_ticks'   : os.sysconf('SC_CLK_TCK'),
                'page_size'     : os.sysconf('SC_PAGE_SIZE'),
            })
        elif system == 'Windows':
            import wmi
            c = wmi.WMI()
            nic = []
            for inf in c.Win32_NetworkAdapterConfiguration(IPEnabled=1):
                nic.append(inf.Description)
            pf.update({
                'wmi'           : c,
                'nic'           : nic
            })

        return pf

    def __parseCpuInfo(self):

        system = platform.system()
        cores = []
        if system == 'Linux':
            core = {}
            for line in open('/proc/cpuinfo').readlines():
                kv = line.split(':', 1)
                if len(kv) == 1:
                    cores.append(core)
                    core = {}
                else:
                    core[kv[0].strip()] = kv[1].strip()
        elif system == 'Windows':
            import wmi
            c = wmi.WMI()
            for cpu in c.Win32_Processor():
                cores.extend(map(lambda n: {cpu.Name : n}, range(cpu.NumberOfCores)))
        return cores
        


class Serializer(object):
    def __init__(self):
        pass

    def header(self, params = {}):
        ts_millis = long(time.time() * 1000)
        header = {
            'ts'    : ts_millis,
        }
        header.update(params)
        return header

class JsonSerializer(Serializer):
    def __init__(self):
        super(JsonSerializer, self).__init__()
        pass

    def feed(self, body, params):
        import json
        return [json.dumps(self.header(params)), json.dumps(body)]

class ThreadRunner(threading.Thread):
    def __init__(self, func, *args, **kwargs):
        super(ThreadRunner, self).__init__()
        self.__args = args
        self.__func = func
        self.__kwargs = kwargs
        self.__timeout = False
        self.__result = None
        
    def run(self):
        self.__timeout = True
        self.__result = self.__func(*self.__args, **self.__kwargs)
        self.__timeout = False

    def result(self):
        return self.__result

    def timeout(self):
        return self.__timeout

class Metric(object):

    def __init__(self):
        pass

    def init(self, platforms, config, logger):
        for k, v in platforms.items():
            setattr(self, '_%s' % k, v)
        self._platform = platforms
        self._config = config
        self._logger = logger
        self._elapsed = None
        self._last_updated = None
        self._now = None
        self._shared = {}

    def beforeFetch(self):
        now = time.time()
        if self._now:
            self._elapsed = now - self._now
        self._now = now

    def afterFetch(self, result):
        if result:
            self._last_updated = time.time()

    def fetch(self):
        self._logger.error("Must Override fetch or fetch%s" % self._system)

    def execute(self, *args):
        runner = ThreadRunner(self._execute, *args)
        runner.start()
        runner.join(3)
        if runner.timeout():
            self._logger.error("Timeout at Execute %s", ' '.join(args))
            return None
        else:
            out, err = runner.result()
            return out

    def _execute(self, *args):
        import subprocess
        import platform
        if platform.system() == 'Linux':
            close_fds = True
        else:
            close_fds = False

        result = '' 
        try:
            proc = subprocess.Popen(map(lambda s: str(s), args), 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE, 
                                    close_fds=close_fds)
            result = proc.communicate()
        except OSError, e:
            self._logger.exception(args[0])
            result = '',str(e)
        except Exception, e:
            self._logger.exception(args[0])
            result = '',str(e) 
        return result

    def urlget(self, url, user = None, passwd = None):
        runner = ThreadRunner(self._urlget, url, user, passwd)
        runner.start()
        runner.join(3)
        if runner.timeout():
            self._logger.error("Timeout at URL fetch %s", ' '.join(url))
            return None
        else:
            return runner.result()
        
    def _urlget(self, url, user = None, passwd = None):
        headers = {
            'User-Agent'    : 'ZAgent',
            'Content-Type'  : 'application/x-www-form-urlencoded',
            'Accept'        : 'text/html, */*'
        }
        try:
            if user:
                auth = base64.encodestring('%s:%s' % (user, passwd)).replace('\n', '')
                headers["Authorization"] = "Basic %s" % auth

            req = urllib2.Request(url, None, headers)
            request = urllib2.urlopen(req)

            response = request.read()
            return response
        except:
            self._logger.exception(url)
            return None

class StdoutLogger(object):

    def write(self, *args):
        for m in args:
            sys.stdout.write(m)

    def exception(self, *args):
        import traceback
        sys.stdout.write(args[0])
        sys.stdout.write(traceback.format_exc())

    info = write
    debug = write
    warn = write
    error = write

def get_ips():
    return [ip for name, ip in get_interfaces() if name != 'lo' ]

def get_interfaces():
    def format_ip(addr):
        return str(ord(addr[0])) + '.' + \
               str(ord(addr[1])) + '.' + \
               str(ord(addr[2])) + '.' + \
               str(ord(addr[3]))
    max_possible = 128  # arbitrary. raise if needed.
    bytes = max_possible * 32
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    names = array.array('B', '\0' * bytes)
    system = platform.system()
    if system == 'Linux':
        import fcntl
        outbytes = struct.unpack('iL', fcntl.ioctl(
            s.fileno(),
            0x8912,  # SIOCGIFCONF
            struct.pack('iL', bytes, names.buffer_info()[0])
        ))[0]
        namestr = names.tostring()
        lst = []
        for i in range(0, outbytes, 40):
            name = namestr[i:i+16].split('\0', 1)[0]
            ip   = namestr[i+20:i+24]
            lst.append((name, format_ip(ip)))
        return lst
    elif system == 'Windows':
        import wmi
        c = wmi.WMI()
        lst = []
        for inf in c.Win32_NetworkAdapterConfiguration(IPEnabled=1):
            lst.append((inf.Description, inf.IPAddress[0]))
        return lst
