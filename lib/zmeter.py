import os
import sys
import time
import platform
import subprocess
import tempfile
import logging
import logging.handlers
import zmq
import signal

class ZMeter(object):

    def __init__(self,  endpoints = 'tcp://127.0.0.1:5555',
                        serializer = None,
                        logger = None,
                        config = None,
                        identity = None,
                        hwm = 10000):
        self.__serializer = serializer or JsonSerializer()
        self.__config = config

        self.__plugins = {}

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
        platform = self.platform()
        plugin_dir = []
        plugin_dir.append(os.path.join(os.path.dirname(__file__), 'plugins'))
        for path in plugin_dir:
            sys.path.append(path)
            for plugin in os.listdir(path):
                if not plugin.endswith('.py') \
                    or plugin.startswith('__init__'):
                    continue
                name, ext = os.path.splitext(plugin)
                loaded_module = __import__( name ) 
                
                for comp in dir(loaded_module):
                    mod = getattr(loaded_module, comp) 
                    if type(mod) == type and issubclass(mod, Metric):
                        inst = mod()
                        inst.init(platform, self.__config, self.__logger)
                        self.__plugins[name] = inst
 
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
        return inst and inst.fetch() or None

    def send(self, name, params = {}):
        try:
            frames = self.__serializer.feed({name: self.fetch(name)}, params)
            for frame in frames[:-1]:
                self.__inbox.send(frame, zmq.SNDMORE)
            self.__inbox.send(frames[-1])
        except Exception, e:
            self.__logger.error(e, exc_info=sys.exc_info())

    def sendall(self, params = {}):
        try:
            data = {}
            for name in self.__plugins.keys():
                data[name] = self.fetch(name)
            frames = self.__serializer.feed(data, params)
            for frame in frames[:-1]:
                self.__inbox.send(frame, zmq.SNDMORE)
            self.__inbox.send(frames[-1])
        except Exception, e:
            self.__logger.error(e, exc_info=sys.exc_info())

    def close(self):
        self.__inbox.close()
        self.__ctx.term()
        
    def platform(self):
        pf = {'machine'     : platform.machine(), 
              'processor'   : platform.processor(), 
              'system'      : platform.system(),
              'dist'        : None }

        if pf['system'] in ['Linux']:
            pf['dist'] = platform.linux_distribution()

        return pf


class Serializer(object):
    def __init__(self):
        import socket
        self.__host = platform.node()
        self.__ip = [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] 
                            if not ip.startswith("127.")][0]

    def header(self, params = {}):
        ts_millis = long(time.time() * 1000)
        header = {
            'ts'    : ts_millis,
            'host'  : self.__host,
            'ip'    : self.__ip,
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


class Metric(object):

    def __init__(self):
        pass

    def init(self, platform, config, logger):
        self._platform = platform
        self._config = config
        self._logger = logger

    def fetch(self):
        raise Exception("Must Override fetch")

    def execute(self, *args):
        try:
            signal.signal(signal.SIGALRM, self.sigHandler)
            signal.alarm(15)

            try:
                proc = subprocess.Popen(args, stdout=subprocess.PIPE, close_fds=True)
                return proc.communicate()[0]
            except OSError, e:
                self._logger.error(args[0], exc_info=sys.exc_info())
                return None
            except Exception, e:
                self._logger.error(args[0], exc_info=sys.exc_info())
                return None
        finally:
            signal.alarm(0)

    def sigHandler(self, signum, frame):
        raise Exception('Timeout')

class StdoutLogger(object):

    def write(self, *args):
        sys.stdout.write(args)

    info = write
    debug = write
    warn = write
    error = write
