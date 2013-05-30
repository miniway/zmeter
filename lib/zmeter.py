import os
import sys
import time
import platform
import subprocess
import tempfile
import logging
import logging.handlers
import zmq

class ZMeter(object):

    def __init__(self,  endpoints = 'tcp://127.0.0.1:5555',
                        serializer = None,
                        logdir = tempfile.gettempdir(),
                        hwm = 10000):
        self.__serializer = serializer or JsonSerializer()

        self.__plugins = {}

        self.loadLogger(logdir)
        self.loadPlugins()
        self.loadZMQ(endpoints, hwm)

    def loadLogger(self, logdir):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        log_handler = logging.handlers.TimedRotatingFileHandler(os.path.join(logdir, __name__ + ".log"))
        log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_handler.setFormatter(log_formatter)

        self.logger.addHandler(log_handler)

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
                        inst.init(platform, self.logger)
                        self.__plugins[name] = inst
 
    def loadZMQ(self, endpoints, hwm):
        endpoints = self.asList(endpoints)
        self.__ctx = zmq.Context()
        self.__inbox = self.__ctx.socket(zmq.PUSH)
        self.__inbox.linger = 5000
        self.__inbox.hwm = hwm
        for endpoint in endpoints:
            self.__inbox.connect(endpoint)

    def asList(self, val):
        import types
        return isinstance(val, types.StringTypes) and [val] or val

    def fetch(self, name):
        inst = self.__plugins.get(name)
        return inst and inst.fetch() or None

    def send(self, name):
        frames = self.__serializer.feed(name, self.fetch(name))
        for frame in frames[:-1]:
            self.__inbox.send(frame, zmq.SNDMORE)
        self.__inbox.send(frames[-1])

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
        pass

    def header(self, name):
        ts_millis = long(time.time() * 1000)
        return {
            'ts'    : ts_millis,
            'host'  : platform.node(),
            'kind'  : name,
        }

class JsonSerializer(Serializer):
    def __init__(self):
        super(JsonSerializer, self).__init__()
        pass

    def feed(self, name, body):
        import json
        return [json.dumps(self.header(name)), json.dumps(body)]


class Metric(object):

    def __init__(self):
        self._logger = None

    def init(self, platform, logger):
        self._platform = platform
        self._logger = logger

    def fetch(self):
        raise Exception("Must Override fetch")

    def execute(self, *args):
        try:
            proc = subprocess.Popen(args, stdout=subprocess.PIPE, close_fds=True)
            return proc.communicate()[0]
        except OSError, e:
            self.logger.error(args[0], exc_info=sys.exc_info())
            return None

