import os
import sys
import time
import errno
import socket
import select
import struct
import threading
import logging
import Queue
from urlparse import urlparse

PAIR        = 0X00
PUB         = 0X01
SUB         = 0X02
REQ         = 0X03
REP         = 0X04
DEALER      = 0X05
ROUTER      = 0X06
PULL        = 0X07
PUSH        = 0X08

SNDMORE     = 0x01
DONTWAIT    = 0x02

class Context(object):

    def __init__(self):
        self.io_thread = IoThread()
        self.io_thread.start()

    def socket(self, socket_type):
        return Socket(self, socket_type)

    def term(self):
        self.io_thread.close()
        self.io_thread.join()

class Socket(object):

    def __init__(self, ctx, socket_type):
        self.ctx = ctx
        self.connected = False

        self.__socket_type = socket_type

        self.__peer = []

        self.handshake = None
        self.endpoint = None
        self.identity = ''
        self.sock = None


    def connect(self, endpoint):
        self.handshake = self.__handshake()
        self.endpoint = endpoint

        ret = self.ctx.io_thread.startConnect(self)
        if ret == 0 or ret == errno.EINPROGRESS or 10035: # errno.WSAEWOULDBLOCK
            pass
        else: 
            raise Exception("Connection Error " + str(ret))


    def send(self, message, flags = 0):
        self.ctx.io_thread.send(self, message, flags)

    def recv(self, flags = 0):
        self.ctx.io_thread.recv(self, flags)
 
    def __handshake(self):
        # greeting    = signature revision socket-type identity
        # signature   = %xFF 8%x00 %x7F
        # revision    = %x01
        # identity    = final-short body
        # final-short = %x00 OCTET
        # body        = *OCTET

        # message     = *more-frame final-frame
        # final-frame = final body
        # final       = final-short | final-long
        # final-long  = %x02 8OCTET
        # more-frame  = more body
        # more        = more-short | more-long
        # more-short  = %x01 OCTET
        # more-long   = %x03 8OCTET
        identity_len = len(self.identity)
        signature = chr(0xFF) + struct.pack('>q', identity_len+1) + chr(0x7F) \
                        + chr(0x01) \
                        + chr(self.__socket_type)
        identity = chr(0x00) + chr(identity_len) + self.identity

        return signature + identity

    def close(self):
        self.ctx.io_thread.disconnect(self)

class Pipe(object):

    def __init__(self):
        self.__r, self.__w = os.pipe()
        self.__peek = None

    def fileno(self):
        return self.__r

    def write(self, data):
        try:
            os.write(self.__w, data)
        except OSError:
            pass

    def peek(self):
        if self.__peek is not None:
            return True
        self.__peek = os.read(self.__r, 1)
        if self.__peek is None:
            return False
        return True
    
    def recv(self):
        if self.__peek:
            data = self.__peek
            self.__peek = None
            return data
        return os.read(self.__r, 1)

    def close(self):
        os.close(self.__r)
        os.close(self.__w)

class WinPipe(object):

    def __init__(self):
        self.__q = Queue.Queue()
        self.__peek = None

    def fileno(self):
        return -1

    def write(self, data):
        self.__q.put(data)

    def peek(self, tout):
        if self.__peek is not None:
            return True
        try:
            self.__peek = self.__q.get(True, tout)
        except Queue.Empty:
            self.__peek = None
        if self.__peek is None:
            return False
        return True
    
    def recv(self):
        if self.__peek:
            data = self.__peek
            self.__peek = None
            return data
        return self.__q.get(True)
        
    def close(self):
        self.__q = None
        
class IoThread(threading.Thread):


    def __init__(self):
        import platform
        threading.Thread.__init__(self)
        if platform.system() == 'Windows':
            self.__pipe = WinPipe()
        else:
            self.__pipe = Pipe()
        self.__sockets = {}
        self.__send_bufs = {}
        self.__recv_bufs = {}
        self.__sendings = {}
        self.__recievings = {}
        self.__handshaking = []
        self.__disconnected = []

        self.loadLogger("./", logging.INFO)

        self.__poller = Poller(self.logger)
        self.__poller.register(self.__pipe, Poller.POLLIN)
        self.__sockets[self.__pipe] = self
        self.__cmds = Queue.Queue()

        self.__stop = False

    def stopped(self):
        return self.__stop

    def loadLogger(self, logdir, level):
        name = 'zmq'
        self.logger = logging.getLogger(name)
        self.logger.propagate = True
        self.logger.setLevel(logging.INFO)

    def run(self):
        self.__timeout = -1
        self.__retry_at = time.time()
        self.logger.info("Start IO Thread")
        try:
            while not self.__stop:
                items = self.__poller.poll(self.__timeout)

                if self.__timeout > 0 and \
                    (time.time() - self.__retry_at) * 1000 > self.__timeout:
                    for sock in self.__disconnected:
                        self.startConnect(sock)
                    self.__disconnected = []
                    self.__timeout = -1

                for fo, event in items:
                    self.handlePoll(fo, event)

        except Exception:
            self.logger.exception("Error at polling")

        # end while
        self.__poller.close()
        self.__pipe.close()
        self.__stop = True
        self.logger.info("End IO Thread")

    def handlePoll(self, fo, event):

        sock = self.__sockets.get(fo)

        if sock is None:
            # Already disconnected
            return

        if event & Poller.POLLIN:
            if fo == self.__pipe:
                data = fo.recv()
                if not data:
                    raise Exception("Pipe Disconnected")
                for i in range(len(data)):
                    if self.__stop:
                        break
                    cmd, arg = self.__cmds.get()
                    if cmd == 'connect':
                        self.__sockets[arg.sock] = arg
                        self.__poller.register(arg.sock, Poller.POLLOUT)
                        #self.__connecting.append(arg.sock)
                        self.logger.info("Register Connect")
                    elif cmd == 'disconnect':
                        self.__disconnect(arg.sock)
                    elif cmd == 'send':
                        self.__poller.register(arg.sock, Poller.POLLOUT)
                    elif cmd == 'term':
                        self.__poller.unregister(self.__pipe, Poller.POLLIN | Poller.POLLOUT)
                        self.__stop = True
                    else:
                        raise Exception("Unknown Command " + cmd)
                return

            try:
                data = fo.recv(65536)
            except socket.error, e:
                data = None
                self.logger.exception("Error at Read")

            if not data:
                self.logger.info("Disconnected at Read")
                self.__disconnect(fo)
                self.startConnect(sock)
                return

            recieving = self.__recievings.get(fo)
            if recieving:
                data = recieving + data

            if fo in self.__handshaking:
                parsed = self.__parseHandShaking(data)
                if parsed:
                    data = data[parsed:]
                    self.__handshaking.remove(fo)
                    self.logger.info("Connect Completed")
                else:
                    self.__recievings[fo] = data
                    return

            bufs = self.__recv_bufs[sock]
            while data:
                message, more, parsed = self.__parse(data)
                if not parsed:
                    break
                bufs.put((message, more))
                data = data[parsed:]

            self.__recievings[fo] = data

        if event & Poller.POLLOUT:

            if sock.connected:
                bufs = self.__send_bufs[sock]
                data = self.__sendings.get(fo)
                if not data:
                    try:
                        data = bufs.get(False)
                    except Queue.Empty:
                        self.__poller.unregister(fo, Poller.POLLOUT)
                        return
                
                try:
                    sent = fo.send(data)
                except socket.error, e:
                    if e.args[0] in [errno.ECONNRESET, errno.ECONNREFUSED]:
                       sent = -1
                       self.logger.exception("Error at Send")
                    else:
                       raise
                if sent < 0:
                    self.logger.info("Disconnected at Send")
                    self.__disconnect(fo)
                    self.startConnect(sock)
                    return
            
                if sent == len(data):
                    self.__sendings[fo] = None
                    
                    if self.__send_bufs[sock].empty():
                        self.__poller.unregister(fo, Poller.POLLOUT)
                elif sent < len(data):
                    data = data[sent:]
                    self.__sendings[fo] = data
                else:
                    raise Exception("Send Error")
            else:
                sent = fo.send(sock.handshake)
                if sent == len(sock.handshake):
                    self.logger.info("Sent Handshaking")
                    sock.connected = True
                    if self.__send_bufs[sock].empty():
                        self.__poller.unregister(fo, Poller.POLLOUT)
                    self.__poller.register(fo, Poller.POLLIN)
                    self.__sendings[fo] = None
                    self.__handshaking.append(fo)
                    #self.__connecting.remove(fo)
                elif sent < len(handshake):
                    sock.handshake = sock.handshake[sent:]
                else:
                    raise Exception("Connection Refused")

        if event & Poller.POLLERR:
            if sock.connected:
                self.logger.info("Already Connected")
            else:
                self.__disconnect(fo)
                self.__disconnected.append(sock)
                self.__timeout = 5000
                self.__retry_at = time.time()

    def startConnect(self, s):
        self.logger.info("Start Connect " + s.endpoint)
        if not self.__send_bufs.has_key(s):
            self.__send_bufs[s] = Queue.Queue()
        if not self.__recv_bufs.has_key(s):
            self.__recv_bufs[s] = Queue.Queue()
        parsed = urlparse(s.endpoint)
        assert parsed[0] == 'tcp'
        if sys.version_info < (2,6):
            netloc = parsed[2].replace('//','')
        else:
            netloc = parsed[1]
        assert netloc.find(':') > 0
        host, port = netloc.split(':')
        if s.sock is not None:
            s.sock.close()
        s.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.sock.setblocking(0)
        ret = s.sock.connect_ex((host, int(port)))
        if ret == 0 or ret == errno.EINPROGRESS or ret == 10035: # errno.WSAEWOULDBLOCK
            self.__cmds.put(("connect", s))
            self.__pipe.write('C')
        else:
            self.logger.info("Connect Fail. ErrorCode " + ret)

        return ret

    def __disconnect(self, fo):
        if fo is None:
            return
        self.logger.info('Processing disconnect')
        sock = self.__sockets[fo]
        sock.sock = None
        sock.connected = False
        del self.__sockets[fo]
        #del self.__send_bufs[fo]
        #del self.__recv_bufs[fo]
        self.__poller.unregister(fo, Poller.POLLIN | Poller.POLLOUT)
        fo.close()
        self.logger.info('Closed Socket')

    def disconnect(self, s):
        if self.__stop:
            raise Exception("IoThread Stopped")
        self.__cmds.put(("disconnect", s))
        self.__pipe.write('X')

    def send(self, s, message, flags):
        if self.__stop:
            raise Exception("IoThread Stopped")
        self.__send_bufs[s].put(self.__build(message, flags))
        self.__cmds.put(("send", s))
        self.__pipe.write('S')

    def recv(self, s, flags):
        if self.__stop:
            raise Exception("IoThread Stopped")
        return self.__recv_bufs[s].get(flags != DONTWAIT)

    def close(self):
        if self.__stop:
            return
        self.__cmds.put(("term", None))
        self.__pipe.write('T')

    def __build(self, msg, flag):
        if len(msg) >= 255:
            flag += 2
            length = struct.pack('>q', len(msg))
        else:
            length = chr(len(msg))

        return chr(flag) + length + msg

    def __parseHandShaking(self, data):
        data_len = len(data)
        if data_len < 10:
            return 0
        first = data[0]

        if first != chr(0xff):
            raise Exception("Not Supported Peer")
        if data[9] != chr(0x7f):
            raise Exception("Not Supported Peer")

        if data_len < 12:
            return 0

        if data[10] != chr(0x01):
            raise Exception("Not Supported Revision")

        return 12

    def __parse(self, data):
        data_len = len(data)
        if data_len < 2:
            return None, false, 0

        flag = ord(data[0])
        more = flag & 0x01

        if flag & 0x10:
            length = struct.unpack('>q', data[1:7])
            pos = 9
        else:
            length = ord(data[1])
            pos = 2

        if data_len < length + pos:
            return None, false, 0

        return data[pos:pos+length], more, (length + pos)


class Poller(object):
    POLLIN = 1
    POLLOUT = 2
    POLLERR = 4

    def __init__(self, logger):
        self.__epoll = None
        self.__poll = None
        self.logger = logger

        if hasattr(select,'epoll'):
            self.__epoll = select.epoll()
        elif hasattr(select, 'poll'):
            self.__poll = select.poll()

        self.__events = {}
        self.__reverse = {}
        self.__test = 0
        pass
    
    def close(self):
        if self.__epoll:
            self.__epoll.close()

    def register(self, f, event):
        if f is None:
            return
        old_event = self.__events.get(f, 0)
        new_event = old_event | event
        self.__events[f] = new_event
        self.__reverse[f.fileno()] = f

        if self.__epoll:
            poll_event = 0
            if new_event & Poller.POLLIN:
                poll_event |= select.EPOLLIN
            if new_event & Poller.POLLOUT:
                poll_event |= select.EPOLLOUT

            #print f.fileno(), poll_event
            if old_event:
                self.__epoll.modify(f, poll_event)
            else:
                self.__epoll.register(f, poll_event)

        elif self.__poll:
            poll_event = 0
            if new_event & Poller.POLLIN:
                poll_event |= select.POLLIN
            if new_event & Poller.POLLOUT:
                poll_event |= select.POLLOUT

            self.__poll.register(f, poll_event)

    def unregister(self, f, event):
        if not self.__events.has_key(f):
            return
        old_event = self.__events[f]
        new_event = old_event &~ event;

        if self.__epoll:
            poll_event = 0
            if new_event & Poller.POLLIN:
                poll_event |= select.EPOLLIN
            if new_event & Poller.POLLOUT:
                poll_event |= select.EPOLLOUT

            if new_event:
                self.__epoll.modify(f, poll_event)
            else:
                self.__epoll.unregister(f)
                del self.__events[f]

        elif self.__poll:
            poll_event = 0
            if new_event & Poller.POLLIN:
                poll_event |= select.POLLIN
            if new_event & Poller.POLLOUT:
                poll_event |= select.POLLOUT

            if new_event:
                self.__poll.register(f, poll_event)
            else:
                self.__poll.unregister(f)
                del self.__events[f]
        else:
            if new_event:
                self.__events[f] = new_event
            else:
                del self.__events[f]

    def poll(self, timeout):
        result = []
        self.__retry = []
        if self.__epoll:
            for fd, event in self.__epoll.poll(timeout / 1000.0):

                result_event = 0
                if event & select.EPOLLIN:
                    result_event |= Poller.POLLIN
                    event = event &~ select.EPOLLIN;
                if event & select.EPOLLOUT:
                    result_event |= Poller.POLLOUT
                    event = event &~ select.EPOLLOUT;
                if event & select.EPOLLERR:
                    result_event |= Poller.POLLERR
                    event = event &~ select.EPOLLERR;
                if event & select.EPOLLHUP:
                    result_event |= Poller.POLLERR
                    event = event &~ select.EPOLLHUP;

                if event > 0:
                    raise Exception("Unhandled Event " + str(event))
                result.append((self.__reverse[fd], result_event))

        elif self.__poll:
            for fd, event in self.__poll.poll(timeout):

                result_event = 0
                if event & select.POLLIN:
                    result_event |= Poller.POLLIN
                    event = event &~ select.POLLIN;
                if event & select.POLLOUT:
                    result_event |= Poller.POLLOUT
                    event = event &~ select.POLLOUT;
                if event & select.POLLERR:
                    result_event |= Poller.POLLERR
                    event = event &~ select.POLLERR;
                if event & select.POLLHUP:
                    result_event |= Poller.POLLERR
                    event = event &~ select.POLLHUP;

                if event > 0:
                    raise Exception("Unhandled Event " + str(event))
                result.append((self.__reverse[fd], result_event))
        else:
            if timeout < 0:
                timeout = 1.0
            else:
                timeout = timeout / 1000.0
            read_target = []
            write_target = []
            pipes = []
            for f, event in self.__events.items():
                if isinstance(f, WinPipe):
                    pipes.append(f)
                    continue
                if event & Poller.POLLIN:
                    read_target.append(f)
                if event & Poller.POLLOUT:
                    write_target.append(f)

            if read_target or write_target:
                r, w, e = select.select(read_target, write_target,[], timeout)
                if not w:
                    e = write_target
            else:
                r, w, e = [],[],[]
            
            result_event = {}
            for f in r:
                result_event.setdefault(f, 0)
                result_event[f] |= Poller.POLLIN
            for f in w:
                result_event.setdefault(f, 0)
                result_event[f] |= Poller.POLLOUT
            for f in e:
                result_event.setdefault(f, 0)
                result_event[f] |= Poller.POLLERR

            for p in pipes:
                if p.peek(0):
                    result_event[p] = Poller.POLLIN
            
            result = result_event.items()
            
        return result

