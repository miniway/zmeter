import os
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
        if ret == 0 or ret == errno.EINPROGRESS:
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

    def fileno(self):
        return self.__r

    def write(self, data):
        try:
            os.write(self.__w, data)
        except OSError:
            pass

    def recv(self):
        return os.read(self.__r, 1)

    def close(self):
        os.close(self.__r)
        os.close(self.__w)

class IoThread(threading.Thread):


    def __init__(self):
        threading.Thread.__init__(self)
        self.__pipe = Pipe()
        self.__sockets = {}
        self.__send_bufs = {}
        self.__recv_bufs = {}
        self.__sendings = {}
        self.__recievings = {}
        self.__handshaking = []
        self.__disconnected = []

        self.__poller = Poller()
        self.__poller.register(self.__pipe, Poller.POLLIN)
        self.__sockets[self.__pipe] = self
        self.__cmds = Queue.Queue()

        self.__stop = False
        self.loadLogger("./", logging.INFO)

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
                        if not self.__send_bufs.has_key(arg):
                            self.__send_bufs[arg] = Queue.Queue()
                        if not self.__recv_bufs.has_key(arg):
                            self.__recv_bufs[arg] = Queue.Queue()
                        self.__poller.register(arg.sock, Poller.POLLOUT)
                        #self.__connecting.append(arg.sock)
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
                if e.args[0] == errno.ECONNRESET:
                    data = None
                else:
                    raise

            if not data:
                self.logger.info("Disconnected")
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
                        
                sent = fo.send(data)
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
                raise Exception("Poll Error")
            else:
                self.__disconnect(fo)
                self.__disconnected.append(sock)
                self.__timeout = 5000
                self.__retry_at = time.time()

    def startConnect(self, s):
        parsed = urlparse(s.endpoint)
        assert parsed.scheme == 'tcp'
        assert parsed.netloc.find(':') > 0
        host, port = parsed.netloc.split(':')
        if s.sock is not None:
            s.sock.close()
        s.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.sock.setblocking(0)
        ret = s.sock.connect_ex((host, int(port)))
        if ret == 0 or ret == errno.EINPROGRESS:
            self.__cmds.put(("connect", s))
            self.__pipe.write('C')

        return ret

    def __disconnect(self, fo):
        if fo is None:
            return
        self.logger.debug('Disconnect')
        sock = self.__sockets[fo]
        sock.sock = None
        sock.connected = False
        del self.__sockets[fo]
        #del self.__send_bufs[fo]
        #del self.__recv_bufs[fo]
        self.__poller.unregister(fo, Poller.POLLIN | Poller.POLLOUT)
        fo.close()

    def disconnect(self, s):
        self.__cmds.put(("disconnect", s))
        self.__pipe.write('X')

    def send(self, s, message, flags):
        self.__send_bufs[s].put(self.__build(message, flags))
        self.__cmds.put(("send", s))
        self.__pipe.write('S')

    def recv(self, s, flags):
        return self.__recv_bufs[s].get(flags != DONTWAIT)

    def close(self):
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

    def __init__(self):
        self.__epoll = select.epoll()
        self.__events = {}
        self.__reverse = {}
        pass

    def close(self):
        self.__epoll.close()

    def register(self, f, event):
        if f is None:
            return
        old_event = self.__events.get(f, 0)
        new_event = old_event | event
        self.__events[f] = new_event
        self.__reverse[f.fileno()] = f

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

    def unregister(self, f, event):
        if not self.__events.has_key(f):
            return
        old_event = self.__events[f]
        new_event = old_event &~ event;

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

    def poll(self, timeout):
        result = []
        for fd, event in self.__epoll.poll(timeout / 1000):

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

        return result
