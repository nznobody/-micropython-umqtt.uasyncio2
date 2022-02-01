import uasyncio as asyncio
from uasyncio.stream import Stream
import usocket as socket
import ussl as ssl
import usocket as socket
from utime import ticks_add, ticks_ms, ticks_diff
import logging
log = logging.getLogger("umqtt.uasyncio2")
log.setLevel(logging.WARNING)

class MQTTException(Exception):
    pass


def pid_gen(pid=0):
    while True:
        pid = pid + 1 if pid < 65535 else 1
        yield pid


class MQTTClient:

    def __init__(self, client_id, host, port=0, user=None, password=None, keepalive=0,
                 ssl=False, ssl_params=None, socket_timeout=10, message_timeout=30):
        """
        Default constructor, initializes MQTTClient object.

        :param client_id:  Unique MQTT ID attached to client.
        :type client_id: str
        :param host: MQTT host address.
        :type host str
        :param port: MQTT Port, typically 1883. If unset, the port number will default to 1883 of 8883 base on ssl.
        :type port: int
        :param user: Username if your server requires it.
        :type user: str
        :param password: Password if your server requires it.
        :type password: str
        :param keepalive: The Keep Alive is a time interval measured in seconds since the last
                          correct control packet was received.
        :type keepalive: int
        :param ssl: Require SSL for the connection.
        :type ssl: bool
        :param ssl_params: Required SSL parameters.
        :type ssl_params: dict
        :param socket_timeout: The time in seconds after which the socket interrupts the connection to the server when
                               no data exchange takes place. None - wait forever, positive number - seconds to wait.
        :type socket_timeout: int
        :param message_timeout: The time in seconds after which the library recognizes that a message with QoS=1
                                or topic subscription has not been received by the server.
        :type message_timeout: int
        """
        if port == 0:
            port = 8883 if ssl else 1883
        self.client_id = client_id
        self.sock = None
        self.host = host
        self.port = port
        self.ssl = ssl
        self.ssl_params = ssl_params if ssl_params else {}
        self.newpid = pid_gen()
        if not getattr(self, 'cb', None):
            self.cb = None
        if not getattr(self, 'cbstat', None):
            self.cbstat = lambda p, s: None
        self.user = user
        self.pswd = password
        self.keepalive = keepalive
        self.lw_topic = None
        self.lw_msg = None
        self.lw_qos = 0
        self.lw_retain = False
        self.rcv_pids = {}  # PUBACK and SUBACK pids awaiting ACK response

        self.last_ping = ticks_ms()  # Time of the last PING sent
        self.last_cpacket = ticks_ms()  # Time of last Control Packet

        self.async_stream = None

        self.socket_timeout_s = socket_timeout
        self.message_timeout_s = message_timeout

    async def _read(self, n):
        """
        Private class method.

        :param n: Expected length of read bytes
        :type n: int
        :return:
        """
        # log.debug("Starting async read for {} bytes".format(n))
        try:
            if self.ssl:
                # some weird things happen with ssl... there is a bug that even if bytes are available to be read, ioctl / poll won't trigger if it has previously
                # triggered for these bytes. E.g. a previous read that only ready part of the packet. Even though some remain, they won't trigger an ioctl in ssl mode :(
                # This needs refactoring...
                try:
                    # socket should be set to 0.001s timeout so this should not block for long
                    msg = self.sock.read(n)
                    log.info("ssl pre-reading success read: {}".format(len(msg)))
                    return msg
                except:
                    pass
            msg = await asyncio.wait_for_ms(self.async_stream.read(n), self.socket_timeout_s * 1000)
            log.debug("async read got: {}".format(msg))
        except asyncio.TimeoutError as exc:
            log.debug("_read: timeout")
            raise exc
        except AttributeError:
            log.error("_read_async: 8")
            raise MQTTException(8)
        if msg == b'':  # Connection closed by host (?)
            log.error("_read_async: Connection closed by host")
            raise MQTTException(1)
        if len(msg) != n:
            log.error("_read_async: 2")
            raise MQTTException(2)
        return msg

    async def _write(self, bytes_wr, length=-1):
        """Streams do not have a concept of amount written, so ignore that branch of code"""
        log.debug("Starting async write for {} bytes".format(len(bytes_wr)))
        # Truncate data if too much is given
        if length != -1 and length != len(bytes_wr):
            log.debug("truncating write from {} to {}".format(bytes_wr, bytes_wr[:length]))
            bytes_wr = bytes_wr[:length]
        # Todo: handle expections
        self.async_stream.write(bytes_wr)
        await self.async_stream.drain()
        log.debug("async write for '{}' bytes completed".format(bytes_wr))
        return len(bytes_wr)

    async def _send_str(self, s):
        """
        Private class method.
        :param s:
        :type s: byte
        :return: None
        """
        assert len(s) < 65536
        await self._write(len(s).to_bytes(2, 'big'))
        await self._write(s)

    async def _recv_len(self):
        """
        Private class method.
        :return:
        :rtype int
        """
        n = 0
        sh = 0
        while 1:
            b = (await self._read(1))[0]
            n |= (b & 0x7f) << sh
            if not b & 0x80:
                return n
            sh += 7

    def _varlen_encode(self, value, buf, offset=0):
        assert value < 268435456  # 2**28, i.e. max. four 7-bit bytes
        while value > 0x7f:
            buf[offset] = (value & 0x7f) | 0x80
            value >>= 7
            offset += 1
        buf[offset] = value
        return offset + 1

    def set_callback(self, f):
        """
        Set callback for received subscription messages.

        :param f: callable(topic, msg, retained, duplicate)
        """
        self.cb = f

    def set_callback_status(self, f):
        """
        Set the callback for information about whether the sent packet (QoS=1)
        or subscription was received or not by the server.

        :param f: callable(pid, status)

        Where:
            status = 0 - timeout
            status = 1 - successfully delivered
            status = 2 - Unknown PID. It is also possible that the PID is outdated,
                         i.e. it came out of the message timeout.
        """
        self.cbstat = f

    def set_last_will(self, topic, msg, retain=False, qos=0):
        """
        Sets the last will and testament of the client. This is used to perform an action by the broker
        in the event that the client "dies".
        Learn more at https://www.hivemq.com/blog/mqtt-essentials-part-9-last-will-and-testament/

        :param topic: Topic of LWT. Takes the from "path/to/topic"
        :type topic: byte
        :param msg: Message to be published to LWT topic.
        :type msg: byte
        :param retain: Have the MQTT broker retain the message.
        :type retain: bool
        :param qos: Sets quality of service level. Accepts values 0 to 2. PLEASE NOTE qos=2 is not actually supported.
        :type qos: int
        :return: None
        """
        assert 0 <= qos <= 2
        assert topic
        self.lw_topic = topic
        self.lw_msg = msg
        self.lw_qos = qos
        self.lw_retain = retain


    def _connect_socket(self):
        """
        Creates and established the socket only
        """
        self.sock = socket.socket()
        addr = socket.getaddrinfo(self.host, self.port)[0][-1]
        if self.ssl:
            self.sock = ssl.wrap_socket(self.sock, **self.ssl_params)

        # We use blocking sockets since we are using an ASYNC interface anyway
        self.sock.settimeout(0)
        try:
            self.sock.connect(addr)
        except OSError as e:
            if '119' in str(e): # For non-Blocking sockets 119 is EINPROGRESS
                # print("In Progress")
                pass
            else:
                raise e

        if self.ssl:
            log.debug("Doing SSL Handshake")
            self.sock.do_handshake()
            log.debug("SSL Handshake Done")
            # Attempt to fix weird SSL + socket + poll issues
            self.sock.settimeout(0.001)

        # Setup asynchronous primitives
        self.async_stream = Stream(self.sock)

    async def _perform_mqtt_connect(self, clean_session):
        # Byte nr - desc
        # 1 - \x10 0001 - Connect Command, 0000 - Reserved
        # 2 - Remaining Length
        # PROTOCOL NAME (3.1.2.1 Protocol Name)
        # 3,4 - protocol name length len('MQTT')
        # 5-8 = 'MQTT'
        # PROTOCOL LEVEL (3.1.2.2 Protocol Level)
        # 9 - mqtt version 0x04
        # CONNECT FLAGS
        # 10 - connection flags
        #  X... .... = User Name Flag
        #  .X.. .... = Password Flag
        #  ..X. .... = Will Retain
        #  ...X X... = QoS Level
        #  .... .X.. = Will Flag
        #  .... ..X. = Clean Session Flag
        #  .... ...0 = (Reserved) It must be 0!
        # KEEP ALIVE
        # 11,12 - keepalive
        # 13,14 - client ID length
        # 15-15+len(client_id) - byte(client_id)
        premsg = bytearray(b"\x10\0\0\0\0\0")
        msg = bytearray(b"\0\x04MQTT\x04\0\0\0")

        sz = 10 + 2 + len(self.client_id)

        msg[7] = bool(clean_session) << 1
        # Clean session = True, remove current session
        if bool(clean_session):
            self.rcv_pids.clear()
        if self.user is not None:
            sz += 2 + len(self.user)
            msg[7] |= 1 << 7  # User Name Flag
            if self.pswd is not None:
                sz += 2 + len(self.pswd)
                msg[7] |= 1 << 6  # # Password Flag
        if self.keepalive:
            assert self.keepalive < 65536
            msg[8] |= self.keepalive >> 8
            msg[9] |= self.keepalive & 0x00FF
        if self.lw_topic:
            sz += 2 + len(self.lw_topic) + 2 + len(self.lw_msg)
            msg[7] |= 0x4 | (self.lw_qos & 0x1) << 3 | (self.lw_qos & 0x2) << 3
            msg[7] |= self.lw_retain << 5

        plen = self._varlen_encode(sz, premsg, 1)
        await self._write(premsg, plen)
        await self._write(msg)
        await self._send_str(self.client_id)
        if self.lw_topic:
            await self._send_str(self.lw_topic)
            await self._send_str(self.lw_msg)
        if self.user is not None:
            await self._send_str(self.user)
            if self.pswd is not None:
                await self._send_str(self.pswd)
        log.debug("_perform_mqtt_connect: Sent connection strings")
        resp = await self._read(4)
        log.debug("_perform_mqtt_connect: Got response, {}".format(resp))
        if not (resp[0] == 0x20 and resp[1] == 0x02):  # control packet type, Remaining Length == 2
            raise MQTTException(29)
        if resp[3] != 0:
            log.error("_perform_mqtt_connect: got connection response error: {}".format(resp[3]))
            if 1 <= resp[3] <= 5:
                raise MQTTException(20 + resp[3])
            else:
                raise MQTTException(20, resp[3])
        self.last_cpacket = ticks_ms()
        return resp[2] & 1  # Is existing persistent session of the client from previous interactions.

    async def connect(self, clean_session=True):
        """
        Establishes connection with the MQTT server.

        :param clean_session: Starts new session on true, resumes past session if false.
        :type clean_session: bool
        :return: Existing persistent session of the client from previous interactions.
        :rtype: bool
        """
        self._connect_socket()
        resp = await self._perform_mqtt_connect(clean_session)
        log.info("MQTT client connected, restored session: {}".format(bool(resp)))
        return resp


    async def disconnect(self):
        """
        Disconnects from the MQTT server.
        :return: None
        """
        await self._write(b"\xe0\0")
        # self.async_stream.wait_closed()  # This just calls close on the socket...
        self.sock.close()
        self.async_stream = None
        self.sock = None

    async def ping(self):
        """
        Pings the MQTT server.
        :return: None
        """
        await self._write(b"\xc0\0")
        self.last_ping = ticks_ms()

    async def publish(self, topic, msg, retain=False, qos=0, dup=False):
        """
        Publishes a message to a specified topic.

        :param topic: Topic you wish to publish to. Takes the form "path/to/topic"
        :type topic: byte
        :param msg: Message to publish to topic.
        :type msg: byte
        :param retain: Have the MQTT broker retain the message.
        :type retain: bool
        :param qos: Sets quality of service level. Accepts values 0 to 2. PLEASE NOTE qos=2 is not actually supported.
        :type qos: int
        :param dup: Duplicate delivery of a PUBLISH Control Packet
        :type dup: bool
        :return: None
        """
        assert qos in (0, 1)
        log.debug("publish: publishing {}, qos: {}".format(topic, qos))
        pkt = bytearray(b"\x30\0\0\0\0")
        pkt[0] |= qos << 1 | retain | int(dup) << 3
        sz = 2 + len(topic) + len(msg)
        if qos > 0:
            sz += 2
        plen = self._varlen_encode(sz, pkt, 1)
        await self._write(pkt, plen)
        await self._send_str(topic)
        if qos > 0:
            pid = next(self.newpid)
            await self._write(pid.to_bytes(2, 'big'))
        else:
            pid = None
        await self._write(msg)
        log.debug("publish: published {}, qos: {}".format(topic, qos))
        if pid:
            self.rcv_pids[pid] = ticks_add(ticks_ms(), self.message_timeout_s * 1000)
            return pid

    async def subscribe(self, topic, qos=0):
        """
        Subscribes to a given topic.

        :param topic: Topic you wish to publish to. Takes the form "path/to/topic"
        :type topic: byte
        :param qos: Sets quality of service level. Accepts values 0 to 1. This gives the maximum QoS level at which
                    the Server can send Application Messages to the Client.
        :type qos: int
        :return: None
        """
        assert qos in (0, 1)
        assert self.cb is not None, "Subscribe callback is not set"
        log.debug("subscribe: subscribing {}, qos: {}".format(topic, qos))
        pkt = bytearray(b"\x82\0\0\0\0\0\0")
        pid = next(self.newpid)
        sz = 2 + 2 + len(topic) + 1
        plen = self._varlen_encode(sz, pkt, 1)
        pkt[plen:plen + 2] = pid.to_bytes(2, 'big')
        await self._write(pkt, plen + 2)
        await self._send_str(topic)
        await self._write(qos.to_bytes(1, "little"))  # maximum QOS value that can be given by the server to the client
        self.rcv_pids[pid] = ticks_add(ticks_ms(), self.message_timeout_s * 1000)
        log.debug("subscribe: subscribed {}, qos: {}".format(topic, qos))
        return pid

    def _message_timeout(self):
        curr_tick = ticks_ms()
        for pid, timeout in self.rcv_pids.items():
            if ticks_diff(timeout, curr_tick) <= 0:
                self.rcv_pids.pop(pid)
                self.cbstat(pid, 0)

    async def _process_check_msg(self, response):
        """
        Processes such messages:
        - response to PING
        - messages from subscribed topics that are processed by functions set by the set_callback method.
        - reply from the server that he received a QoS=1 message or subscribed to a topic
        """
        if response == b"\xd0":  # PINGRESP
            log.debug("_process_check_msg: PINGRESP")
            if (await self._read(1))[0] != 0:
                MQTTException(-1)
            self.last_cpacket = ticks_ms()
            return

        op = response[0]

        if op == 0x40:  # PUBACK
            log.debug("_process_check_msg: PUBACK")
            sz = await self._read(1)
            if sz != b"\x02":
                raise MQTTException(-1)
            rcv_pid = int.from_bytes(await self._read(2), 'big')
            if rcv_pid in self.rcv_pids:
                self.last_cpacket = ticks_ms()
                self.rcv_pids.pop(rcv_pid)
                self.cbstat(rcv_pid, 1)
            else:
                self.cbstat(rcv_pid, 2)

        if op == 0x90:  # SUBACK Packet fixed header
            log.debug("_process_check_msg: SUBACK")
            resp = await self._read(4)
            # Byte - desc
            # 1 - Remaining Length 2(varible header) + len(payload)=1
            # 2,3 - PID
            # 4 - Payload
            if resp[0] != 0x03:
                raise MQTTException(40, resp)
            if resp[3] == 0x80:
                raise MQTTException(44)
            if resp[3] not in (0, 1, 2):
                raise MQTTException(40, resp)
            pid = resp[2] | (resp[1] << 8)
            if pid in self.rcv_pids:
                self.last_cpacket = ticks_ms()
                self.rcv_pids.pop(pid)
                self.cbstat(pid, 1)
            else:
                raise MQTTException(5)

        self._message_timeout()

        if op & 0xf0 != 0x30:  # 3.3 PUBLISH – Publish message
            return op
        sz = await self._recv_len()
        log.debug("_process_check_msg: PUBLISH, sz: {}".format(sz))
        topic_len = int.from_bytes(await self._read(2), 'big')
        topic = await self._read(topic_len)
        sz -= topic_len + 2
        if op & 6:  # QoS level > 0
            pid = int.from_bytes(await self._read(2), 'big')
            sz -= 2
        msg = (await self._read(sz)) if sz else b''
        retained = op & 0x01
        dup = op & 0x08
        assert self.cb, "cb not set"
        self.cb(topic, msg, bool(retained), bool(dup))
        self.last_cpacket = ticks_ms()
        if op & 6 == 2:  # QoS==1
            await self._write(b"\x40\x02")  # Send PUBACK
            await self._write(pid.to_bytes(2, 'big'))
        elif op & 6 == 4:  # QoS==2
            raise NotImplementedError()
        elif op & 6 == 6:  # 3.3.1.2 QoS - Reserved – must not be used
            raise MQTTException(-1)

    async def check_msg(self):
        """
        Checks whether a pending message from server is available.

        Will return after the time set in the socket_timeout.
        """
        if self.sock:
            try:
                log.debug("check_msg: waiting first byte")
                res = await self._read(1)
            except asyncio.TimeoutError as _:
                log.debug("check_msg: timed out")
                return None
        else:
            raise MQTTException(28)
        log.debug("check_msg: {}".format(res))
        await self._process_check_msg(res)

    async def wait_msg(self):
        """
        This method waits for a message from the server.

        Compatibility with previous versions.

        It is recommended not to use this method.
        """
        msg = None
        while not msg:
            log.debug("wait_msg waiting...")
            msg = await self.check_msg()
        return msg
