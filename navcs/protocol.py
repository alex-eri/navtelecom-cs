import struct
import operator
import functools
import asyncio
from . import constants

class Transport:
    data: bytes

    def __init__(self, tcp: asyncio.transports.Transport):
        self.data = bytes()
        self.app = App(self)
        self.tcp = tcp

    def send(self, data):
        self.tcp.write(data)

    def feed(self, data):
        self.data += data

        match self.data[0]:
            case 64:  # @
                self.data = self.app.on_ntc(self.data)
            case 126:  # ~
                self.data = self.app.on_flex(self.data)
            case 0x7F:  # DEL
                self.data = self.data[1:]

        # i = self.data.find(b'@NTC')
        # if i>0:
        #     self.data = self.data[i:]
        # elif i<0:
        #     self.data = self.data[-2:]
        #     return

        # head = self.data[4:16]
        # idr,ids,n,csd,csp = struct.unpack_from('<LLHBB',  head, 4)

        # if functools.reduce(operator.xor,head,0) != 0:
        #     self.data = self.data[4:]
        #     return self.feed(b'')

        # if n > len(data)-16:
        #     return

        # if functools.reduce(operator.xor,self.data[16:],csd) != 0:
        #     self.data = self.data[4:]
        #     return self.feed(b'')




class App:
    def __init__(self, transport: Transport):
        self.transport = transport

    def on_flex(self, data: bytes):
        # """

        # 7e540700000007000000501208c19d680000631308c19d6830c45501f0a8fb010000000000000000000000000000000000000049
        # NumPage = 7; Code = 4688; Time = 1755169032; State = 0; Module1 = 0; GSM = 99; StateNGauge = 19; LastTime = 1755169032; Latitude = 22398000; Longitude = 33270000; Speed = 0; Course = 0; Run = 0; Power = 0; Reserv = 0; StateIn1 = 0; Motochas = 0;

        # 7e41010400000050128cc09d68000063138cc09d6830c45501f0a8fb0100000000000000000000000000000000000000bd

        # numPage = 4  Code = 4688  Time = 1755168908  State = 0  Module1 = 0  GSM = 99  StateGauge = 19  LastTime = 1755168908  Lat = 22398000  Lon = 33270000  Speed = 0  Course = 0  Mileage = 0  Power = 0  Reserv = 0  StateIn1 = 0  Motochas = 0

        # """

        cursor = data[:]

        match data[1]:
            case 0x41 as evtype:  # A архив
                count = data[2]
                cursor = data[3:]
                while count := count - 1:
                    cursor = self.on_message(cursor, evtype)
            case 0x45 as evtype:  # E доп архив
                count = data[2]
                cursor = data[3:]
                while count := count - 1:
                    cursor = self.on_ext_message(cursor, evtype)
            case 0x54 as evtype:  # T эвент
                index = data[2]
                self.on_message(data[3:], evtype)
            case 0x58 as evtype:  # X доп эвэнт
                index = data[2]
                self.on_ext_message(data[3:], evtype)
            case 0x43 as evtype:  # C текущая
                self.on_message(data[2:], evtype)
        
        return b''



    def on_message(self, message: bytes, evtype):
        cursor = message[:]

        for byte_n, bits in enumerate(self.bitfield):
            for i in range(8):
                if bool(bits & (1<<i)):
                    key = byte_n+i
                    self.on_value(key, cursor[: constants.FLEX_SIZES[key] ])
                

    def on_value(self, key, value):
        pass

    def on_ext_message(self, message: bytes, evtype):
        pass


    def on_ntc(self, data: bytes):
        if data[0:4] != b"@NTC":
            raise Exception("bad packet")
        head = data[0:16]
        idr, ids, n, csd, csp = struct.unpack_from("<LLHBB", head, 4)
        if functools.reduce(operator.xor, head, 0) != 0:
            raise Exception("bad csp")
        if n > len(data) - 16:
            raise Exception("short data")
        if functools.reduce(operator.xor, data[16 : 16 + n], csd) != 0:
            raise Exception("bad csd")
        self.on_ntc_data(data[16 : 16 + n], idr, ids)
        return data[16 + n :]

    def on_ntc_data(self, body: bytes, idr, ids):
        
        if body[:4] == b"*>S:":
            self.on_imei(body[4:])
            self.send_ntc(b"*<S", idr, ids)

        elif body[:6] == b"*>FLEX":
            prot, pver, struct_ver, data_size = struct.unpack_from("<BBBB", body, 6)
            bitfield = body[10 : 10 + (data_size // 8) + 1]
            prot, pver, struct_ver = self.on_flex_desc(prot, pver, struct_ver, bitfield)
            resp = b"*<FLEX" + struct.pack("<BBB", prot, pver, struct_ver)
            self.send_ntc(resp, idr, ids)

    def on_flex_desc(self, prot, pver, struct_ver, bitfield):
        self.version = prot, pver, struct_ver
        self.bitfield = bitfield
        return prot, pver, struct_ver

    def send_ntc(self, body, idr, ids):
        csd = functools.reduce(operator.xor, body, 0)
        data = b"@NTC" + struct.pack("<LLHB", ids, idr, len(body), csd)
        data += bytes([functools.reduce(operator.xor, data, 0)])
        data += body
        self.transport.send(data)

    def on_imei(self, imei: bytes):
        self.imei = imei.decode()
