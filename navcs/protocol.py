import struct
import operator
import functools
import asyncio

class Transport():
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
            case 64: # @
                self.data = self.app.on_ntc(self.data)
            case 126: # ~
                self.data = self.app.on_flex(self.data)
            case 0x7f: # DEL
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
        
        





class App():
    def __init__(self, transport: Transport):
        self.transport = transport
    
    def on_ntc(self, data):
        if data[0:4] != b'@NTC':
            raise Exception('bad packet')
        head = data[0:16]
        idr,ids,n,csd,csp = struct.unpack_from('<LLHBB',  head, 4)
        if functools.reduce(operator.xor,head,0) != 0:
            raise Exception('bad csp')
        if n > len(data)-16:
            raise Exception('short data')
        if functools.reduce(operator.xor,data[16:16+n],csd) != 0:
            raise Exception('bad csd')
        self.on_ntc_data(data[16:16+n], idr,ids)
        return data[16+n:]
    
    def on_ntc_data(self, body, idr,ids):
        if body[:3] == b'*>S:':
            self.on_imei(body[4:])
            self.send_ntc(b'*<S',  idr,ids)

        elif body[:6] == b'*>FLEX':
            prot, pver, struct_ver, data_size = struct.unpack_from( '<BBBB', body, 6 )
            bitfield=body[10:10+(data_size//8)]
            self.on_flex_desc(prot, pver, struct_ver,bitfield)
            

    def on_flex_desc(self, prot, pver, struct_ver,bitfield):
        self.version = prot, pver, struct_ver
        self.bitfield = bitfield

    def send_ntc(self, body, idr,ids):
        csd = functools.reduce(operator.xor,body,0)
        data = b'@NTC' + struct.pack('<LLHB', ids, idr, len(body), csd)
        data += bytes([functools.reduce(operator.xor,data,0)]) 
        data += body
        self.transport.send(data)
        

    def on_imei(self,imei:bytes):
        self.imei=imei.decode()

