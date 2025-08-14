import asyncio
import protocol

class NavCSServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        self.protocol = protocol.Transport(transport)
        return super().connection_made(transport)
    
    def connection_lost(self, exc):
        return super().connection_lost(exc)
    
    def data_received(self, data):
        self.protocol.feed(data)
        