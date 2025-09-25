import asyncio
from . import protocol
import logging
logger = logging.Logger('server')


class NavCSServerProtocol(asyncio.Protocol):
    
    def __init__(self,queue,*a,**kw):
        self.queue = queue
        super().__init__(*a,**kw)
    
    def connection_made(self, transport):
        self.protocol = protocol.Transport(transport, self.queue)
        return super().connection_made(transport)
    
    def connection_lost(self, exc):
        logging.info('connection_lost %s', self.protocol.app.imei)
        logging.debug(exc)
        return super().connection_lost(exc)
    
    def data_received(self, data):
        self.protocol.feed(data)
        