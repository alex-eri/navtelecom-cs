import asyncio
from . import protocol
import logging
logger = logging.getLogger('server')
import time

class NavCSServerProtocol(asyncio.Protocol):
    
    ping_task : asyncio.Task
    
    def __init__(self,queue,*a,**kw):
        self.queue = queue
        super().__init__(*a,**kw)
        
    
    def connection_made(self, transport):
        logger.debug('cm %f', time.time())
        self.protocol = protocol.Transport(transport, self.queue)
        self.ping_task = asyncio.create_task(self.protocol.ping())
        return super().connection_made(transport)
    
    def connection_lost(self, exc):
        logger.debug('cl %f', time.time())
        logger.info('connection_lost %s', self.protocol.app.imei)
        logger.debug(exc)
        self.ping_task.cancel()
        return super().connection_lost(exc)
    
    def data_received(self, data):
        t = time.time()
        logger.debug('dr %f', t)
        self.protocol.feed(data)
        logger.debug(time.time()-t)
        