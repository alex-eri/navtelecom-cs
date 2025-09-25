import asyncio
from . import server
from . import db
import sys, os

async def main():
    loop = asyncio.get_running_loop()

    queue = asyncio.Queue()
    writer = db.Writer(queue=queue,db=os.environ.get('DATABADE'))
    await writer.start()
    
    srv = await loop.create_server(
        lambda:server.NavCSServerProtocol(queue=queue),
        '0.0.0.0', int(os.environ.get('GNSS_PORT',4000)))

    async with srv:
        await srv.serve_forever()

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())