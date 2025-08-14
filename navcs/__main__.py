import asyncio
from . import server

async def main():
    loop = asyncio.get_running_loop()

    srv = await loop.create_server(
        server.NavCSServerProtocol,
        '0.0.0.0', 9001)

    async with srv:
        await srv.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())