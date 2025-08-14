import asyncio
import server

async def main():
    loop = asyncio.get_running_loop()

    server = await loop.create_server(
        server.NavCSServerProtocol,
        '0.0.0.0', 8888)

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())