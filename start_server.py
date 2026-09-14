import asyncio
from bigbrain.bus.bus import MessageBus
from bigbrain.web.server import WebServer

async def main():
    bus = MessageBus()
    server = WebServer(bus=bus, host="127.0.0.1", port=8080)
    
    await bus.start()
    await server.start()
    print("Web server running at http://127.0.0.1:8080")
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        await server.stop()
        await bus.stop()

if __name__ == "__main__":
    asyncio.run(main())
