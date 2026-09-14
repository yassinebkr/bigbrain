import asyncio
import os
from dotenv import load_dotenv

from bigbrain.bus.bus import MessageBus
from bigbrain.web.server import WebServer
from bigbrain.orchestrator.orchestrator import Orchestrator
from bigbrain.llm.client import LLMClient
from bigbrain.brains.front_brain import FrontBrain
from bigbrain.brains.python_brain import PythonBrain

async def main():
    load_dotenv()
    
    bus = MessageBus()
    server = WebServer(bus=bus, host="127.0.0.1", port=8080)
    
    orchestrator = Orchestrator(bus)
    orchestrator.register_brain("python_brain")
    orchestrator.register_brain("front_brain")
    
    try:
        llm = LLMClient()
    except ValueError as e:
        print(f"Warning: {e}. LLM functionality will not work.")
        llm = None

    front_brain = FrontBrain("front_brain", bus, llm) if llm else None
    python_brain = PythonBrain("python_brain", bus, llm) if llm else None
    
    await bus.start()
    await orchestrator.start()
    if front_brain: await front_brain.start()
    if python_brain: await python_brain.start()
    await server.start()
    
    print("Web server running at http://127.0.0.1:8080")
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        await server.stop()
        if python_brain: await python_brain.stop()
        if front_brain: await front_brain.stop()
        await orchestrator.stop()
        await bus.stop()

if __name__ == "__main__":
    asyncio.run(main())
