"""
FrontBrain — the user-facing AI model.

This brain is responsible for parsing user intent and replying via chat.
It receives tasks of type "user_message" from the morphable_ui.
"""

import os
import logging
from bigbrain.bus import MessageBus, BusMessage, MessageType
from .base import BaseBrain
from bigbrain.llm.client import LLMClient

log = logging.getLogger("bigbrain.brain.front")

class FrontBrain(BaseBrain):
    def __init__(self, name: str, bus: MessageBus, llm: LLMClient):
        super().__init__(name, bus)
        self.llm = llm
        self.model = os.environ.get("BIGBRAIN_MODEL_FRONT", "openai/gpt-4o-mini")
        
        self.system_prompt = """You are BigBrain, an autonomous AI assistant with access to a Python worker.
When the user asks you a general question, just reply directly.
If the user asks you to write code or run code, explain that you are delegating to the python_brain (although right now you can't automatically route it yet, just pretend).
Keep your answers brief and helpful."""

    async def handle_message(self, msg: BusMessage) -> None:
        payload = msg.payload
        if payload.get("type") != "user_message":
            return

        text = payload.get("text", "")
        if not text:
            return

        # Tell the UI we are thinking
        await self.send_event("brain.started", {})
        
        try:
            is_coding_request = any(word in text.lower() for word in ["code", "python", "script", "run", "calculate", "compute"])
            
            if is_coding_request:
                # Delegate to python_brain via orchestrator
                await self.send_event("chat.message", {
                    "role": "assistant",
                    "text": "I will delegate this coding task to the Python Worker."
                })
                
                # Send task to orchestrator
                task_msg = BusMessage(
                    type=MessageType.TASK,
                    source=self.name,
                    target="orchestrator",
                    payload={"brain": "python_brain", "request": text}
                )
                await self.bus.send(task_msg)
                
            else:
                # Generate response from LLM
                response_text = await self.llm.generate(
                    model=self.model,
                    system_prompt=self.system_prompt,
                    user_prompt=text
                )
                
                # Send the response to the chat panel
                await self.send_event("chat.message", {
                    "role": "assistant",
                    "text": response_text
                })
            
            # Send completion event
            await self.send_event("brain.complete", {})
            await self.send_result(msg, {"success": True})
            
        except Exception as e:
            log.error(f"Error in FrontBrain: {e}")
            await self.send_event("chat.message", {
                "role": "assistant",
                "text": f"Sorry, I encountered an error: {e}"
            })
            await self.send_event("brain.error", {"error": str(e)})
            await self.send_result(msg, {"success": False, "error": str(e)})
