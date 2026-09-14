"""
PythonBrain — executes Python code in a subprocess.

This is the simplest useful brain: receive code, run it, return output.
Later versions will add LLM code generation, verification loops (Honk pattern),
pytest/mypy integration, and reflexion memory.

v0.1 scope:
  - Receives a TASK with payload {"code": "print('hello')"}
  - Runs the code in a subprocess with a timeout
  - Returns RESULT with {"stdout": "...", "stderr": "...", "returncode": 0}
  - Sends brain.started and brain.complete events
  - If the code times out or crashes, returns the error cleanly

Security:
  - subprocess with timeout (default 10s) — no infinite loops
  - No shell=True — no shell injection
  - Future: bubblewrap sandbox for real isolation

You implement:
  - __init__(): call super().__init__(), store timeout setting
  - handle_message(): the main logic — extract code, run it, send result
  - _run_code(): helper that runs Python code in a subprocess, returns (stdout, stderr, returncode)
"""

from __future__ import annotations

import sys

import asyncio
import logging

from bigbrain.bus import MessageBus, BusMessage, MessageType
from .base import BaseBrain
from bigbrain.llm.client import LLMClient
import os

log = logging.getLogger("bigbrain.brain.python")


class PythonBrain(BaseBrain):
    def __init__(self, name: str, bus: MessageBus, llm: LLMClient, timeout: int = 10):
        super().__init__(name, bus)
        self.timeout = timeout
        self.llm = llm
        self.model = os.environ.get("BIGBRAIN_MODEL_WORKER", "anthropic/claude-3.5-sonnet")

    async def handle_message(self, msg: BusMessage) -> None:
        """
        Process a TASK message containing Python code.

        Steps:
        1. Extract the code from msg.payload["code"]
           - If "code" key is missing, send a RESULT with an error and return
        2. Send a "brain.started" event (use self.send_event)
        3. Call self._run_code(code) to execute it
        4. Send a "brain.complete" event
        5. Send the result back with self.send_result()
           - payload: {"stdout": ..., "stderr": ..., "returncode": ...}
        """
        request = msg.payload.get("request")
        code = msg.payload.get("code")
        
        if not code and not request:
           await self.send_result(msg, {"stdout": "", "stderr": "No code or request provided", "returncode": 1})
           return
           
        await self.send_event("brain.started", {})
        
        if not code and request:
            # Tell UI we are generating code
            await self.send_event("progress", {"stage": "generating_code", "progress": 25})
            try:
                system_prompt = "You are an expert Python coder. Write python code to fulfill the user's request. Output ONLY valid python code, no markdown formatting or backticks."
                code = await self.llm.generate(model=self.model, system_prompt=system_prompt, user_prompt=request)
                
                # Send the generated code back to UI chat
                await self.send_event("chat.message", {
                    "role": "assistant",
                    "text": f"Generated code:\n```python\n{code}\n```"
                })
            except Exception as e:
                log.error(f"Error generating code: {e}")
                await self.send_result(msg, {"stdout": "", "stderr": f"Error generating code: {e}", "returncode": 1})
                return

        await self.send_event("progress", {"stage": "executing", "progress": 60})
        stdout, stderr, returncode = await self._run_code(code)
        
        # Send results back to UI chat
        if returncode == 0:
            result_msg = f"Output:\n```\n{stdout}\n```"
        else:
            result_msg = f"Error:\n```\n{stderr}\n```"
        
        await self.send_event("chat.message", {
            "role": "assistant",
            "text": result_msg
        })
        
        await self.send_event("brain.complete", {})
        await self.send_result(msg, {"stdout": stdout, "stderr": stderr, "returncode": returncode, "generated_code": code})

    async def _run_code(self, code: str) -> tuple[str, str, int]:
        """
        Run Python code in a subprocess and return (stdout, stderr, returncode).

        Steps:
        1. Create a subprocess using asyncio.create_subprocess_exec()
           - Command: "python3", "-c", code
           - Capture stdout and stderr with asyncio.subprocess.PIPE
        2. Wait for it to finish with a timeout using asyncio.wait_for()
           - Use self.timeout as the timeout value
        3. If it times out (asyncio.TimeoutError):
           - Kill the process (process.kill())
           - Return ("", "Timeout: execution exceeded {self.timeout}s", 1)
        4. If it completes normally:
           - Decode stdout and stderr from bytes to str
           - Return (stdout, stderr, process.returncode)
        """
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-c", code,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
           stdout_bytes, stderr_bytes = await asyncio.wait_for(
               process.communicate(), timeout=self.timeout
               )
        except asyncio.TimeoutError:
            process.kill()
            return ("", f"Timeout: execution exceeded {self.timeout}s", 1)
        return (stdout_bytes.decode(), stderr_bytes.decode(), process.returncode)
        

