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

import asyncio
import logging

from bigbrain.bus import MessageBus, BusMessage, MessageType
from .base import BaseBrain

log = logging.getLogger("bigbrain.brain.python")


class PythonBrain(BaseBrain):
    """
    Brain that executes Python code in an isolated subprocess.

    Usage:
        brain = PythonBrain("python_brain", bus, timeout=10)
        await brain.start()

        # Send it a task:
        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="python_brain",
            payload={"code": "print('hello world')"},
        ))
        # Brain will send back a RESULT with stdout/stderr/returncode
    """

    def __init__(self, name: str, bus: MessageBus, timeout: int = 10):
        # TODO:
        # 1. Call the parent __init__ (super().__init__) with name and bus
        # 2. Store the timeout value as self.timeout
        pass

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
        # TODO: Implement
        pass

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
        # TODO: Implement
        pass
