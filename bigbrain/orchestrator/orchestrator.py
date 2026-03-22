"""
Orchestrator — routes tasks to brains and collects results.

v0.1 scope (simple router):
  - Receives tasks from the user/front brain
  - Routes each task to the right brain based on payload["brain"]
  - Tracks pending tasks (waiting for results)
  - When a result comes back, stores it and optionally calls a callback
  - Knows which brains are registered

Future:
  - Task decomposition (split one task into N subtasks with dependencies)
  - DAG-based execution (subtask B waits for subtask A)
  - ReWOO-style variable references (#E1, #E2)
  - Multi-brain handoff (auto vs human gate per brain)

Design:
  - The orchestrator subscribes to the bus as "orchestrator"
  - It receives TASK messages (from user/front brain) and RESULT messages (from brains)
  - For TASKs: look at payload["brain"] to know where to send it, forward the task
  - For RESULTs: match reply_to to a pending task, store the result, call the callback

You implement:
  - __init__(): store bus, set up brain registry and pending tasks dict
  - register_brain(): add a brain name to the known brains set
  - start(): subscribe to the bus for "orchestrator" target, set running
  - stop(): unsubscribe, set not running
  - _on_message(): route incoming messages (TASK → forward, RESULT → collect)
  - submit_task(): create and send a task to a specific brain, track it as pending
  - _handle_task(): extract brain target from payload, forward the task
  - _handle_result(): match result to pending task, store it, call callback
"""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Awaitable

from bigbrain.bus import MessageBus, BusMessage, MessageType

log = logging.getLogger("bigbrain.orchestrator")

# Callback type: called when a result comes back
ResultCallback = Callable[[BusMessage], Awaitable[None]]


class Orchestrator:
    """
    Routes tasks to brains and collects results.

    Usage:
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")
        await orch.start()

        # Submit a task and get notified when done
        results = []
        async def on_done(result_msg):
            results.append(result_msg)

        await orch.submit_task("python_brain", {"code": "print(42)"}, callback=on_done)
    """

    def __init__(self, bus: MessageBus):
        # TODO: Store these:
        #   self.bus        — the MessageBus instance
        #   self._brains    — set() of registered brain names
        #   self._pending   — dict mapping task_id → {"msg": original_msg, "callback": callback_fn}
        #   self._running   — bool
        pass

    def register_brain(self, name: str) -> None:
        """
        Register a brain name so the orchestrator knows it exists.
        Just add the name to self._brains set.
        """
        # TODO: Implement (one line)
        pass

    async def start(self) -> None:
        """
        Start the orchestrator.

        Steps:
        1. Set self._running = True
        2. Subscribe self._on_message to the bus with target "orchestrator"
        3. Log that the orchestrator started
        """
        # TODO: Implement
        pass

    async def stop(self) -> None:
        """
        Stop the orchestrator.

        Steps:
        1. Set self._running = False
        2. Unsubscribe self._on_message from the bus
        3. Log that the orchestrator stopped
        """
        # TODO: Implement
        pass

    async def _on_message(self, msg: BusMessage) -> None:
        """
        Handle incoming messages.

        Routing:
        - If not self._running → return
        - If msg.type is TASK → call self._handle_task(msg)
        - If msg.type is RESULT → call self._handle_result(msg)
        - Anything else → ignore (just return)

        Wrap in try/except Exception to log errors without crashing.
        """
        # TODO: Implement
        pass

    async def submit_task(
        self,
        brain: str,
        payload: dict,
        callback: ResultCallback | None = None,
    ) -> str:
        """
        Submit a task to a specific brain. Returns the task message ID.

        Steps:
        1. Check that brain is in self._brains — if not, raise ValueError
        2. Create a BusMessage with:
           - type = MessageType.TASK
           - source = "orchestrator"
           - target = brain
           - payload = payload
        3. Store it in self._pending: self._pending[msg.id] = {"msg": msg, "callback": callback}
        4. Send the message on the bus
        5. Return msg.id
        """
        # TODO: Implement
        pass

    async def _handle_task(self, msg: BusMessage) -> None:
        """
        Forward an incoming task to the right brain.

        Steps:
        1. Get brain name from msg.payload["brain"]
           - If missing, log a warning and return
        2. Check that brain is in self._brains
           - If not, log a warning and return
        3. Create a new TASK BusMessage:
           - source = "orchestrator"
           - target = brain name
           - payload = msg.payload (forward everything)
           - reply_to = msg.id (so we can trace it back)
        4. Store in self._pending: self._pending[new_msg.id] = {"msg": msg, "callback": None}
        5. Send it on the bus
        """
        # TODO: Implement
        pass

    async def _handle_result(self, msg: BusMessage) -> None:
        """
        Handle a result coming back from a brain.

        Steps:
        1. Look up msg.reply_to in self._pending
           - If not found, log debug and return (might be a result for something else)
        2. Remove it from self._pending (task is done)
        3. If the pending entry has a callback, await it with the result message
        """
        # TODO: Implement
        pass

    def get_pending_count(self) -> int:
        """Return the number of tasks still waiting for results."""
        # TODO: Implement (one line)
        pass
