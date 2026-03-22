"""
MessageBus — async pub/sub backbone for BigBrain.

Design:
  - Subscribers register for a target name (their own name, or "*" for all)
  - send() routes to matching subscribers via asyncio queues
  - Each subscriber gets its own queue (backpressure per consumer)
  - "*" target = broadcast to ALL subscribers
  - Bus never drops messages — if queue is full, send() awaits

You implement:
  - subscribe(): register a handler coroutine for a target name
  - unsubscribe(): remove a subscription
  - send(): route a BusMessage to the right subscriber(s)
  - start(): background task that drains queues and calls handlers
  - stop(): graceful shutdown
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Callable, Awaitable

from .message import BusMessage
from .logger import BusLogger

log = logging.getLogger("bigbrain.bus")

# Type alias for handler: async function that takes a BusMessage
Handler = Callable[[BusMessage], Awaitable[None]]


class MessageBus:
    """
    Async message bus. Subscribe handlers by target name, send messages.
    
    Usage:
        bus = MessageBus(log_dir="./logs")
        
        async def on_task(msg: BusMessage):
            print(f"Got task: {msg.payload}")
        
        bus.subscribe("python_brain", on_task)
        await bus.start()
        
        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="python_brain",
            payload={"instruction": "write hello world"}
        ))
        
        await bus.stop()
    """

    def __init__(self, log_dir: str | None = None):
        # TODO: Initialize these data structures
        #
        # self._handlers  — maps target name → list of Handler
        #                    e.g. {"python_brain": [handler_fn], "*": [monitor_fn]}
        #
        # self._queue     — single asyncio.Queue for all messages
        #                    (simpler than per-subscriber queues for v0.1)
        #
        # self._logger    — BusLogger instance (if log_dir provided)
        #
        # self._running   — bool flag for the dispatch loop
        # self._task      — reference to the asyncio dispatch task
        self.handlers = defaultdict(list)
        self._queue = asyncio.Queue()
        if log_dir:
            self._logger = BusLogger(log_dir)
        else:
           self._logger = None
        self._running = False
        self._task = None

    def subscribe(self, target: str, handler: Handler) -> None:
        """
        Register a handler for messages sent to `target`.
        
        - target="python_brain" → receives messages where msg.target == "python_brain"
        - target="*" → receives ALL messages (useful for monitoring/logging)
        
        Multiple handlers per target are allowed.
        """
        self._handlers[target].append(handler)

    def unsubscribe(self, target: str, handler: Handler) -> None:
        """Remove a previously registered handler."""
        try:
            self._handlers[target].remove(handler)
        except ValueError:
            pass

    async def send(self, msg: BusMessage) -> None:
        """
        Put a message on the bus.
        
        Steps:
        1. Log the message to disk (if logger exists)
        2. Put it in the queue for async dispatch
        
        This returns immediately — dispatch happens in the background.
        """
        if self._logger:
            self._logger.log(msg)
        await self._queue.put(msg)

    async def _dispatch(self, msg: BusMessage) -> None:
        """
        Route a single message to the right handlers.
        
        Routing rules:
        1. If msg.target == "*" → call ALL handlers in self._handlers (every key)
        2. Otherwise → call handlers registered for msg.target
        3. ALSO always call handlers registered for "*" (wildcard subscribers)
        4. Wrap each handler call in try/except — one bad handler shouldn't kill the bus
        """

        if msg.target == "*" :
            for handler_list in self._handlers.values():
                for handler in handler_list:
                    try:
                        await handler(msg)
                    except:
                        pass
        else:
            for handler in self._handlers[msg.target]:
                try:
                    await handler(msg)
                except:
                    pass
            for handler in self._handlers["*"]:
                try:
                    await handler(msg)
                except:
                    pass

        


    async def start(self) -> None:
        """
        Start the background dispatch loop.
        
        The loop:
        1. self._running = True
        2. while self._running: await msg from queue → dispatch(msg)
        3. Store the task in self._task so stop() can cancel it
        """
        # TODO: Create asyncio task running the dispatch loop
        pass

    async def stop(self) -> None:
        """
        Graceful shutdown.
        
        1. self._running = False
        2. Put a None sentinel in the queue to unblock the loop
        3. await self._task (with a timeout)
        4. Close the logger
        """
        # TODO: Implement graceful shutdown
        pass
