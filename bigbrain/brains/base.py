"""
BaseBrain — abstract base class for all BigBrain brains.

Every brain (Python, Researcher, DevOps, etc.) inherits from this.
It handles the bus plumbing so subclasses only implement the interesting part.

Lifecycle:
    1. __init__(): store name + bus reference
    2. start(): subscribe to the bus, set running flag
    3. handle_message(): ABSTRACT — subclass implements this
    4. stop(): unsubscribe, cleanup

Design:
  - A brain subscribes to messages targeted at its own name
  - When a TASK arrives, it processes it and sends a RESULT back
  - Events (progress, errors) are sent via helper methods
  - Each brain has a name (like "python_brain" or "researcher")
  - The brain never calls other brains directly — everything goes through the bus

You implement:
  - __init__(): store self.name (str) and self.bus (MessageBus)
  - start(): subscribe self._on_message to the bus using self.name as target, set self._running = True
  - stop(): unsubscribe self._on_message, set self._running = False
  - _on_message(): wrapper that catches exceptions, then calls self.handle_message()
  - handle_message(): ABSTRACT — the subclass implements the actual brain logic
  - send_result(): helper to send a RESULT message back to the source
  - send_event(): helper to send an EVENT message (for progress, errors, etc.)
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from bigbrain.bus import MessageBus, BusMessage, MessageType

log = logging.getLogger("bigbrain.brain")


class BaseBrain(ABC):
    """
    Abstract base brain. Subclass this and implement handle_message().

    Usage:
        class PythonBrain(BaseBrain):
            async def handle_message(self, msg: BusMessage) -> None:
                # do work with msg.payload
                result = {"code": "print('hello')"}
                await self.send_result(msg, result)

        brain = PythonBrain("python_brain", bus)
        await brain.start()
    """

    def __init__(self, name: str, bus: MessageBus):
        self.name = name
        self.bus = bus
        self._running = False

    async def start(self) -> None:
        """
        Start the brain: subscribe to the bus for messages targeting self.name.

        Steps:
        1. Set self._running = True
        2. Subscribe self._on_message to the bus with target = self.name
        3. Log that the brain started
        """
        self._running = True

        self.bus.subscribe(self.name, self._on_message)
        log.info(f"{self.name} started")

    async def stop(self) -> None:
        """
        Stop the brain: unsubscribe from the bus.

        Steps:
        1. Set self._running = False
        2. Unsubscribe self._on_message from the bus
        3. Log that the brain stopped
        """
        self._running = False

        self.bus.unsubscribe(self.name, self._on_message)
        log.info(f"{self.name} stopped")

    async def _on_message(self, msg: BusMessage) -> None:
        """
        Internal message handler — wraps handle_message with safety.

        Steps:
        1. If not self._running, return (ignore messages after stop)
        2. Log that we received a message (msg.type, msg.source)
        3. Call await self.handle_message(msg)
        4. If handle_message raises an exception:
           - Log the error
           - Send an error event back via send_event() with type "brain.error"
             and the error details in the payload
        """
        # TODO: Implement
        pass

    @abstractmethod
    async def handle_message(self, msg: BusMessage) -> None:
        """
        Process an incoming message. Subclasses MUST implement this.

        This is where the brain's actual logic lives.
        Use self.send_result() to reply, self.send_event() for progress updates.
        """
        ...

    async def send_result(self, original_msg: BusMessage, payload: dict) -> None:
        """
        Send a RESULT back to whoever sent the original message.

        Creates a new BusMessage with:
        - type = MessageType.RESULT
        - source = self.name (this brain)
        - target = original_msg.source (reply to sender)
        - payload = the result data
        - reply_to = original_msg.id (links it to the original)
        """
        # TODO: Build a BusMessage and await self.bus.send(msg)
        pass

    async def send_event(self, event_type: str, payload: dict, target: str = "*") -> None:
        """
        Broadcast an event on the bus (progress, error, status, etc.)

        Creates a new BusMessage with:
        - type = MessageType.EVENT
        - source = self.name
        - target = target (default "*" = broadcast)
        - payload = {"event": event_type, **payload}
          (merge event_type into the payload dict so receivers know what kind of event)
        """
        # TODO: Build a BusMessage and await self.bus.send(msg)
        pass
