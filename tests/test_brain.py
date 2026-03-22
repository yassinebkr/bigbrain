"""
Tests for the brain base class. Run with: pytest tests/test_brain.py -v

These tests verify your BaseBrain implementation works correctly.
Don't modify the tests — make your code pass them.
"""

import asyncio

import pytest

from bigbrain.bus import BusMessage, MessageType, MessageBus
from bigbrain.brains import BaseBrain


# ─── Concrete test brain ─────────────────────────────────────────────

class EchoBrain(BaseBrain):
    """Simple brain that echoes back whatever it receives."""

    async def handle_message(self, msg: BusMessage) -> None:
        await self.send_result(msg, {"echo": msg.payload})


class FailBrain(BaseBrain):
    """Brain that always crashes — for testing error handling."""

    async def handle_message(self, msg: BusMessage) -> None:
        raise RuntimeError("brain exploded")


class ProgressBrain(BaseBrain):
    """Brain that sends progress events before completing."""

    async def handle_message(self, msg: BusMessage) -> None:
        await self.send_event("brain.started", {"task": msg.payload.get("task", "unknown")})
        await self.send_event("brain.progress", {"percent": 50})
        await self.send_result(msg, {"done": True})


# ─── Tests ────────────────────────────────────────────────────────────

class TestBaseBrain:
    @pytest.mark.asyncio
    async def test_brain_receives_and_replies(self):
        """Brain receives a TASK and sends a RESULT back."""
        bus = MessageBus()
        brain = EchoBrain("echo_brain", bus)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()

        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="echo_brain",
            payload={"hello": "world"},
        ))

        await asyncio.sleep(0.05)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["echo"] == {"hello": "world"}
        assert results[0].source == "echo_brain"
        assert results[0].target == "orchestrator"

    @pytest.mark.asyncio
    async def test_result_has_reply_to(self):
        """RESULT message links back to the original TASK via reply_to."""
        bus = MessageBus()
        brain = EchoBrain("echo_brain", bus)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()

        task = BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="echo_brain",
            payload={"data": 42},
        )
        await bus.send(task)

        await asyncio.sleep(0.05)
        await brain.stop()
        await bus.stop()

        assert results[0].reply_to == task.id

    @pytest.mark.asyncio
    async def test_brain_ignores_messages_after_stop(self):
        """After stop(), brain doesn't process new messages."""
        bus = MessageBus()
        brain = EchoBrain("echo_brain", bus)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()
        await brain.stop()

        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="echo_brain",
            payload={"late": True},
        ))

        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_brain_error_sends_event(self):
        """When handle_message crashes, brain sends a brain.error event."""
        bus = MessageBus()
        brain = FailBrain("fail_brain", bus)
        events = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.EVENT:
                events.append(msg)

        bus.subscribe("*", collect)
        await bus.start()
        await brain.start()

        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="fail_brain",
        ))

        await asyncio.sleep(0.05)
        await brain.stop()
        await bus.stop()

        assert len(events) >= 1
        error_events = [e for e in events if e.payload.get("event") == "brain.error"]
        assert len(error_events) == 1
        assert "brain exploded" in str(error_events[0].payload)

    @pytest.mark.asyncio
    async def test_brain_sends_progress_events(self):
        """Brain can send events during processing."""
        bus = MessageBus()
        brain = ProgressBrain("progress_brain", bus)
        all_msgs = []

        async def collect(msg: BusMessage):
            all_msgs.append(msg)

        bus.subscribe("*", collect)
        await bus.start()
        await brain.start()

        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="progress_brain",
            payload={"task": "build"},
        ))

        await asyncio.sleep(0.05)
        await brain.stop()
        await bus.stop()

        event_types = [m.payload.get("event") for m in all_msgs if m.type == MessageType.EVENT]
        assert "brain.started" in event_types
        assert "brain.progress" in event_types

        results = [m for m in all_msgs if m.type == MessageType.RESULT]
        assert len(results) == 1
        assert results[0].payload["done"] is True

    @pytest.mark.asyncio
    async def test_multiple_brains_independent(self):
        """Two brains on the same bus only receive their own messages."""
        bus = MessageBus()
        brain_a = EchoBrain("brain_a", bus)
        brain_b = EchoBrain("brain_b", bus)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain_a.start()
        await brain_b.start()

        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="brain_a",
            payload={"for": "a"},
        ))

        await asyncio.sleep(0.05)
        await brain_a.stop()
        await brain_b.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].source == "brain_a"
        assert results[0].payload["echo"]["for"] == "a"
