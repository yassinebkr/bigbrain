"""
Tests for PythonBrain. Run with: pytest tests/test_python_brain.py -v

Don't modify the tests — make your code pass them.
"""

import asyncio

import pytest

from bigbrain.bus import BusMessage, MessageType, MessageBus
from bigbrain.brains.python_brain import PythonBrain


class TestPythonBrain:
    @pytest.mark.asyncio
    async def test_runs_simple_code(self):
        """PythonBrain executes code and returns stdout."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
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
            target="python_brain",
            payload={"code": "print('hello world')"},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["stdout"].strip() == "hello world"
        assert results[0].payload["returncode"] == 0

    @pytest.mark.asyncio
    async def test_captures_stderr(self):
        """PythonBrain captures stderr from failing code."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
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
            target="python_brain",
            payload={"code": "raise ValueError('oops')"},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert "oops" in results[0].payload["stderr"]
        assert results[0].payload["returncode"] != 0

    @pytest.mark.asyncio
    async def test_timeout_kills_process(self):
        """Code that runs too long gets killed."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus, timeout=1)
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
            target="python_brain",
            payload={"code": "import time; time.sleep(30)"},
        ))

        await asyncio.sleep(2)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert "Timeout" in results[0].payload["stderr"]
        assert results[0].payload["returncode"] != 0

    @pytest.mark.asyncio
    async def test_missing_code_key(self):
        """Task without 'code' in payload returns an error result."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
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
            target="python_brain",
            payload={"wrong_key": "print('hi')"},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert "error" in results[0].payload or results[0].payload["returncode"] != 0

    @pytest.mark.asyncio
    async def test_sends_lifecycle_events(self):
        """PythonBrain sends brain.started and brain.complete events."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
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
            target="python_brain",
            payload={"code": "print(1+1)"},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        event_types = [e.payload.get("event") for e in events]
        assert "brain.started" in event_types
        assert "brain.complete" in event_types

    @pytest.mark.asyncio
    async def test_multiline_code(self):
        """PythonBrain handles multiline code."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()

        code = """
x = 10
y = 20
print(x + y)
"""
        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="python_brain",
            payload={"code": code},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["stdout"].strip() == "30"
