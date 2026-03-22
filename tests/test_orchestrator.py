"""
Tests for the Orchestrator. Run with: pytest tests/test_orchestrator.py -v

Don't modify the tests — make your code pass them.
"""

import asyncio

import pytest

from bigbrain.bus import BusMessage, MessageType, MessageBus
from bigbrain.brains.python_brain import PythonBrain
from bigbrain.orchestrator import Orchestrator


class TestOrchestrator:
    @pytest.mark.asyncio
    async def test_submit_task_and_get_result(self):
        """Orchestrator sends task to brain and receives result via callback."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        results = []

        async def on_result(msg: BusMessage):
            results.append(msg)

        await bus.start()
        await brain.start()
        await orch.start()

        await orch.submit_task(
            "python_brain",
            {"code": "print('orchestrated')"},
            callback=on_result,
        )

        await asyncio.sleep(0.5)
        await orch.stop()
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["stdout"].strip() == "orchestrated"

    @pytest.mark.asyncio
    async def test_submit_returns_task_id(self):
        """submit_task returns the message ID for tracking."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        await bus.start()
        await brain.start()
        await orch.start()

        task_id = await orch.submit_task(
            "python_brain",
            {"code": "print(1)"},
        )

        assert task_id is not None
        assert isinstance(task_id, str)
        assert len(task_id) > 0

        await asyncio.sleep(0.5)
        await orch.stop()
        await brain.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_unknown_brain_raises(self):
        """Submitting to an unregistered brain raises ValueError."""
        bus = MessageBus()
        orch = Orchestrator(bus)

        await bus.start()
        await orch.start()

        with pytest.raises(ValueError):
            await orch.submit_task("nonexistent_brain", {"code": "print(1)"})

        await orch.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_pending_count(self):
        """Pending count tracks in-flight tasks."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        await bus.start()
        await brain.start()
        await orch.start()

        assert orch.get_pending_count() == 0

        await orch.submit_task("python_brain", {"code": "print(1)"})
        # Task is pending right after submit
        assert orch.get_pending_count() >= 0  # might already be processed

        await asyncio.sleep(0.5)
        # After brain processes, pending should be 0
        assert orch.get_pending_count() == 0

        await orch.stop()
        await brain.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_multiple_tasks(self):
        """Orchestrator handles multiple tasks to the same brain."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        results = []

        async def on_result(msg: BusMessage):
            results.append(msg)

        await bus.start()
        await brain.start()
        await orch.start()

        await orch.submit_task("python_brain", {"code": "print('one')"}, callback=on_result)
        await orch.submit_task("python_brain", {"code": "print('two')"}, callback=on_result)
        await orch.submit_task("python_brain", {"code": "print('three')"}, callback=on_result)

        await asyncio.sleep(1)
        await orch.stop()
        await brain.stop()
        await bus.stop()

        assert len(results) == 3
        outputs = sorted([r.payload["stdout"].strip() for r in results])
        assert outputs == ["one", "three", "two"]

    @pytest.mark.asyncio
    async def test_handle_incoming_task(self):
        """Orchestrator forwards external TASK messages to the right brain."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT and msg.target == "orchestrator":
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()
        await orch.start()

        # External system sends a task TO the orchestrator
        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="front_brain",
            target="orchestrator",
            payload={"brain": "python_brain", "code": "print('routed')"},
        ))

        await asyncio.sleep(0.5)
        await orch.stop()
        await brain.stop()
        await bus.stop()

        # The result should come back to orchestrator (which also received it via collect)
        # At minimum, the brain processed it
        assert len(results) >= 1

    @pytest.mark.asyncio
    async def test_task_without_brain_key_ignored(self):
        """TASK without 'brain' in payload is logged and ignored."""
        bus = MessageBus()
        orch = Orchestrator(bus)

        await bus.start()
        await orch.start()

        # This should not crash
        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="someone",
            target="orchestrator",
            payload={"no_brain_key": True},
        ))

        await asyncio.sleep(0.1)
        await orch.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_no_callback_still_works(self):
        """Tasks without callbacks still complete without error."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        await bus.start()
        await brain.start()
        await orch.start()

        # No callback — should not crash
        await orch.submit_task("python_brain", {"code": "print('silent')"})

        await asyncio.sleep(0.5)
        assert orch.get_pending_count() == 0

        await orch.stop()
        await brain.stop()
        await bus.stop()
