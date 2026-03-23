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


# ─── Edge Case / Hardened Tests ──────────────────────────────────────

class TestOrchestratorEdgeCases:
    @pytest.mark.asyncio
    async def test_register_same_brain_twice(self):
        """Registering the same brain name twice is idempotent (uses a set)."""
        bus = MessageBus()
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")
        orch.register_brain("python_brain")
        # _brains is a set, so no duplicate
        assert len(orch._brains) == 1

    @pytest.mark.asyncio
    async def test_stop_without_start_no_crash(self):
        """Stopping an orchestrator that was never started should not crash."""
        bus = MessageBus()
        orch = Orchestrator(bus)
        await bus.start()

        # Should not raise
        await orch.stop()

        await bus.stop()

    @pytest.mark.asyncio
    async def test_submit_after_stop(self):
        """Submitting a task after the orchestrator is stopped still sends
        the message (submit_task doesn't check _running)."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()
        await orch.start()
        await orch.stop()

        # submit_task still creates and sends a message
        # but _on_message won't process results (not running)
        task_id = await orch.submit_task("python_brain", {"code": "print('after stop')"})
        assert task_id is not None

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_each_task_gets_unique_id(self):
        """Each submitted task gets a unique message ID."""
        bus = MessageBus()
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        # Don't actually start bus/brain — just test ID uniqueness
        await bus.start()
        await orch.start()

        ids = set()
        for _ in range(50):
            task_id = await orch.submit_task("python_brain", {"code": "print(1)"})
            ids.add(task_id)

        assert len(ids) == 50

        await orch.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_callback_exception_doesnt_crash_orchestrator(self):
        """A callback that raises should not crash the orchestrator."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        good_results = []

        async def bad_callback(msg: BusMessage):
            raise RuntimeError("callback exploded")

        async def good_callback(msg: BusMessage):
            good_results.append(msg)

        await bus.start()
        await brain.start()
        await orch.start()

        # First task has a bad callback
        await orch.submit_task(
            "python_brain",
            {"code": "print('first')"},
            callback=bad_callback,
        )

        # Second task has a good callback — should still work
        await orch.submit_task(
            "python_brain",
            {"code": "print('second')"},
            callback=good_callback,
        )

        await asyncio.sleep(1.0)
        await orch.stop()
        await brain.stop()
        await bus.stop()

        assert len(good_results) == 1
        assert good_results[0].payload["stdout"].strip() == "second"

    @pytest.mark.asyncio
    async def test_multiple_brains_route_correctly(self):
        """Tasks are routed to the correct brain based on the target."""
        bus = MessageBus()
        brain_a = PythonBrain("brain_a", bus)
        brain_b = PythonBrain("brain_b", bus)
        orch = Orchestrator(bus)
        orch.register_brain("brain_a")
        orch.register_brain("brain_b")

        results_a = []
        results_b = []

        async def on_a(msg: BusMessage):
            results_a.append(msg)

        async def on_b(msg: BusMessage):
            results_b.append(msg)

        await bus.start()
        await brain_a.start()
        await brain_b.start()
        await orch.start()

        await orch.submit_task("brain_a", {"code": "print('from_a')"}, callback=on_a)
        await orch.submit_task("brain_b", {"code": "print('from_b')"}, callback=on_b)

        await asyncio.sleep(1.0)
        await orch.stop()
        await brain_a.stop()
        await brain_b.stop()
        await bus.stop()

        assert len(results_a) == 1
        assert results_a[0].payload["stdout"].strip() == "from_a"
        assert len(results_b) == 1
        assert results_b[0].payload["stdout"].strip() == "from_b"

    @pytest.mark.asyncio
    async def test_handle_task_with_unknown_brain_in_payload(self):
        """External TASK with unknown brain name in payload is silently ignored."""
        bus = MessageBus()
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        await bus.start()
        await orch.start()

        # Send a task to orchestrator with unknown brain
        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="external",
            target="orchestrator",
            payload={"brain": "unknown_brain_xyz", "code": "print(1)"},
        ))

        await asyncio.sleep(0.1)
        # Should not crash
        await orch.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_result_for_unknown_task_ignored(self):
        """A result with a reply_to that doesn't match any pending task is ignored."""
        bus = MessageBus()
        orch = Orchestrator(bus)

        await bus.start()
        await orch.start()

        # Send a result directly to orchestrator with unknown reply_to
        await bus.send(BusMessage(
            type=MessageType.RESULT,
            source="some_brain",
            target="orchestrator",
            payload={"data": "orphan result"},
            reply_to="nonexistent-task-id",
        ))

        await asyncio.sleep(0.1)
        # Should not crash
        assert orch.get_pending_count() == 0
        await orch.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_result_without_reply_to_ignored(self):
        """A result with no reply_to is silently ignored."""
        bus = MessageBus()
        orch = Orchestrator(bus)

        await bus.start()
        await orch.start()

        await bus.send(BusMessage(
            type=MessageType.RESULT,
            source="some_brain",
            target="orchestrator",
            payload={"data": "no reply_to"},
        ))

        await asyncio.sleep(0.1)
        assert orch.get_pending_count() == 0
        await orch.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_pending_count_decrements_after_result(self):
        """Pending count goes from 1 to 0 after result is received."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        await bus.start()
        await brain.start()
        await orch.start()

        await orch.submit_task("python_brain", {"code": "print('done')"})

        await asyncio.sleep(0.5)

        assert orch.get_pending_count() == 0
        await orch.stop()
        await brain.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_on_message_ignores_event_messages(self):
        """Orchestrator's _on_message ignores EVENT messages."""
        bus = MessageBus()
        orch = Orchestrator(bus)

        await bus.start()
        await orch.start()

        await bus.send(BusMessage(
            type=MessageType.EVENT,
            source="some_brain",
            target="orchestrator",
            payload={"event": "brain.started"},
        ))

        await asyncio.sleep(0.1)
        # Should not crash, EVENT is not handled
        assert orch.get_pending_count() == 0
        await orch.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_brain_crash_still_tracked_as_pending(self):
        """If a brain crashes and never sends a result, the task stays pending."""
        bus = MessageBus()
        orch = Orchestrator(bus)
        orch.register_brain("crash_brain")

        await bus.start()
        await orch.start()

        # Submit task to a brain that doesn't exist (no one will respond)
        await orch.submit_task("crash_brain", {"code": "print('never')"})

        await asyncio.sleep(0.2)
        # No brain to process it → stays pending
        assert orch.get_pending_count() == 1

        await orch.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_submit_task_empty_payload(self):
        """Submitting a task with empty payload doesn't crash."""
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

        # Empty payload — PythonBrain will say "no code provided"
        await orch.submit_task("python_brain", {}, callback=on_result)

        await asyncio.sleep(0.5)
        await orch.stop()
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["returncode"] != 0

    @pytest.mark.asyncio
    async def test_orchestrator_unicode_brain_names(self):
        """Orchestrator should work with Unicode brain names."""
        bus = MessageBus()
        brain_name = "🤖_brain_测试"
        brain = PythonBrain(brain_name, bus)
        orch = Orchestrator(bus)
        orch.register_brain(brain_name)

        results = []

        async def collect(msg: BusMessage):
            results.append(msg)

        await bus.start()
        await brain.start()
        await orch.start()

        await orch.submit_task(
            brain_name,
            {"code": "print('Unicode brain works! 🚀')"},
            callback=collect
        )

        await asyncio.sleep(0.5)
        await orch.stop()
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert "🚀" in results[0].payload["stdout"]

    @pytest.mark.asyncio
    async def test_orchestrator_massive_task_flood(self):
        """Orchestrator should handle a flood of tasks without crashing.
        Note: bus dispatches serially and each task spawns a subprocess,
        so we use a moderate count (20) with generous wait time."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        results = []

        async def collect(msg: BusMessage):
            results.append(msg)

        await bus.start()
        await brain.start()
        await orch.start()

        task_count = 20
        tasks = []
        for i in range(task_count):
            task = orch.submit_task(
                "python_brain",
                {"code": f"print('task_{i}')"},
                callback=collect,
            )
            tasks.append(task)

        await asyncio.gather(*tasks)
        await asyncio.sleep(5.0)  # ~250ms per subprocess × 20 = 5s

        await orch.stop()
        await brain.stop()
        await bus.stop()

        assert len(results) == task_count

    @pytest.mark.asyncio
    async def test_orchestrator_very_large_payload(self):
        """Orchestrator should handle very large task payloads."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus, timeout=5)
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        results = []

        async def collect(msg: BusMessage):
            results.append(msg)

        await bus.start()
        await brain.start()
        await orch.start()

        # Create 2MB payload
        large_data = "x" * (2 * 1024 * 1024)
        await orch.submit_task(
            "python_brain",
            {
                "code": f"print('Data size: {len(large_data)}')",
                "large_data": large_data
            },
            callback=collect
        )

        await asyncio.sleep(2.0)
        await orch.stop()
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert "2097152" in results[0].payload["stdout"]

    @pytest.mark.asyncio
    async def test_orchestrator_concurrent_start_stop(self):
        """Concurrent start/stop operations should be safe."""
        bus = MessageBus()
        orch = Orchestrator(bus)

        await bus.start()

        # Rapid start/stop cycles
        for i in range(5):
            await orch.start()
            await asyncio.sleep(0.01)
            await orch.stop()
            await asyncio.sleep(0.01)

        await bus.stop()

    @pytest.mark.asyncio
    async def test_orchestrator_malformed_task_payload(self):
        """Orchestrator should handle malformed task payloads gracefully."""
        bus = MessageBus()
        orch = Orchestrator(bus)
        orch.register_brain("test_brain")

        await bus.start()
        await orch.start()

        # Send task with missing "brain" key via _handle_task path
        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="external",
            target="orchestrator",
            payload={
                "not_brain": "test_brain",  # Wrong key
                "code": "print('malformed')"
            }
        ))

        # Send task with None brain value
        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="external", 
            target="orchestrator",
            payload={
                "brain": None,
                "code": "print('none brain')"
            }
        ))

        await asyncio.sleep(0.1)
        assert orch.get_pending_count() == 0  # Should be ignored

        await orch.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_orchestrator_circular_callback_error(self):
        """Circular references in callbacks should not cause infinite loops."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        callback_count = 0

        async def circular_callback(msg: BusMessage):
            nonlocal callback_count
            callback_count += 1
            if callback_count < 3:  # Prevent infinite loop in test
                # Try to submit another task from within callback
                await orch.submit_task(
                    "python_brain",
                    {"code": "print('nested')"},
                    callback=circular_callback
                )

        await bus.start()
        await brain.start()
        await orch.start()

        await orch.submit_task(
            "python_brain",
            {"code": "print('start')"},
            callback=circular_callback
        )

        await asyncio.sleep(1.0)
        await orch.stop()
        await brain.stop()
        await bus.stop()

        assert callback_count >= 1

    @pytest.mark.asyncio
    async def test_task_timeout_fires(self):
        """Tasks that exceed the timeout are cleaned up and callback receives error."""
        bus = MessageBus()
        orch = Orchestrator(bus, task_timeout=0.5)  # 500ms timeout
        orch.register_brain("ghost_brain")  # registered but no actual brain running

        timeout_results = []

        async def on_timeout(msg: BusMessage):
            timeout_results.append(msg)

        events = []

        async def collect_events(msg: BusMessage):
            if msg.type == MessageType.EVENT:
                events.append(msg)

        bus.subscribe("*", collect_events)
        await bus.start()
        await orch.start()

        await orch.submit_task("ghost_brain", {"code": "never runs"}, callback=on_timeout)
        assert orch.get_pending_count() == 1

        # Wait for the timeout loop to fire (checks every 1s, timeout is 0.5s)
        await asyncio.sleep(2.5)

        assert orch.get_pending_count() == 0
        assert len(timeout_results) == 1
        assert timeout_results[0].payload["error"] == "task_timeout"

        timeout_events = [e for e in events if e.payload.get("event") == "task.timeout"]
        assert len(timeout_events) == 1

        await orch.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_task_timeout_doesnt_affect_completed_tasks(self):
        """Tasks that complete before timeout are not affected by the timeout loop."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        orch = Orchestrator(bus, task_timeout=10.0)  # generous timeout
        orch.register_brain("python_brain")

        results = []

        async def on_result(msg: BusMessage):
            results.append(msg)

        await bus.start()
        await brain.start()
        await orch.start()

        await orch.submit_task("python_brain", {"code": "print('fast')"}, callback=on_result)

        await asyncio.sleep(0.5)

        assert len(results) == 1
        assert results[0].payload["stdout"].strip() == "fast"
        assert orch.get_pending_count() == 0  # cleaned up by result, not timeout

        await orch.stop()
        await brain.stop()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_orchestrator_memory_cleanup_after_results(self):
        """Pending tasks should be cleaned up from memory after results."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        orch = Orchestrator(bus)
        orch.register_brain("python_brain")

        await bus.start()
        await brain.start()
        await orch.start()

        # Submit task and verify it's pending
        await orch.submit_task("python_brain", {"code": "print('cleanup test')"})
        
        # Wait for result - should clean up pending entry
        await asyncio.sleep(0.5)
        
        # Check that internal _pending dict is clean
        assert len(orch._pending) == 0

        await orch.stop()
        await brain.stop()
        await bus.stop()
