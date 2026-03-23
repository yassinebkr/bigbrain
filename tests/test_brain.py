"""
Tests for the brain base class. Run with: pytest tests/test_brain.py -v

These tests verify your BaseBrain implementation works correctly.
Don't modify the tests — make your code pass them.
"""

import asyncio

import pytest

from bigbrain.bus import BusMessage, MessageType, MessageBus
from bigbrain.brains import BaseBrain


# ─── Concrete test brains ────────────────────────────────────────────

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


class SlowBrain(BaseBrain):
    """Brain that takes time to process — for concurrency tests."""

    async def handle_message(self, msg: BusMessage) -> None:
        delay = msg.payload.get("delay", 0.1)
        await asyncio.sleep(delay)
        await self.send_result(msg, {"slow": True, "processed_after": delay})


class ConcurrentBrain(BaseBrain):
    """Brain that tracks concurrent processing count."""

    def __init__(self, name: str, bus: MessageBus):
        super().__init__(name, bus)
        self.processing_count = 0

    async def handle_message(self, msg: BusMessage) -> None:
        self.processing_count += 1
        await asyncio.sleep(0.1)  # Simulate work
        await self.send_result(msg, {"order": self.processing_count})
        self.processing_count -= 1


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


# ─── Edge Case / Hardened Tests ──────────────────────────────────────

class TestBrainEdgeCases:
    @pytest.mark.asyncio
    async def test_start_twice_is_idempotent(self):
        """Starting a brain twice is a no-op — guard prevents duplicate subscriptions."""
        bus = MessageBus()
        brain = EchoBrain("echo_brain", bus)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()
        await brain.start()  # second start — should be no-op

        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="echo_brain",
            payload={"data": 1},
        ))

        await asyncio.sleep(0.05)
        await brain.stop()
        await bus.stop()

        # Guard prevents double subscription → exactly 1 result
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_stop_without_start_no_crash(self):
        """Stopping a brain that was never started should not crash."""
        bus = MessageBus()
        brain = EchoBrain("echo_brain", bus)

        # Should not raise — unsubscribing a non-existent handler is a no-op
        await brain.stop()
        assert brain._running is False

    @pytest.mark.asyncio
    async def test_brain_ignores_non_task_messages(self):
        """Brain's _on_message only processes TASK messages, ignores all others."""
        bus = MessageBus()
        brain = EchoBrain("echo_brain", bus)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()

        for msg_type in [MessageType.RESULT, MessageType.EVENT, MessageType.QUERY, MessageType.COMMAND]:
            await bus.send(BusMessage(
                type=msg_type,
                source="orchestrator",
                target="echo_brain",
                payload={"data": "should be ignored"},
            ))

        # Send one actual TASK
        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="echo_brain",
            payload={"real_task": True},
        ))

        await asyncio.sleep(0.1)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["echo"]["real_task"] is True

    @pytest.mark.asyncio
    async def test_brain_with_special_characters_in_name(self):
        """Brain with special characters in name still works on the bus."""
        bus = MessageBus()
        brain = EchoBrain("brain-with-dashes_and_underscores.v2", bus)
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
            target="brain-with-dashes_and_underscores.v2",
            payload={"test": True},
        ))

        await asyncio.sleep(0.05)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].source == "brain-with-dashes_and_underscores.v2"

    @pytest.mark.asyncio
    async def test_brain_error_event_has_correct_source(self):
        """Error events from a failing brain have the correct source name."""
        bus = MessageBus()
        brain = FailBrain("my_fail_brain", bus)
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
            target="my_fail_brain",
        ))

        await asyncio.sleep(0.05)
        await brain.stop()
        await bus.stop()

        error_events = [e for e in events if e.payload.get("event") == "brain.error"]
        assert len(error_events) == 1
        assert error_events[0].source == "my_fail_brain"

    @pytest.mark.asyncio
    async def test_send_event_with_custom_target(self):
        """Brain can send events with a specific target (not just broadcast)."""
        bus = MessageBus()
        brain = EchoBrain("echo_brain", bus)
        targeted_events = []

        async def collect_targeted(msg: BusMessage):
            if msg.type == MessageType.EVENT:
                targeted_events.append(msg)

        bus.subscribe("dashboard", collect_targeted)
        await bus.start()
        await brain.start()

        await brain.send_event("brain.status", {"status": "idle"}, target="dashboard")

        await asyncio.sleep(0.05)
        await brain.stop()
        await bus.stop()

        assert len(targeted_events) == 1
        assert targeted_events[0].payload["event"] == "brain.status"
        assert targeted_events[0].target == "dashboard"

    @pytest.mark.asyncio
    async def test_result_reply_to_links_correctly_multiple(self):
        """Each result links back to the correct originating task."""
        bus = MessageBus()
        brain = EchoBrain("echo_brain", bus)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()

        tasks = []
        for i in range(3):
            task = BusMessage(
                type=MessageType.TASK,
                source="orchestrator",
                target="echo_brain",
                payload={"i": i},
            )
            tasks.append(task)
            await bus.send(task)

        await asyncio.sleep(0.1)
        await brain.stop()
        await bus.stop()

        assert len(results) == 3
        task_ids = {t.id for t in tasks}
        reply_ids = {r.reply_to for r in results}
        assert reply_ids == task_ids

    @pytest.mark.asyncio
    async def test_brain_with_empty_payload(self):
        """Brain handles tasks with empty payload without crashing."""
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
        ))

        await asyncio.sleep(0.05)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["echo"] == {}

    @pytest.mark.asyncio
    async def test_concurrent_tasks_to_same_brain(self):
        """Brain handles multiple concurrent tasks correctly."""
        bus = MessageBus()
        brain = SlowBrain("slow_brain", bus)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()

        for i in range(5):
            await bus.send(BusMessage(
                type=MessageType.TASK,
                source="orchestrator",
                target="slow_brain",
                payload={"task": i},
            ))

        # Bus dispatches concurrently: all 5 fire in parallel (~0.1s total)
        await asyncio.sleep(1.0)
        await brain.stop()
        await bus.stop()

        assert len(results) == 5

    @pytest.mark.asyncio
    async def test_brain_handle_malformed_message_data(self):
        """Brain should handle messages with unexpected payload structures."""
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
            payload={
                "nested": {"deep": {"very": {"structure": None}}},
                "special_chars": "null\x00byte\ttab\nnewline",
                "unicode": "🤖💾🚀",
                "large_number": 999999999999999999999999999999,
                "empty_structures": {"dict": {}, "list": [], "string": ""},
            },
        ))

        await asyncio.sleep(0.05)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_brain_name_collisions_independent_buses(self):
        """Two brains with the same name on different buses work independently."""
        bus1 = MessageBus()
        bus2 = MessageBus()

        brain1 = EchoBrain("same_name", bus1)
        brain2 = EchoBrain("same_name", bus2)

        results1 = []
        results2 = []

        async def collect1(msg):
            if msg.type == MessageType.RESULT:
                results1.append(msg)

        async def collect2(msg):
            if msg.type == MessageType.RESULT:
                results2.append(msg)

        bus1.subscribe("orchestrator", collect1)
        bus2.subscribe("orchestrator", collect2)

        await bus1.start()
        await bus2.start()
        await brain1.start()
        await brain2.start()

        await bus1.send(BusMessage(
            type=MessageType.TASK, source="orchestrator", target="same_name",
            payload={"bus": 1},
        ))
        await bus2.send(BusMessage(
            type=MessageType.TASK, source="orchestrator", target="same_name",
            payload={"bus": 2},
        ))

        await asyncio.sleep(0.05)
        await brain1.stop()
        await brain2.stop()
        await bus1.stop()
        await bus2.stop()

        assert len(results1) == 1
        assert len(results2) == 1
        assert results1[0].payload["echo"]["bus"] == 1
        assert results2[0].payload["echo"]["bus"] == 2

    @pytest.mark.asyncio
    async def test_brain_process_tasks_after_stop(self):
        """Brain should ignore tasks sent after it's been stopped."""
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
            type=MessageType.TASK, source="orchestrator", target="echo_brain",
            payload={"before_stop": True},
        ))
        await asyncio.sleep(0.05)

        await brain.stop()

        await bus.send(BusMessage(
            type=MessageType.TASK, source="orchestrator", target="echo_brain",
            payload={"after_stop": True},
        ))
        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["echo"]["before_stop"] is True

    @pytest.mark.asyncio
    async def test_brain_empty_name_handling(self):
        """Brain with empty name should work (though not recommended)."""
        bus = MessageBus()
        brain = EchoBrain("", bus)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()

        await bus.send(BusMessage(
            type=MessageType.TASK, source="orchestrator", target="",
            payload={"empty_name": True},
        ))

        await asyncio.sleep(0.05)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_brain_concurrent_message_processing(self):
        """Brain handles multiple concurrent messages safely."""
        bus = MessageBus()
        brain = ConcurrentBrain("concurrent_brain", bus)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()

        send_tasks = []
        for i in range(10):
            send_tasks.append(bus.send(BusMessage(
                type=MessageType.TASK,
                source="orchestrator",
                target="concurrent_brain",
                payload={"task_id": i},
            )))

        await asyncio.gather(*send_tasks)
        # Bus dispatches concurrently: all 10 fire in parallel (~0.1s total)
        await asyncio.sleep(1.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 10

    @pytest.mark.asyncio
    async def test_brain_memory_pressure_large_messages(self):
        """Brain handles large message payloads without crashing."""
        bus = MessageBus()
        brain = EchoBrain("echo_brain", bus)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()

        large_data = "x" * (512 * 1024)
        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="echo_brain",
            payload={"large_data": large_data, "size": len(large_data)},
        ))

        await asyncio.sleep(0.2)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["echo"]["size"] == 512 * 1024

    @pytest.mark.asyncio
    async def test_brain_exception_in_handle_triggers_error_event(self):
        """When handle_message raises, the _on_message wrapper catches it
        and sends a brain.error event via send_event (which still works
        because the bus reference is intact)."""
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
            type=MessageType.TASK, source="orchestrator", target="fail_brain",
        ))

        await asyncio.sleep(0.05)
        await brain.stop()
        await bus.stop()

        error_events = [e for e in events if e.payload.get("event") == "brain.error"]
        assert len(error_events) == 1
        assert "brain exploded" in error_events[0].payload["error"]

    @pytest.mark.asyncio
    async def test_brain_unicode_names_and_content(self):
        """Brain works with Unicode names and handles Unicode content."""
        bus = MessageBus()
        brain_name = "🤖_brain_测试"
        brain = EchoBrain(brain_name, bus)
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
            target=brain_name,
            payload={
                "message": "Hello 世界! 🌍🚀",
                "chinese": "你好",
                "japanese": "こんにちは",
                "emoji": "🔥💻⚡🤖",
                "arabic": "مرحبا",
            },
        ))

        await asyncio.sleep(0.05)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].source == brain_name
        assert "世界" in results[0].payload["echo"]["message"]

    @pytest.mark.asyncio
    async def test_brain_rapid_start_stop_cycles(self):
        """Brain handles rapid start/stop cycles without issues."""
        bus = MessageBus()
        brain = EchoBrain("echo_brain", bus)
        await bus.start()

        for _ in range(5):
            await brain.start()
            await brain.stop()
            await asyncio.sleep(0.01)

        await bus.stop()
        # Should complete without crashes
