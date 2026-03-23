"""
Tests for the message bus. Run with: pytest tests/test_bus.py -v

These tests verify your implementation works correctly.
Don't modify the tests — make your code pass them.
"""

import asyncio
import json
import tempfile
from pathlib import Path

import pytest

from bigbrain.bus import BusMessage, MessageType, MessageBus, BusLogger


# ─── BusMessage Tests ────────────────────────────────────────────────

class TestBusMessage:
    def test_create_with_defaults(self):
        msg = BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="python_brain",
        )
        assert msg.id  # auto-generated, non-empty
        assert msg.type == MessageType.TASK
        assert msg.source == "orchestrator"
        assert msg.target == "python_brain"
        assert msg.payload == {}
        assert msg.timestamp > 0
        assert msg.reply_to is None
        assert msg.project is None

    def test_create_with_all_fields(self):
        msg = BusMessage(
            type=MessageType.RESULT,
            source="python_brain",
            target="orchestrator",
            project="bigbrain",
            payload={"code": "print('hello')"},
            reply_to="abc123",
        )
        assert msg.project == "bigbrain"
        assert msg.payload["code"] == "print('hello')"
        assert msg.reply_to == "abc123"

    def test_serialization_roundtrip(self):
        msg = BusMessage(
            type=MessageType.EVENT,
            source="bus",
            target="*",
            payload={"event": "started"},
        )
        data = msg.model_dump()
        restored = BusMessage(**data)
        assert restored.id == msg.id
        assert restored.type == msg.type
        assert restored.payload == msg.payload

    def test_unique_ids(self):
        msgs = [
            BusMessage(type=MessageType.TASK, source="a", target="b")
            for _ in range(100)
        ]
        ids = {m.id for m in msgs}
        assert len(ids) == 100  # all unique


# ─── BusLogger Tests ─────────────────────────────────────────────────

class TestBusLogger:
    def test_log_creates_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = BusLogger(tmpdir)
            msg = BusMessage(
                type=MessageType.TASK,
                source="test",
                target="test",
            )
            logger.log(msg)
            logger.close()

            # Should have created a .jsonl file
            files = list(Path(tmpdir).glob("*.jsonl"))
            assert len(files) == 1

    def test_log_writes_valid_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = BusLogger(tmpdir)
            msg = BusMessage(
                type=MessageType.RESULT,
                source="brain",
                target="orchestrator",
                payload={"answer": 42},
            )
            logger.log(msg)
            logger.close()

            files = list(Path(tmpdir).glob("*.jsonl"))
            line = files[0].read_text().strip()
            data = json.loads(line)
            assert data["source"] == "brain"
            assert data["payload"]["answer"] == 42

    def test_multiple_messages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = BusLogger(tmpdir)
            for i in range(5):
                logger.log(BusMessage(
                    type=MessageType.EVENT,
                    source="test",
                    target="*",
                    payload={"i": i},
                ))
            logger.close()

            files = list(Path(tmpdir).glob("*.jsonl"))
            lines = files[0].read_text().strip().split("\n")
            assert len(lines) == 5


# ─── MessageBus Tests ────────────────────────────────────────────────

class TestMessageBus:
    @pytest.mark.asyncio
    async def test_send_and_receive(self):
        """Basic: send a message, subscriber receives it."""
        bus = MessageBus()
        received = []

        async def handler(msg: BusMessage):
            received.append(msg)

        bus.subscribe("brain_a", handler)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="orchestrator",
            target="brain_a",
            payload={"instruction": "hello"},
        ))

        await asyncio.sleep(0.05)  # let dispatch run
        await bus.stop()

        assert len(received) == 1
        assert received[0].payload["instruction"] == "hello"

    @pytest.mark.asyncio
    async def test_message_not_delivered_to_wrong_target(self):
        """Messages only go to the right subscriber."""
        bus = MessageBus()
        received_a = []
        received_b = []

        async def handler_a(msg): received_a.append(msg)
        async def handler_b(msg): received_b.append(msg)

        bus.subscribe("brain_a", handler_a)
        bus.subscribe("brain_b", handler_b)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="orch",
            target="brain_a",
        ))

        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(received_a) == 1
        assert len(received_b) == 0

    @pytest.mark.asyncio
    async def test_wildcard_subscriber(self):
        """A '*' subscriber sees all messages."""
        bus = MessageBus()
        all_msgs = []

        async def monitor(msg): all_msgs.append(msg)

        bus.subscribe("*", monitor)
        await bus.start()

        await bus.send(BusMessage(type=MessageType.TASK, source="a", target="brain_x"))
        await bus.send(BusMessage(type=MessageType.EVENT, source="b", target="brain_y"))

        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(all_msgs) == 2

    @pytest.mark.asyncio
    async def test_broadcast_message(self):
        """A message with target='*' goes to all subscribers."""
        bus = MessageBus()
        received_a = []
        received_b = []

        async def handler_a(msg): received_a.append(msg)
        async def handler_b(msg): received_b.append(msg)

        bus.subscribe("brain_a", handler_a)
        bus.subscribe("brain_b", handler_b)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.EVENT,
            source="system",
            target="*",
            payload={"event": "shutdown"},
        ))

        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(received_a) == 1
        assert len(received_b) == 1

    @pytest.mark.asyncio
    async def test_handler_error_doesnt_kill_bus(self):
        """A failing handler shouldn't prevent other messages."""
        bus = MessageBus()
        received = []

        async def bad_handler(msg):
            raise RuntimeError("boom")

        async def good_handler(msg):
            received.append(msg)

        bus.subscribe("brain_a", bad_handler)
        bus.subscribe("brain_a", good_handler)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.TASK, source="orch", target="brain_a"
        ))

        await asyncio.sleep(0.05)
        await bus.stop()

        # good_handler should still have received it
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_disk_logging(self):
        """Messages are logged to disk when log_dir is set."""
        with tempfile.TemporaryDirectory() as tmpdir:
            bus = MessageBus(log_dir=tmpdir)
            await bus.start()

            await bus.send(BusMessage(
                type=MessageType.TASK,
                source="test",
                target="nobody",
            ))

            await asyncio.sleep(0.05)
            await bus.stop()

            files = list(Path(tmpdir).glob("*.jsonl"))
            assert len(files) == 1
            lines = files[0].read_text().strip().split("\n")
            assert len(lines) == 1

    @pytest.mark.asyncio
    async def test_unsubscribe(self):
        """After unsubscribe, handler no longer receives messages."""
        bus = MessageBus()
        received = []

        async def handler(msg): received.append(msg)

        bus.subscribe("brain_a", handler)
        await bus.start()

        await bus.send(BusMessage(type=MessageType.TASK, source="a", target="brain_a"))
        await asyncio.sleep(0.05)
        assert len(received) == 1

        bus.unsubscribe("brain_a", handler)

        await bus.send(BusMessage(type=MessageType.TASK, source="a", target="brain_a"))
        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(received) == 1  # still 1, not 2


# ─── Edge Case / Hardened Tests ──────────────────────────────────────

class TestBusEdgeCases:
    @pytest.mark.asyncio
    async def test_subscribe_same_callback_twice(self):
        """Subscribe the same callback twice → delivers the message twice."""
        bus = MessageBus()
        received = []

        async def handler(msg):
            received.append(msg)

        bus.subscribe("brain_a", handler)
        bus.subscribe("brain_a", handler)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.TASK, source="orch", target="brain_a"
        ))

        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(received) == 2

    @pytest.mark.asyncio
    async def test_unsubscribe_during_dispatch(self):
        """Handler that unsubscribes itself during dispatch should not crash the bus."""
        bus = MessageBus()
        call_count = []

        async def self_unsubscribing_handler(msg: BusMessage):
            call_count.append(1)
            bus.unsubscribe("brain_a", self_unsubscribing_handler)

        bus.subscribe("brain_a", self_unsubscribing_handler)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.TASK, source="orch", target="brain_a"
        ))
        await asyncio.sleep(0.05)

        # Second message should NOT be received (handler unsubscribed itself)
        await bus.send(BusMessage(
            type=MessageType.TASK, source="orch", target="brain_a"
        ))
        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(call_count) == 1

    @pytest.mark.asyncio
    async def test_publish_to_no_subscribers(self):
        """Publishing to a target with no subscribers should not error."""
        bus = MessageBus()
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.TASK, source="orch", target="nonexistent_brain"
        ))

        await asyncio.sleep(0.05)
        await bus.stop()

    @pytest.mark.asyncio
    async def test_massive_message_flood(self):
        """Bus handles a high volume of messages without dropping any."""
        bus = MessageBus()
        received = []

        async def handler(msg):
            received.append(msg)

        bus.subscribe("brain_a", handler)
        await bus.start()

        count = 1000
        for i in range(count):
            await bus.send(BusMessage(
                type=MessageType.TASK, source="orch", target="brain_a",
                payload={"i": i},
            ))

        await asyncio.sleep(0.5)
        await bus.stop()

        assert len(received) == count

    @pytest.mark.asyncio
    async def test_empty_target(self):
        """Messages with empty string target delivered to '' subscribers."""
        bus = MessageBus()
        received = []

        async def handler(msg):
            received.append(msg)

        bus.subscribe("", handler)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.TASK, source="orch", target=""
        ))

        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(received) == 1

    def test_none_payload_uses_default(self):
        """BusMessage with no explicit payload defaults to empty dict."""
        msg = BusMessage(type=MessageType.TASK, source="a", target="b")
        assert msg.payload == {}

    @pytest.mark.asyncio
    async def test_wildcard_subscriber_does_not_duplicate_on_broadcast(self):
        """A wildcard subscriber receives a broadcast exactly once."""
        bus = MessageBus()
        received = []

        async def monitor(msg):
            received.append(msg)

        bus.subscribe("*", monitor)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.EVENT, source="sys", target="*"
        ))

        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(received) == 1

    def test_unsubscribe_nonexistent_handler_no_error(self):
        """Unsubscribing a handler that was never subscribed should not error."""
        bus = MessageBus()

        async def handler(msg):
            pass

        async def other_handler(msg):
            pass

        bus.subscribe("brain_a", handler)
        bus.unsubscribe("brain_a", other_handler)  # never subscribed
        bus.unsubscribe("nonexistent", handler)  # target never subscribed

    @pytest.mark.asyncio
    async def test_stop_without_start(self):
        """Stopping a bus that was never started should not crash."""
        bus = MessageBus()
        try:
            await bus.stop()
        except Exception:
            pytest.fail("bus.stop() raised an exception when called without start()")

    @pytest.mark.asyncio
    async def test_double_start(self):
        """Starting the bus twice should not create duplicate dispatch."""
        bus = MessageBus()
        received = []

        async def handler(msg):
            received.append(msg)

        bus.subscribe("brain_a", handler)
        await bus.start()
        await bus.start()  # second start

        await bus.send(BusMessage(
            type=MessageType.TASK, source="orch", target="brain_a"
        ))

        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_send_before_start_queued(self):
        """Messages sent before start() are queued and delivered after start."""
        bus = MessageBus()
        received = []

        async def handler(msg):
            received.append(msg)

        bus.subscribe("brain_a", handler)

        await bus.send(BusMessage(
            type=MessageType.TASK, source="orch", target="brain_a"
        ))

        await bus.start()
        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_multiple_wildcard_subscribers(self):
        """Multiple wildcard subscribers each see all messages."""
        bus = MessageBus()
        received_1 = []
        received_2 = []
        received_3 = []

        async def monitor_1(msg): received_1.append(msg)
        async def monitor_2(msg): received_2.append(msg)
        async def monitor_3(msg): received_3.append(msg)

        bus.subscribe("*", monitor_1)
        bus.subscribe("*", monitor_2)
        bus.subscribe("*", monitor_3)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.TASK, source="orch", target="brain_a"
        ))

        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(received_1) == 1
        assert len(received_2) == 1
        assert len(received_3) == 1

    @pytest.mark.asyncio
    async def test_targeted_and_wildcard_both_receive(self):
        """Both targeted handler and wildcard handler receive the same message."""
        bus = MessageBus()
        targeted = []
        wildcard = []

        async def target_handler(msg): targeted.append(msg)
        async def wildcard_handler(msg): wildcard.append(msg)

        bus.subscribe("brain_a", target_handler)
        bus.subscribe("*", wildcard_handler)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.TASK, source="orch", target="brain_a"
        ))

        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(targeted) == 1
        assert len(wildcard) == 1

    @pytest.mark.asyncio
    async def test_handler_exception_doesnt_prevent_subsequent_messages(self):
        """After a handler throws, the bus continues processing new messages."""
        bus = MessageBus()
        received = []

        async def bad_then_good(msg):
            if msg.payload.get("fail"):
                raise RuntimeError("intentional failure")
            received.append(msg)

        bus.subscribe("brain_a", bad_then_good)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.TASK, source="orch", target="brain_a",
            payload={"fail": True},
        ))
        await asyncio.sleep(0.05)

        await bus.send(BusMessage(
            type=MessageType.TASK, source="orch", target="brain_a",
            payload={"fail": False},
        ))
        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_large_payload(self):
        """Bus handles messages with large payloads."""
        bus = MessageBus()
        received = []

        async def handler(msg): received.append(msg)

        bus.subscribe("brain_a", handler)
        await bus.start()

        large_data = {"key_" + str(i): "x" * 1000 for i in range(100)}
        await bus.send(BusMessage(
            type=MessageType.TASK, source="orch", target="brain_a",
            payload=large_data,
        ))

        await asyncio.sleep(0.1)
        await bus.stop()

        assert len(received) == 1
        assert len(received[0].payload) == 100

    @pytest.mark.asyncio
    async def test_all_message_types_delivered(self):
        """All MessageType enum values can be sent and received."""
        bus = MessageBus()
        received = []

        async def handler(msg): received.append(msg)

        bus.subscribe("brain_a", handler)
        await bus.start()

        for mt in MessageType:
            await bus.send(BusMessage(
                type=mt, source="orch", target="brain_a"
            ))

        await asyncio.sleep(0.1)
        await bus.stop()

        received_types = {m.type for m in received}
        assert received_types == set(MessageType)

    @pytest.mark.asyncio
    async def test_concurrent_publish_subscribe(self):
        """Concurrent publishing and subscribing works safely."""
        bus = MessageBus()
        received = []

        async def handler(msg):
            received.append(msg)

        await bus.start()

        async def publisher():
            for i in range(100):
                await bus.send(BusMessage(
                    type=MessageType.TASK, source="pub",
                    target=f"brain_{i % 10}", payload={"i": i},
                ))

        async def subscriber():
            for i in range(10):
                bus.subscribe(f"brain_{i}", handler)
                await asyncio.sleep(0.001)

        await asyncio.gather(publisher(), subscriber())
        await asyncio.sleep(0.2)
        await bus.stop()

        assert len(received) > 0

    @pytest.mark.asyncio
    async def test_wildcard_with_handler_exceptions(self):
        """Wildcard subscriber exceptions don't affect targeted delivery."""
        bus = MessageBus()
        received_target = []
        received_wildcard = []

        async def bad_wildcard(msg):
            received_wildcard.append(msg)
            raise RuntimeError("wildcard fails")

        async def good_target(msg):
            received_target.append(msg)

        bus.subscribe("*", bad_wildcard)
        bus.subscribe("brain", good_target)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.TASK, source="a", target="brain"
        ))
        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(received_target) == 1
        assert len(received_wildcard) == 1

    @pytest.mark.asyncio
    async def test_slow_handler_doesnt_block_other_targets(self):
        """With concurrent dispatch, a slow handler on brain_a doesn't block brain_b."""
        bus = MessageBus()
        received_a = []
        received_b = []

        async def slow_handler_a(msg):
            await asyncio.sleep(0.5)
            received_a.append(msg)

        async def fast_handler_b(msg):
            received_b.append(msg)

        bus.subscribe("brain_a", slow_handler_a)
        bus.subscribe("brain_b", fast_handler_b)
        await bus.start()

        await bus.send(BusMessage(type=MessageType.TASK, source="orch", target="brain_a"))
        await bus.send(BusMessage(type=MessageType.TASK, source="orch", target="brain_b"))

        await asyncio.sleep(0.1)  # brain_b should have received, brain_a still processing

        assert len(received_b) == 1  # fast handler processed immediately
        assert len(received_a) == 0  # slow handler still sleeping

        await asyncio.sleep(0.5)  # wait for slow handler to finish
        await bus.stop()

        assert len(received_a) == 1
        assert len(received_b) == 1

    @pytest.mark.asyncio
    async def test_stop_during_dispatch(self):
        """Stopping bus during message dispatch is graceful."""
        bus = MessageBus()
        received = []

        async def slow_handler(msg):
            await asyncio.sleep(0.1)
            received.append(msg)

        bus.subscribe("brain", slow_handler)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.TASK, source="a", target="brain"
        ))
        await asyncio.sleep(0.01)
        await bus.stop()

    @pytest.mark.asyncio
    async def test_unicode_in_messages(self):
        """Unicode content in messages works correctly."""
        bus = MessageBus()
        received = []

        async def handler(msg): received.append(msg)

        bus.subscribe("brain", handler)
        await bus.start()

        await bus.send(BusMessage(
            type=MessageType.TASK,
            source="テスト",
            target="brain",
            payload={
                "text": "Hello 世界! 🚀🤖",
                "chinese": "你好世界",
                "arabic": "مرحبا بالعالم",
                "special": "café naïve résumé",
            },
        ))

        await asyncio.sleep(0.05)
        await bus.stop()

        assert len(received) == 1
        assert received[0].source == "テスト"
        assert "世界" in received[0].payload["text"]

    @pytest.mark.asyncio
    async def test_large_payload_memory_pressure(self):
        """Large payloads don't crash the bus."""
        bus = MessageBus()
        received = []

        async def handler(msg):
            received.append(len(str(msg.payload)))

        bus.subscribe("brain", handler)
        await bus.start()

        large_data = "x" * (1024 * 1024)
        await bus.send(BusMessage(
            type=MessageType.TASK, source="test", target="brain",
            payload={"large": large_data},
        ))

        await asyncio.sleep(0.2)
        await bus.stop()

        assert len(received) == 1
        assert received[0] > 1000000


# ─── BusLogger Edge Cases ────────────────────────────────────────────

class TestBusLoggerEdgeCases:
    def test_close_without_logging(self):
        """Closing a logger that never logged should not error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = BusLogger(tmpdir)
            logger.close()

    def test_double_close(self):
        """Closing a logger twice should not error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = BusLogger(tmpdir)
            msg = BusMessage(type=MessageType.TASK, source="a", target="b")
            logger.log(msg)
            logger.close()
            logger.close()

    def test_log_preserves_all_fields(self):
        """Logged message retains all fields including reply_to and project."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = BusLogger(tmpdir)
            msg = BusMessage(
                type=MessageType.RESULT,
                source="brain",
                target="orch",
                project="myproject",
                payload={"answer": 42},
                reply_to="original-id-123",
            )
            logger.log(msg)
            logger.close()

            files = list(Path(tmpdir).glob("*.jsonl"))
            data = json.loads(files[0].read_text().strip())
            assert data["project"] == "myproject"
            assert data["reply_to"] == "original-id-123"
            assert data["source"] == "brain"

    def test_unicode_in_payload(self):
        """Logger handles unicode characters in payload."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = BusLogger(tmpdir)
            msg = BusMessage(
                type=MessageType.EVENT,
                source="test",
                target="*",
                payload={"text": "こんにちは世界 🌍 émojis"},
            )
            logger.log(msg)
            logger.close()

            files = list(Path(tmpdir).glob("*.jsonl"))
            data = json.loads(files[0].read_text().strip())
            assert data["payload"]["text"] == "こんにちは世界 🌍 émojis"

    def test_logger_directory_creation(self):
        """BusLogger creates directory if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nested_path = Path(tmpdir) / "nested" / "logs"
            assert not nested_path.exists()

            logger = BusLogger(str(nested_path))
            assert nested_path.exists()

            msg = BusMessage(type=MessageType.TASK, source="test", target="test")
            logger.log(msg)
            logger.close()

    def test_logger_handles_edge_case_payloads(self):
        """BusLogger handles messages with problematic JSON content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = BusLogger(tmpdir)

            problematic_payloads = [
                {"nested": {"very": {"deep": {"nesting": {"level": 5}}}}},
                {"empty_dict": {}, "empty_list": [], "none_val": None},
                {"quotes": 'He said "Hello" with \'quotes\''},
                {"newlines": "Line 1\nLine 2\r\nLine 3"},
                {"backslashes": "C:\\Windows\\System32\\test.exe"},
            ]

            for payload in problematic_payloads:
                msg = BusMessage(
                    type=MessageType.TASK, source="test", target="test",
                    payload=payload,
                )
                logger.log(msg)

            logger.close()

            files = list(Path(tmpdir).glob("*.jsonl"))
            lines = files[0].read_text().strip().split("\n")
            assert len(lines) == len(problematic_payloads)
