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
