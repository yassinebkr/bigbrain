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


# ─── Edge Case / Hardened Tests ──────────────────────────────────────

class TestPythonBrainEdgeCases:
    @pytest.mark.asyncio
    async def test_syntax_error_in_code(self):
        """Code with syntax errors returns non-zero returncode and stderr."""
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
            payload={"code": "def foo(:\n  pass"},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["returncode"] != 0
        assert "SyntaxError" in results[0].payload["stderr"]

    @pytest.mark.asyncio
    async def test_multiple_syntax_errors(self):
        """PythonBrain handles various syntax errors gracefully."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()

        syntax_errors = [
            "print('unclosed string",
            "if True\n    print('missing colon')",
            "def func(\n    pass",
            "1 +",
        ]

        for bad_code in syntax_errors:
            await bus.send(BusMessage(
                type=MessageType.TASK,
                source="orchestrator",
                target="python_brain",
                payload={"code": bad_code},
            ))

        await asyncio.sleep(2.0)
        await brain.stop()
        await bus.stop()

        assert len(results) == len(syntax_errors)
        for result in results:
            assert result.payload["returncode"] != 0

    @pytest.mark.asyncio
    async def test_import_error(self):
        """Code with import errors returns meaningful stderr."""
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
            payload={"code": "import nonexistent_module_xyz_123"},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["returncode"] != 0
        assert "ModuleNotFoundError" in results[0].payload["stderr"]

    @pytest.mark.asyncio
    async def test_empty_code_string(self):
        """Empty code string returns error (PythonBrain treats falsy as 'no code')."""
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
            payload={"code": ""},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        # Empty string is falsy, so PythonBrain treats it as "no code provided"
        assert results[0].payload["returncode"] != 0

    @pytest.mark.asyncio
    async def test_unicode_output(self):
        """Code that prints unicode is captured correctly."""
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
            payload={"code": """
print("Hello 世界! 🌍🚀")
print("Chinese: 你好世界")
print("Japanese: こんにちは世界")
print("Emoji: 🤖💾🔥⚡")
print("Special chars: café naïve résumé")
"""},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        stdout = results[0].payload["stdout"]
        assert "世界" in stdout
        assert "🌍" in stdout
        assert "café" in stdout
        assert results[0].payload["returncode"] == 0

    @pytest.mark.asyncio
    async def test_large_output(self):
        """Code that produces large output is captured fully."""
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
            payload={"code": "for i in range(1000): print(f'line {i}')"},
        ))

        await asyncio.sleep(1.0)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        lines = results[0].payload["stdout"].strip().split("\n")
        assert len(lines) == 1000
        assert results[0].payload["returncode"] == 0

    @pytest.mark.asyncio
    async def test_massive_output(self):
        """PythonBrain handles 1MB+ output without crashing."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus, timeout=5)
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
            payload={"code": "print('x' * 1024 * 1024)"},
        ))

        await asyncio.sleep(3.0)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert len(results[0].payload["stdout"]) > 1000000

    @pytest.mark.asyncio
    async def test_concurrent_executions(self):
        """Multiple tasks sent rapidly are all executed and return results."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus)
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
                target="python_brain",
                payload={"code": f"print({i})"},
            ))

        await asyncio.sleep(2.0)
        await brain.stop()
        await bus.stop()

        assert len(results) == 5
        outputs = sorted([r.payload["stdout"].strip() for r in results])
        assert outputs == ["0", "1", "2", "3", "4"]

    @pytest.mark.asyncio
    async def test_code_with_stderr_but_success(self):
        """Code that writes to stderr but exits successfully returns returncode 0."""
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
            payload={"code": "import sys; sys.stderr.write('warning\\n'); print('ok')"},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["stdout"].strip() == "ok"
        assert "warning" in results[0].payload["stderr"]
        assert results[0].payload["returncode"] == 0

    @pytest.mark.asyncio
    async def test_code_with_exit_code(self):
        """Code that calls sys.exit(42) returns returncode 42."""
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
            payload={"code": """
import sys
print("Before exit")
sys.exit(42)
print("This should not print")
"""},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert "Before exit" in results[0].payload["stdout"]
        assert "This should not print" not in results[0].payload["stdout"]
        assert results[0].payload["returncode"] == 42

    @pytest.mark.asyncio
    async def test_timeout_short_value(self):
        """A very short timeout still triggers correctly."""
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
            payload={"code": "import time; time.sleep(10)"},
        ))

        await asyncio.sleep(2.0)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert "Timeout" in results[0].payload["stderr"]

    @pytest.mark.asyncio
    async def test_very_short_timeout(self):
        """100ms timeout kills long-running code quickly."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus, timeout=0.5)
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
            payload={"code": "import time; time.sleep(5)"},
        ))

        await asyncio.sleep(1.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert "Timeout" in results[0].payload["stderr"]
        assert results[0].payload["returncode"] != 0

    @pytest.mark.asyncio
    async def test_code_that_reads_stdin_doesnt_hang(self):
        """Code that tries to read stdin should not hang indefinitely."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus, timeout=3)
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
            payload={"code": "import sys; data = sys.stdin.read(); print(f'got: {len(data)}')"},
        ))

        await asyncio.sleep(1.0)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["returncode"] == 0

    @pytest.mark.asyncio
    async def test_stdin_input_prompt_handled(self):
        """Code that uses input() should either timeout or get EOF."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus, timeout=2)
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
            payload={"code": "input('Enter something: ')"},
        ))

        await asyncio.sleep(3.0)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert results[0].payload["returncode"] != 0

    @pytest.mark.asyncio
    async def test_lifecycle_events_on_failure(self):
        """PythonBrain sends both brain.started and brain.complete even when code fails."""
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
            payload={"code": "raise Exception('fail')"},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        event_types = [e.payload.get("event") for e in events]
        assert "brain.started" in event_types
        assert "brain.complete" in event_types

    @pytest.mark.asyncio
    async def test_disk_writes(self):
        """PythonBrain allows code that writes to disk."""
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
            payload={"code": """
import tempfile
import os
with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
    f.write('test data')
    fname = f.name
print(f'Written to {fname}')
os.unlink(fname)
print('Cleaned up')
"""},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert "Written to" in results[0].payload["stdout"]
        assert "Cleaned up" in results[0].payload["stdout"]

    @pytest.mark.asyncio
    async def test_subprocess_creation(self):
        """Code that creates subprocesses works."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus, timeout=5)
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
            payload={"code": """
import subprocess
result = subprocess.run(['echo', 'Hello from subprocess'], capture_output=True, text=True)
print(f"Subprocess output: {result.stdout.strip()}")
"""},
        ))

        await asyncio.sleep(2.0)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert "Hello from subprocess" in results[0].payload["stdout"]

    @pytest.mark.asyncio
    async def test_encoding_edge_cases(self):
        """Code with encoding edge cases handled gracefully."""
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
            payload={"code": """
import sys
print(f"Default encoding: {sys.getdefaultencoding()}")
print("UTF-8: ñoño café")
data = "café".encode('utf-8')
print(f"Bytes: {data}")
"""},
        ))

        await asyncio.sleep(0.5)
        await brain.stop()
        await bus.stop()

        assert len(results) == 1
        assert "café" in results[0].payload["stdout"]

    @pytest.mark.asyncio
    async def test_rapid_task_submission(self):
        """Submitting many tasks rapidly should not overwhelm the brain."""
        bus = MessageBus()
        brain = PythonBrain("python_brain", bus, timeout=1)
        results = []

        async def collect(msg: BusMessage):
            if msg.type == MessageType.RESULT:
                results.append(msg)

        bus.subscribe("orchestrator", collect)
        await bus.start()
        await brain.start()

        send_tasks = []
        for i in range(30):
            send_tasks.append(bus.send(BusMessage(
                type=MessageType.TASK,
                source="orchestrator",
                target="python_brain",
                payload={"code": f"print({i})"},
            )))

        await asyncio.gather(*send_tasks)
        await asyncio.sleep(5.0)
        await brain.stop()
        await bus.stop()

        assert len(results) == 30
        outputs = {int(r.payload["stdout"].strip()) for r in results}
        assert outputs == set(range(30))
