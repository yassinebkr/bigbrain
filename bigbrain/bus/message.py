"""
BusMessage — the single message type on the BigBrain bus.

Every inter-module communication goes through this. No direct calls.
Inspired by CAN bus: typed, source/target addressed, fully logged.
"""

from __future__ import annotations

import uuid
import time
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class MessageType(str, Enum):
    """What kind of message this is."""
    TASK = "task"              # Orchestrator → Brain: "do this work"
    RESULT = "result"          # Brain → Orchestrator: "here's my output"
    EVENT = "event"            # Anyone → Anyone: lifecycle events (started, failed, progress)
    QUERY = "query"            # Memory lookups, status checks
    COMMAND = "command"        # System commands (shutdown, pause, reload)


class BusMessage(BaseModel):
    """
    The universal message format. Every field has a reason:
    
    - id: unique per message, for dedup and reply tracking
    - type: determines how the receiver processes it
    - source: who sent it (for routing replies and audit)
    - target: who should receive it ("*" = broadcast)
    - project: scopes the message to a project context (None = global)
    - payload: the actual data, structure depends on type
    - timestamp: when it was created (unix float)
    - reply_to: links this message to the one it's responding to
    """

    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    type: MessageType
    source: str
    target: str                          # brain name, "orchestrator", or "*"
    project: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: float = Field(default_factory=time.time)
    reply_to: str | None = None          # id of the message this responds to
