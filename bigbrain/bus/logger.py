"""
BusLogger — writes every bus message to disk as JSONL.

Why:
  - Full replay for debugging ("what happened in that session?")
  - Audit trail (which brain said what, when)
  - Crash recovery (replay messages to rebuild state)

Format: one JSON line per message, appended to a date-stamped file.
  logs/bus/2026-03-16.jsonl

You implement:
  - log(): serialize BusMessage to JSON, append to today's file
  - close(): flush and close the file handle
  - _get_file(): manage file handle (open new file when date changes)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from .message import BusMessage


class BusLogger:
    """
    Append-only JSONL logger for bus messages.
    
    Usage:
        logger = BusLogger("./logs/bus")
        logger.log(message)   # synchronous — fast enough for v0.1
        logger.close()
    
    File rotation: new file per day (UTC).
    """

    def __init__(self, log_dir: str):
        # TODO: Initialize these:
        #
        # self._log_dir   — Path object, create if doesn't exist
        # self._current_date — str like "2026-03-16", tracks which file is open
        # self._file      — open file handle (or None)
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._current_date = None
        self._file = None
        pass

    def log(self, msg: BusMessage) -> None:
        """
        Write a single message to the log.
        
        Steps:
        1. Get today's date string (UTC)
        2. If date changed from self._current_date, rotate the file
        3. Serialize msg with msg.model_dump() → json.dumps()
        4. Write line + "\n" to file
        5. Flush (we want crash-safe logs)
        """
        date_str = datetime.now(datetime.timezone.utc).strtime("%Y-%m-%d")
        if date_str != self.current_date:
            self._rotate(date_str)
        line = json.dumps(msg.model_dump())
        self._file.write(line + "\n")
        self._file.flush()
        # TODO: Implement
        pass

    def _rotate(self, date_str: str) -> None:
        """
        Close current file (if any) and open a new one for the given date.
        
        File path: {self._log_dir}/{date_str}.jsonl
        Open in append mode ("a").
        """
        # TODO: Implement
        pass

    def close(self) -> None:
        """Flush and close the current file handle."""
        # TODO: Implement
        pass
