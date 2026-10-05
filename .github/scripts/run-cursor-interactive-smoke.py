#!/usr/bin/env python3
"""Run one Cursor interactive turn in a PTY, then exit after its answer renders."""

import fcntl
import json
import os
import pty
import select
import signal
import struct
import sys
import termios
import time
from pathlib import Path


PROMPT = "Say exactly: Cursor tracing smoke test passed."
EXPECTED = "cursor tracing smoke test passed"
TIMEOUT_SECS = 600
KILL_GRACE_SECS = 30


def traced_turn_completed() -> bool:
    summary_path = os.environ.get("MOCK_COLLECTOR_OUT")
    if not summary_path:
        return False
    try:
        rows = json.loads(Path(summary_path).read_text())["rows"]
    except (OSError, KeyError, json.JSONDecodeError):
        return False
    saw_answer = False
    saw_closed_turn = False
    for row in rows:
        attributes = row.get("span_attributes") or {}
        if attributes.get("type") == "llm":
            saw_answer |= EXPECTED in json.dumps(row.get("output")).lower()
        if (
            attributes.get("type") == "task"
            and (attributes.get("name") or "").startswith("Turn ")
            and (row.get("metadata") or {}).get("status") is not None
        ):
            saw_closed_turn = True
    return saw_answer and saw_closed_turn


def main() -> int:
    os.environ.setdefault("TERM", "xterm-256color")
    child, terminal = pty.fork()
    if child == 0:
        plugin = os.environ["CURSOR_PLUGIN_DIR"]
        os.execvp("agent", ["agent", "--plugin-dir", plugin, PROMPT])

    fcntl.ioctl(terminal, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 140, 0, 0))
    sent_interrupt = False
    deadline = time.monotonic() + TIMEOUT_SECS
    hard_kill_deadline = None
    status = None
    while time.monotonic() < deadline:
        ready, _, _ = select.select([terminal], [], [], 0.25)
        if ready:
            try:
                chunk = os.read(terminal, 8192)
            except OSError:
                chunk = b""
            if chunk:
                sys.stdout.buffer.write(chunk)
                sys.stdout.buffer.flush()
        if not sent_interrupt and traced_turn_completed():
            os.write(terminal, b"\x03")
            sent_interrupt = True
            hard_kill_deadline = time.monotonic() + KILL_GRACE_SECS
        waited, child_status = os.waitpid(child, os.WNOHANG)
        if waited:
            status = child_status
            break
        if hard_kill_deadline is not None and time.monotonic() > hard_kill_deadline:
            os.kill(child, signal.SIGKILL)
            _, status = os.waitpid(child, 0)
            break

    if status is None:
        os.kill(child, signal.SIGKILL)
        _, status = os.waitpid(child, 0)
    os.close(terminal)
    if not sent_interrupt:
        print("Cursor smoke did not trace a completed assistant turn", file=sys.stderr)
        return 1
    if os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0:
        return 0
    if os.WIFEXITED(status) and os.WEXITSTATUS(status) == 130 and sent_interrupt:
        return 0
    if os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGINT and sent_interrupt:
        return 0
    print(f"Cursor CLI exited unexpectedly with wait status {status}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
