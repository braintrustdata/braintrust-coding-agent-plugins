#!/usr/bin/env python3
"""Run one Cursor interactive turn in a PTY, then exit after its answer renders."""

import fcntl
import os
import pty
import re
import select
import signal
import struct
import sys
import termios
import time


PROMPT = "Say exactly: Cursor tracing smoke test passed."
EXPECTED = b"Cursor tracing smoke test passed."
TIMEOUT_SECS = 600


def main() -> int:
    os.environ.setdefault("TERM", "xterm-256color")
    child, terminal = pty.fork()
    if child == 0:
        plugin = os.environ["CURSOR_PLUGIN_DIR"]
        os.execvp("agent", ["agent", "--plugin-dir", plugin, PROMPT])

    fcntl.ioctl(terminal, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 140, 0, 0))
    output = bytearray()
    sent_interrupt = False
    deadline = time.monotonic() + TIMEOUT_SECS
    exit_deadline = None
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
                output.extend(chunk)
                if not sent_interrupt:
                    plain = re.sub(rb"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\\\))", b"", output)
                    if EXPECTED in plain:
                        # The stop hook runs before the completed answer is
                        # rendered. Interrupt the now-idle interactive UI.
                        time.sleep(1)
                        os.write(terminal, b"\x03")
                        sent_interrupt = True
                        exit_deadline = time.monotonic() + 20
        waited, child_status = os.waitpid(child, os.WNOHANG)
        if waited:
            status = child_status
            break
        if exit_deadline is not None and time.monotonic() > exit_deadline:
            os.write(terminal, b"\x03")
            exit_deadline = time.monotonic() + 5
        if exit_deadline is not None and time.monotonic() > exit_deadline + 5:
            os.kill(child, signal.SIGKILL)
            _, status = os.waitpid(child, 0)
            break

    if status is None:
        os.kill(child, signal.SIGKILL)
        _, status = os.waitpid(child, 0)
    os.close(terminal)
    plain = re.sub(rb"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\\\))", b"", output)
    if EXPECTED not in plain:
        print("Cursor interactive smoke did not render the expected response", file=sys.stderr)
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
