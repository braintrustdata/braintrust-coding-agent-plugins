#!/usr/bin/env python3
"""Explicit, opt-in real Cursor smoke against the offline standalone sink.

Run after building the daemon; requires an authenticated `agent` CLI and makes
real model requests. Both child processes are supervised foreground commands.
The private temporary evidence directory is retained for inspection.
"""

import argparse
import fcntl
import json
import os
from pathlib import Path
import pty
import select
import shlex
import shutil
import struct
import subprocess
import tempfile
import termios
import time


def read_jsonl(path):
    if not path.exists():
        return []
    records = []
    for line in path.read_text().splitlines():
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            pass  # The foreground daemon may still be appending a record.
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="authorize real model requests")
    parser.add_argument("--model", default="auto")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    if not args.run:
        print("Skipped real Cursor smoke; pass --run to execute it.")
        return

    root = Path(__file__).resolve().parents[4]
    binary = root / "bt-daemon/target/debug/bt-daemon"
    assert binary.is_file(), "Build the standalone daemon with --all-features first"
    agent_executable = shutil.which("agent")
    assert agent_executable, "Cursor CLI `agent` is required"
    evidence = Path(tempfile.mkdtemp(prefix="cursor-real-smoke-"))
    print(f"Private smoke evidence: {evidence}", flush=True)
    # Keep the running server and hook client on the same immutable build even
    # when another developer command rebuilds the repository target directory.
    daemon_binary = evidence / "bt-daemon"
    shutil.copy2(binary, daemon_binary)
    socket = evidence / "daemon.sock"
    config = evidence / "braintrust.json"
    config.write_text(json.dumps({
        "trace_to_braintrust": True,
        "route": {
            "destination": {"type": "project_logs", "project_name": "cursor-real-smoke"},
            "flush_mode": "flush_on_turn_end",
        },
    }))
    workspace = evidence / "workspace"
    (workspace / ".cursor").mkdir(parents=True)
    (workspace / "sample.txt").write_text("audit fixture hello\n")
    (workspace / ".cursor/hooks.json").write_text(json.dumps({
        "version": 1,
        "hooks": {
            "beforeSubmitPrompt": [{"command": "printf '%s\\n' '{\"continue\":true}'"}],
            "afterAgentResponse": [{"command": "printf '%s\\n' '{}'"}],
            "stop": [{"command": "printf '%s\\n' '{}'"}],
        },
    }))
    wrapper = evidence / "bt"
    adapter = root / "src/plugins/cursor/test/bt-standalone-wrapper.sh"
    # Export this socket only inside our hook wrapper: unrelated imported user
    # hooks must not connect to or replace the development debug daemon.
    wrapper.write_text(
        "#!/bin/sh\n"
        f"BT_DAEMON_BIN={shlex.quote(str(daemon_binary))} "
        f"BT_DAEMON_SOCKET={shlex.quote(str(socket))} "
        f"exec {shlex.quote(str(adapter))} \"$@\"\n"
    )
    wrapper.chmod(0o700)
    environment = os.environ.copy()
    environment.pop("BT_DAEMON_SOCKET", None)
    environment.pop("BT_DAEMON_BIN", None)
    environment["BT_BIN"] = str(wrapper)
    environment["BT_DAEMON_CONFIG"] = str(config)
    foreground_daemon = None
    foreground_agent = None
    master = slave = None
    deadline = time.monotonic() + args.timeout
    try:
        with (evidence / "daemon.log").open("wb") as daemon_log:
            foreground_daemon = subprocess.Popen([
                str(daemon_binary), "serve", "--debug-sink", "--socket", str(socket),
                "--data-dir", str(evidence / "state"), "--idle-timeout-secs", "0",
            ], stdout=daemon_log, stderr=daemon_log)
            while not socket.exists():
                assert foreground_daemon.poll() is None, "Daemon exited before listening"
                assert time.monotonic() < deadline, "Daemon startup timed out"
                time.sleep(0.05)

            master, slave = pty.openpty()
            fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))
            foreground_agent = subprocess.Popen([
                agent_executable, "--workspace", str(workspace), "--plugin-dir",
                str(root / "src/plugins/cursor/content"), "--trust", "--force",
                "--model", args.model,
                'Read sample.txt and run printf "audit-shell-ok\\n", then summarize both.',
            ], stdin=slave, stdout=slave, stderr=slave, env=environment)
            os.close(slave)
            slave = None
            submitted_second = False
            final_rows = None
            with (evidence / "agent.terminal").open("wb") as terminal_log:
                while time.monotonic() < deadline:
                    ready, _, _ = select.select([master], [], [], 0.1)
                    if ready:
                        data = os.read(master, 65536)
                        terminal_log.write(data)
                        if b"\x1b]11;?" in data:
                            os.write(master, b"\x1b]11;rgb:0000/0000/0000\x07")
                    assert foreground_agent.poll() is None, "Cursor exited before completing turns"
                    hooks = [record for path in (evidence / "state/journal").glob("*.ndjson")
                             for record in read_jsonl(path) if "payload" in record]
                    completed = [r for r in hooks if r["event"] == "stop"
                                 and r["payload"].get("status") == "completed"]
                    if completed and not submitted_second:
                        os.write(master, b"Read sample.txt again and reply exactly AUDIT_RESUMED.\x1b[13u")
                        submitted_second = True
                    rows = {}
                    for path in (evidence / "state/spans").glob("*.ndjson"):
                        for op in read_jsonl(path):
                            row = next(iter(op.values()))
                            previous = rows.setdefault(row["span_id"], {})
                            for key, value in row.items():
                                if key == "name" and not value:
                                    continue  # Sink treats empty merge names as absent.
                                if isinstance(value, dict) and isinstance(previous.get(key), dict):
                                    previous[key].update(value)
                                else:
                                    previous[key] = value
                    turns = [r for r in rows.values() if r["name"].startswith("Turn ")]
                    if len(completed) == 2 and len(turns) == 2 and any(
                        "AUDIT_RESUMED" in str(r.get("output", "")) for r in turns
                    ):
                        final_rows = list(rows.values())
                        break
            assert final_rows, "Timed out waiting for two translated successful turns"
            sessions = [r for r in final_rows if r["name"] == "Cursor session"]
            turns = [r for r in final_rows if r["name"].startswith("Turn ")]
            tools = [r for r in final_rows if r["span_type"] == "tool"]
            models = [r for r in final_rows if r["span_type"] == "llm"]
            assert len(sessions) == 1 and len(turns) == 2 and len(tools) == 3
            assert models, "No reconstructed model spans"
            turn_ids = {r["span_id"] for r in turns}
            for turn in turns:
                assert turn["parent_span_ids"] == [sessions[0]["span_id"]]
                assert turn["metadata"]["status"] == "completed"
                assert turn["metrics"]["prompt_tokens"] > 0
                assert turn["metrics"]["tokens"] == (turn["metrics"]["prompt_tokens"]
                                                    + turn["metrics"]["completion_tokens"])
            for child in tools + models:
                assert len(child["parent_span_ids"]) == 1
                assert child["parent_span_ids"][0] in turn_ids
            for model in models:
                assert model["metadata"]["input_reconstructed"] is True
                if args.model != "auto":
                    assert model["metadata"]["model"] == args.model
                assert not model.get("metrics"), "Turn usage was assigned to a model span"
                choices = model["output"]
                assert isinstance(choices, list) and len(choices) == 1
                message = choices[0]["message"]
                assert message["role"] == "assistant"
                for call in message.get("tool_calls", []):
                    assert call["type"] == "function"
                    assert isinstance(call["function"]["arguments"], str)
                texts = [message["content"]] if message.get("content") else []
                for index, text in enumerate(texts):
                    others = texts[:index] + texts[index + 1:]
                    assert not others or text != "".join(others), "Duplicate aggregate model output"
            print(f"PASS: 1 session, 2 completed turns, {len(tools)} tools, "
                  f"{len(models)} reconstructed model spans; parentage and turn usage verified.")
    finally:
        for process in [foreground_agent, foreground_daemon]:
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for fd in [master, slave]:
            if fd is not None:
                os.close(fd)


if __name__ == "__main__":
    main()
