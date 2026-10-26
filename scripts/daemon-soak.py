#!/usr/bin/env python3
"""Drive an isolated daemon with long-lived synthetic Claude sessions.

No model or account is required. Duration 0 runs until Ctrl-C. Results stay in
--output: samples.csv, turns.jsonl, status.json, summary.json, and daemon data.
"""
import argparse
from collections import deque
import csv
import datetime
import json
import math
import os
from pathlib import Path
import random
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import uuid


def interval(rng, turns_per_minute, sigma):
    """Lognormal turn lengths with the requested arithmetic mean."""
    mean = 60 / turns_per_minute
    return rng.lognormvariate(math.log(mean) - sigma * sigma / 2, sigma)


def slope(points):
    """Least-squares RSS growth in MiB/minute over a bounded sample window."""
    if len(points) < 2:
        return 0.0
    mean_t = sum(t for t, _ in points) / len(points)
    mean_y = sum(y for _, y in points) / len(points)
    variance = sum((t - mean_t) ** 2 for t, _ in points)
    return 60 * sum((t - mean_t) * (y - mean_y) for t, y in points) / variance if variance else 0.0


class Backend(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128

    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.base = f"http://127.0.0.1:{self.server_port}"
        self.lock = threading.Lock()
        self.requests = self.bytes = 0

    def handle_error(self, request, client_address):
        # SDK clients may close idle HTTP keep-alive connections between batches.
        if not isinstance(sys.exc_info()[1], (ConnectionResetError, BrokenPipeError)):
            super().handle_error(request, client_address)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        project = {"id": "soak-project", "name": "soak", "org_id": "soak-org"}
        if self.path.startswith("/logs3"):
            with self.server.lock:
                self.server.requests += 1
                self.server.bytes += len(body)
            result = {}
        elif self.path.startswith("/api/apikey/login"):
            result = {"org_info": [{"id": "soak-org", "name": "soak", "api_url": self.server.base,
                                    "proxy_url": self.server.base, "is_universal_api": True}]}
        elif self.path.startswith("/api/project/register"):
            result = {"project": project}
        elif "project" in self.path:
            result = {"objects": [project], **project}
        else:
            result = {}
        encoded = json.dumps(result).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    do_GET = do_POST


class Transcript:
    def __init__(self, path, session, rng):
        self.path, self.session, self.rng = path, session, rng
        self.file = path.open("a")
        self.parent = None
        self.last_compact = 0

    def text(self, kib):
        return "".join(self.rng.choices("abcdefghijklmnopqrstuvwxyz ", k=kib * 1024))

    def write(self, kind, **fields):
        uid = str(uuid.uuid4())
        record = {"type": kind, "uuid": uid, "parentUuid": self.parent,
                  "sessionId": self.session, "cwd": "/soak", "version": "2.1.310",
                  "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  "isSidechain": False, "userType": "external", **fields}
        self.file.write(json.dumps(record) + "\n")
        self.file.flush()
        self.parent = uid

    def assistant(self, content, stop="end_turn"):
        self.write("assistant", requestId="req_" + uuid.uuid4().hex,
                   message={"id": "msg_" + uuid.uuid4().hex, "type": "message", "role": "assistant",
                            "model": "claude-soak", "content": content, "stop_reason": stop,
                            "usage": {"input_tokens": 1000, "output_tokens": 200}})


class Run:
    def __init__(self, args, root, env, backend):
        self.args, self.root, self.env, self.backend = args, root, env, backend
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.turns = self.compactions = self.hooks = self.failures = 0
        self.error = None
        self.started = time.monotonic()
        self.deadline = self.started + args.duration if args.duration else float("inf")
        self.timings = (root / "turns.jsonl").open("a", buffering=1)

    def hook(self, session, transcript, event, **fields):
        payload = {"session_id": session, "transcript_path": str(transcript.path), "cwd": "/soak",
                   "permission_mode": "bypassPermissions", "hook_event_name": event, **fields}
        result = subprocess.run([str(self.args.daemon_bin), "hook", "--source", "claude-code"],
                                input=json.dumps(payload).encode(), env=self.env,
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=60)
        with self.lock:
            self.hooks += 1
            self.failures += result.returncode != 0
        if result.returncode:
            raise RuntimeError(f"{event}: {result.stderr.decode(errors='replace')[-1000:]}")

    def session(self, index):
        rng = random.Random(self.args.seed + index)
        session = str(uuid.uuid4())
        transcript = Transcript(self.root / "agent" / f"{session}.jsonl", session, rng)
        hook = lambda event, **kw: self.hook(session, transcript, event, **kw)
        try:
            # Stagger startup while retaining every session until process cleanup.
            if self.stop.wait(index * min(.2, 60 / self.args.turns_per_minute / self.args.sessions)):
                return
            hook("SessionStart", source="startup", model="claude-soak")
            turn = 0
            while not self.stop.is_set() and time.monotonic() < self.deadline:
                started = time.monotonic()
                target = interval(rng, self.args.turns_per_minute, self.args.sigma)
                transcript.write("user", message={"role": "user", "content": f"Turn {turn}: " + transcript.text(1)})
                hook("UserPromptSubmit", prompt=f"Synthetic turn {turn}")
                for tool in range(self.args.tools_per_turn):
                    if self.stop.is_set() or time.monotonic() >= self.deadline:
                        return
                    if self.args.compact_kib and transcript.file.tell() - transcript.last_compact >= self.args.compact_kib * 1024:
                        transcript.write("system", subtype="compact_boundary", content="Compacted",
                                         compactMetadata={"trigger": "auto", "preTokens": 190000,
                                                          "preservedMessages": {"uuids": []}})
                        transcript.write("user", isCompactSummary=True,
                                         message={"role": "user", "content": "Summary: " + transcript.text(8)})
                        hook("PostCompact", trigger="auto", compact_summary="Summary")
                        transcript.last_compact = transcript.file.tell()
                        with self.lock:
                            self.compactions += 1
                    tid = "toolu_" + uuid.uuid4().hex
                    inp = {"command": f"cat synthetic-{tool}.txt"}
                    transcript.assistant([{"type": "thinking", "thinking": transcript.text(1), "signature": "sig"},
                                          {"type": "tool_use", "id": tid, "name": "Bash", "input": inp}], "tool_use")
                    hook("PreToolUse", tool_name="Bash", tool_input=inp, tool_use_id=tid)
                    output = transcript.text(self.args.result_kib)
                    transcript.write("user", message={"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": tid, "content": output}]})
                    hook("PostToolUse", tool_name="Bash", tool_input=inp, tool_use_id=tid,
                         tool_response={"stdout": output, "stderr": "", "interrupted": False}, duration_ms=50)
                    until = min(self.deadline, started + target * (tool + 1) / (self.args.tools_per_turn + 1))
                    if self.stop.wait(max(0, until - time.monotonic())):
                        return
                transcript.assistant([{"type": "text", "text": f"Completed turn {turn}"}])
                hook("Stop", stop_hook_active=False, last_assistant_message="Done", background_tasks=[], session_crons=[])
                self.stop.wait(max(0, min(self.deadline, started + target) - time.monotonic()))
                with self.lock:
                    self.turns += 1
                    self.timings.write(json.dumps({"session": index, "turn": turn + 1,
                        "target_seconds": target, "actual_seconds": time.monotonic() - started,
                        "elapsed_seconds": time.monotonic() - self.started}) + "\n")
                turn += 1
        except Exception as error:
            with self.lock:
                self.error = str(error)
            self.stop.set()
        finally:
            transcript.file.close()


def sample(run, pid):
    row = {"elapsed_seconds": round(time.monotonic() - run.started, 2)}
    ps = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True, check=True)
    row["rss_mib"] = int(ps.stdout.strip()) / 1024
    with run.lock:
        row.update(turns=run.turns, compactions=run.compactions, hooks=run.hooks, failed_hooks=run.failures)
    with run.backend.lock:
        row.update(upload_requests=run.backend.requests, upload_bytes=run.backend.bytes)
    for key in ["data_bytes", "data_allocated_bytes", "journal_bytes", "transcript_bytes", "sqlite_bytes",
                "log_bytes", "ledger_bytes", "retained_ops", "retained_op_bytes", "origins", "span_state", "batches", "receipts", "routes", "head_sum", "tail_sum", "database_errors"]:
        row[key] = 0
    for path in (run.root / "data").rglob("*"):
        if not path.is_file():
            continue
        try:
            st = path.stat()
            row["data_bytes"] += st.st_size
            row["data_allocated_bytes"] += st.st_blocks * 512
            category = path.relative_to(run.root / "data").parts[0]
            key = {"journal": "journal_bytes", "transcripts": "transcript_bytes", "pipeline": "sqlite_bytes",
                   "delivery-ledger": "ledger_bytes"}.get(category, "log_bytes" if path.suffix == ".log" else None)
            if key:
                row[key] += st.st_size
        except FileNotFoundError:
            pass
    for path in (run.root / "data" / "pipeline").glob("*.sqlite"):
        try:
            with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=.2) as db:
                # One read transaction makes heads, publication, and backlog consistent.
                db.execute("BEGIN")
                count, size = db.execute("SELECT count(*),coalesce(sum(length(value)),0) FROM ops").fetchone()
                row["retained_ops"] += count
                row["retained_op_bytes"] += size
                for table in ["origins", "span_state", "batches", "receipts", "heads"]:
                    row["routes" if table == "heads" else table] += db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                row["head_sum"] += db.execute("SELECT coalesce(sum(position),0) FROM heads").fetchone()[0]
                row["tail_sum"] += db.execute("SELECT tail FROM publication WHERE id=1").fetchone()[0]
        except sqlite3.Error:
            row["database_errors"] += 1
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daemon-bin", type=Path, required=True, help="standalone all-feature release binary")
    parser.add_argument("--output", type=Path, help="new output directory (default: temporary directory)")
    parser.add_argument("--duration", type=float, default=0, help="workload seconds; 0 runs until Ctrl-C")
    parser.add_argument("--sessions", type=int, default=10)
    parser.add_argument("--turns-per-minute", type=float, default=10, help="target rate per session; excludes generator overhead")
    parser.add_argument("--sigma", type=float, default=1.2, help="lognormal spread; 0 gives constant turn lengths")
    parser.add_argument("--tools-per-turn", type=int, default=20)
    parser.add_argument("--result-kib", type=int, default=8)
    parser.add_argument("--compact-kib", type=int, default=700, help="per-session compaction interval; 0 disables compaction")
    parser.add_argument("--sample-seconds", type=float, default=2)
    parser.add_argument("--settle-seconds", type=float, default=60, help="final quiet period; no SessionEnd is sent")
    parser.add_argument("--trend-window-seconds", type=float, default=180, help="rolling RSS trend window during active workload")
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()
    if min(args.sessions, args.turns_per_minute, args.tools_per_turn, args.sample_seconds, args.trend_window_seconds) <= 0 or min(args.duration, args.sigma, args.result_kib, args.compact_kib, args.settle_seconds) < 0:
        parser.error("rates and counts must be positive; durations, sizes, and sigma must be nonnegative")
    args.daemon_bin = args.daemon_bin.resolve(strict=True)
    root = args.output.resolve() if args.output else Path(tempfile.mkdtemp(prefix="bt-soak-"))
    if args.output:
        root.mkdir(parents=True, exist_ok=False)
    (root / "agent").mkdir()
    backend = Backend()
    server_thread = threading.Thread(target=backend.serve_forever, daemon=True)
    server_thread.start()
    env = {k: v for k, v in os.environ.items() if not k.startswith(("BRAINTRUST_", "BT_DAEMON_", "BT_TRACE_", "BRAINTRUST_DAEMON_"))}
    config = {"trace_to_braintrust": True, "route": {"destination": {"type": "project_logs", "project_name": "soak"}, "flush_mode": "flush_on_turn_end"}}
    (root / "braintrust.json").write_text(json.dumps(config))
    env.update(BRAINTRUST_API_KEY="sk-local-soak", BRAINTRUST_NO_INPUT="1", BRAINTRUST_API_URL=backend.base,
               BRAINTRUST_APP_URL=backend.base, BRAINTRUST_DAEMON_CONFIG=str(root / "braintrust.json"),
               BRAINTRUST_DAEMON_SOCKET=str(root / "daemon.sock"), BRAINTRUST_DAEMON_DATA_DIR=str(root / "data"))
    (root / "manifest.json").write_text(json.dumps({**vars(args), "daemon_bin": str(args.daemon_bin), "output": str(root), "idle_retirement": False}, default=str, indent=2))
    log = (root / "process.log").open("w")
    daemon = subprocess.Popen([str(args.daemon_bin), "serve", "--socket", str(root / "daemon.sock"), "--data-dir", str(root / "data"), "--idle-timeout-secs", "0", "--session-idle-timeout-secs", "0"], env=env, stdout=log, stderr=log)
    run = Run(args, root, env, backend)
    workers = []
    peak_rss = 0
    trend = deque()
    def stop(_signal, _frame):
        run.stop.set()
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        for _ in range(100):
            if daemon.poll() is not None:
                raise RuntimeError(f"daemon exited; see {root / 'process.log'}")
            if (root / "daemon.sock").exists():
                break
            time.sleep(.05)
        else:
            raise RuntimeError("daemon socket was not created")
        print(f"Results: {root}\nDaemon PID: {daemon.pid}; {args.sessions} sessions at {args.turns_per_minute} turns/min/session", flush=True)
        for index in range(args.sessions):
            worker = threading.Thread(target=run.session, args=(index,))
            worker.start()
            workers.append(worker)
        writer = None
        settling_until = None
        next_report = 0
        with (root / "samples.csv").open("w", buffering=1) as output:
            while True:
                if daemon.poll() is not None:
                    raise RuntimeError("daemon exited during workload")
                row = sample(run, daemon.pid)
                peak_rss = max(peak_rss, row["rss_mib"])
                active = any(worker.is_alive() for worker in workers)
                if active:
                    trend.append((row["elapsed_seconds"], row["rss_mib"]))
                    while len(trend) > 1 and trend[0][0] < row["elapsed_seconds"] - args.trend_window_seconds:
                        trend.popleft()
                row["rss_growth_mib_per_minute"] = slope(trend)
                row["phase"] = "workload" if active else "quiet"
                if writer is None:
                    writer = csv.DictWriter(output, fieldnames=list(row))
                    writer.writeheader()
                writer.writerow(row)
                if row["elapsed_seconds"] >= next_report:
                    print(f"{row['elapsed_seconds']:.0f}s turns={row['turns']} RSS={row['rss_mib']:.1f}MiB growth={row['rss_growth_mib_per_minute']:.2f}MiB/min pending={row['retained_ops']} origins={row['origins']} disk={row['data_bytes']/2**20:.1f}MiB", flush=True)
                    next_report += 30
                if not active:
                    if settling_until is None:
                        settling_until = time.monotonic() + args.settle_seconds
                        print("Workload stopped; sampling the resident sessions during quiet period", flush=True)
                    if time.monotonic() >= settling_until:
                        break
                time.sleep(args.sample_seconds)
        status = subprocess.run([str(args.daemon_bin), "--json", "status", "--socket", str(root / "daemon.sock")], env=env, capture_output=True, text=True, check=True, timeout=30)
        (root / "status.json").write_text(status.stdout)
        summary = {"peak_rss_mib": peak_rss, "final": row, "error": run.error,
                   "sessions_resident": len(json.loads(status.stdout)["sessions"])}
        (root / "summary.json").write_text(json.dumps(summary, indent=2))
        print(json.dumps(summary, indent=2), flush=True)
        if run.error:
            raise RuntimeError(run.error)
    finally:
        run.stop.set()
        for worker in workers:
            worker.join(timeout=65)
        run.timings.close()
        # Measure resident sessions first, then let the isolated daemon drain.
        daemon.send_signal(signal.SIGINT)
        try:
            daemon.wait(timeout=10)
        except subprocess.TimeoutExpired:
            daemon.kill()
            daemon.wait()
        log.close()
        backend.shutdown()
        backend.server_close()


if __name__ == "__main__":
    main()
