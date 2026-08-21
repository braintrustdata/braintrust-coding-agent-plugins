# bt-daemon wire protocol (v1)

Status: **frozen for the prototype.** This is the contract between plugin shims
(`hook` clients) and the daemon (`serve`), and between the embedded-in-`bt`
front-end and the standalone test binary.

Hook clients use an agent-specific non-credential `braintrust.json` file. Codex,
Claude Code, OpenCode, and Pi therefore retain independent persistent profile,
organization, and destination selections. `$BT_DAEMON_CONFIG` may explicitly
override the file path. The hook front-end applies `trace_to_braintrust` and the
stored `route` before sending an event. A managed run sets
`BT_TRACE_INVOCATION_SETTINGS` for only its child process tree, keeping global
agent configuration and generated hook commands unchanged.
The profile is optional and defaults through `bt`. Credentials and backend
URLs are resolved and refreshed inside the daemon and never enter this file.

`PROTOCOL_VERSION = 1`.

## Transport

- **Unix domain socket (Linux/macOS).** Default path resolution (first match wins):
  1. `--socket <path>` flag / `BT_DAEMON_SOCKET` env (explicit override; used by
     tests to sandbox a daemon per test).
  2. `$XDG_RUNTIME_DIR/braintrust/daemon.sock` if `XDG_RUNTIME_DIR` is set.
  3. `$HOME/.braintrust/run/daemon.sock`.
  The containing directory is created mode `0700`. macOS caps `sockaddr_un`
  paths at 104 bytes; all defaults stay well under.
- **Framing: newline-delimited JSON.** Exactly one JSON value per line,
  terminated by `\n`. `serde_json` never emits a bare newline inside a value,
  so `\n` is an unambiguous frame delimiter. Max line length is bounded
  (default 64 MiB) to cap memory on a malformed/huge payload; over-length lines
  are a protocol error and close the connection.
- **Windows named pipe.** `--socket` / `BT_DAEMON_SOCKET` may provide an
  explicit full pipe name. Otherwise the daemon uses
  `\\.\pipe\braintrust-bt-daemon-<user-hash>`, where the suffix is derived
  from the Windows domain and user name so concurrent users do not share a
  daemon. The pipe is byte-mode, so framing is identical to Unix.

## RPC: JSON-RPC 2.0

Each frame is a JSON-RPC 2.0 Request, Response, or Notification.

Request:
```json
{ "jsonrpc": "2.0", "id": 1, "method": "event.log", "params": { ... } }
```
Response (success):
```json
{ "jsonrpc": "2.0", "id": 1, "result": { ... } }
```
Response (error):
```json
{ "jsonrpc": "2.0", "id": 1, "error": { "code": -32602, "message": "...", "data": { ... } } }
```
Notification (no `id`, no response):
```json
{ "jsonrpc": "2.0", "method": "event.log", "params": { ... } }
```

`id` is an integer or string. Error `code` uses the JSON-RPC reserved ranges
for protocol errors (`-32700` parse, `-32600` invalid request, `-32601` method
not found, `-32602` invalid params, `-32603` internal); application errors use
`-32000 … -32099`.

### Ordering & delivery

A subprocess-style shim opens a connection, does one `event.log`, and exits.
Per-session ordering is guaranteed because (a) the agent runs hooks in blocking
mode, so it does not fire the next hook until the current one returns, and (b)
`event.log` is a **request** whose success response means *the event has been
appended to that session's ordered queue* (not that it has been delivered to
Braintrust). The shim must await that response before exiting. Long-lived
in-process clients (opencode/pi, later) hold one connection and may send
`event.log` as a **notification** for the hot path, relying on the single
connection for ordering.

## Methods

### `initialize` (request)

First message on every connection.

Params:
```json
{
  "protocol_version": 1,
  "client": { "source": "codex", "plugin_version": "1.2.3", "pid": 12345 }
}
```
Result:
```json
{
  "protocol_version": 1,
  "daemon_version": "0.1.0",
  "capabilities": { "sources": ["codex", "claude-code", "opencode", "pi", "debug"] }
}
```
If `protocol_version` is incompatible the daemon returns an application error;
the client decides whether to drop events or (if the client is newer) trigger a
version handover (`daemon.shutdown` → respawn).

### `event.log` (request or notification)

The hot path. Params are the **Envelope** (see below). Request result:
```json
{ "accepted": true }
```
`accepted: true` means durably recorded: normally journaled and enqueued to the
session's ordered queue, or held in the daemon's private correlation journal
while multiple parent calls remain indistinguishable.
For an event that opens a tool call, it also means the daemon has made that
active-tool marker visible to local child-session correlation. A child hook
that runs immediately after its parent's blocking pre-tool hook can therefore
attach without an intervening flush.
The daemon never fails the caller's turn for a downstream (Braintrust) error;
those are handled asynchronously and surfaced via `status.get`. The queue is
bounded, so a session whose sink has stalled applies backpressure here instead
of accumulating events without limit; the event is already journaled by then,
so this costs latency, never data.

### `session.flush` (request)

Block until every route's spans for the session are delivered, or `timeout_ms`
elapses. A session_id may have opened more than one route (see "Multiple
routes per session" below); this flushes all of them.

Params:
```json
{ "session_id": "…", "timeout_ms": 10000 }
```
Result:
```json
{ "flushed": true, "pending": 0, "accepted_sessions": 1 }
```
`flushed: false` with `pending > 0` means the timeout was hit with work
outstanding across one or more routes. `accepted_sessions` counts how many
independent routes this session_id has open. Used by session-end hooks and
flush-on-turn-end mode.

### `managed_run.flush` (request)

Block until every session accepted from one `bt trace run` child process tree
has flushed. Params:
```json
{ "managed_run_id": "…", "timeout_ms": 10000 }
```
The result has the same shape as `session.flush`. The managed-run identifier is
invocation-local and prevents the shared daemon from flushing unrelated agent
sessions.

### `status.get` (request)

Params: `{ "session_id": "…" }` (omit `session_id` for daemon-wide status).
Result:
```json
{
  "daemon_version": "0.1.0",
  "uptime_ms": 123456,
  "sessions": [
    {
      "session_id": "…",
      "source": "codex",
      "route": {
        "auth": { "profile": "work", "org_name": "acme" },
        "destination": { "type": "project_logs", "project_name": "codex" }
      },
      "queued": 0,
      "spans_emitted": 42,
      "permalink": "https://www.braintrust.dev/app/…",
      "last_error": null
    }
  ]
}
```
`sessions` lists one entry **per route**, not per session_id: a session_id
reporting to two destinations appears twice, each entry carrying its own
`route`, counters, and permalink. Powers a `status` CLI and pi's trace-link
widget.

### `daemon.shutdown` (request)

Graceful: stop accepting new events, drain all session queues, flush sinks,
release the local endpoint, exit. Result `{ "ok": true }` is sent before exit.
Used for version handover and by tests.

## Envelope (`event.log` params)

```json
{
  "source": "codex",
  "source_version": "1.2.3",
  "session_id": "0f9d…",
  "event": "PostToolUse",
  "ts_ms": 1753639552123,
  "managed_run_id": "invocation-uuid",
  "payload": { "…raw agent-native hook payload…": true },
  "plugin_env": { "CI": "true" },
  "route": {
    "auth": {
      "profile": "work",
      "org_name": "acme"
    },
    "destination": {
      "type": "project_logs",
      "project_id": "project-uuid",
      "project_name": "codex"
    },
    "flush_mode": "fire_and_forget",
    "additional_metadata": { "…": "…" },
    "span_plugins": ["/absolute/path/redact.mjs"]
  }
}
```

Field notes:

- **`source`** selects the daemon-side translator. `debug` is a built-in
  pass-through translator used by the prototype and tests.
- **`session_id`** identifies the source agent session. Combined with `route`
  it forms the queue + state key (see "Multiple routes per session" below).
  The shim extracts it from the payload (default JSON field `session_id`,
  overridable with `--session-id-field`); both Claude Code and Codex use
  `session_id`.
- **`event`** is the agent-native hook name (not normalized). Extracted from
  the payload (default field `hook_event_name`, overridable with `--event`).
- **`ts_ms`** is stamped by the shim **at capture time** (epoch millis),
  because the daemon processes later than the hook fired. Never stamped by the
  daemon.
- **`payload`** is opaque to transport and to everything except the translator
  for `source`.
- **`plugin_env`** is the event producer's string environment map, exposed to
  span plugins as `context.env`. It is transported live but never journaled.
- **`managed_run_id`** is present only for events inherited from a
  `bt trace run` process tree. It groups native sessions for the final
  invocation flush and is not trace metadata.
- **`capture`** is optional daemon-owned process evidence. After
  `initialize`, the daemon snapshots the connecting client's PID and bounded
  ancestry as PID/start-time pairs and adds it before journaling. Hooks do not
  construct this field and it contains no command line, environment, or
  working-directory data. The daemon uses it as a local side channel to attach
  an instrumented child session to an active parent tool span; agent-native
  input and output payloads are reduced to hashes only when more than one
  active call is a candidate. For Codex, whose hook payload references its
  rollout rather than embedding the prompt, matching also considers at most
  the final 256 KiB of native JSONL records. Active parent-tool state is
  atomically snapshotted under `<data_dir>/correlation/parents` before a
  blocking tool lifecycle hook is acknowledged. The compact snapshot contains
  only process identities, span attachment components, non-secret routing, and
  hashed matching fingerprints. It contains no raw prompt, tool input/output,
  command line, environment, or resolved credential, and lets a child attach
  even if the daemon restarts between the parent spawn and child session start.
- **`route`** carries non-secret auth selection and trace settings. `profile`
  is optional and resolves through `bt`'s default profile when absent;
  `org_name` optionally constrains organization selection. The daemon resolves
  the live credential, pins the returned canonical profile for the lifetime
  of that route's pipeline, and refreshes an expiring lease without changing
  the route. `destination` is required so setup/run must make project or
  parent selection explicit. `flush_mode` ∈ `fire_and_forget` |
  `flush_on_turn_end`. New front-ends set the typed `destination`:
  `project_logs` accepts a project id and/or name, `experiment` accepts an
  experiment id, and `parent_span` carries the complete exported
  `SpanComponents` object.
- **Multiple routes per session.** `session_id` plus the exact `route` forms
  one independent delivery pipeline: its own auth resolution, translator,
  sink, and queue. A session_id is not pinned to a single route — events for
  the same session_id but a different route open a second, fully independent
  pipeline rather than replacing or rejecting the first. This lets one source
  session report concurrently to multiple destinations, including multiple
  organizations (e.g. two `bt trace import` runs, or an active hook capture
  alongside a concurrent import, targeting different projects or orgs for the
  same underlying session).

### Redaction

Live credentials returned by the host provider are **never** written to the
journal, logs, status, or RPC response. The live plugin environment is likewise
omitted because it commonly contains secrets. Envelopes journal only their
non-secret `route`, allowing restart recovery to resolve a fresh lease.

## Daemon lifecycle

- **Spawn-on-demand.** The shim connects; when no endpoint is available it
  spawns the daemon detached (a separate process group on Unix; a detached
  process group on Windows; stdio → log file) using a host-supplied argv
  (`[bt, daemon, serve]` when embedded; `[bt-daemon, serve]` standalone), then
  retries connect with backoff (~50 × 20 ms). `--no-spawn` turns spawning off
  (tests / diagnostics) and makes a missing daemon a hard error.
- **Bind race.** Two shims may spawn simultaneously. The daemon claims the
  endpoint exclusively and probes a rival with `initialize`. Unix removes an
  unresponsive stale socket before rebinding. Windows uses
  `FILE_FLAG_FIRST_PIPE_INSTANCE`; named-pipe names disappear with their last
  handle, so it retries the exclusive claim without filesystem cleanup.
- **Idle exit.** The daemon exits after `--idle-timeout` (default 300 s) with
  zero active sessions and empty queues.
- **Session retirement.** A delivery pipeline with no traffic for
  `--session-idle-timeout` (default 300 s) and an empty queue is flushed and
  dropped: its translator state, sink handles, credential lease, and journal
  handle are released rather than held for the daemon's lifetime. A later
  event rebuilds it from the journal, and deterministic span ids merge the
  re-emitted rows. This matters because the idle exit above requires *every*
  session to be quiet, which for a continuously active user never happens.
- **Version handover.** `initialize` compares versions. A newer client sends
  `daemon.shutdown`, waits until the endpoint no longer accepts connections,
  and spawns its own daemon. In-flight session state is rebuilt from the
  journal.

## Durability & idempotence

Journal recovery and explicit transcript import are separate operations.
Recovery consumes the daemon's auth-redacted event WAL to rebuild state and
may idempotently re-emit rows under their original deterministic ids. The
`import <codex|claude> <session-id> [project_logs:<project-id> |
experiment:<experiment-id>]` command instead locates the selected agent's
native transcript and creates a trace for that past coding-agent session by
routing synthetic lifecycle events through the regular translator. `--parent
<SpanComponents>` attaches it below an exported span and is mutually exclusive
with an object destination.

`--attach` keeps a single translator and sink alive, tails new native records,
and finalizes the active turn on Ctrl-C.
`run <codex|claude|opencode|pi> [ARGS...]` launches the selected agent with
inherited stdio and injects its tracing integration for that invocation, so it
works without plugin setup. Codex and Claude receive live hook configuration;
OpenCode receives its npm plugin through inline OpenCode configuration; Pi
receives its npm extension through `-e`. A private
inherited environment marker makes installed Braintrust plugin hooks no-op for
that managed child, while a private hook flag authorizes the injected hook process.
Codex does not bypass hook trust: the user reviews the injected hook once through
`/hooks`, and Codex reuses its hash-based trust while the definition is unchanged.
The resulting native hook events follow the regular journal, translator, and
sink path; transcript tailing remains specific to `import --attach`.
The managed child inherits a non-secret invocation-settings value containing
`trace_to_braintrust: true` and its immutable `SessionRoute`. This overrides the
persistent setup route only within that process tree. Other agent processes
continue using setup settings, and concurrent managed runs can select distinct
profiles, organizations, and destinations while sharing one daemon.

- **Journal (WAL).** Every accepted event is appended (auth-redacted) to
  `<data_dir>/journal/<session_id>.ndjson` before/at enqueue — one journal
  file per session_id, shared across every route that session_id has opened.
  `data_dir` defaults to `$XDG_STATE_HOME/braintrust/bt-daemon` or
  `$HOME/.braintrust/state/bt-daemon` on Unix, and
  `%LOCALAPPDATA%\Braintrust\bt-daemon` on Windows. On restart the daemon
  rebuilds each route's unfinished correlation state independently, replaying
  only the journal entries whose delivery route matches that pipeline into a
  fresh translator. Span plugin paths are ignored for this comparison so raw
  events can be replayed through the current plugin chain. The resulting rows
  may be resubmitted to repair delivery
  interrupted by a crash, but their deterministic ids target the same backend
  rows and must never create duplicate spans, and a route never receives
  another route's rows. Replay streams the journal and is bounded to the
  bytes recorded before the replaying session was created, so recovering a
  long session costs no more memory than running it. The journal itself is
  never capped or truncated — dropping entries would silently cost recovery
  fidelity — so its size is governed by writing each transcript byte once
  (below) and by GC after 7 days.
- **Transcript mirrors.** Claude transcript files are external mutable state,
  so a lifecycle event must stay replayable after the agent rewrites the path
  it came from. The daemon appends new transcript bytes to
  `<data_dir>/transcripts/<uuid>.jsonl` — one mirror per
  (session_id, transcript path) — and the journal entry carries only
  `_bt_transcript_mirror: {path, mirror, through}`. `through` is the mirror's
  high-water offset at acceptance, which bounds replay to exactly the bytes
  the live run saw. Storing each byte once keeps a journal proportional to a
  session's transcript rather than to its transcript times its turn count.
  Entries written before mirroring instead carry the whole transcript inline
  as `_bt_transcript_snapshot`, which translators still accept. Mirrors are
  GC'd after 7 days like the journal.
- **Managed-run acceptance records.** Alongside the journal, each accepted
  event that carries a `managed_run_id` also appends `{session_id, route}` to
  `<data_dir>/managed-runs/<managed_run_id>.ndjson`. `managed_run.flush` reads
  this record when the daemon that accepted the events has since restarted or
  idle-exited, so flush accounting for a child process tree survives a daemon
  generation change. GC'd after 7 days like the journal.
- **Deterministic span ids.** Translators derive span ids as UUIDv5 over stable
  keys (`session_id`, `turn_id`, `call_id`, …) so a replayed re-emit merges
  server-side (`_is_merge`) instead of duplicating.
