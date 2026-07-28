# bt-daemon wire protocol (v1)

Status: **frozen for the prototype.** This is the contract between plugin shims
(`hook` clients) and the daemon (`serve`), and between the embedded-in-`bt`
front-end and the standalone test binary.

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
  "capabilities": { "sources": ["codex", "claude-code", "debug"] }
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
`accepted: true` means enqueued to the session's ordered queue and journaled.
The daemon never fails the caller's turn for a downstream (Braintrust) error;
those are handled asynchronously and surfaced via `status.get`.

### `session.flush` (request)

Block until the session's spans are delivered, or `timeout_ms` elapses.

Params:
```json
{ "session_id": "…", "timeout_ms": 10000 }
```
Result:
```json
{ "flushed": true, "pending": 0 }
```
`flushed: false` with `pending > 0` means the timeout was hit with work
outstanding. Used by session-end hooks and flush-on-turn-end mode.

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
      "queued": 0,
      "spans_emitted": 42,
      "permalink": "https://www.braintrust.dev/app/…",
      "last_error": null
    }
  ]
}
```
Powers a `status` CLI and pi's trace-link widget.

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
  "payload": { "…raw agent-native hook payload…": true },
  "config": {
    "auth": {
      "token": "sk-…",
      "api_url": "https://api.braintrust.dev",
      "app_url": "https://www.braintrust.dev",
      "org_name": "acme"
    },
    "project": "codex",
    "parent_span_id": null,
    "root_span_id": null,
    "flush_mode": "fire_and_forget",
    "additional_metadata": { "…": "…" }
  }
}
```

Field notes:

- **`source`** selects the daemon-side translator. `debug` is a built-in
  pass-through translator used by the prototype and tests.
- **`session_id`** is the per-session queue + state key. The shim extracts it
  from the payload (default JSON field `session_id`, overridable with
  `--session-id-field`); both Claude Code and Codex use `session_id`.
- **`event`** is the agent-native hook name (not normalized). Extracted from
  the payload (default field `hook_event_name`, overridable with `--event`).
- **`ts_ms`** is stamped by the shim **at capture time** (epoch millis),
  because the daemon processes later than the hook fired. Never stamped by the
  daemon.
- **`payload`** is opaque to transport and to everything except the translator
  for `source`.
- **`config`** carries shim-resolved credentials and trace settings. The shim
  attaches it on **every** event (stateless shim); the daemon keeps the latest
  per session and only re-inits the Braintrust sink when it changes. `auth` is
  filled by `bt`'s `resolve_auth` when embedded, or from env/flags in the
  standalone binary. `flush_mode` ∈ `fire_and_forget` | `flush_on_turn_end`.

### Redaction

`config.auth.token` (and any nested secret) is **never** written to the
journal or logs. The journal stores the envelope with `config.auth` reduced to
a non-secret fingerprint (`{ "api_url", "app_url", "org_name", "token_sha256_prefix" }`)
so replay can detect a credential change without persisting the secret; on
replay the live credentials must be re-supplied.

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
- **Version handover.** `initialize` compares versions. A newer client sends
  `daemon.shutdown`, waits until the endpoint no longer accepts connections,
  and spawns its own daemon. In-flight session state is rebuilt from the
  journal.

## Durability & idempotence

- **Journal (WAL).** Every accepted event is appended (auth-redacted) to
  `<data_dir>/journal/<session_id>.ndjson` before/at enqueue. `data_dir`
  defaults to `$XDG_STATE_HOME/braintrust/bt-daemon` or
  `$HOME/.braintrust/state/bt-daemon` on Unix, and
  `%LOCALAPPDATA%\Braintrust\bt-daemon` on Windows. On restart the daemon
  rebuilds a session's state by replaying its journal through the translator.
  Journals are GC'd after 7 days.
- **Deterministic span ids.** Translators derive span ids as UUIDv5 over stable
  keys (`session_id`, `turn_id`, `call_id`, …) so a replayed re-emit merges
  server-side (`_is_merge`) instead of duplicating.
