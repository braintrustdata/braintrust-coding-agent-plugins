# bt-daemon

Shared Rust project for Braintrust coding-agent tracing plugins. A local,
stateful daemon that plugin **hook shims** forward events to; it owns the
event→trace state machine and sends spans to Braintrust out-of-band. See
[`docs/protocol.md`](docs/protocol.md) for the wire contract.

> **Placeholder name** — the real name is TBD. The subcommand framing
> (`serve` / `hook` / `status` / `import` / `run`) should survive a rename.

## Layout

One self-contained Cargo crate, liftable to its own repo by copying
`bt-daemon/` verbatim:

- `src/wire` — the wire protocol module: envelope types + JSON-RPC framing.
- `src/translate` and `src/sink` — agent state machines and Braintrust output.
- `src/lib.rs` — the embeddable library: clap `Args` + async entry points
- `src/trace_command.rs`, `src/trace_runtime.rs`, and `src/setup.rs` — the
  complete mounted `bt trace` command schema, dispatch, daemon lifecycle, and
  agent-specific persistent setup behavior. Hosts supply only credential and
  destination-resolution services.
  (`run_serve`, `run_hook`, `run_status`, `run_import`, `run_traced`). This is what `bt`
  depends on.
- `src/main.rs` — the standalone **`bt-daemon` binary**, compiled only with
  the `cli` feature for isolated testing/development. Env/flag static-token
  auth only; not an end-user artifact.

## Dual consumption and authentication

Hooks send only a non-secret `SessionRoute`: an optional profile and
organization selection plus the trace destination. The long-lived daemon asks
its host's `AuthProvider` for a credential lease, pins the returned canonical
profile to that session, and refreshes expiring leases as needed. Independent
sessions can therefore use different `bt` profiles without exposing tokens to
hook processes or JavaScript plugins.

1. **Embedded in `bt`** (production): the provider uses `bt`'s existing
   profile, OAuth, refresh, keychain, organization, and backend URL machinery.
2. **Standalone binary** (testing): the provider uses `BRAINTRUST_API_KEY` and
   related environment variables.

## Per-agent plugin settings

Each coding agent reads an independent non-credential `braintrust.json` file:

- Codex: `~/.codex/braintrust.json`
- Claude Code: `~/.claude/braintrust.json`
- OpenCode: `$XDG_CONFIG_HOME/opencode/braintrust.json`, falling back to
  `~/.config/opencode/braintrust.json`
- Pi: `~/.pi/agent/braintrust.json`

`BT_DAEMON_CONFIG` can override the path for isolated tests and managed hosts.

See [`config.json.example`](config.json.example). `trace_to_braintrust` controls
enablement and `route` stores the selected profile, organization, typed
destination, flush mode, and metadata. Omitting `route.auth.profile` selects
the default `bt` profile. Credentials and backend URLs are never stored here;
production resolves and refreshes them through `bt`. `bt trace run` supplies a
process-local settings overlay and never changes any of these files.

### JavaScript span plugins

`--plugin PATH` registers a synchronous ES module that transforms each
sink-neutral span row after translation and immediately before delivery. Repeat
the flag to compose plugins from left to right. Setup persists its ordered list;
run and import append invocation plugins after the persisted list. Each path is
canonicalized to an absolute path before it is validated or stored.

```bash
bt trace setup codex --plugin ./redact.mjs --plugin ./tag-ci.mjs
bt trace run --plugin ./local.mjs codex -- "summarize this change"
bt trace import codex SESSION_ID --plugin ./sanitize-history.mjs
```

Each module must default-export a synchronous function. It receives a span and
`{ operation, source, session_id, env }`, and must return a JSON-compatible span
object. Span, root, and parent identities cannot be changed:

```js
export default function mapSpan(span, context) {
  return {
    ...span,
    metadata: { ...span.metadata, deployment: context.env.DEPLOYMENT },
  };
}
```

The environment map is captured from the daemon process when each worker-local
span processor is constructed. Plugins execute in bounded, thread-local
QuickJS runtimes with no filesystem or network host APIs. Modules must be
self-contained and transforms must be stateless: module globals belong to a
worker thread, not a session. If a plugin fails, that worker reports and skips
only that plugin on subsequent spans; the remaining plugins continue to run.

### Additional root metadata

`additional_metadata` is a JSON object merged into each traced session's root
span. Standard agent metadata (such as the session id, source, and workspace)
takes precedence over keys supplied by users. Set it persistently during setup
or provide it for one invocation with `bt trace run`. Both accept
`--additional-metadata` or `BRAINTRUST_ADDITIONAL_METADATA`:

```bash
bt trace setup claude --additional-metadata '{"team":"platform"}'
BRAINTRUST_ADDITIONAL_METADATA='{"ci":true,"run_id":"'"$JOB_ID"'"}' \
  bt trace run codex -- "summarize this change"
```

`bt trace setup`, `bt trace run`, and `bt trace import` are explicit,
user-invoked commands, so all three accept routing (profile, organization,
project, destination) and `additional_metadata` from either a flag or the
matching `BRAINTRUST_*` environment variable, with the flag winning if both
are set. `bt trace hook` is different: it fires automatically on every event,
so it never reads the environment directly — its route comes only from the
agent's persisted `braintrust.json`, or, for a child launched by `bt trace
run`, from that invocation's settings. An explicit `--additional-metadata`
flag or environment variable on `bt trace run` overrides the persisted route
for that invocation only, without mutating the file.

## Build / test

```bash
cd bt-daemon
cargo test                                      # library + pipeline tests
cargo test --features cli                       # also compile/test the CLI
cargo build --features cli --bin bt-daemon      # standalone test binary
```

CI runs the all-feature build, test suite, and Clippy on Linux, macOS, and
Windows. The pipeline integration tests use Unix-domain sockets on Unix and
real Windows named pipes on Windows.

## Try it (standalone, debug sink)

```bash
export BT_DAEMON_SOCKET=/tmp/btd.sock BT_DAEMON_DATA_DIR=/tmp/btd
cargo build --features cli --bin bt-daemon
echo '{"session_id":"s1","hook_event_name":"SessionStart"}' | ./target/debug/bt-daemon hook --source debug
echo '{"session_id":"s1","hook_event_name":"Stop"}'         | ./target/debug/bt-daemon hook --source debug
./target/debug/bt-daemon status
# journaled events:  $BT_DAEMON_DATA_DIR/journal/s1.ndjson
# emitted span rows: $BT_DAEMON_DATA_DIR/spans/s1.ndjson
```

The first `hook` spawns the daemon detached; it idles out after 5 minutes.

`import <codex|claude> <session-id>` has a different purpose from restart
recovery. It locates the native transcript in the selected agent's standard
session store, synthesizes the lifecycle triggers that can be recovered from
that transcript, and sends them through the normal translator and sink to
create a trace for the past session. Hook-only facts absent from a native
transcript are not invented.

Add `--attach` to keep following an active Codex or Claude transcript until
Ctrl-C. `run <codex|claude> [ARGS...]` launches the selected agent with
inherited stdio and injects live Braintrust hooks for that invocation, so it
does not depend on the tracing plugin being installed or enabled. Managed runs
suppress inherited Braintrust plugin hooks to avoid logging the same session
twice; the injected hooks still use the normal daemon translator and sink.
Codex applies its normal hook-review flow, so the first run requires trusting
the injected Braintrust hook through `/hooks`; later runs reuse that trust while
the hook definition remains unchanged.

Managed-run settings are scoped to the launched agent process tree. They enable
tracing for that invocation and override the persistent setup route without
rewriting it, so ordinary agent sessions and concurrent managed runs may use
different profiles, organizations, projects, experiments, or parent spans.

## Status

Phases 0–5 are implemented: protocol, daemon lifecycle, Braintrust sink,
Codex and Claude translators, `bt daemon` integration, and thin hook shims for
both shipped plugins. Restart recovery replays the redacted journal with
deterministic span ids, so resubmitted rows merge into the same spans instead
of creating duplicates. Claude lifecycle entries reference a daemon-owned
transcript mirror, so recovery does not depend on mutable external paths
without re-recording the transcript on every turn. Explicit turn/session-end
flushes are bounded, and sessions can target project logs or an experiment.

Memory is bounded end to end, while on-disk records stay complete: the daemon
never holds a transcript or a whole journal in memory, mirroring and replay
both stream, session queues apply backpressure, and sessions that go quiet are
retired and rebuilt from their journal on the next event. Nothing on disk —
journal, mirror, or conversation content — is capped or truncated; only
in-memory caches are bounded, and each is re-derivable from disk.

Windows named-pipe transport, detached spawning, lifecycle handover, and
cross-platform pipeline tests are implemented. The remaining host follow-ups
are OpenCode and pi, which are not present in this monorepo.

- The Braintrust sink pins `braintrust-sdk-rust` commit `d33e806`, which adds
  deterministic span ids, `span_origin`/`span_attributes` passthrough, and
  per-session credential isolation. This follows the same exact-revision Git
  dependency policy as `bt`.
