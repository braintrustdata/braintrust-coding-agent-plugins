# bt-daemon

The Rust tracing daemon embedded in `bt`. Plugins send it native agent events;
it builds spans, journals events for recovery, and uploads traces to Braintrust.
See [the protocol](docs/protocol.md) for the wire contract.

## Layout

The daemon is one self-contained Cargo crate:

- `src/wire` — the wire protocol module: envelope types + JSON-RPC framing.
- `src/translate` and `src/sink` — agent state machines and Braintrust output.
- `src/lib.rs` — command arguments and async entry points, including
  `run_serve`, `run_hook`, `run_status`, `run_import`, and `run_traced`.
- `src/trace_command.rs`, `src/trace_runtime.rs`, and `src/setup.rs` — the
  complete mounted `bt trace` command schema, dispatch, daemon lifecycle, and
  agent-specific persistent setup behavior. Hosts supply only credential and
  destination-resolution services.
- `src/main.rs` — the standalone **`bt-daemon` binary**, compiled only with
  the `cli` feature for isolated testing/development. Env/flag static-token
  auth only; not an end-user artifact.

## Authentication

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
- Muse Code: `$XDG_CONFIG_HOME/muse/braintrust.json`, falling back to
  `~/.config/muse/braintrust.json`
- Claude Code: `~/.claude/braintrust.json`
- OpenCode: `$XDG_CONFIG_HOME/opencode/braintrust.json`, falling back to
  `~/.config/opencode/braintrust.json`
- Pi: `~/.pi/agent/braintrust.json`
- Grok: `~/.grok/braintrust.json`
- Antigravity: `~/.gemini/config/braintrust.json`

`BT_DAEMON_CONFIG` can override the path for isolated tests and managed hosts.

See [`config.json.example`](config.json.example). `trace_to_braintrust` controls
enablement and `route` stores the selected profile, organization, typed
destination, flush mode, and metadata. Setup saves a stable `route.auth.profile_id`; older files can use
`route.auth.profile`. If neither is set, `bt` resolves the default profile. Credentials and backend URLs are never stored here;
production resolves and refreshes them through `bt`. `bt trace run` supplies a
process-local settings overlay and never changes any of these files.

### JavaScript span plugins

`--plugin PATH` registers a synchronous ES module that transforms each
sink-neutral span row after translation and immediately before delivery. Repeat
the flag to compose plugins from left to right. `enable` persists its ordered list
for ordinary agent sessions. Managed runs and imports are isolated from that
list and use only the `--plugin` flags passed to their command. Each path is
canonicalized to an absolute path before it is validated or stored.

Each module must default-export a synchronous function. It receives a span and
`{ operation, source, session_id, env }`, and must return a JSON-compatible span
object. Span, root, and parent identities cannot be changed:

```js
// redact.mjs
function redact(value) {
  if (typeof value === "string") {
    return value.replace(/sk-[A-Za-z0-9_-]+/g, "[REDACTED]");
  }
  if (Array.isArray(value)) return value.map(redact);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, child]) => [key, redact(child)]),
    );
  }
  return value;
}

export default function redactSpan(span) {
  const next = { ...span };
  for (const field of ["input", "output", "error"]) {
    if (field in next) next[field] = redact(next[field]);
  }
  return next;
}
```

The context can drive a second transform without changing the first one:

```js
// tag-ci.mjs
export default function tagCi(span, context) {
  if (!context.env.CI) return span;

  return {
    ...span,
    tags: [...new Set([...(span.tags ?? []), "ci"])],
    metadata: {
      ...(span.metadata ?? {}),
      deployment: context.env.DEPLOYMENT_ENV ?? "unknown",
      trace_source: context.source,
    },
  };
}
```

Register both transforms persistently for ordinary Codex sessions. The
redactor runs first and its returned span becomes the tagger's input:

```bash
bt trace enable codex --plugin ./redact.mjs --plugin ./tag-ci.mjs
```

`run` and `import` plugins apply only to that command. They replace, rather than
merge with, plugins saved by `enable`:

```bash
# Only local.mjs runs; redact.mjs and tag-ci.mjs remain global enable behavior.
bt trace run --plugin ./local.mjs codex -- "summarize this change"

# Only sanitize-history.mjs transforms spans produced by this import.
bt trace import codex SESSION_ID --plugin ./sanitize-history.mjs
```

The journal stores raw input events, not transformed spans. After daemon
recovery, replayed events therefore pass through the resumed session's current
route: ordinary sessions use the current globally configured plugins, while a
managed session continues using only that run's isolated plugins.

`context.operation` is `"insert"` or `"merge"`; `context.source` and
`context.session_id` identify the translated event stream; and `context.env`
contains the daemon process environment. Environment variable names are
uppercased on Windows so common lookups such as `context.env.PATH` remain
portable.

The environment map is captured from the daemon process when each worker-local
span processor is constructed. Plugins execute in bounded, thread-local
QuickJS runtimes with no filesystem or network host APIs. Modules must be
self-contained and transforms must be stateless: module globals belong to a
worker thread, not a session. Every configured plugin is mandatory. If any
plugin fails, the daemon discards that span operation instead of delivering
untransformed data, and that worker continues discarding operations that would
use the failed plugin. Modifying the plugin file causes workers to retry it. The
raw event remains journaled, so restarting the daemon after fixing the plugin
replays the withheld data through the current chain.

Plugin failures are deduplicated in the daemon's private local state, including
the raw QuickJS exception and stack. Inspect them with:

```bash
bt trace doctor codex
```

The doctor output reports the plugin path, exception, occurrence count, and
timestamps. Managed-run diagnostics are copied out of their temporary daemon
directory before it is removed.
Plugins are trusted local code: although they have no host APIs, they can copy
environment values into spans that are delivered to Braintrust. Read only the
specific variables needed by the transform; never attach `context.env` itself.

### Additional root metadata

`additional_metadata` is a JSON object merged into each traced session's root
span. Standard agent metadata (such as the session id, source, and workspace)
takes precedence over keys supplied by users. Set it persistently during setup
or provide it for one invocation with `bt trace run`. Both accept
`--additional-metadata` or `BRAINTRUST_ADDITIONAL_METADATA`:

```bash
bt trace enable claude --additional-metadata '{"team":"platform"}'
BRAINTRUST_ADDITIONAL_METADATA='{"ci":true,"run_id":"'"$JOB_ID"'"}' \
  bt trace run codex -- "summarize this change"
```

`bt trace enable`, `bt trace run`, and `bt trace import` are explicit,
user-invoked commands, so all three accept routing (profile, organization,
project, destination) and `additional_metadata` from either a flag or the
matching `BRAINTRUST_*` environment variable, with the flag winning if both
are set. `bt trace hook` is different: it fires automatically on every event,
so it never reads the environment directly — its route comes only from the
agent's persisted `braintrust.json`, or, for a child launched by `bt trace
run`, from that invocation's settings. An explicit `--additional-metadata`
flag or environment variable on `bt trace run` overrides the persisted route
for that invocation only, without mutating the file.

### Root-span tags

Use repeatable `--tag` options to add filterable Braintrust tags to every root
span produced by a route. The option is available on persistent setup, one-off
runs, and transcript imports.

```bash
bt trace enable claude --tag ci --tag release-validation
bt trace run codex --tag ci -- "summarize this change"
bt trace import claude session-id --tag historical-import
```

For automation, set `BRAINTRUST_TAGS` to a comma-separated list before running
one of those commands:

```bash
BRAINTRUST_TAGS=ci,release-validation bt trace run codex -- "summarize this change"
```

Use `bt trace disable <agent>` to remove the installed tracing plugin and its
Braintrust settings. `bt trace setup <agent>` remains an alias for `bt trace
enable <agent>` for backwards compatibility.

## Build and test

```bash
cd bt-daemon
cargo test --locked                             # Library and pipeline tests
cargo test --all-features --locked              # Include the standalone CLI
cargo build --features cli --locked --bin bt-daemon
```

CI runs the all-feature build, test suite, and Clippy on Linux, macOS, and
Windows. The pipeline integration tests use Unix-domain sockets on Unix and
real Windows named pipes on Windows. Real-agent tests are separate from the
default suite; see the [test harness guide](tests/support/README.md).

## Local debugging

From the monorepo root, in a Unix shell:

```bash
export BT_DAEMON_SOCKET=/tmp/btd.sock BT_DAEMON_DATA_DIR=/tmp/btd
cargo build --manifest-path bt-daemon/Cargo.toml --features cli --locked --bin bt-daemon
echo '{"session_id":"s1","hook_event_name":"SessionStart"}' | ./bt-daemon/target/debug/bt-daemon hook --source debug
echo '{"session_id":"s1","hook_event_name":"Stop"}'         | ./bt-daemon/target/debug/bt-daemon hook --source debug
./bt-daemon/target/debug/bt-daemon status
# Inspect journal/ and spans/ under $BT_DAEMON_DATA_DIR
```

The first `hook` spawns the daemon detached; it idles out after 5 minutes.

`import <codex|claude|antigravity|muse> <session-id>` has a different purpose from restart
recovery. It locates the native transcript in the selected agent's standard
session store, synthesizes the lifecycle triggers that can be recovered from
that transcript, and sends them through the normal translator and sink to
create a trace for the past session. Hook-only facts absent from a native
transcript are not invented.

Muse imports invoke its documented local `muse export --session` interface and
read export schema version 1. `import muse --all` enumerates durable session
IDs through Muse's read-only MSP `session/list` interface, then exports each
completed session. Exports are parsed incrementally, one session at a time.
Muse Code 1.1.1 setup captures the six verified lifecycle and model hooks:
`SessionStart`, `UserPromptSubmit`, `PreLLMCall`, `PostLLMCall`, `Stop`, and
`SessionEnd`. Tool, permission, subagent, and compaction details are not
available from those hooks and are not inferred by the live translator.
`--attach` is not available yet because it needs a durable
MSP subscription and cursor-following implementation. Muse also has no safe
invocation-local configuration overlay, so `run muse` is deliberately
unavailable; use persistent `enable muse` setup.

Add `--attach` to keep following an active Codex, Claude, or Antigravity transcript until
Ctrl-C. `run <codex|claude|opencode|pi> [ARGS...]` launches the selected agent with
inherited stdio and injects Braintrust hooks or an adapter for that invocation, so it
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

## Recovery and storage

Capture requests return after the event is flushed to its journal. Daemon
workers handle authentication, translation, and delivery. Restart recovery
replays journaled events with deterministic span IDs, so repeated delivery
updates the same spans.

Claude and Codex events reference daemon-owned transcript mirrors. Grok uses
separate updates and events mirrors. Replay does not depend on the original
transcript staying at its old path.

The daemon streams journals and transcripts, applies queue backpressure, and
retires idle sessions. Retired sessions are rebuilt from their journals when
another event arrives. Journals and mirrors preserve conversation content;
credentials are excluded from journaled routing data.

Recovery files are removed after seven days without modification. Cleanup runs
at startup and hourly while the daemon is running. This retention period is
currently fixed in `src/server.rs`.

## Platform and integration coverage

The daemon supports Linux, macOS, and Windows, using Unix sockets or Windows
named pipes. Translators exist for Antigravity, Claude Code, Codex, Grok,
OpenCode, and Pi. CI runs packaged Claude Code, Codex, OpenCode, and Pi against
mock inference and mock Braintrust ingest on all three platforms.

The Rust SDK is pinned to an exact Git revision in [Cargo.toml](Cargo.toml).
Use `--locked` for reproducible builds.
