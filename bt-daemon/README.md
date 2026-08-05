# bt-daemon

Shared Rust project for Braintrust coding-agent tracing plugins. A local,
stateful daemon that plugin **hook shims** forward events to; it owns the
event→trace state machine and sends spans to Braintrust out-of-band. See
[`docs/protocol.md`](docs/protocol.md) for the wire contract.

> **Placeholder name** — the real name is TBD. The subcommand framing
> (`serve` / `hook` / `status` / `import`) should survive a rename.

## Layout

One self-contained Cargo crate, liftable to its own repo by copying
`bt-daemon/` verbatim:

- `src/wire` — the wire protocol module: envelope types + JSON-RPC framing.
- `src/translate` and `src/sink` — agent state machines and Braintrust output.
- `src/lib.rs` — the embeddable library: clap `Args` + async entry points
  (`run_serve`, `run_hook`, `run_status`, `run_import`). This is what `bt`
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

Legacy clients that send a resolved `SessionConfig` remain wire-compatible
during migration.

## Shared plugin settings

Codex, Claude Code, and future hook plugins read the same non-credential
settings file through the embedded daemon. Set `BT_DAEMON_CONFIG` explicitly,
or use `config.json` under `BT_DAEMON_DATA_DIR` (by default
`~/.braintrust/state/bt-daemon/config.json` on Unix and
`%LOCALAPPDATA%\Braintrust\bt-daemon\config.json` on Windows).

See [`config.json.example`](config.json.example). Supported settings are
`traceToBraintrust`, `project`, `flushOnTurnEnd`, and
`additionalMetadata`. File values override plugin environment fallbacks.
Credentials, auth tokens, organization selection, and backend URLs are not
settings here; production resolves them through `bt`.

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

## Status

Phases 0–5 are implemented: protocol, daemon lifecycle, Braintrust sink,
Codex and Claude translators, `bt daemon` integration, and thin hook shims for
both shipped plugins. Restart recovery replays the redacted journal with
deterministic span ids, so resubmitted rows merge into the same spans instead
of creating duplicates. Claude lifecycle entries embed transcript snapshots, so
recovery does not depend on mutable external paths. Explicit turn/session-end
flushes are bounded, and sessions can target project logs or an experiment.

Windows named-pipe transport, detached spawning, lifecycle handover, and
cross-platform pipeline tests are implemented. The remaining host follow-ups
are OpenCode and pi, which are not present in this monorepo.

- The Braintrust sink pins `braintrust-sdk-rust` commit `d33e806`, which adds
  deterministic span ids, `span_origin`/`span_attributes` passthrough, and
  per-session credential isolation. This follows the same exact-revision Git
  dependency policy as `bt`.
