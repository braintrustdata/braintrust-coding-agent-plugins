# AGENTS.md — trace-codex

This opt-in plugin is a thin hook shim over the shared Rust tracing daemon in
the `bt` CLI:

```text
Codex hook → bin/codex-hook.sh|cmd → bt daemon hook --source codex
           → local socket/pipe → per-session Rust translator → Braintrust
```

The launcher must always fail open: log errors to stderr and exit 0 so tracing
cannot fail or stall a Codex turn. Keep hook commands platform-neutral through
the `.sh`/`.cmd` launchers.

Agent-specific state-machine logic belongs in
`bt-daemon/src/translate/codex.rs`, not in this plugin.

Run:

```bash
make test
```

The shared daemon crate owns translator and pipeline tests:

```bash
cd ../../../../../bt-daemon
cargo test
```
