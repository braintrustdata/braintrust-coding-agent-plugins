# bt-daemon

Shared Rust project for Braintrust coding-agent plugins. **Placeholder name** —
the real name is TBD.

Long-term, the shared logic behind the per-agent tracing plugins (currently
TypeScript, e.g. `trace-codex`) moves here to Rust. Right now it's a stub.

## Self-contained

This directory is its own Cargo workspace root (see the empty `[workspace]`
table in `Cargo.toml`), so it does not depend on anything else in the monorepo
and can be moved to a standalone repo by copying `bt-daemon/` as-is.

## Build / run

```bash
cd bt-daemon
cargo run
```
