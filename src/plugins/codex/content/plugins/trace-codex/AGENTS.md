# trace-codex

This opt-in plugin is a thin hook shim over the shared Rust tracing daemon in
the `bt` CLI:

```text
Codex hook -> bin/codex-hook.sh|cmd -> bt daemon hook --source codex
           -> local socket or named pipe -> Rust translator -> Braintrust
```

The launchers must always fail open: log errors to stderr and exit successfully
so tracing cannot fail a Codex turn. They may discover or install `bt`, map
agent-specific environment compatibility variables, and forward invocation
metadata. They must not parse event sequences, construct spans, persist daemon
state, resolve credentials, or deliver rows.

Codex state-machine logic belongs in
`bt-daemon/src/translate/codex.rs`. Shared lifecycle, journal, transport, and
Braintrust delivery logic belongs elsewhere in that crate. The daemon's shared
`config.json` is for non-secret tracing behavior only; authentication and
backend selection belong to `bt`. Parent/root attachment remains per invocation
because it describes the surrounding trace context rather than a global plugin
setting.

Build and validate all plugin packages with:

```bash
make test
```

Test the tracing implementation with:

```bash
cargo test --manifest-path bt-daemon/Cargo.toml --all-features
```

Keep `.sh` and `.cmd` behavior aligned. Add translator or pipeline tests for
behavioral changes, and update the plugin README and
`bt-daemon/config.json.example` when shared settings change.
