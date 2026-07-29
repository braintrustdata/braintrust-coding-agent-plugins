# Braintrust Codex tracing plugin

An opt-in Codex plugin that forwards lifecycle events to the shared Braintrust
daemon built into the `bt` CLI. The daemon tails Codex rollout transcripts and
creates a session, turn, LLM, and tool trace without doing parsing or network
delivery in the blocking hook process.

## Setup

Install and authenticate the current `bt` CLI:

```bash
curl -fsSL https://bt.dev/cli/install.sh | bash
bt auth login
```

Then install the plugin and enable tracing:

```bash
codex plugin marketplace add braintrustdata/braintrust-codex-plugin
codex plugin add trace-codex@braintrust-codex-plugins

TRACE_TO_BRAINTRUST=true BRAINTRUST_PROJECT=my-coding-agent codex
```

The hook is fail-open: tracing errors are written to stderr and never fail a
Codex turn. If a daemon-capable `bt` is unavailable, the Unix launcher queues
the current event when possible and starts a best-effort background install.

## Shared configuration

All coding-agent plugins connected to the daemon use the same non-credential
JSON settings. Set `BT_DAEMON_CONFIG` to choose the file. Otherwise it is
`config.json` inside `BT_DAEMON_DATA_DIR`, or inside the daemon's default state
directory (`~/.braintrust/state/bt-daemon` on Unix and
`%LOCALAPPDATA%\Braintrust\bt-daemon` on Windows).

```json
{
  "traceToBraintrust": true,
  "project": "coding-agents",
  "flushOnTurnEnd": false,
  "additionalMetadata": {
    "team": "platform"
  }
}
```

For supported keys, a value in the shared file overrides its environment
fallback. Omitted keys retain the existing plugin environment behavior:

| Shared key | Environment fallback | Default |
|---|---|---:|
| `traceToBraintrust` | `TRACE_TO_BRAINTRUST` | `false` |
| `project` | `BRAINTRUST_PROJECT` / `bt` project configuration | resolved by `bt` |
| `flushOnTurnEnd` | `BRAINTRUST_FLUSH_ON_TURN_END` | `false` |
| `additionalMetadata` | `BRAINTRUST_ADDITIONAL_METADATA` | unset |

Do not put API keys, auth tokens, organization credentials, or backend URLs in
this file. They are intentionally ignored: `bt` owns profile, OAuth, keychain,
token refresh, and backend resolution.

`CODEX_PARENT_SPAN_ID` and `CODEX_ROOT_SPAN_ID` remain per-process environment
variables because they attach this Codex invocation to an existing trace.

Both `bin/codex-hook.sh` and `bin/codex-hook.cmd` forward events to `bt`. On
Windows, the daemon uses a local named pipe.

## CI

For short-lived jobs, enable `flushOnTurnEnd` in the shared file or set
`BRAINTRUST_FLUSH_ON_TURN_END=true`:

```bash
BRAINTRUST_FLUSH_ON_TURN_END=true \
TRACE_TO_BRAINTRUST=true \
BRAINTRUST_PROJECT=ci-agents \
codex exec --dangerously-bypass-hook-trust "summarize this repository"
```

Inspect the daemon and its sessions with:

```bash
bt daemon status
```
