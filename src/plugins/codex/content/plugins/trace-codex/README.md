# Braintrust Codex tracing plugin

An opt-in Codex plugin that sends lifecycle events to the shared Braintrust
daemon built into the `bt` CLI. The daemon tails Codex rollout transcripts and
creates a session → turn → LLM/tool trace without doing parsing or network
delivery in the blocking hook process.

## Setup

Install the current `bt` CLI:

```bash
curl -fsSL https://bt.dev/cli/install.sh | bash
bt auth login
```

Then install the plugin and enable tracing when starting Codex:

```bash
codex plugin marketplace add braintrustdata/braintrust-codex-plugin
codex plugin add trace-codex@braintrust-codex-plugins

TRACE_TO_BRAINTRUST=true BRAINTRUST_PROJECT=my-coding-agent codex
```

`BRAINTRUST_API_KEY` and the normal Braintrust URL/org environment variables
also work; `bt` owns profile, OAuth, keychain, and token-refresh handling.

## Configuration

| Environment variable | Default | Meaning |
|---|---:|---|
| `TRACE_TO_BRAINTRUST` | `false` | Master opt-in switch. |
| `BRAINTRUST_PROJECT` | `codex` | Destination project resolved by `bt`. |
| `BRAINTRUST_FLUSH_ON_TURN_END` | `false` | Flush after `Stop`; useful for short-lived CI jobs. |
| `CODEX_PARENT_SPAN_ID` | unset | Attach the Codex session below this span. |
| `CODEX_ROOT_SPAN_ID` | parent id | Existing trace root when attaching below a non-root span. |
| `BRAINTRUST_ADDITIONAL_METADATA` | unset | JSON object merged into root metadata. |

The hook is fail-open: tracing errors are written to stderr and never fail the
Codex turn. If `bt` is missing, the launcher securely queues the current event,
starts one best-effort background install with the official installer, and
replays the event after installation. If the event cannot be queued, the
installer still starts and only that event is skipped.

Both `bin/codex-hook.sh` and `bin/codex-hook.cmd` forward the full hook
configuration to `bt`. On Windows, the daemon uses a local named pipe.

## CI

Set `BRAINTRUST_FLUSH_ON_TURN_END=true` so the terminal hook waits for the
session queue and SDK batch to drain before the job exits:

```bash
BRAINTRUST_FLUSH_ON_TURN_END=true \
TRACE_TO_BRAINTRUST=true \
BRAINTRUST_PROJECT=ci-agents \
codex exec --dangerously-bypass-hook-trust "summarize this repository"
```

Inspect the local daemon and its sessions with:

```bash
bt daemon status
```
