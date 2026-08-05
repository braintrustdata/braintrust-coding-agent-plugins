# @braintrust/pi-extension

[![npm version](https://img.shields.io/npm/v/%40braintrust%2Fpi-extension)](https://www.npmjs.com/package/@braintrust/pi-extension)

Braintrust extension for [pi](https://github.com/earendil-works/pi-coding-agent).

Today this extension automatically traces pi sessions, turns, model calls, and tool executions to Braintrust.

## What gets traced

- **Session spans**: one root span per pi session that actually produces at least one turn
- **Turn spans**: one span per user prompt / agent run
- **LLM spans**: one span per model response inside a turn, including canonical token, cache, reasoning, estimated-cost, and time-to-first-token metrics
- **Tool spans**: one span per tool execution, including tools activated through pi's dynamic/deferred tool-loading flow
- **Compaction spans**: one span per session compaction, including trigger/retry metadata when available
- **Branch summary spans**: one span per summarized `/tree` navigation branch

Trace shape:

```text
Session (task)
├── Turn 1 (task)
│   ├── anthropic/claude-sonnet-4 (llm)
│   │   ├── read: package.json (tool)
│   │   └── bash: pnpm test (tool)
│   └── anthropic/claude-sonnet-4 (llm)
├── Compaction (task)
├── Branch Summary (task)
└── Turn 2 (task)
```

## Install

### From npm

```bash
pi install npm:@braintrust/pi-extension
```

### From this repo

```bash
pi install .
```

Or load it just for one run:

```bash
pi -e .
```

## Compatibility

This package supports the **latest patch release from each of the last five stable pi minor versions**, currently excluding pi versions before `0.65.0`.

Our GitHub Actions compatibility job automatically resolves and tests that compatibility window, so new pi releases are picked up without manually updating the matrix.

## Quick start

Tracing is disabled by default.

Set these environment variables:

```bash
export TRACE_TO_BRAINTRUST=true
bt auth login
export BRAINTRUST_PROJECT=pi
```

Then start pi normally.

In interactive mode, the footer shows a `Braintrust` status indicator while tracing is active, and a widget below the editor shows a shortened clickable trace link when available.

## Configuration

You can configure the extension with environment variables or JSON config files.

Config precedence is:

1. defaults
2. `~/.pi/agent/braintrust.json`
3. `.pi/braintrust.json`
4. environment variables

### Config file locations

- Global: `~/.pi/agent/braintrust.json`
- Project: `.pi/braintrust.json`

Example:

```json
{
  "trace_to_braintrust": true,
  "project": "pi",
  "debug": true,
  "additional_metadata": {
    "team": "platform"
  }
}
```

## Supported settings

| Config key | Env var | Default |
|---|---|---|
| `trace_to_braintrust` | `TRACE_TO_BRAINTRUST` | `false` |
| `api_key` | `BRAINTRUST_API_KEY` | legacy; tracing authentication is owned by `bt auth login` |
| `api_url` | `BRAINTRUST_API_URL` | `https://api.braintrust.dev` |
| `app_url` | `BRAINTRUST_APP_URL` | `https://www.braintrust.dev` |
| `org_name` | `BRAINTRUST_ORG_NAME` | unset |
| `project` | `BRAINTRUST_PROJECT` | `pi` |
| `debug` | `BRAINTRUST_DEBUG` | `false` |
| `additional_metadata` | `BRAINTRUST_ADDITIONAL_METADATA` | `{}` |
| `log_file` | `BRAINTRUST_LOG_FILE` | unset |
| `state_dir` | `BRAINTRUST_STATE_DIR` | `~/.pi/agent/state/braintrust-pi-extension` |
| `show_ui` | `BRAINTRUST_SHOW_UI` | `true` |
| `show_trace_link` | `BRAINTRUST_SHOW_TRACE_LINK` | `true` |
| `parent_span_id` | `PI_PARENT_SPAN_ID` | unset |
| `root_span_id` | `PI_ROOT_SPAN_ID` | unset |

## Notes

- Project config overrides global config.
- Environment variables override both config files.
- Project config follows pi's configured project config directory, which defaults to `.pi`.
- The extension does not persist local span state; recovery and incomplete-operation cleanup are owned by the daemon journal.
- Span construction and Braintrust delivery run in the installed `bt` tracing daemon.
- Provider request tracing is allowlisted to effective model, thinking, output-limit, and tool-count settings; full provider payloads and thinking signatures are never logged.
- If Braintrust is unavailable, pi should continue working normally.
- If `PI_PARENT_SPAN_ID` is set, the pi session span is attached under an existing Braintrust trace.
- `PI_ROOT_SPAN_ID` can be used when the parent span is not the trace root.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for development setup, validation, and repository conventions.
