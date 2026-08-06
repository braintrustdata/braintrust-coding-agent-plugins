# @braintrust/pi-extension

[![npm version](https://img.shields.io/npm/v/%40braintrust%2Fpi-extension)](https://www.npmjs.com/package/@braintrust/pi-extension)

Braintrust extension for [pi](https://github.com/earendil-works/pi-coding-agent).

Today this extension automatically traces pi sessions, turns, model calls, and tool executions to Braintrust.
The extension forwards native pi events to the installed `bt` tracing daemon;
all span construction, authentication, recovery, and Braintrust delivery happen
inside the daemon.

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
  "profile": "work",
  "project": "pi",
  "additional_metadata": {
    "team": "platform"
  }
}
```

## Supported settings

| Config key | Env var | Default |
|---|---|---|
| `trace_to_braintrust` | `TRACE_TO_BRAINTRUST` | `false` |
| `org_name` | `BRAINTRUST_ORG_NAME` | unset |
| `profile` | `BRAINTRUST_PROFILE` | default `bt` profile |
| `project` | `BRAINTRUST_PROJECT` | `pi` |
| `additional_metadata` | `BRAINTRUST_ADDITIONAL_METADATA` | `{}` |
| `show_ui` | `BRAINTRUST_SHOW_UI` | `true` |
| `show_trace_link` | `BRAINTRUST_SHOW_TRACE_LINK` | `true` |

## Notes

- Project config overrides global config.
- Environment variables override both config files.
- Project config follows pi's configured project config directory, which defaults to `.pi`.
- The extension does not persist local span state; recovery and incomplete-operation cleanup are owned by the daemon journal.
- Span construction and Braintrust delivery run in the installed `bt` tracing daemon.
- The extension never reads or stores Braintrust credentials. Profile selection is
  non-secret, optional, and resolved by the daemon through `bt` authentication.
- Provider request tracing is allowlisted to effective model, thinking, output-limit, and tool-count settings; full provider payloads and thinking signatures are never logged.
- If Braintrust is unavailable, pi should continue working normally.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for development setup, validation, and repository conventions.
