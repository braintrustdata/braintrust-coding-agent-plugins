# @braintrust/trace-opencode

Braintrust tracing plugin for [OpenCode](https://opencode.ai). The JavaScript
adapter forwards native OpenCode events to the installed `bt` daemon, which
constructs and delivers the trace.

Version 1 requires a current `bt` CLI with the OpenCode daemon translator. If
`bt` or the translator is unavailable, tracing fails open and OpenCode keeps
running.

- **Session spans**: Root span for each OpenCode session with metadata (workspace, hostname, etc.)
- **Turn spans**: Captures each user-assistant interaction
- **Tool spans**: Records individual tool executions with inputs and outputs

## Quick Start

```bash
bt auth login
bt trace enable opencode
opencode
```

For one invocation without changing OpenCode's global tracing configuration,
use `bt trace run --project <PROJECT> opencode -- [OPENCODE_ARGS...]`.

## Configuration

You can configure the plugin using a config file. `braintrust.json` is the
only source of persistent tracing configuration; routing and enablement are
never read directly from the environment.

### Config File

Create a `braintrust.json` file in one of these locations:

- `.opencode/braintrust.json` - Project-level config
- `~/.config/opencode/braintrust.json` - Global config

```json
{
  "trace_to_braintrust": true,
  "enable_tools": true,
  "route": {
    "auth": { "profile": "work", "org_name": "acme" },
    "destination": { "type": "project_logs", "project_name": "my-project" }
  },
  "debug": true
}
```

### Config Options

| Config Key | Env Var | Type | Default | Description |
|------------|---------|------|---------|-------------|
| `trace_to_braintrust` | — | boolean | `false` | Enable/disable tracing |
| `enable_tools` | `BRAINTRUST_OPENCODE_ENABLE_TOOLS` | boolean | `true` | Register Braintrust tools in OpenCode |
| `profile` | — | string | current `bt` profile | Select the `bt` auth profile used by tracing and tools |
| `project` | — | string | `"opencode"` | Project name for traces and project-scoped tools |
| `debug` | `BRAINTRUST_DEBUG` | boolean | `false` | Enable debug logging |
| `org_name` | — | string | profile default | Organization selected within the tracing profile and for tools |
| `additional_metadata` | — | | | JSON object of additional metadata to attach to the root span. Standard metadata keys take precedence on conflict. |

`enable_tools` and `debug` control local plugin behavior and can still be set
from the environment. Tracing routing and enablement (`trace_to_braintrust`,
`profile`, `project`, `org_name`, `additional_metadata`) come only from
`braintrust.json` and `bt trace run`.

### Precedence

Configuration is loaded with the following precedence (later overrides earlier):

1. Default values
2. `~/.config/opencode/braintrust.json` (global config)
3. `.opencode/braintrust.json` (project config)
4. `bt trace run` invocation settings (tracing only, per-invocation, highest
   priority; never written back to the config files)

## Disabling Braintrust Tools

Set `enable_tools` to `false` to trace OpenCode sessions without registering Braintrust-branded tools (`braintrust_query_logs`, `braintrust_list_projects`, `braintrust_log_data`, `braintrust_get_experiments`):

```json
{
  "trace_to_braintrust": true,
  "enable_tools": false,
  "project": "my-project"
}
```

Or use the environment variable:

```bash
BRAINTRUST_OPENCODE_ENABLE_TOOLS=false opencode
```

## Adding Dynamic Metadata

Set `additional_metadata` via the config file to attach custom key-value pairs to the root span. This is useful for tagging traces in CI or linking them back to a specific run.

For one invocation without changing the persistent configuration, use
`bt trace run --additional-metadata '{"ci": true, "run_id": "abc-123"}' opencode -- run "do the thing"`,
or set `BRAINTRUST_ADDITIONAL_METADATA` before that command (`bt trace run`
still accepts it; a plain `opencode` session's plugin does not).

You can also set it via the config file:

```json
{
  "additional_metadata": {
    "team": "platform"
  }
}
```

The value must be a JSON object. Any keys that conflict with standard root span metadata (`session_id`, `workspace`, `directory`, `hostname`, `username`, `os`) will be overridden by the standard values.

## Trace Structure

Sessions are traced with the following hierarchy:

```
Session (task span)
├── metadata: session_id, workspace, hostname, username, os
├── Turn 1 (task span)
│   ├── input: "user message"
│   ├── metadata: turn_number, agent, model
│   ├── Tool 1 (tool span)
│   │   ├── input: tool arguments
│   │   └── output: tool result
│   └── Tool 2 (tool span)
├── Turn 2 (task span)
│   └── ...
└── metrics: total_turns, total_tool_calls
```

## Runtime architecture

The package never calls the Braintrust API from JavaScript. Tracing forwards
native events over local JSON-RPC to `bt-daemon`. The four optional data-access
tools invoke non-interactive `bt` CLI commands. In both cases, `bt` owns profile
selection, credential storage, refresh, backend resolution, and API transport.
