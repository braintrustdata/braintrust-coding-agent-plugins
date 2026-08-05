# @braintrust/trace-opencode

Braintrust tracing plugin for [OpenCode](https://opencode.ai). Automatically traces your OpenCode sessions to Braintrust with hierarchical spans.

- **Session spans**: Root span for each OpenCode session with metadata (workspace, hostname, etc.)
- **Turn spans**: Captures each user-assistant interaction
- **Tool spans**: Records individual tool executions with inputs and outputs

## Quick Start

Add to your OpenCode configuration (`opencode.json` or `~/.config/opencode/opencode.json`):

```json
{
  "plugin": ["@braintrust/trace-opencode@0.0.x"], <--- replace with latest version
}
```

Then,

```bash
# Authenticate the installed bt CLI used by the tracing daemon
bt auth login
export TRACE_TO_BRAINTRUST="true"

# Run OpenCode
opencode

# View traces at:
# https://www.braintrust.dev/app/projects/opencode/logs
```

## Configuration

You can configure the plugin using a config file or environment variables.

### Config File

Create a `braintrust.json` file in one of these locations:

- `.opencode/braintrust.json` - Project-level config
- `~/.config/opencode/braintrust.json` - Global config

```json
{
  "trace_to_braintrust": true,
  "enable_tools": true,
  "profile": "work",
  "project": "my-project",
  "api_key": "your-api-key",
  "debug": true
}
```

### Config Options

| Config Key | Env Var | Type | Default | Description |
|------------|---------|------|---------|-------------|
| `trace_to_braintrust` | `TRACE_TO_BRAINTRUST` | boolean | `false` | Enable/disable tracing |
| `enable_tools` | `BRAINTRUST_OPENCODE_ENABLE_TOOLS` | boolean | `true` | Register Braintrust tools in OpenCode |
| `profile` | `BRAINTRUST_PROFILE` | string | current `bt` profile | Select the `bt` auth profile used by tracing; credentials stay in the daemon |
| `project` | `BRAINTRUST_PROJECT` | string | `"opencode"` | Project name for traces |
| `debug` | `BRAINTRUST_DEBUG` | boolean | `false` | Enable debug logging |
| `api_key` | `BRAINTRUST_API_KEY` | string | | API key used only by the optional data-access tools; tracing uses `bt auth login` |
| `api_url` | `BRAINTRUST_API_URL` | string | `"https://api.braintrust.dev"` | API URL |
| `app_url` | `BRAINTRUST_APP_URL` | string | `"https://www.braintrust.dev"` | App URL |
| `org_name` | `BRAINTRUST_ORG_NAME` | string | profile default | Organization selected within the tracing profile and for tools |
| `additional_metadata` | `BRAINTRUST_ADDITIONAL_METADATA` | string | | JSON object of additional metadata to attach to the root span. Standard metadata keys take precedence on conflict. |

### Precedence

Configuration is loaded with the following precedence (later overrides earlier):

1. Default values
2. `~/.config/opencode/braintrust.json` (global config)
3. `.opencode/braintrust.json` (project config)
4. Environment variables (highest priority)

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
BRAINTRUST_OPENCODE_ENABLE_TOOLS=false TRACE_TO_BRAINTRUST=true opencode
```

## Adding Dynamic Metadata

Use `BRAINTRUST_ADDITIONAL_METADATA` to attach custom key-value pairs to the root span. This is useful for tagging traces in CI or linking them back to a specific run.

```bash
BRAINTRUST_ADDITIONAL_METADATA='{"ci": true, "run_id": "abc-123"}' opencode run "do the thing"
```

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

## Releasing

This package is published from GitHub Actions via `.github/workflows/publish-package.yaml` using npm trusted publishing with OIDC and npm provenance (`npm publish --provenance`).

To cut a release:

1. Bump `package.json` to the version you want to ship.
2. Run the **Publish package** workflow from the branch you want to release.
3. The workflow validates the package, publishes to npm, pushes the matching `v<version>` git tag, and creates a GitHub release.
