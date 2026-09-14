# @braintrust/trace-opencode

Trace [OpenCode](https://opencode.ai) sessions in Braintrust. The plugin sends
events to the local `bt` daemon, which builds and uploads traces. OpenCode keeps
running if tracing fails.

- **Session spans**: Root span for each OpenCode session with metadata (workspace, hostname, etc.)
- **Turn spans**: Captures each user-assistant interaction
- **LLM spans**: Records completed model messages with available inputs, outputs, and usage
- **Tool spans**: Records individual tool executions with inputs and outputs

## Quickstart

Install OpenCode and the
[Braintrust CLI](https://www.braintrust.dev/docs/reference/cli/quickstart), then run:

```bash
bt login
bt trace enable opencode --project my-coding-agent
opencode
```

This registers the npm plugin and saves its configuration. Use `--profile` or
`--org` to choose a profile or organization. Restart OpenCode if it is already open.

For one invocation without changing global tracing configuration:

```bash
bt trace run --project my-coding-agent opencode -- run "summarize this repository"
```

Historical import and live attach are not supported for OpenCode.

## Compatibility

The package declares `@opencode-ai/plugin` and `@opencode-ai/sdk` peers at
`>=1.2.25`. CI checks package installation at that minimum and at `latest`,
and runs real-agent integration tests with the latest OpenCode CLI.

## Configuration

Settings load from these files, with project settings overriding global settings:

- Global: `$XDG_CONFIG_HOME/opencode/braintrust.json`, or
  `~/.config/opencode/braintrust.json` if `XDG_CONFIG_HOME` is unset
- Project: `.opencode/braintrust.json`

`bt trace run` overrides tracing settings for one invocation. Files are merged
at the top level: a project `route` replaces the global `route`.

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

### Settings

| Config key | Default | Purpose |
|---|---|---|
| `trace_to_braintrust` | `false` | Enable tracing |
| `enable_tools` | `true` | Register Braintrust tools; override with `BRAINTRUST_OPENCODE_ENABLE_TOOLS` |
| `route.auth.profile_id` | unset | Saved profile ID written by `bt` setup |
| `route.auth.profile` | current `bt` profile | Select a profile by name |
| `route.auth.org_name` | profile default | Select an organization |
| `route.destination` | project logs in `opencode` | Select the trace destination |
| `route.additional_metadata` | unset | Add root-span metadata |
| `route.flush_mode` | `fire_and_forget` in the default route | Control delivery flushing |
| `debug` | `false` | Enable debug logging; override with `BRAINTRUST_DEBUG` |

Older files can still use top-level `profile`, `org_name`, `project`, and
`additional_metadata`. For new files, use `bt trace enable` or the nested
`route` format above. Include a destination when supplying a route.

Only `enable_tools` and `debug` read environment variables directly. Tracing
settings come from these files or `bt trace run`.

## Disable Braintrust tools

Set `enable_tools` to `false` to trace OpenCode sessions without registering Braintrust-branded tools (`braintrust_query_logs`, `braintrust_list_projects`, `braintrust_log_data`, `braintrust_get_experiments`):

```json
{
  "trace_to_braintrust": true,
  "enable_tools": false,
  "route": {
    "destination": { "type": "project_logs", "project_name": "my-project" }
  }
}
```

Or use the environment variable:

```bash
BRAINTRUST_OPENCODE_ENABLE_TOOLS=false opencode
```

## Add root metadata

Set `route.additional_metadata` to add fields to the root span:

For one invocation without changing the persistent configuration, use
`bt trace run --additional-metadata '{"ci": true, "run_id": "abc-123"}' opencode -- run "do the thing"`,
or set `BRAINTRUST_ADDITIONAL_METADATA` before that command (`bt trace run`
still accepts it; a plain `opencode` session's plugin does not).

You can also set it via the config file:

```json
{
  "route": {
    "destination": { "type": "project_logs", "project_name": "my-project" },
    "additional_metadata": { "team": "platform" }
  }
}
```

The value must be a JSON object. Built-in session metadata wins if keys conflict.

## Trace structure

The daemon reconstructs sessions, turns, model messages, and tool executions:

```text
Session (task)
├── Turn 1 (task)
│   ├── Model response (llm)
│   ├── Tool execution (tool)
│   └── Model response (llm)
└── Turn 2 (task)
```

Model usage and metadata are included when OpenCode provides them. Child
sessions are attached to their parent's active turn when that relationship is
available to the translator.

## Manage tracing

```bash
bt trace doctor opencode
bt trace status
bt trace update opencode
bt trace disable opencode
```

## Runtime architecture

Tracing sends events to the local daemon over JSON-RPC. The four optional
Braintrust tools call `bt` commands. Both use `bt` for credentials and API access.

## Development

From the monorepo root, run `make validate-opencode` to build, check, and test
the package. See the
[contribution guide](https://github.com/braintrustdata/braintrust-coding-agent-plugins/blob/main/src/plugins/opencode/content/CONTRIBUTING.md)
for development instructions. The npm package contains compiled entrypoints;
source changes belong in the monorepo.
