# @braintrust/pi-extension

[![npm version](https://img.shields.io/npm/v/%40braintrust%2Fpi-extension)](https://www.npmjs.com/package/@braintrust/pi-extension)

Trace [Pi](https://github.com/earendil-works/pi-coding-agent) sessions in
Braintrust. The extension sends events to the local `bt` daemon, which builds
and uploads traces. Pi keeps running if tracing fails.

## What gets traced

- **Session spans**: one root span per Pi session with at least one turn
- **Turn spans**: one span per user prompt / agent run
- **LLM spans**: one span per model response inside a turn, with token usage, cache usage, reasoning tokens, estimated cost, and time to first token
- **Tool spans**: one span per tool execution, including dynamically loaded tools
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

## Quickstart

Install Pi and the
[Braintrust CLI](https://www.braintrust.dev/docs/reference/cli/quickstart), then run:

```bash
bt login
bt trace enable pi --project my-coding-agent
pi
```

This installs the extension and saves its configuration. Use `--profile` or
`--org` to choose a profile or organization. Restart Pi if it is already open.
The footer shows tracing status and a link to the trace when available.

To install the npm extension separately, use
`pi install npm:@braintrust/pi-extension`, then configure tracing with
`bt trace enable pi`.

For one invocation without changing global tracing configuration:

```bash
bt trace run --project my-coding-agent pi -- -p "summarize this repository"
```

The `bt trace run` routing and metadata flags also accept their matching
`BRAINTRUST_*` environment variables; a plain `pi` session's extension does not.
Historical import and live attach are not supported for Pi.

## Compatibility

CI installs the package against the latest patch from each of the last five
stable Pi release lines. The compatibility job resolves these versions on each
run, including releases from Pi's former npm package name when needed.

## Configuration

Settings load in this order, with later values taking precedence:

1. Defaults
2. `~/.pi/agent/braintrust.json`
3. `.pi/braintrust.json` in the project (or Pi's configured project config directory)
4. `bt trace run` settings for that invocation

Example:

```json
{
  "trace_to_braintrust": true,
  "route": {
    "auth": { "profile": "work", "org_name": "acme" },
    "destination": { "type": "project_logs", "project_name": "pi" },
    "additional_metadata": { "team": "platform" }
  }
}
```

### Settings

| Config key | Default | Purpose |
|---|---|---|
| `trace_to_braintrust` | `false` | Enable tracing |
| `route.auth.profile_id` | unset | Saved profile ID written by `bt` setup |
| `route.auth.profile` | default `bt` profile | Select a profile by name |
| `route.auth.org_name` | profile default | Select an organization |
| `route.destination` | project logs in `pi` | Select the trace destination |
| `route.additional_metadata` | unset | Add root-span metadata |
| `route.flush_mode` | `flush_on_turn_end` in the default route | Control delivery flushing |
| `show_ui` | `true` | Show the status indicator; override with `BRAINTRUST_SHOW_UI` |
| `show_trace_link` | `true` | Show the trace link; override with `BRAINTRUST_SHOW_TRACE_LINK` |

Older files can still use top-level `profile`, `org_name`, `project`, and
`additional_metadata`. For new files, use `bt trace enable` or the nested
`route` format above. Include a destination when supplying a route.

Only the display settings read environment variables directly. Tracing settings
come from these files or `bt trace run`. Credentials are managed by `bt`.

Provider request metadata is limited to model, thinking, output-limit, and
tool-count settings. Full provider payloads and thinking signatures are omitted.

## Manage tracing

```bash
bt trace doctor pi
bt trace status
bt trace update pi
bt trace disable pi
```

## Local development

From the monorepo root, build the extension before loading it locally:

```bash
make build-pi
pi -e ./dist/pi/dist/index.mjs
```

This loads the extension for one run; it still needs an enabled Braintrust
configuration. Run `make validate-pi` for package checks. See the
[contribution guide](https://github.com/braintrustdata/braintrust-coding-agent-plugins/blob/main/src/plugins/pi/content/CONTRIBUTING.md)
for source development instructions.
