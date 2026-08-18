# Braintrust coding-agent plugins

A monorepo of [Braintrust](https://braintrust.dev) coding-agent integrations.
Every integration forwards native events to the same local `bt` tracing daemon,
so authentication, routing, recovery, and trace construction stay consistent.

For further instructions, see the instructions for your desired coding agent

| Agent       | distribution repository |
|-------------|-------------------------|
| Claude Code | [braintrustdata/braintrust-claude-plugin](https://github.com/braintrustdata/braintrust-claude-plugin) |
| Codex       | [braintrustdata/braintrust-codex-plugin](https://github.com/braintrustdata/braintrust-codex-plugin)   |
| OpenCode    | npm: [`@braintrust/trace-opencode`](https://www.npmjs.com/package/@braintrust/trace-opencode) |
| Pi          | npm: [`@braintrust/pi-extension`](https://www.npmjs.com/package/@braintrust/pi-extension) |

## Feature coverage

| Agent | Persistent setup | Managed run | Import / attach | Braintrust tools |
|---|---:|---:|---:|---:|
| Claude Code | Yes | Yes | Yes / Yes | No |
| Codex | Yes | Yes | Yes / Yes | No |
| OpenCode | Yes | Yes | No / No | Yes |
| Pi | Yes | Yes | No / No | No |

All four integrations support `bt trace setup`, `bt trace disable`,
`bt trace uninstall`, and invocation-local `bt trace run`. Import and attach
are intentionally limited to agents whose native transcript stores preserve
the data needed by their daemon translators.

## Development & releasing

See [AGENTS.md](./AGENTS.md) for the repo structure, the build/deploy model, the
per-plugin versioning, and the manual release workflows.
