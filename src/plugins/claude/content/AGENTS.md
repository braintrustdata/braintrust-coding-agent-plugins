# Agent guidelines

## About this repository

This is the **Braintrust Claude Code plugin marketplace** - a generated repository that distributes the Braintrust MCP plugin and Claude Code tracing for Braintrust.

### Structure

```
claude-plugin/
├── .claude-plugin/
│   └── marketplace.json      # Marketplace catalog (lists available plugins)
├── plugins/
│   ├── braintrust/           # Plugin: Hosted Braintrust MCP server (directory listing)
│   └── trace-claude-code/    # Plugin: Session tracing to Braintrust
└── evals/                    # Evaluation suite for Braintrust MCP behavior
```

### Plugins

| Plugin | Description |
|--------|-------------|
| `braintrust` | Connects to the hosted Braintrust MCP server over HTTP with OAuth. No hooks, skills, commands, or local processes. Submitted to Anthropic's directory. |
| `trace-claude-code` | Forwards Claude Code lifecycle hooks to `bt trace hook --source claude-code`; the shared daemon builds and delivers traces. |

Both plugins share one release version. Make source changes in
`braintrustdata/braintrust-coding-agent-plugins` and release through its Release
Plugin workflow; do not edit or tag this generated repository manually. Never
rename the `braintrust` plugin: directory and marketplace installs are keyed
by name.

### Terminology

- **Marketplace**: A repository with a `marketplace.json` that catalogs multiple plugins for distribution
- **Plugin**: An installable unit with its own `.claude-plugin/plugin.json` manifest

## Style conventions

- Use sentence case for all text (capitalize first word only, except for proper nouns and code references)
- Keep criteria concise and specific
- Reference exact function/method names with proper casing (e.g., `init_dataset()`, `Eval()`, `Factuality`)
