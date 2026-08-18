# Braintrust Claude Code Marketplace

> **This repository is generated.** It is built from
> [braintrustdata/braintrust-coding-agent-plugins](https://github.com/braintrustdata/braintrust-coding-agent-plugins).
> Don't edit files here — make changes and file issues in that repository, and they
> will be rebuilt into this one.

A Claude Code plugin marketplace for [Braintrust](https://braintrust.dev) integration - LLM evaluation, logging, observability, and session tracing.

## Prerequisites

- A [Braintrust account](https://braintrust.dev)
- The `bt` CLI, authenticated with `bt login`

## Installation

Add the marketplace:

```bash
claude plugin marketplace add braintrustdata/braintrust-claude-plugin
```

Then install the plugins you need:

## Plugins

### braintrust

Enables AI agents to use Braintrust for LLM evaluation, logging, and observability.

- Query Braintrust projects, experiments, datasets, and logs
- Instrument your code with the Braintrust SDK and write evals

```bash
claude plugin install braintrust@braintrust-claude-plugin
```

### trace-claude-code

Automatically traces Claude Code conversations to Braintrust through the shared
Braintrust daemon. The plugin contains only a fail-open hook forwarder; `bt`
owns authentication, trace construction, and delivery.

```bash
bt trace setup claude --project my-coding-agent
```

Use `bt trace disable claude` to keep the marketplace plugin installed while
turning off persistent tracing. Use `bt trace uninstall claude` to remove only
the Braintrust tracing plugin registration and route.

Use `--profile` or `--org` when needed. Setup stores only non-secret routing
settings under `~/.claude/braintrust.json`. Restart Claude Code after setup.

Every registered lifecycle event is forwarded synchronously to
`bt trace hook --source claude-code`, preserving per-session ordering. Hook
failures never fail a Claude Code turn.

#### Additional root metadata

Set `BRAINTRUST_ADDITIONAL_METADATA` to a JSON object to tag the root span of a
Claude Code session. Standard session metadata takes precedence if keys
conflict.

```bash
BRAINTRUST_ADDITIONAL_METADATA='{"ci":true,"run_id":"abc-123"}' claude
```

For a persistent route, pass the same object to `bt trace setup claude
--additional-metadata '<JSON>'`.
