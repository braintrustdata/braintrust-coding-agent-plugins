# Braintrust tracing for Grok

> **This repository is generated.** It is built from
> [braintrustdata/braintrust-coding-agent-plugins](https://github.com/braintrustdata/braintrust-coding-agent-plugins).
> Don't edit files here — make changes and file issues in that repository, and they
> will be rebuilt into this one.

Trace Grok coding sessions in Braintrust.

## Quickstart

Prerequisites:

- The [Grok CLI](https://grok.com/build)
- The [Braintrust CLI (`bt`)](https://www.braintrust.dev/docs/reference/cli/quickstart)

Install:

```bash
bt login
bt trace enable grok --project my-coding-agent
```

Setup installs and enables the published plugin and saves non-secret routing
settings in `~/.grok/braintrust.json`. Use `--profile` or `--org` to select a
Braintrust profile or organization. Restart Grok after setup.

If the plugin is enabled but hooks do not fire, try `/reload-plugins` at the
start of the session. Grok 1.0.3 has a reported plugin-hook dispatch issue;
see [the upstream report](https://github.com/xai-org/plugin-marketplace/issues/236)
for details.

## What is captured

Each traced session includes:

- a root span for the Grok session;
- a turn span for each user request and visible assistant response;
- reconstructed LLM spans with available model output and reasoning;
- tool spans with observable inputs, outputs, duration, outcome, and errors;
- per-turn token, cache, reasoning, model-call, API-duration, and raw cost
  metrics when Grok records them;
- the system prompt and first user message on the first LLM span when available;
- session metadata such as Grok and plugin versions, working directory,
  workspace, and native session ID.

The plugin invokes the installed `bt` CLI directly for each event. `bt` owns
credentials, daemon startup, and trace delivery. It records the installed
plugin and Grok versions dynamically; no release-specific hook edits are
needed.

## Caveats

Grok's hooks and transcripts do not expose the complete provider request or
native boundaries for every model call. As a result:

- token and usage totals are accurate at the **turn** level, but are not
  available for each individual LLM call; the turn aggregate is also attached
  to the final reconstructed LLM span and labeled as turn-level usage;
- exact model input is available only for the first reconstructed LLM span when
  the system prompt and first user message can be recovered;
- LLM boundaries are reconstructed from Grok stream timing and may not match
  provider-side spans exactly;
- permission, compaction, and subagent activity do not receive dedicated span
  hierarchies, and web or MCP operations may appear as generic tools;
- managed `bt trace run grok` and historical `bt trace import grok` workflows
  are not currently supported.

Grok keeps running if tracing fails.

## Manage tracing

```bash
bt trace doctor grok
bt trace status
bt trace update grok
bt trace disable grok
```
