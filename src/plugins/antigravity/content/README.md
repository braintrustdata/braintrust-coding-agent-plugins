# Braintrust tracing for Google Antigravity

> **This repository is generated.** It is built from
> [braintrustdata/braintrust-coding-agent-plugins](https://github.com/braintrustdata/braintrust-coding-agent-plugins).
> Don't edit files here — make changes and file issues in that repository, and they
> will be rebuilt into this one.

Trace Google Antigravity coding sessions in Braintrust.

## Quickstart

Prerequisites:

- [Google Antigravity](https://antigravity.google/)
- A Unix-compatible `sh` (persistent setup is not supported on Windows)
- The [Braintrust CLI (`bt`)](https://www.braintrust.dev/docs/reference/cli/quickstart)

Install the plugin and choose where traces are sent:

```bash
bt login
bt trace enable antigravity --project my-coding-agent
```

Setup installs the hooks and saves non-secret routing settings in
`~/.gemini/config/braintrust.json`. Use `--profile` or `--org` to select a
Braintrust profile or organization. Restart Antigravity after setup.

## What is captured

Each traced session includes:

- a root span for the Antigravity session;
- a turn span for each user request and visible assistant response;
- LLM spans with available model inputs, outputs, and token usage;
- tool spans with observable inputs, outputs, duration, outcome, and errors;
- session metadata such as the Antigravity version, model, workspace,
  and native conversation ID.

The plugin sends events to the local `bt` daemon, which handles credentials
and uploads traces.

## Transcript import

```bash
bt trace import antigravity CONVERSATION_ID
bt trace import antigravity CONVERSATION_ID --attach
```

Import reconstructs a trace from a saved conversation; `--attach` follows an
active conversation until Ctrl-C. Only information present in the transcript
can be recovered. `bt trace run antigravity` is not supported.

## Manage tracing

```bash
bt trace doctor antigravity
bt trace status
bt trace update antigravity
bt trace disable antigravity
```
