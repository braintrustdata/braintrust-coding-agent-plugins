# Braintrust tracing for Google Antigravity

> **This repository is generated.** It is built from
> [braintrustdata/braintrust-coding-agent-plugins](https://github.com/braintrustdata/braintrust-coding-agent-plugins).
> Don't edit files here — make changes and file issues in that repository, and they
> will be rebuilt into this one.

Trace Google Antigravity coding sessions in Braintrust.

## Quickstart

Prerequisites:

- The latest [Google Antigravity](https://antigravity.google/)
- The latest [Braintrust CLI (`bt`)](https://www.braintrust.dev/docs/reference/cli/quickstart)

Install the plugin and choose where traces are sent:

```bash
bt login --profile myprofile
bt trace --profile myprofile -p my-coding-agent-project enable antigravity
```

This causes Google Antigravity sessions to report to your configured project.
You can disable the plugin later with:

```bash
bt trace disable antigravity
```

## What is captured

Each traced session includes:

- a root span for the Antigravity session;
- a turn span for each user request and visible assistant response;
- LLM spans with available model inputs, outputs, and token usage;
- tool spans with observable inputs, outputs, duration, outcome, and errors;
- useful session metadata such as the Antigravity version, model, workspace,
  and native conversation ID.

The plugin forwards events only to the local Braintrust daemon. It does not
contain Braintrust credentials or send traces directly to Braintrust.
