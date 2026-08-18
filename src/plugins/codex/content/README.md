# Braintrust Codex Plugins

> **This repository is generated.** It is built from
> [braintrustdata/braintrust-coding-agent-plugins](https://github.com/braintrustdata/braintrust-coding-agent-plugins).
> Don't edit files here — make changes and file issues in that repository, and they
> will be rebuilt into this one.

Braintrust [Codex plugins](https://developers.openai.com/codex/plugins) — skills and daemon-backed session tracing.

## Quickstart

Add this repo as a Codex plugin marketplace:

```bash
codex plugin marketplace add braintrustdata/braintrust-codex-plugin
# OPTIONAL: TRACE CODEX PLUGIN
codex plugin add trace-codex@braintrust-codex-plugins
# OPTIONAL: SKILLS PLUGIN
codex plugin add braintrust@braintrust-codex-plugins
```

The recommended tracing setup is:

```bash
bt trace setup codex --project my-coding-agent
```

Use `bt trace disable codex` to keep the marketplace plugin installed while
turning off persistent tracing. Use `bt trace uninstall codex` to remove only
the Braintrust tracing plugin registration and route.

This installs the tracing plugin and stores only non-secret routing settings.
The `bt` CLI owns authentication and forwards hook events through the shared
daemon. Restart Codex after setup.

## trace codex plugin

See the plugin's [README](/plugins/trace-codex/README.md) for details.

## skills plugin

see the plugin's [README](/plugins/braintrust-codex-plugin/README.md) for details
