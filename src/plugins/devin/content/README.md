# Braintrust for Devin

Connect Devin to [Braintrust MCP](https://docs.braintrust.dev/integrations/developer-tools/mcp)
to investigate traces, compare experiments, and run evals. Sign in with your
Braintrust account through OAuth. Tools use that account’s permissions.
No `bt` installation is required, and session tracing is not enabled.

## Install

In Devin Cloud, use **Customize → Plugins → Add plugin → From repository** with
`braintrustdata/braintrust-coding-agent-plugins` and subdirectory
`src/plugins/devin/content` after this package merges to the default branch.
The marketplace references this directory with a `git-subdir` entry pinned to
an exact commit.

To test locally:

```bash
make validate-devin
devin plugins install --local ./dist/devin
```

Complete OAuth, start a new session, and ask Devin to list your Braintrust projects.

The package connects to the US data plane. For EU, change
`mcpServers.braintrust.url` in a local/team copy’s `.devin-plugin/plugin.json` to
`https://api-eu.braintrust.dev/mcp`. For self-hosted deployments, use the complete
MCP URL from **Settings → Data plane**, preserving any path prefix.
