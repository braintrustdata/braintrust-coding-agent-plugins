# Braintrust

Explore traces, investigate agent failures, manage prompts and datasets, and run
evaluations with the hosted [Braintrust MCP server](https://docs.braintrust.dev/integrations/developer-tools/mcp).
Requires [Antigravity](https://antigravity.google/) and a Braintrust account with
access to your organization's data plane. Tools use your account's permissions
and can read or change Braintrust data.

## Install and connect

After publication to the generated distribution repository, install with:

```bash
agy plugin install https://github.com/braintrustdata/braintrust-antigravity-plugin/tree/main/plugins/braintrust
```

For local development, run `make validate-antigravity` in the source
monorepo, then `agy plugin install ./dist/antigravity/plugins/braintrust`.

Open **Customizations > Installed > MCP Servers** in the Antigravity App and
authenticate the `braintrust_braintrust` server. Complete the browser sign-in
flow and any authorization-code prompt. In the CLI, use `/mcp` to inspect the
connection. Start a conversation and try:

- "Show me my Braintrust projects."
- "Summarize recent errors in my Braintrust project."
- "Compare the results of two experiments in my Braintrust project."

Antigravity discovers OAuth authorization and dynamically registers its client
with the server. No API key, client secret, or `bt` installation is required.
Antigravity stores and refreshes OAuth tokens; credentials are never bundled in
this plugin. This package connects to MCP independently of the optional
[Antigravity session tracing plugin](https://github.com/braintrustdata/braintrust-antigravity-plugin).

## Data planes

This distribution connects to the NA data plane. For EU or a custom deployment,
copy this package into a local/team distribution and change only
`mcpServers.braintrust.serverUrl` in `mcp_config.json` before installing it:

| Data plane | MCP URL |
| --- | --- |
| NA (US in Braintrust docs) | `https://api.braintrust.dev/mcp` |
| EU | `https://api-eu.braintrust.dev/mcp` |
| Custom / self-hosted | Copy the complete MCP URL from **Settings > Data plane**. |

Preserve any deployment-specific API path prefix before `/mcp`. Use the data
plane hosting your organization's data. Editing an installed plugin's cache
is temporary because upgrades replace its files. Avoid enabling duplicate
connections to the same server.

## Manage and troubleshoot

```bash
agy plugin list --json
agy plugin disable braintrust
agy plugin enable braintrust
agy plugin uninstall braintrust
```

If tools are missing, check that the plugin is enabled and the MCP connection
is authenticated in Customizations or `/mcp`. If projects are missing, check
your Braintrust organization, permissions, and data plane.

See [Braintrust troubleshooting](https://docs.braintrust.dev/kb/troubleshooting-guides)
or contact [support@braintrust.dev](mailto:support@braintrust.dev).
[Privacy policy](https://www.braintrust.dev/legal/privacy-policy) ·
[Terms of service](https://www.braintrust.dev/legal/terms-of-service).

This repository is generated from
[braintrustdata/braintrust-coding-agent-plugins](https://github.com/braintrustdata/braintrust-coding-agent-plugins).
Make source changes and file issues there.
