# Braintrust

Query traces, investigate failures, manage prompts and datasets, and run
evaluations using the hosted [Braintrust MCP server](https://www.braintrust.dev/docs/integrations/developer-tools/mcp).
Requires a Braintrust account with access to your organization's data plane.
Tools use that account's permissions and can read or change Braintrust data.

## Install

```text
/plugin install braintrust --marketplace braintrustdata/braintrust-claude-plugin
```

Or from your shell:

```bash
claude plugin marketplace add braintrustdata/braintrust-claude-plugin
claude plugin install braintrust@braintrust-claude-plugin
```

Then run `/mcp`, select **braintrust**, and complete the browser sign-in.
Try "Show me my Braintrust projects" or "Summarize recent errors in my
Braintrust project."

Installing this plugin does not enable session tracing. To trace Claude Code
sessions to Braintrust, use the separate `trace-claude-code` plugin through
`bt trace enable claude`.

## What this plugin runs and sends

This plugin contains one remote MCP server, `braintrust`, reached over
Streamable HTTP at `https://api.braintrust.dev/mcp`. It has no hooks, skills,
commands, scripts, or local processes, and it does not require the `bt` CLI.

Sign-in uses OAuth, which Claude Code discovers from the server. The plugin
contains no API key, client secret, or authorization header. Claude sends the
arguments of each Braintrust tool call it makes, such as project names, SQL
queries, prompt contents, or dataset rows, to that server, and receives
Braintrust data in response.

## Other data planes

The packaged server URL is Braintrust's North America (US) data plane. To use
another data plane, copy this plugin into your own marketplace and replace
`mcpServers.braintrust.url` in `.mcp.json` with the URL for your deployment.
Do not enable this plugin and a second connection to the same tools at once.

| Data plane | MCP URL |
| --- | --- |
| North America (US) | `https://api.braintrust.dev/mcp` |
| EU | `https://api-eu.braintrust.dev/mcp` |
| Custom / self-hosted | Copy the complete MCP URL from **Settings > Data plane**. |

## Support

See [Braintrust troubleshooting](https://www.braintrust.dev/docs/kb/troubleshooting-guides)
or contact [support@braintrust.dev](mailto:support@braintrust.dev).
[Privacy policy](https://www.braintrust.dev/legal/privacy-policy) ·
[Terms of service](https://www.braintrust.dev/legal/terms-of-service).
