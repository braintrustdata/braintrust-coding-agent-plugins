# Braintrust for Devin

Investigate production traces, compare experiments, manage datasets and prompts,
and run evaluations through the hosted
[Braintrust MCP server](https://docs.braintrust.dev/integrations/developer-tools/mcp).

Requires a Braintrust account with access to your organization’s data plane.
Devin discovers OAuth from the server and prompts you to sign in. Tools use the
permissions of that account and can read or change Braintrust data. This package
requires no local process, API key, or `bt` installation and does not enable
session tracing.

## Install and test

In Devin Cloud, open **Customize → Plugins → Add plugin → From repository**.
Use `braintrustdata/braintrust-coding-agent-plugins` with subdirectory
`src/plugins/devin/content`. Repository installs use the default branch, so this
path becomes available after the source PR merges. Before then, upload a ZIP
of the built package through **Add plugin → Upload .zip**.

For the Devin CLI, from the source monorepo:

```bash
make validate-devin
devin plugins install --local ./dist/devin
```

Complete OAuth, start a new session, and ask Devin to list your Braintrust
projects. Then ask it to inspect recent errors or compare two experiments.
A successful build or an unauthenticated OAuth challenge is not an authenticated
session test.

## Data planes

The package connects to the US data plane at `https://api.braintrust.dev/mcp`.
For EU or self-hosted installations, use a local/team copy and change
`mcpServers.braintrust.url` in `.devin-plugin/plugin.json` before installation.
EU uses `https://api-eu.braintrust.dev/mcp`; self-hosted users should copy the
complete MCP URL from **Settings → Data plane**, preserving any path prefix.

## Source and marketplace distribution

This package is maintained in
[braintrust-coding-agent-plugins](https://github.com/braintrustdata/braintrust-coding-agent-plugins).
The Devin marketplace can reference this source subtree directly with a
`git-subdir` entry pinned to a full commit SHA and
`path: "src/plugins/devin/content"`. Update that pin after reviewed source changes
merge; no separate distribution repository is needed for this installation path.

`make build-devin` copies the installable tree to `dist/devin`, and
`make validate-devin` checks it. The repository’s `make test` includes this
package automatically. `scripts/set-plugin-version.py devin <version>` stamps
its manifest. If a generated distribution repository is needed later, the
standard `DIST_REPO=owner/repo make publish-devin` command supports
`DRY_RUN=1`; the production Release Plugin workflow is not configured for Devin.

Workflow instructions remain in
[braintrust-skills](https://github.com/braintrustdata/braintrust-skills); this
package does not duplicate them. Braintrust MCP also exposes guided workflows
through its tools.

For connection problems, see
[Braintrust troubleshooting](https://docs.braintrust.dev/kb/troubleshooting-guides)
or contact [support@braintrust.dev](mailto:support@braintrust.dev).
