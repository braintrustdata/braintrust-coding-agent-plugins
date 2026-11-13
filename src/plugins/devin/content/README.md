# Braintrust plugins for Devin

This tree contains two independent Devin plugins:

| Package | Purpose | Status |
| --- | --- | --- |
| [`braintrust`](plugins/braintrust/README.md) | Hosted Braintrust MCP access through OAuth | Awaiting authenticated Devin verification |
| [`braintrust-trace-devin`](plugins/braintrust-trace-devin/README.md) | Devin session tracing | Inactive scaffold; hooks and daemon support are not implemented |

Install the individual plugin subdirectory. This root is a build output container,
not an installable plugin or a bundle that installs both packages.

The Devin marketplace should reference
`src/plugins/devin/content/plugins/braintrust` in this monorepo using a
`git-subdir` source pinned to the merged commit. Keep the tracing scaffold out of
the marketplace until tracing is implemented and verified.

From the monorepo root:

```bash
make validate-devin
devin plugins install --local ./dist/devin/plugins/braintrust
devin mcp login braintrust
```

Start a new Devin session and call a Braintrust tool to verify the connection.

Updating a marketplace listing requires advancing its source commit pin.
Existing SHA-pinned installations also require their source pin to be updated;
reindexing alone does not advance a pin. Plugin changes apply to new sessions.
