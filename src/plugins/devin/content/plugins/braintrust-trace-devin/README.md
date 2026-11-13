# Braintrust Trace Devin

This package reserves the `braintrust-trace-devin` plugin for future opt-in
Devin session tracing. Its `hooks.json` is an empty JSON object. Installing it
runs no hooks, captures no events, and starts no daemon or MCP server.

Tracing capture, daemon translation, and `bt trace` setup/run/import support
will be added separately with end-to-end verification. Local and Cloud tracing
need their respective runtime and authentication paths verified before support
is advertised.

To inspect the scaffold locally, from the monorepo root:

```bash
make validate-devin
devin plugins install --local ./dist/devin/plugins/braintrust-trace-devin
```

The independent [`braintrust` plugin](../braintrust/README.md) provides hosted
MCP access and can be installed on its own. Keep this scaffold out of the Devin
marketplace until tracing is implemented and verified.
