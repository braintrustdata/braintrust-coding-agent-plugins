# Development Guide

OpenCode tracing is deliberately split into two independent runtime paths:

- `src/tracing/daemon.ts` forwards raw OpenCode events to `bt` over the shared daemon client.
- `src/tools/` implements the four optional Braintrust data-access tools.

JavaScript must not construct spans, queue trace delivery, persist trace state,
or send tracing data to the Braintrust API. All tracing translation and
delivery belongs to `bt-daemon`.

## Local checks

```bash
bun install --frozen-lockfile
bun run check
bun run typecheck
bun test
bun run build
```

From the monorepo root, `make validate-opencode` additionally builds and checks
the npm tarball. The real-agent integration test runs OpenCode with deterministic
inference through the daemon and mock Braintrust ingest.

## Adding hooks

Forward the unmodified native input/output in `src/tracing/daemon.ts`, then add
or update the corresponding Rust translator behavior and fixtures under
`bt-daemon/`. Do not add JavaScript processing state.

## Adding tools

Add tool definitions under `src/tools/`. Tool-only API access must remain
contained in `src/tools/client.ts` and must not be imported by tracing code.
