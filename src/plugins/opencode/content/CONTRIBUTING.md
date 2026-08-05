# Development Guide

OpenCode tracing is deliberately split into two independent runtime paths:

- `src/tracing/daemon.ts` forwards raw OpenCode events to `bt` over the shared daemon client.
- `src/tools/` delegates the four optional Braintrust data-access tools to the
  installed `bt` CLI.

JavaScript must not construct spans, queue trace delivery, persist trace state,
manage credentials, or call the Braintrust API. Translation and delivery belong
to `bt-daemon`; data-access tools invoke `bt` with profile/org/project selection.

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

Add tool definitions under `src/tools/` and expose the required operation in
`src/tools/bt-cli.ts`. Use argument arrays with `execFile`; never invoke a shell,
read credentials, or implement Braintrust HTTP requests in the package.
