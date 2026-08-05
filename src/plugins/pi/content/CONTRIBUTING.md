# Contributing to @braintrust/pi-extension

Thanks for contributing.

This document is for repository contributors and humans. Keep it in sync with [AGENTS.md](./AGENTS.md), which is consumed by agents. For installation and usage, see [README.md](./README.md). For publishing and release steps, see [docs/PUBLISHING.md](./docs/PUBLISHING.md). For agent-specific instructions, see [AGENTS.md](./AGENTS.md).

## Project scope

This repository contains `@braintrust/pi-extension`, a **pi extension/package** that adds **automatic Braintrust tracing** to pi sessions today.

This project is intentionally **tracing-only**:

- no MCP integration
- no Braintrust query tools exposed to the model
- no UI-heavy features unless directly required for tracing/debugging

## Goals

When working in this repo, optimize for:

1. **Correct trace structure** in Braintrust
2. **Low overhead / non-blocking behavior** during pi sessions
3. **Safe failure behavior** — tracing must never break the pi session
4. **Simple install/use as a pi package**

## Development setup

This repo is set up for [Vite+](https://viteplus.dev/guide/).

This repo is managed by [mise](https://mise.jdx.dev/). Use the pinned project toolchain from `.tool-versions`, including `node`, `pnpm`, and `vite-plus` (which provides `vp`):

```bash
mise install
pnpm install
pnpm run sync:version
pnpm run check
pnpm run pack
pnpm run smoke
```

Or through package scripts:

```bash
pnpm run check
pnpm run typecheck
pnpm run test:integration
pnpm run pack
pnpm run smoke
```

Notes:

- `pnpm run sync:version` regenerates `src/version.ts` from `package.json` after a version bump.
- `pnpm run check` verifies `src/version.ts` is in sync, then runs the main formatting, linting, and type-check entrypoint.
- `pnpm run typecheck` delegates to `vp check`.
- `pnpm run pack` builds an optional library bundle in `dist/`.
- pi loads the extension directly from `src/index.ts`, so local development does not require a build step.
- Publishing is handled by `.github/workflows/publish.yml` using npm trusted publishing with OIDC and provenance attestations; see [docs/PUBLISHING.md](./docs/PUBLISHING.md).

## Repository layout

- `src/index.ts`
  - main pi extension entrypoint
  - subscribes to pi lifecycle events
  - creates session / turn / llm / tool spans
- `src/client.ts`
  - thin wrapper around the official Braintrust JavaScript SDK
  - initializes the project logger
  - creates, updates, and flushes spans safely
- `src/config.ts`
  - loads config from:
    1. defaults
    2. `~/.pi/agent/braintrust.json`
    3. `.pi/braintrust.json`
    4. env vars
- `src/state.ts`
  - persistent local session bookkeeping
- `src/utils.ts`
  - content normalization, truncation, IDs, small helpers
- `src/types.ts`
  - shared TypeScript types for config, spans, state, and normalized messages
- `src/version.ts`
  - generated extension version constant derived from `package.json`
  - do not edit by hand; run `pnpm run sync:version` after changing the package version

## Repo conventions

### Language / module system

- Use **TypeScript ESM** (`.ts`).
- Keep dependencies minimal.
- Prefer Node built-ins over adding packages.
- This repo uses **Vite+** for checks and packaging.
- pi loads the extension directly from TypeScript; no build output directory is required for local development.
- `src/version.ts` is generated at package/build time from `package.json`; `prepack` regenerates it and `check` fails if it is stale.

### Style

- Keep code small and readable.
- Prefer small helper functions over deeply nested event handlers.
- Avoid clever abstractions unless they clearly improve tracing correctness.
- Do not introduce formatting churn.

### Error handling

- Tracing must be **best-effort**.
- Never throw from normal extension event paths if it can be avoided.
- Log failures and continue.
- If Braintrust is unavailable, pi should still work normally.

### Performance

- Avoid blocking network calls in critical event handlers whenever possible.
- Prefer the Braintrust SDK's async/background flushing path.
- Keep payloads reasonably small.
- Truncate large values before sending.

## Trace model

The intended hierarchy is:

```text
Session (task)
├── Turn N (task)
│   ├── LLM call(s) (llm)
│   └── Tool call(s) (tool)
```

Expected behavior:

- one **session/root span** per pi session that produces at least one turn
- one **turn span** per user prompt / agent run
- one **llm span** per assistant/model response
- one **tool span** per tool execution

If changing event wiring, preserve this structure unless there is a strong reason to change it.

## Configuration expectations

This extension uses its own config files:

- global: `~/.pi/agent/braintrust.json`
- project: `.pi/braintrust.json`

This is modeled after the OpenCode Braintrust plugin, not Claude Code’s settings-based setup.

Important:

- `.pi/braintrust.json` is **extension-specific**, not a built-in pi config file.
- environment variables should remain the highest-precedence source.

## What to avoid

Unless the change explicitly calls for it, do **not**:

- add MCP-related code
- add Braintrust query tools for the agent
- add unrelated pi commands/UI features
- add heavy dependencies
- make tracing depend on interactive-only pi behavior

## Safe ways to extend

Good additions include:

- better parent/root span linking
- better session/fork/subagent handling
- improved metadata and token accounting
- more robust recovery across session switches
- test coverage / smoke checks
- publish/install polish for pi packages

## Validation

After code changes, at minimum run:

```bash
pnpm run check
pnpm test
pnpm run smoke
```

If packaging changes or release version bumps were made, also run:

```bash
pnpm run sync:version
pnpm run check
pnpm run pack
pnpm pack --dry-run
```

Test commands:

- `pnpm test` — full unit + integration test suite
- `pnpm run test:integration` — integration coverage for real pi extension loading/session flows

## Additional notes

- Read `README.md` first for external behavior.
- Preserve package identity in `package.json` as a pi package.
- Keep the extension entrypoint at `src/index.ts` unless there is a strong reason to change it.
- Prefer evolving the current design over rewriting it.
