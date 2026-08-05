# AGENTS.md

This file is consumed by agents. Keep it in sync with [CONTRIBUTING.md](./CONTRIBUTING.md), which is written for humans. For contributor-facing setup, repository conventions, architecture, and validation commands, see `CONTRIBUTING.md`; this file keeps the agent-specific guidance concise.

## Project

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

## Agent reminders

- Read `README.md` first for external behavior.
- Use `CONTRIBUTING.md` for development workflow, repo layout, and validation details.
- This repo is managed by [mise](https://mise.jdx.dev/) using `.tool-versions`; prefer that as the toolchain source of truth.
- Preserve package identity in `package.json` as a pi package.
- Keep the extension entrypoint at `src/index.ts` unless there is a strong reason to change it.
- Keep `src/version.ts` generated from `package.json`; do not edit it by hand. After a package version bump, run `pnpm run sync:version` and commit the generated change.
- Prefer evolving the current design over rewriting it.
- Tracing must be best-effort: log failures and continue.
- Preserve the intended trace hierarchy unless there is a strong reason to change it:

```text
Session (task)
├── Turn N (task)
│   ├── LLM call(s) (llm)
│   └── Tool call(s) (tool)
```

- Configuration precedence should remain:
  1. defaults
  2. `~/.pi/agent/braintrust.json`
  3. `.pi/braintrust.json`
  4. environment variables
- `.pi/braintrust.json` is extension-specific, not a built-in pi config file.
- Prefer integration coverage for lifecycle/session regressions: use the real pi SDK harness in `src/index.integration.test.ts` when testing restore, reload, session switch/fork, headless behavior, or cross-version compatibility.

## What to avoid

Unless explicitly requested, do **not**:

- add MCP-related code
- add Braintrust query tools for the agent
- add unrelated pi commands/UI features
- add heavy dependencies
- make tracing depend on interactive-only pi behavior

## Validation

After code changes, at minimum run:

```bash
pnpm run check
pnpm test
pnpm run smoke
```

If the change affects extension lifecycle behavior, session persistence/restore, reload/fork/switch flows, or pi compatibility assumptions, also run:

```bash
pnpm run test:integration
```

If packaging changes or release version bumps were made, also run:

```bash
pnpm run sync:version
pnpm run check
pnpm run pack
pnpm pack --dry-run
```
