# Braintrust coding-agent plugins

Trace coding-agent sessions in [Braintrust](https://www.braintrust.dev).
This monorepo contains the plugins and the shared Rust tracing daemon embedded
in the Braintrust CLI (`bt`). Plugins forward native events locally; the daemon
builds traces, journals events for recovery, and delivers spans to Braintrust.

## Get started

Install your coding agent and the
[Braintrust CLI](https://www.braintrust.dev/docs/reference/cli/quickstart),
then authenticate and enable tracing. For example:

```bash
bt login
bt trace enable claude --project my-coding-agent
```

Replace `claude` with your agent's CLI name below. Restart the agent after
setup and follow any plugin or hook trust prompts.

## Supported agents

All six support `bt trace enable`, `update`, `disable`, and `doctor`.

| Agent and setup guide | CLI name | One-off traced run | Import / live attach | Distribution |
|---|---|---|---|---|
| [Google Antigravity](src/plugins/antigravity/content/README.md) | `antigravity` | — | Yes | [GitHub](https://github.com/braintrustdata/braintrust-antigravity-plugin) |
| [Claude Code](src/plugins/claude/content/README.md) | `claude` | Yes | Yes | [GitHub](https://github.com/braintrustdata/braintrust-claude-plugin) |
| [Codex](src/plugins/codex/content/README.md) | `codex` | Yes | Yes | [GitHub](https://github.com/braintrustdata/braintrust-codex-plugin) |
| [Grok](src/plugins/grok/content/README.md) | `grok` | — | — | [GitHub](https://github.com/braintrustdata/braintrust-grok-plugin) |
| [OpenCode](src/plugins/opencode/content/README.md) | `opencode` | Yes | — | [npm](https://www.npmjs.com/package/@braintrust/trace-opencode) |
| [Pi](src/plugins/pi/content/README.md) | `pi` | Yes | — | [npm](https://www.npmjs.com/package/@braintrust/pi-extension) |

Use `run` to trace one invocation, `import` to trace a saved session, or
`import --attach` to follow an active transcript:

```bash
bt trace run --project my-coding-agent claude -- -p "summarize this repository"
bt trace import claude SESSION_ID
bt trace import claude SESSION_ID --attach
```

See the agent guides for limitations. Antigravity setup requires a
Unix-compatible shell.

## Manage tracing

```bash
bt trace doctor claude
bt trace status
bt trace update claude
bt trace disable claude
```

`doctor` shows which configuration is in use. `status` reports trace delivery.
`update` keeps your settings; `disable` removes the plugin and its settings.

## Development

```text
src/plugins/<agent>/content/   Plugin source copied into distribution builds
src/runtime/js-daemon-client/  Shared JavaScript event-forwarding client
src/skills/                   Shared skills included in plugin builds
bt-daemon/                    Rust daemon, translators, and integration tests
scripts/                      Shared build and publishing tools
.github/workflows/            CI and release automation
```

Use Bash and Python 3 for plugin scripts, Node.js 24 and pnpm 11.21.0 for the
JavaScript packages, and stable Rust for the daemon. Run from the monorepo root:

```bash
make build                 # Build all plugins into dist/<agent>
make build-codex            # Build one plugin
make test                  # Build and validate plugin packages and forwarders
cargo test --manifest-path bt-daemon/Cargo.toml --all-features --locked
```

Real-agent tests require installed agents and are run separately; see the
[test harness guide](bt-daemon/tests/support/README.md). See the
[daemon README](bt-daemon/README.md) for architecture and local debugging.

## Releases and contributions

Edit plugin sources here. The per-agent GitHub distribution repositories and
npm packages are generated artifacts. See [AGENTS.md](AGENTS.md) for the release
PR and approval workflow, sandbox publishing, and npm release workflows.
