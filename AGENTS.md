# Braintrust coding-agent plugins — monorepo

This repo is the single source of truth for Braintrust's coding-agent plugins.
Each plugin is developed here, built, and deployed to a per-agent distribution
repository that its marketplace installs from.

## Layout

```text
src/plugins/<agent>/         one directory per agent
  content/                   the deployable plugin tree, verbatim
  build.sh <dir>             assemble the deployable tree into <dir>
  validate.sh <dir>          validate manifests and required files
  publish.sh                 deploy a built tree to a distribution repo
scripts/publish.sh           dispatch PUBLISH_TARGETS to each plugin
bt-daemon/                   shared Rust tracing crate embedded by bt
Makefile                     build, test, and publish entry points
.github/workflows/           CI and release automation
```

Everything an agent installs lives under `src/plugins/<agent>/content/`.
Plugin hooks are thin fail-open shell launchers that invoke
`bt trace hook`; they do not contain or compile a second tracing runtime.

## Adding a coding-agent integration

For complete integrations, use `$add-coding-agent-integration` to delegate the
tracing audit, translator, capture, setup, managed run, import/attach, testing,
and shipping work to the corresponding repo-local skills. Use a component skill
directly for focused work. Every integration uses one source identity and daemon
translator; hooks and plugins remain thin, fail-open, credential-free forwarders.

## Local development

```bash
make build
make build-codex
make test
cargo test --manifest-path bt-daemon/Cargo.toml --all-features
```

## Versioning and distribution

Versioning is per distribution. Claude and Codex plugins carry their version in
their plugin manifests, and `scripts/set-plugin-version.py` updates those
manifests for a release. Antigravity's schema does not expose a version field,
so its distribution is versioned by repository tags and GitHub Releases.
Claude's marketplace manifest carries a separate catalog version; bump it
independently for breaking marketplace changes. The Codex marketplace manifest
is unversioned.

| Agent | Distribution repository |
|---|---|
| antigravity | `braintrustdata/braintrust-antigravity-plugin` |
| claude | `braintrustdata/braintrust-claude-plugin` |
| codex | `braintrustdata/braintrust-codex-plugin` |

A distribution repository is a generated artifact. Each deploy clones it,
replaces the tracked tree with a fresh build, and pushes the result.
`braintrustdata/test-coding-agent-dist` is the shared release sandbox.

To deploy manually, provide a comma-separated `plugin:repo` map:

```bash
PUBLISH_TARGETS="codex:braintrustdata/test-coding-agent-dist" make publish
DRY_RUN=1 PUBLISH_TARGETS="codex:braintrustdata/test-coding-agent-dist" make publish
```

Cross-repository pushes use `GH_TOKEN` or ambient Git credentials.

## Releasing

The manual `release.yml` workflow deploys a production release, records the
version bump on `main`, tags it, and creates a GitHub Release. The manual
`test-release.yml` workflow exercises the same deployment against the test
repository without committing or tagging. Both call `_release.yml`.

A Codex deployment can run `smoke-codex.yml`, which installs the deployed
plugin and runs a real Codex session through the daemon when
`OPENAI_API_KEY` is available.

CI builds and validates both plugin packages and builds, tests, and lints the
Rust daemon on Linux, macOS, and Windows. Concurrent runs for an obsolete
branch revision are cancelled.

## Secrets

- `BRAINTRUST_BOT_APP_ID` / `BRAINTRUST_BOT_PRIVATE_KEY` are the
  `braintrustdata/braintrust-bot` GitHub App credentials. Release workflows mint
  a short-lived installation token from them, scoped to `contents:write` on the
  one distribution repository being deployed. The app must be installed on every
  distribution repository, or the token step fails.
- `OPENAI_API_KEY` enables the optional real Codex smoke test.

Braintrust authentication is deliberately not stored in plugin or daemon
settings. The embedding `bt` CLI owns profiles, OAuth, keychain access, API
keys, token refresh, and backend URL resolution.

## bt-daemon

`bt-daemon/` is one self-contained Rust crate. The library is embedded by `bt`;
the feature-gated standalone binary exists for development and integration
tests. It owns event journaling, agent-specific translation, span construction,
recovery, transport, and delivery. See `bt-daemon/README.md`.
