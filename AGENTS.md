# Braintrust coding-agent plugins — monorepo

This repo is the **single source of truth** for Braintrust's coding-agent plugins
(Claude Code, Codex, …). Each agent's plugin is developed here, then **built** and
**deployed** to a per-agent **distribution repo** that the agent's marketplace
installs from. Keeping one monorepo means a fix to shared behavior lands in one
place instead of N hand-maintained repos.

## Layout

```
src/plugins/<agent>/         one directory per agent (claude, codex)
  content/                   the deployable plugin tree, verbatim (what ships)
  build.sh <dir>             assemble the deployable tree into <dir>
  validate.sh <dir>          sanity-check a built tree (manifests, required files)
  publish.sh                 deploy a built tree to a distribution repo
scripts/publish.sh           read the PUBLISH_TARGETS map, dispatch to each plugin's publish.sh
Makefile                     build / test / publish entry points
.github/workflows/           CI + release automation
bt-daemon/                   shared Rust project (self-contained; placeholder for now)
```

Everything an agent installs lives under `src/plugins/<agent>/content/`. `build.sh`
is mostly a copy of that tree; the one exception is codex, whose `build.sh` also
compiles the `trace-codex` hook binaries into the tree at deploy time.

## How versioning works

Versioning is **per plugin**. Each plugin carries its own version in its manifest
JSON (`content/plugins/<plugin>/.<agent>-plugin/plugin.json`), and a release bumps
those version fields (never the marketplace manifest). `scripts/set-plugin-version.py`
does the bump.

## Distribution repos

Built plugin trees are pushed to dedicated repos that marketplaces install from
(the install URLs users already use):

| Agent  | Distribution repo                            |
|--------|----------------------------------------------|
| claude | `braintrustdata/braintrust-claude-plugin`    |
| codex  | `braintrustdata/braintrust-codex-plugin`     |

A distribution repo is a **generated artifact**: each deploy clones it, replaces
its whole tracked tree with a fresh build, and pushes. `braintrustdata/test-coding-agent-dist`
is a shared sandbox used for dry runs.

## Local development

```
make build            # build every plugin into dist/<agent>
make build-codex      # build just one
make test             # build + validate every plugin
```

To deploy manually, set the `PUBLISH_TARGETS` map (`plugin:repo`, comma-separated)
and run `make publish`. It validates the map first (rejects unknown plugins,
malformed entries, or two plugins pointing at the same repo), then for each target
clones the dist repo, rebuilds, and pushes:

```
PUBLISH_TARGETS="codex:braintrustdata/test-coding-agent-dist" make publish
DRY_RUN=1 PUBLISH_TARGETS="..." make publish   # build + commit locally, skip the push
```

Cross-repo pushes need a token with `contents:write` on the target repo, supplied
as `GH_TOKEN` (or ambient git credentials for an ssh URL).

## Releasing (CI)

Releases are **manual** GitHub Actions (Actions tab → Run workflow):

- **Release plugin** (`release.yml`) — pick `version` + `plugin`. Deploys to the
  **production** dist repo and does the full flow: bump the plugin's version JSON,
  commit to `main`, tag `v<version>-<plugin>`, create a GitHub Release, then deploy.
- **Release plugin (test)** (`test-release.yml`) — same dropdowns, deploys to the
  **test** repo, and **skips** the commit/tag/release so it leaves no trace on the
  monorepo and can be re-run freely. Use it to dry-run a release end to end.

Both are thin callers of the reusable `_release.yml`, which holds the shared logic
(a `record` flag gates the monorepo commit/tag/release).

After a codex deploy, a **smoke test** (`smoke-codex.yml`) installs the just-deployed
plugin from the marketplace on Linux + both macOS arches and asserts a real Codex
session traces to Braintrust. It's a post-deploy verification (not a gate) and is
skipped if no `OPENAI_API_KEY` secret is configured.

`ci.yml` runs `make test` on pushes and PRs to `main`.

## Secrets (CI)

- `PUBLISH_TOKEN` — `contents:write` on the distribution repos; used for cross-repo
  deploys and to clone private dist repos.
- `OPENAI_API_KEY` — used by the codex smoke test; optional (smoke skips without it).

## bt-daemon

`bt-daemon/` is a self-contained Rust project (its own Cargo workspace) intended to
become shared plugin logic. It's a placeholder today and depends on nothing else in
the repo, so it can be lifted into its own repo later. See `bt-daemon/README.md`.
