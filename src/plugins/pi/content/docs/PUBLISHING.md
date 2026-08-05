# Publishing @braintrust/pi-extension

This document describes how this package is released to npm and what the GitHub Actions publish workflow does.

## Overview

Publishing is handled by the manual workflow at:

- `.github/workflows/publish.yml`

It runs the **shared, centrally-maintained release actions** from
[`braintrustdata/sdk-actions`](https://github.com/braintrustdata/sdk-actions), pinned by commit
SHA. Bumping that SHA pulls in upstream release-tooling improvements. This repo adds no release
logic of its own — the workflow is just the shared actions wired together.

Key properties:

- The **version comes entirely from `package.json` at the released SHA** — the workflow never
  computes, synthesizes, or overrides it. Everything published corresponds to a commit you can
  check out, so **you must merge a version bump before releasing.**
- **`channel`** picks the npm dist-tag: `latest` (stable — also git tag + GitHub release) or
  `rc`/`next`/`beta` (that dist-tag only). A prerelease is simply a committed prerelease version
  (e.g. `0.8.0-rc.1`) published to a non-`latest` tag.
- npm **trusted publishing** via GitHub Actions OIDC + provenance — no long-lived `NPM_TOKEN`.
- You release an **explicit commit SHA** (not a branch), pinning the release to a reviewed commit.
- The publish job is gated by a **GitHub Environment** (manual approval) and posts **Slack**
  notifications before approval and on completion.
- The package ships its `src/` directly (no build step); `npm publish`'s `prepack` regenerates
  `src/version.ts`.

The environments, npm trusted publisher, and Slack secret/variable are one-time setup —
see the `SETUP REQUIREMENTS` comment block at the top of `.github/workflows/publish.yml`.

## Release steps

1. **Bump the version first.** Update `package.json` to the target version (stable e.g. `0.8.0`,
   or a prerelease e.g. `0.8.0-rc.1`), run `pnpm run sync:version`, commit, and **merge to `main`**.
   The workflow reads the version from the commit you release — it cannot be set at dispatch.
2. In GitHub, open **Actions → Publish package** and **Run workflow**.
3. Set the inputs:
   - `channel`: `latest` for a stable release; `rc`/`next`/`beta` for a prerelease.
   - `sha`: the full 40-character SHA of the version-bump commit.
   - `prev_release` (optional): release-notes anchor; defaults to the previous release tag.
   - `dry_run` (optional): `true` to run the pipeline with `pnpm publish --dry-run` (no publish).
4. When the `publish` job requests it, **approve the `publish` environment**.
5. After it succeeds, verify:
   - the version exists on npm (`npm view @braintrust/pi-extension dist-tags`)
   - the provenance attestation is present on npm
   - (channel `latest` only) the `pi-extension-v<version>` tag was pushed and the GitHub release created

CI already runs the full quality suite (version-sync, format, lint, types, tests, pack, smoke)
on every push, so the release workflow does **not** re-run it — it is concerned only with release
policy and publishing.

## What the workflow does

Flow: `validate → prepare → notify-pending → [approval gate] → publish`.

1. **`validate`** (shared): reads the version from `package.json` at the SHA, then checks release
   policy — the tag isn't already taken, the npm version isn't already published, the `channel`
   is allowed, and the SHA is well-formed and on `main`. Runs with `build: false` (nothing to build).
2. **`prepare`** (shared): generates the release notes + PR list.
3. **`notify-pending`** (shared): posts a Slack notification that a release is pending approval.
4. **`publish`** (shared, gated): checks out the SHA and publishes the committed version to npm
   with provenance (OIDC) on the chosen `channel`; for `channel: latest` it also pushes the
   `pi-extension-v<version>` tag and creates the GitHub release. Posts a Slack completion notification.

`latest` → `latest` dist-tag + git tag + GitHub release. `rc`/`next`/`beta` → that dist-tag only,
no tag or release.

## Authentication model

The workflow uses GitHub Actions OIDC for npm trusted publishing. The `publish` job requests:

```yaml
permissions:
  contents: write # push the git tag + create the GitHub release
  id-token: write # OIDC token for npm trusted publishing + provenance
```

## Tagging and release naming

- git tag: `pi-extension-v<version>` (channel `latest` only)
- GitHub release name: `@braintrust/pi-extension v<version>`

Example for `0.8.0`: tag `pi-extension-v0.8.0`, release `@braintrust/pi-extension v0.8.0`.

## Failure modes

- the target tag already exists, or the npm version is already published → `validate` fails early
- the channel isn't in the allowlist, or the SHA is malformed → `validate` fails
- the npm trusted publisher isn't configured for the `publish` environment → publish fails
  with `ENEEDAUTH`. A `dry_run` does **not** exercise OIDC, so the first **real** publish is the
  first true test — canary with a prerelease (`channel: rc`) before any stable release.
- the tag/GitHub release are created only after a successful publish, so a failed publish leaves
  no orphaned tag

## Local preflight checks

The release workflow relies on CI for quality, but before bumping a version it's still useful to run:

```bash
pnpm run sync:version
pnpm run check
pnpm test
pnpm run pack
pnpm pack --dry-run
pnpm run smoke
```

## Notes

- The package is public; `package.json` includes `publishConfig.access = "public"`.
- The workflow is manual (`workflow_dispatch`) and releases an explicit `sha`, not a branch.
- `src/version.ts` is generated from `package.json` (by `scripts/sync-version.mjs`, run via
  `prepack`) so trace metadata reports the version without runtime package-metadata loading.
- The shared actions are pinned to a `braintrustdata/sdk-actions` SHA in `publish.yml`; bumping it
  adopts upstream improvements.
- If the release process changes, update this file, `AGENTS.md`, `CONTRIBUTING.md`, and
  `.github/workflows/publish.yml` as needed.
