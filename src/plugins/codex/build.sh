#!/usr/bin/env bash
#
# build.sh — Assemble the Codex plugin distribution tree into $1.
#
# The Codex marketplace consumes a repo whose ROOT is the marketplace:
#   .agents/plugins/marketplace.json   marketplace manifest
#   plugins/braintrust-codex-plugin/   skills plugin (MCP + skills)
#   plugins/trace-codex/               tracing plugin (TS event server + hooks)
#
# trace-codex ships as SOURCE only. The compiled `codex-hook` binary is not
# committed; the launcher (bin/codex-hook.sh) downloads the matching binary from
# the dist repo's GitHub Releases at runtime, and local dev installs build it
# with `pnpm run build` (tsx/tsup + pkg). So this build is a content assembly,
# not a compile.
#
# Everything deployable lives under content/. Today the assembly is a straight
# copy; the seam is here for when the generic event-server code is extracted to
# a shared location and injected into trace-codex at build time.
#
# Optional env:
#   CODEX_DIST_REPO   owner/name of the dist repo whose Releases host the
#                     codex-hook binaries. When set, rewrites the launcher's
#                     REPO= line so a fork/scratch repo resolves its own binaries.
#
# Usage: build.sh <TARGET_DIR>   (TARGET_DIR is created if missing)

set -euo pipefail

TARGET_DIR="${1:?usage: build.sh <TARGET_DIR>}"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTENT_DIR="$SRC_DIR/content"

mkdir -p "$TARGET_DIR"
rsync -a --delete --exclude '.git' "$CONTENT_DIR/" "$TARGET_DIR/"

# Point the runtime binary launcher at the dist repo we're deploying to, if
# overridden (defaults to the braintrustdata/braintrust-codex-plugin baked into
# the launcher).
if [[ -n "${CODEX_DIST_REPO:-}" ]]; then
  launcher="$TARGET_DIR/plugins/trace-codex/bin/codex-hook.sh"
  sed -i.bak -E "s#^REPO=\"[^\"]*\"#REPO=\"${CODEX_DIST_REPO}\"#" "$launcher"
  rm -f "$launcher.bak"
fi

echo "Built codex dist into $TARGET_DIR (content from $CONTENT_DIR)."
