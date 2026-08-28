#!/usr/bin/env bash
#
# publish.sh — Build the Google Antigravity plugin and deploy the tree to the
# distribution repository that `agy plugin install` clones.
#
# The distribution repository is a generated artifact. This script clones it,
# replaces its tracked tree with a fresh build, validates the result, and pushes
# only when the generated content changed.
#
# Env:
#   DIST_REPO   (required) target dist repo as a git URL or owner/name slug
#   DRY_RUN=1   build + commit locally but skip the push
#   GH_TOKEN    used for HTTPS clone/push auth when DIST_REPO is a slug

set -euo pipefail

: "${DIST_REPO:?set DIST_REPO=<git-url|owner/name> (usually via PUBLISH_TARGETS)}"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SLUG="$(printf '%s' "$DIST_REPO" | sed -E 's#^git@[^:]+:##; s#^https?://[^/]+/##; s#\.git$##')"

case "$DIST_REPO" in
  *://*|*@*) CLONE_URL="$DIST_REPO" ;;
  *)
    if [[ -n "${GH_TOKEN:-}" ]]; then
      CLONE_URL="https://x-access-token:${GH_TOKEN}@github.com/${DIST_REPO}.git"
    else
      CLONE_URL="https://github.com/${DIST_REPO}.git"
    fi
    ;;
esac

WORKTREE="$(mktemp -d)/dist"
cleanup() { rm -rf "$(dirname "$WORKTREE")"; }
trap cleanup EXIT

echo "==> Cloning $SLUG"
git clone --depth 1 "$CLONE_URL" "$WORKTREE" 2>/dev/null \
  || git clone "$CLONE_URL" "$WORKTREE"
git -C "$WORKTREE" config user.name "github-actions[bot]"
git -C "$WORKTREE" config user.email "41898282+github-actions[bot]@users.noreply.github.com"

echo "==> Rebuilding tree from source"
git -C "$WORKTREE" rm -rfq --ignore-unmatch . >/dev/null 2>&1 || true
"$SRC_DIR/build.sh" "$WORKTREE"
"$SRC_DIR/validate.sh" "$WORKTREE"

git -C "$WORKTREE" add -A
if git -C "$WORKTREE" diff --cached --quiet; then
  echo "==> $SLUG already up to date; nothing to publish."
  exit 0
fi

SRC_SHA="$(git -C "$SRC_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
git -C "$WORKTREE" commit -q -m "build: deploy antigravity plugin from monorepo@${SRC_SHA}"

if [[ "${DRY_RUN:-}" == "1" ]]; then
  echo "==> DRY_RUN=1: built + committed locally, skipping push."
  git -C "$WORKTREE" --no-pager show --stat HEAD | head -30
  exit 0
fi

echo "==> Pushing to $SLUG (main)"
git -C "$WORKTREE" push origin HEAD:main
echo "==> Deployed antigravity plugin to $SLUG."
