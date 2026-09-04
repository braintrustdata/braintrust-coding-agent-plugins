#!/usr/bin/env bash
# Build and deploy the Grok plugin to a generated distribution repository.
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

git clone --depth 1 "$CLONE_URL" "$WORKTREE" 2>/dev/null \
  || git clone "$CLONE_URL" "$WORKTREE"
git -C "$WORKTREE" config user.name "github-actions[bot]"
git -C "$WORKTREE" config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git -C "$WORKTREE" rm -rfq --ignore-unmatch . >/dev/null 2>&1 || true
"$SRC_DIR/build.sh" "$WORKTREE"
"$SRC_DIR/validate.sh" "$WORKTREE"

git -C "$WORKTREE" add -A
if git -C "$WORKTREE" diff --cached --quiet; then
  echo "==> $SLUG already up to date; nothing to publish."
  exit 0
fi

SRC_SHA="$(git -C "$SRC_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
git -C "$WORKTREE" commit -q -m "build: deploy grok plugin from monorepo@${SRC_SHA}"
if [[ "${DRY_RUN:-}" == "1" ]]; then
  echo "==> DRY_RUN=1: built + committed locally, skipping push."
  git -C "$WORKTREE" --no-pager show --stat HEAD | head -30
  exit 0
fi

git -C "$WORKTREE" push origin HEAD:main
echo "==> Deployed grok plugin to $SLUG."
