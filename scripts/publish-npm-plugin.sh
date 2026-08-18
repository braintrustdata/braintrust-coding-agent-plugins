#!/usr/bin/env bash
# Shared guarded local publish path for npm-delivered coding-agent plugins.
set -euo pipefail

AGENT="${1:?usage: publish-npm-plugin.sh <pi|opencode>}"
case "$AGENT" in
  pi) DISPLAY="Pi" ;;
  opencode) DISPLAY="OpenCode" ;;
  *) echo "unsupported npm plugin: $AGENT" >&2; exit 2 ;;
esac

NPM_TAG="${NPM_TAG:-latest}"
case "$NPM_TAG" in latest|rc|next|beta) ;; *) echo "unsupported NPM_TAG: $NPM_TAG" >&2; exit 1;; esac

if [[ "${DRY_RUN:-}" != "1" ]]; then
  echo "Real $DISPLAY releases must use .github/workflows/release-$AGENT.yml" >&2
  echo "Set DRY_RUN=1 to validate the package locally." >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
(
  cd "$REPO_ROOT/src/plugins/$AGENT/content"
  pnpm install --frozen-lockfile
  pnpm run build
  pnpm publish --dry-run --no-git-checks --tag "$NPM_TAG"
)
