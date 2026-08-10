#!/usr/bin/env bash
set -euo pipefail

PLUGIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NPM_TAG="${NPM_TAG:-latest}"
case "$NPM_TAG" in latest|rc|next|beta) ;; *) echo "unsupported NPM_TAG: $NPM_TAG" >&2; exit 1;; esac

if [[ "${DRY_RUN:-}" != "1" ]]; then
  echo "Real Pi releases must use .github/workflows/release-pi.yml" >&2
  echo "Set DRY_RUN=1 to validate the package locally." >&2
  exit 1
fi

(
  cd "$PLUGIN_DIR/content"
  pnpm install --frozen-lockfile
  pnpm run build
  pnpm publish --dry-run --no-git-checks --tag "$NPM_TAG"
)
