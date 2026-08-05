#!/usr/bin/env bash
set -euo pipefail

PLUGIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-$PLUGIN_DIR/../../../dist/pi}"
NPM_TAG="${NPM_TAG:-latest}"
case "$NPM_TAG" in latest|rc|next|beta) ;; *) echo "unsupported NPM_TAG: $NPM_TAG" >&2; exit 1;; esac
tarball="$(find "$BUILD_DIR" -maxdepth 1 -name 'braintrust-pi-extension-*.tgz' -print -quit)"
[[ -n "$tarball" ]] || { echo "build Pi before publishing" >&2; exit 1; }
version="$(node -p "require('$BUILD_DIR/package.json').version")"
if npm view "@braintrust/pi-extension@$version" version >/dev/null 2>&1; then
  echo "@braintrust/pi-extension@$version is already published" >&2; exit 1
fi
if [[ "${DRY_RUN:-}" == "1" ]]; then npm publish "$tarball" --tag "$NPM_TAG" --dry-run; exit 0; fi
[[ "${RELEASE_CONFIRM:-}" == "publish-@braintrust/pi-extension@$version" ]] || {
  echo "set RELEASE_CONFIRM=publish-@braintrust/pi-extension@$version for a release-approved publish" >&2; exit 1;
}
npm publish "$tarball" --tag "$NPM_TAG"
