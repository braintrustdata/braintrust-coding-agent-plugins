#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:?usage: build.sh <TARGET_DIR>}"
PLUGIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$PLUGIN_DIR/../../.." && pwd)"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR" "$STAGE/src/runtime"
TARGET_DIR="$(cd "$TARGET_DIR" && pwd)"
tar --exclude=node_modules --exclude=dist -cf - -C "$PLUGIN_DIR/content" . \
  | tar -xf - -C "$STAGE"
cp "$REPO_ROOT/src/runtime/js-daemon-client/src/index.ts" "$STAGE/src/runtime/daemon-client.ts"

(cd "$STAGE" && pnpm install --frozen-lockfile && pnpm exec vp fmt src/runtime/daemon-client.ts && pnpm run sync:version && pnpm run pack)
rm -rf "$STAGE/node_modules"
cp -R "$STAGE/." "$TARGET_DIR/"
(cd "$TARGET_DIR" && pnpm pack --pack-destination "$TARGET_DIR" >/dev/null)

echo "Built Pi npm package and tarball in $TARGET_DIR"
