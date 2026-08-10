#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:?usage: build.sh <TARGET_DIR>}"
PLUGIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$PLUGIN_DIR/../../.." && pwd)"

rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR/src/runtime"
TARGET_DIR="$(cd "$TARGET_DIR" && pwd)"
tar \
  --exclude=node_modules \
  --exclude=dist \
  --exclude=.cache \
  --exclude=.vite \
  --exclude=src/runtime/daemon-client.ts \
  -cf - -C "$PLUGIN_DIR/content" . | tar -xf - -C "$TARGET_DIR"
cp "$REPO_ROOT/src/runtime/js-daemon-client/src/index.ts" \
  "$TARGET_DIR/src/runtime/daemon-client.ts"

(cd "$TARGET_DIR" && pnpm install --frozen-lockfile && pnpm run build:prepared)
rm -rf "$TARGET_DIR/node_modules"

echo "Built Pi npm package in $TARGET_DIR"
