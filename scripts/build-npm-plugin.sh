#!/usr/bin/env bash
# Shared package assembly for the Pi and OpenCode npm distribution family.
set -euo pipefail

AGENT="${1:?usage: build-npm-plugin.sh <pi|opencode> <TARGET_DIR>}"
TARGET_DIR="${2:?usage: build-npm-plugin.sh <pi|opencode> <TARGET_DIR>}"
case "$AGENT" in pi|opencode) ;; *) echo "unsupported npm plugin: $AGENT" >&2; exit 2;; esac

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTENT_DIR="$REPO_ROOT/src/plugins/$AGENT/content"

rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR/src/runtime"
TARGET_DIR="$(cd "$TARGET_DIR" && pwd)"
tar \
  --exclude=node_modules \
  --exclude=dist \
  --exclude=.cache \
  --exclude=.vite \
  --exclude=src/runtime/daemon-client.ts \
  -cf - -C "$CONTENT_DIR" . | tar -xf - -C "$TARGET_DIR"
cp "$REPO_ROOT/src/runtime/js-daemon-client/src/index.ts" \
  "$TARGET_DIR/src/runtime/daemon-client.ts"

(cd "$TARGET_DIR" && pnpm install --frozen-lockfile && pnpm run build:prepared)
rm -rf "$TARGET_DIR/node_modules"

echo "Built $AGENT npm package in $TARGET_DIR"
