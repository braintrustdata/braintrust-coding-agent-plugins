#!/usr/bin/env bash
#
# build.sh — Assemble the Codex plugin distribution tree into $1.
#
# The Codex marketplace consumes a repo whose ROOT is the marketplace:
#   .agents/plugins/marketplace.json   marketplace manifest
#   plugins/braintrust-codex-plugin/   skills plugin (MCP + skills)
#   plugins/trace-codex/               tracing plugin (thin daemon hook shims)
#
# The tracing plugin contains no tracing runtime or platform-specific binary;
# its launchers forward raw hook payloads to `bt trace hook`.
#
# Usage: build.sh <TARGET_DIR>   (TARGET_DIR is created if missing)

set -euo pipefail

TARGET_DIR="${1:?usage: build.sh <TARGET_DIR>}"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTENT_DIR="$SRC_DIR/content"

mkdir -p "$TARGET_DIR"
rsync -a --delete --exclude '.git' --exclude 'node_modules' "$CONTENT_DIR/" "$TARGET_DIR/"

echo "Built codex dist into $TARGET_DIR (content from $CONTENT_DIR)."
