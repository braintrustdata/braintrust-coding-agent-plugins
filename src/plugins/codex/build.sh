#!/usr/bin/env bash
#
# build.sh — Assemble the Codex plugin distribution tree into $1.
#
# The Codex marketplace consumes a repo whose ROOT is the marketplace:
#   .agents/plugins/marketplace.json   marketplace manifest
#   plugins/braintrust-codex-plugin/   skills plugin (MCP + skills)
#   plugins/trace-codex/               thin hooks over the shared bt daemon
#
# The tracing launcher finds (or installs) `bt` and invokes
# `bt daemon hook --source codex`; there is no per-plugin compiled binary.
# Usage: build.sh <TARGET_DIR>   (TARGET_DIR is created if missing)

set -euo pipefail

TARGET_DIR="${1:?usage: build.sh <TARGET_DIR>}"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTENT_DIR="$SRC_DIR/content"

mkdir -p "$TARGET_DIR"
rsync -a --delete --exclude '.git' --exclude 'node_modules' "$CONTENT_DIR/" "$TARGET_DIR/"
echo "Built codex dist into $TARGET_DIR (content from $CONTENT_DIR)."
