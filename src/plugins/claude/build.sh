#!/usr/bin/env bash
#
# build.sh — Assemble the Claude Code plugin distribution tree into $1.
#
# The Claude Code marketplace consumes a repo whose ROOT is the marketplace:
#   .claude-plugin/marketplace.json    marketplace manifest
#   plugins/braintrust/                skills plugin (MCP + skills)
#   plugins/trace-claude-code/         tracing plugin (thin daemon hook shim)
#
# Everything is plain shell + config — no compiled artifacts — so the whole
# deployable tree is checked in under content/ and this build is a straight
# copy.
#
# Usage: build.sh <TARGET_DIR>   (TARGET_DIR is created if missing)

set -euo pipefail

TARGET_DIR="${1:?usage: build.sh <TARGET_DIR>}"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTENT_DIR="$SRC_DIR/content"

mkdir -p "$TARGET_DIR"
rsync -a --delete --exclude '.git' "$CONTENT_DIR/" "$TARGET_DIR/"

echo "Built claude dist into $TARGET_DIR (content from $CONTENT_DIR)."
