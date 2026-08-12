#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:?usage: build.sh <TARGET_DIR>}"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "$TARGET_DIR"
rsync -a --delete --exclude '.git' "$SRC_DIR/content/" "$TARGET_DIR/"
echo "Built antigravity dist into $TARGET_DIR."
