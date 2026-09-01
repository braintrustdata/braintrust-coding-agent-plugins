#!/usr/bin/env bash
PLUGIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$PLUGIN_DIR/../../.." && pwd)"
exec "$REPO_ROOT/scripts/build-npm-plugin.sh" opencode "${1:?usage: build.sh <TARGET_DIR>}"
