#!/usr/bin/env bash
#
# publish.sh — Deploy plugins to their distribution repos from one config map.
#
# Reads the plugin -> dist-repo map from PUBLISH_TARGETS (or a local
# `publish.targets` file if the env var is unset), then runs each plugin's
# src/plugins/<plugin>/publish.sh with DIST_REPO set to its target.
#
# Format (comma-separated, "plugin:repo" — split on the FIRST colon so
# git@github.com:owner/name.git URLs survive):
#
#   PUBLISH_TARGETS="codex:git@github.com:braintrustdata/test-coding-agent-dist.git,claude:braintrustdata/other"
#
# A repo may be a full git URL (ssh or https) or a bare owner/name slug.
#
# Env passthrough: DRY_RUN=1 (build+commit, skip push), GH_TOKEN (https auth).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Fall back to a local, gitignored config file so the map need not be retyped.
if [[ -z "${PUBLISH_TARGETS:-}" && -f "$ROOT/publish.targets" ]]; then
  PUBLISH_TARGETS="$(tr '\n' ',' < "$ROOT/publish.targets")"
fi

if [[ -z "${PUBLISH_TARGETS:-}" ]]; then
  cat >&2 <<'EOF'
error: PUBLISH_TARGETS is not set.

Set the plugin -> dist-repo map, e.g.:
  export PUBLISH_TARGETS="codex:git@github.com:braintrustdata/test-coding-agent-dist.git"
  make publish

Or create a `publish.targets` file (one "plugin:repo" per line; gitignored).
EOF
  exit 1
fi

failures=0
IFS=',' read -ra entries <<< "$PUBLISH_TARGETS"
for entry in "${entries[@]}"; do
  entry="$(echo "$entry" | xargs)"           # trim surrounding whitespace
  [[ -z "$entry" ]] && continue
  plugin="${entry%%:*}"                       # before the first colon
  repo="${entry#*:}"                          # everything after it (keeps URL colons)

  if [[ -z "$plugin" || -z "$repo" || "$plugin" == "$entry" ]]; then
    echo "error: malformed PUBLISH_TARGETS entry: '$entry' (expected plugin:repo)" >&2
    exit 1
  fi

  script="$ROOT/src/plugins/$plugin/publish.sh"
  if [[ ! -x "$script" ]]; then
    echo "error: no publish script for plugin '$plugin' ($script)" >&2
    exit 1
  fi

  echo "==> publish $plugin -> $repo"
  if ! DIST_REPO="$repo" "$script"; then
    echo "!! publish failed for $plugin" >&2
    failures=$((failures + 1))
  fi
done

if [[ "$failures" -gt 0 ]]; then
  echo "publish: $failures target(s) failed" >&2
  exit 1
fi
echo "publish: all targets done"
