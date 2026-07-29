#!/usr/bin/env bash
#
# publish.sh — Deploy plugins to their distribution repos from one config map.
#
# Reads the plugin -> dist-repo map from the PUBLISH_TARGETS env var, then runs
# each plugin's src/plugins/<plugin>/publish.sh with DIST_REPO set to its target.
#
# Format (comma-separated, "plugin:repo" — split on the FIRST colon so
# git@github.com:owner/name.git URLs survive):
#
#   PUBLISH_TARGETS="codex:git@github.com:braintrustdata/test-coding-agent-dist.git,claude:braintrustdata/other"
#
# A repo may be a full git URL (ssh or https) or a bare owner/name slug.
#
# The whole map is validated BEFORE any deploy runs, so a bad map fails fast
# without half-publishing: each entry must be plugin:repo, name a real plugin,
# and carry a repo that resolves to owner/name; no plugin may appear twice, and
# no two plugins may point at the same target repo.
#
# Env passthrough: DRY_RUN=1 (build+commit, skip push), GH_TOKEN (https auth).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${PUBLISH_TARGETS:-}" ]]; then
  cat >&2 <<'EOF'
error: PUBLISH_TARGETS is not set.

Set the plugin -> dist-repo map, e.g.:
  export PUBLISH_TARGETS="codex:git@github.com:braintrustdata/test-coding-agent-dist.git"
  make publish
EOF
  exit 1
fi

die() { echo "error: $*" >&2; exit 1; }

# Normalize any repo form (ssh URL, https URL, or bare slug) to a canonical,
# case-insensitive owner/name key for validity + duplicate-target checks.
canon_slug() {
  printf '%s' "$1" \
    | sed -E 's#^git@[^:]+:##; s#^https?://[^/]+/##; s#\.git$##' \
    | tr '[:upper:]' '[:lower:]'
}

# --- Pass 1: parse + validate the entire map (no side effects). ---
plugins=()          # parallel arrays, index-aligned
repos=()
seen_plugins=" "    # space-delimited membership sets (bash 3.2 safe)
seen_targets=" "

IFS=',' read -ra entries <<< "$PUBLISH_TARGETS"
for entry in "${entries[@]}"; do
  entry="$(echo "$entry" | xargs)"          # trim surrounding whitespace
  [[ -z "$entry" ]] && continue
  plugin="${entry%%:*}"                       # before the first colon
  repo="${entry#*:}"                          # everything after (keeps URL colons)

  [[ -n "$plugin" && -n "$repo" && "$plugin" != "$entry" ]] \
    || die "malformed PUBLISH_TARGETS entry: '$entry' (expected plugin:repo)"

  [[ -x "$ROOT/src/plugins/$plugin/publish.sh" ]] \
    || die "unknown plugin '$plugin' (no src/plugins/$plugin/publish.sh)"

  slug="$(canon_slug "$repo")"
  [[ "$slug" =~ ^[^/[:space:]]+/[^/[:space:]]+$ ]] \
    || die "invalid repo '$repo' for plugin '$plugin' (expected owner/name or a git URL)"

  case "$seen_plugins" in *" $plugin "*) die "plugin '$plugin' is listed more than once";; esac
  case "$seen_targets" in *" $slug "*) die "two plugins point at the same target repo '$slug'";; esac
  seen_plugins+="$plugin "
  seen_targets+="$slug "

  plugins+=("$plugin")
  repos+=("$repo")
done

[[ "${#plugins[@]}" -gt 0 ]] || die "no publish targets parsed from PUBLISH_TARGETS"

# --- Pass 2: deploy each validated target. ---
failures=0
for i in "${!plugins[@]}"; do
  echo "==> publish ${plugins[$i]} -> ${repos[$i]}"
  if ! DIST_REPO="${repos[$i]}" "$ROOT/src/plugins/${plugins[$i]}/publish.sh"; then
    echo "!! publish failed for ${plugins[$i]}" >&2
    failures=$((failures + 1))
  fi
done

if [[ "$failures" -gt 0 ]]; then
  echo "publish: $failures target(s) failed" >&2
  exit 1
fi
echo "publish: all targets done"
