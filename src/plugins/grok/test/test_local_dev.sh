#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../local-dev.sh
source "$SCRIPT_DIR/../local-dev.sh"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
JOURNAL_DIR="$TMP_DIR/journal"
SESSION_ID="grok-local-dev-probe"
mkdir -p "$JOURNAL_DIR"

legacy="$JOURNAL_DIR/$SESSION_ID.ndjson"
printf '%s\n' '{"event":"local_dev_probe"}' >"$legacy"
if find_probe_journal "$JOURNAL_DIR" "$SESSION_ID" >/dev/null; then
  printf 'test: legacy journal path must not satisfy the source-qualified probe\n' >&2
  exit 1
fi

qualified="$JOURNAL_DIR/grok--$SESSION_ID--stable-id.ndjson"
printf '%s\n' '{"source":"grok","event":"local_dev_probe"}' >"$qualified"
observed="$(find_probe_journal "$JOURNAL_DIR" "$SESSION_ID")"
[[ "$observed" == "$qualified" ]] || {
  printf 'test: expected %s, got %s\n' "$qualified" "$observed" >&2
  exit 1
}

printf 'test: grok local-dev journal lookup OK\n'
