#!/usr/bin/env bash
set -euo pipefail

HOOK="${1:?usage: test_hook.sh <hook-adapter>}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

BT_STUB="$TMP/bt"
cp /dev/stdin "$BT_STUB" <<'STUB'
#!/bin/sh
printf '%s\n' "$@" > "$BT_STUB_ARGS"
cp /dev/stdin "$BT_STUB_STDIN"
exit "${BT_STUB_EXIT:-0}"
STUB
chmod +x "$BT_STUB"

export BT_STUB_ARGS="$TMP/args"
export BT_STUB_STDIN="$TMP/stdin"
payload='{"conversationId":"test","transcriptPath":"/tmp/transcript.jsonl"}'

response=$(printf '%s' "$payload" | BT_BIN="$BT_STUB" "$HOOK" PostInvocation)
[[ "$response" == '{}' ]]
cmp -s "$TMP/stdin" <(printf '%s' "$payload")
grep -Fx -- 'trace' "$TMP/args" >/dev/null
grep -Fx -- 'antigravity' "$TMP/args" >/dev/null
grep -Fx -- 'conversationId' "$TMP/args" >/dev/null
grep -Fx -- 'transcriptPath' "$TMP/args" >/dev/null

response=$(printf '%s' "$payload" | BT_STUB_EXIT=1 BT_BIN="$BT_STUB" "$HOOK" Stop)
[[ "$response" == '{"decision":""}' ]]

echo "test: antigravity hook adapter OK"
