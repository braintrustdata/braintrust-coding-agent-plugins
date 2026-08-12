#!/bin/sh
# Test-only bt stub for recording the exact payloads emitted by a real
# Antigravity session. The production plugin never ships this file.
set -eu

capture_dir=${ANTIGRAVITY_CAPTURE_DIR:?set ANTIGRAVITY_CAPTURE_DIR}
mkdir -p "$capture_dir"
payload=$(mktemp "$capture_dir/payload.XXXXXX")
cp /dev/stdin "$payload"
printf '%s\n' "$@" > "$payload.args"
transcript=$(jq -r '.transcriptPath // empty' "$payload" 2>/dev/null || true)
if [ -n "$transcript" ] && [ -f "$transcript" ]; then
  wc -c < "$transcript" > "$payload.transcript-bytes"
  cp "$transcript" "$payload.transcript.jsonl"
fi
