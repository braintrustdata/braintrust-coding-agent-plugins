#!/usr/bin/env bash
set -euo pipefail

HOOK="${1:?usage: test_hook.sh <hook-adapter>}"
HOOK_DIR="${HOOK%/*}"
[[ "$HOOK_DIR" != "$HOOK" ]] || HOOK_DIR="."
HOOKS_JSON="$HOOK_DIR/hooks.json"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cat >"$TMP/bt" <<'STUB'
#!/bin/sh
{
  printf '%s\n' "$#"
  printf '%s\n' "$@"
} >"$BT_CAPTURE_ARGS"
cat >"$BT_CAPTURE_STDIN"
exit "${BT_STUB_STATUS:-0}"
STUB
chmod +x "$TMP/bt"

payload='{"hookEventName":"pre_tool_use","sessionId":"test","transcriptPath":"/tmp/session.jsonl","toolName":"read_file"}'
payload_file="$TMP/payload"
args="$TMP/args"
stdin="$TMP/stdin"
expected_args="$TMP/expected-args"
printf '%s\n' "$payload" >"$payload_file"

cat "$payload_file" | env \
  BT_BIN="$TMP/bt" \
  BT_CAPTURE_ARGS="$args" \
  BT_CAPTURE_STDIN="$stdin" \
  GROK_VERSION= \
  BT_STUB_STATUS=0 \
  "$HOOK"

cat >"$expected_args" <<'ARGS'
12
trace
hook
--source
grok
--plugin-version
0.1.0
--session-id-field
sessionId
--event-field
hookEventName
--transcript-path-field
transcriptPath
ARGS
cmp -s "$expected_args" "$args" \
  || { echo "test: unexpected bt arguments" >&2; exit 1; }
cmp -s "$payload_file" "$stdin" \
  || { echo "test: hook payload changed" >&2; exit 1; }

# Forward an already-available Grok version without launching Grok to discover it.
cat "$payload_file" | env \
  BT_BIN="$TMP/bt" \
  BT_CAPTURE_ARGS="$args" \
  BT_CAPTURE_STDIN="$stdin" \
  BT_STUB_STATUS=0 \
  GROK_VERSION="1.0.13 beta" \
  "$HOOK"
cat >"$expected_args" <<'ARGS'
14
trace
hook
--source
grok
--plugin-version
0.1.0
--session-id-field
sessionId
--event-field
hookEventName
--transcript-path-field
transcriptPath
--source-version
1.0.13 beta
ARGS
cmp -s "$expected_args" "$args" \
  || { echo "test: unexpected source-version arguments" >&2; exit 1; }
cmp -s "$payload_file" "$stdin" \
  || { echo "test: source-version forwarding changed the hook payload" >&2; exit 1; }

# Forwarding failures must not interrupt Grok.
cat "$payload_file" | env \
  BT_BIN="$TMP/bt" \
  BT_CAPTURE_ARGS="$args" \
  BT_CAPTURE_STDIN="$stdin" \
  BT_STUB_STATUS=23 \
  GROK_VERSION= \
  "$HOOK"
cmp -s "$payload_file" "$stdin" \
  || { echo "test: forwarding failure changed the hook payload" >&2; exit 1; }

# A missing host CLI must diagnose and return without attempting installation.
mkdir "$TMP/no-bt" "$TMP/host-home"
cat >"$TMP/no-bt/curl" <<'STUB'
#!/bin/sh
: >"$CURL_CALLED"
exit 0
STUB
chmod +x "$TMP/no-bt/curl"
diagnostic="$TMP/diagnostic"
/usr/bin/env -u BT_BIN -u GROK_VERSION \
  PATH="$TMP/no-bt" \
  HOME="$TMP/host-home" \
  XDG_BIN_HOME="$TMP/host-home/bin" \
  CARGO_HOME="$TMP/host-home/cargo" \
  CURL_CALLED="$TMP/curl-called" \
  /bin/bash "$HOOK" <"$payload_file" 2>"$diagnostic"
[[ "$(<"$diagnostic")" == "trace-grok: bt CLI not found; tracing skipped" ]] \
  || { echo "test: unexpected missing-CLI diagnostic" >&2; exit 1; }
[[ ! -e "$TMP/curl-called" ]] \
  || { echo "test: missing bt attempted an installation" >&2; exit 1; }
shopt -s nullglob dotglob
host_files=("$TMP/host-home"/*)
shopt -u nullglob dotglob
(( ${#host_files[@]} == 0 )) \
  || { echo "test: missing bt mutated the host home" >&2; exit 1; }

python3 - "$HOOKS_JSON" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    hooks = json.load(handle)["hooks"]

assert "SessionEnd" in hooks, "terminal hook must use exact SessionEnd spelling"
assert "SessionStop" not in hooks, "non-native terminal spelling must not be registered"
for event, groups in hooks.items():
    expected_timeout = 15 if event == "SessionEnd" else 5
    timeouts = {
        hook["timeout"]
        for group in groups
        for hook in group["hooks"]
    }
    assert timeouts == {expected_timeout}, (
        f"{event} timeout {sorted(timeouts)} != {expected_timeout}"
    )
PY

echo "test: grok hook adapter OK"
