#!/usr/bin/env bash
set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../local-dev.sh
source "$TEST_DIR/../local-dev.sh"
# shellcheck source=../publish.sh
source "$TEST_DIR/../publish.sh"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

EXISTING_DIR="$TMP_DIR/existing"
mkdir -p "$EXISTING_DIR"
printf 'keep\n' >"$EXISTING_DIR/sentinel"
if (prepare_dev_dir "$EXISTING_DIR") 2>/dev/null; then
  printf 'test: existing local-development directory must be rejected\n' >&2
  exit 1
fi
[[ "$(cat "$EXISTING_DIR/sentinel")" == "keep" ]] || {
  printf 'test: rejected local-development directory was modified\n' >&2
  exit 1
}

NEW_DIR="$TMP_DIR/new/nested"
prepare_dev_dir "$NEW_DIR"
[[ -d "$NEW_DIR" ]] || {
  printf 'test: safe local-development directory was not created\n' >&2
  exit 1
}

[[ "$(daemon_binary_path "$TMP_DIR/target" "")" == "$TMP_DIR/target/debug/bt-daemon" ]] || {
  printf 'test: default Cargo target path is incorrect\n' >&2
  exit 1
}
[[ "$(daemon_binary_path "$TMP_DIR/target" "aarch64-apple-darwin")" == "$TMP_DIR/target/aarch64-apple-darwin/debug/bt-daemon" ]] || {
  printf 'test: configured Cargo build target path is incorrect\n' >&2
  exit 1
}


[[ "$(GH_TOKEN=test-token resolve_clone_url "https://github.com/braintrustdata/braintrust-grok-plugin.git")" == "https://x-access-token:test-token@github.com/braintrustdata/braintrust-grok-plugin.git" ]] || {
  printf 'test: GitHub HTTPS URL did not use GH_TOKEN\n' >&2
  exit 1
}
[[ "$(GH_TOKEN=test-token resolve_clone_url "braintrustdata/braintrust-grok-plugin")" == "https://x-access-token:test-token@github.com/braintrustdata/braintrust-grok-plugin.git" ]] || {
  printf 'test: GitHub repository slug did not use GH_TOKEN\n' >&2
  exit 1
}
[[ "$(GH_TOKEN=test-token resolve_clone_url "git@example.com:owner/repo.git")" == "git@example.com:owner/repo.git" ]] || {
  printf 'test: non-GitHub SSH URL was modified\n' >&2
  exit 1
}
AUTH_HOME="$TMP_DIR/grok-home"
mkdir -p "$AUTH_HOME"
printf 'secret\n' >"$AUTH_HOME/auth.json"
printf 'keep\n' >"$AUTH_HOME/config.toml"
remove_local_auth_copy "$AUTH_HOME"
[[ ! -e "$AUTH_HOME/auth.json" && -e "$AUTH_HOME/config.toml" ]] || {
  printf 'test: cleanup must remove only the copied authentication file\n' >&2
  exit 1
}
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
