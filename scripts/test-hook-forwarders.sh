#!/usr/bin/env bash
# Exercise the packaged hook shims with a fake bt CLI. This proves that raw
# stdin and the canonical source identity reach `bt trace hook`, and that
# installer or forwarding failures never fail an agent hook.

set -euo pipefail

DIST_DIR="${1:-dist}"
TEST_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TEST_DIR"; }
trap cleanup EXIT

cat > "$TEST_DIR/bt" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" > "$BT_CAPTURE_ARGS"
cat > "$BT_CAPTURE_STDIN"
[ -z "${BT_CAPTURE_METADATA:-}" ] || printf '%s' "${BRAINTRUST_ADDITIONAL_METADATA:-}" > "$BT_CAPTURE_METADATA"
exit "${BT_STUB_STATUS:-0}"
EOF
chmod +x "$TEST_DIR/bt"

PAYLOAD='{"session_id":"shim-test","hook_event_name":"SessionStart","message":"unchanged"}'

exercise() {
  local name="$1"
  local source="$2"
  local plugin_version="$3"
  shift 3
  local args_file="$TEST_DIR/$name.args"
  local stdin_file="$TEST_DIR/$name.stdin"

  printf '%s' "$PAYLOAD" | env \
    PATH="$TEST_DIR:$PATH" \
    BT_CAPTURE_ARGS="$args_file" \
    BT_CAPTURE_STDIN="$stdin_file" \
    "$@"

  [[ "$(cat "$args_file")" == "trace hook --source $source --plugin-version $plugin_version" ]]
  [[ "$(cat "$stdin_file")" == "$PAYLOAD" ]]

  # The shim must swallow a daemon-client failure after forwarding the payload.
  printf '%s' "$PAYLOAD" | env \
    PATH="$TEST_DIR:$PATH" \
    BT_CAPTURE_ARGS="$args_file" \
    BT_CAPTURE_STDIN="$stdin_file" \
    BT_STUB_STATUS=23 \
    "$@"
}

CLAUDE_PLUGIN_VERSION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["version"])' "$DIST_DIR/claude/plugins/trace-claude-code/.claude-plugin/plugin.json")"
CODEX_PLUGIN_VERSION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["version"])' "$DIST_DIR/codex/plugins/trace-codex/.codex-plugin/plugin.json")"

exercise claude claude-code "$CLAUDE_PLUGIN_VERSION" \
  bash "$DIST_DIR/claude/plugins/trace-claude-code/hooks/forward.sh"
exercise codex codex "$CODEX_PLUGIN_VERSION" \
  bash "$DIST_DIR/codex/plugins/trace-codex/bin/codex-hook.sh"

# Additional metadata is consumed by the shared `bt trace hook` command from
# the environment, so every thin hook forwarder receives identical behavior.
exercise_metadata() {
  local name="$1"
  shift
  local args_file="$TEST_DIR/$name.metadata.args"
  local stdin_file="$TEST_DIR/$name.metadata.stdin"
  local metadata_file="$TEST_DIR/$name.metadata.env"

  printf '%s' "$PAYLOAD" | env \
    PATH="$TEST_DIR:$PATH" \
    BT_CAPTURE_ARGS="$args_file" \
    BT_CAPTURE_STDIN="$stdin_file" \
    BT_CAPTURE_METADATA="$metadata_file" \
    BRAINTRUST_ADDITIONAL_METADATA='{"ci":true,"run_id":"shim-test"}' \
    "$@"
  [[ "$(cat "$stdin_file")" == "$PAYLOAD" ]]
  [[ "$(cat "$metadata_file")" == '{"ci":true,"run_id":"shim-test"}' ]]
}

exercise_metadata claude \
  bash "$DIST_DIR/claude/plugins/trace-claude-code/hooks/forward.sh"
[[ "$(cat "$TEST_DIR/claude.metadata.args")" == "trace hook --source claude-code --plugin-version $CLAUDE_PLUGIN_VERSION" ]]
exercise_metadata codex \
  bash "$DIST_DIR/codex/plugins/trace-codex/bin/codex-hook.sh"
[[ "$(cat "$TEST_DIR/codex.metadata.args")" == "trace hook --source codex --plugin-version $CODEX_PLUGIN_VERSION" ]]

# Exercise first-use bootstrap without touching the developer's installation.
# The fake curl materializes a fake bt binary and emits a no-op installer body.
BOOTSTRAP_DIR="$TEST_DIR/bootstrap"
mkdir "$BOOTSTRAP_DIR"
cat > "$BOOTSTRAP_DIR/curl" <<'EOF'
#!/bin/bash
printf '%s\n' "$*" > "$BT_INSTALL_CAPTURE"
cp "$BT_INSTALLABLE" "$BT_INSTALL_DEST"
chmod +x "$BT_INSTALL_DEST"
printf '%s\n' '#!/bin/bash' 'exit 0'
EOF
cat > "$TEST_DIR/installable-bt" <<'EOF'
#!/bin/bash
printf '%s\n' "$*" > "$BT_CAPTURE_ARGS"
cat > "$BT_CAPTURE_STDIN"
EOF
chmod +x "$BOOTSTRAP_DIR/curl" "$TEST_DIR/installable-bt"

bootstrap() {
  local name="$1"
  local source="$2"
  local plugin_version="$3"
  shift 3
  local args_file="$TEST_DIR/$name.bootstrap.args"
  local stdin_file="$TEST_DIR/$name.bootstrap.stdin"
  local install_file="$TEST_DIR/$name.install.args"

  rm -f "$BOOTSTRAP_DIR/bt"
  printf '%s' "$PAYLOAD" | env \
    PATH="$BOOTSTRAP_DIR:/usr/bin:/bin" \
    BT_CAPTURE_ARGS="$args_file" \
    BT_CAPTURE_STDIN="$stdin_file" \
    BT_INSTALL_CAPTURE="$install_file" \
    BT_INSTALLABLE="$TEST_DIR/installable-bt" \
    BT_INSTALL_DEST="$BOOTSTRAP_DIR/bt" \
    XDG_BIN_HOME="$BOOTSTRAP_DIR" \
    CARGO_HOME="$BOOTSTRAP_DIR/cargo" \
    "$@"

  [[ "$(cat "$install_file")" == "-fsSL https://bt.dev/cli/install.sh" ]]
  [[ "$(cat "$args_file")" == "trace hook --source $source --plugin-version $plugin_version" ]]
  [[ "$(cat "$stdin_file")" == "$PAYLOAD" ]]
}

bootstrap claude claude-code "$CLAUDE_PLUGIN_VERSION" \
  bash "$DIST_DIR/claude/plugins/trace-claude-code/hooks/forward.sh"
bootstrap codex codex "$CODEX_PLUGIN_VERSION" \
  bash "$DIST_DIR/codex/plugins/trace-codex/bin/codex-hook.sh"

# No bt binary is also fail-open. Use an empty path so the test does not depend
# on whether the host running the suite has bt or curl installed.
EMPTY_PATH="$TEST_DIR/empty-path"
mkdir "$EMPTY_PATH"
printf '%s' "$PAYLOAD" | PATH="$EMPTY_PATH" XDG_BIN_HOME="$EMPTY_PATH" CARGO_HOME="$EMPTY_PATH" /bin/bash \
  "$DIST_DIR/claude/plugins/trace-claude-code/hooks/forward.sh"
printf '%s' "$PAYLOAD" | PATH="$EMPTY_PATH" XDG_BIN_HOME="$EMPTY_PATH" CARGO_HOME="$EMPTY_PATH" /bin/bash \
  "$DIST_DIR/codex/plugins/trace-codex/bin/codex-hook.sh"

echo "test: hook forwarders OK"
