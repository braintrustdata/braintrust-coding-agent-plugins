#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:?usage: validate.sh <TARGET_DIR>}"
PLUGIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$PLUGIN_DIR/../../.." && pwd)"
SOURCE_DIR="$PLUGIN_DIR/content"
fail() { echo "validate: $*" >&2; exit 1; }

TARGET_DIR="$(cd "$TARGET_DIR" && pwd)"
[[ -f "$TARGET_DIR/package.json" ]] || fail "missing package.json"
[[ -f "$TARGET_DIR/dist/index.mjs" ]] || fail "missing production entrypoint"
[[ -f "$TARGET_DIR/dist/index.d.mts" ]] || fail "missing production declarations"

(cd "$SOURCE_DIR" && pnpm install --frozen-lockfile)
(cd "$SOURCE_DIR" && pnpm run check && pnpm test && pnpm run build && pnpm run smoke)
node "$REPO_ROOT/scripts/prepare-js-daemon-client.mjs" pi --check

node "$REPO_ROOT/scripts/validate-npm-artifact.mjs" pi "$TARGET_DIR"
(cd "$TARGET_DIR" && pnpm publish --dry-run --ignore-scripts --no-git-checks >/dev/null)
for removed in client.ts legacy-processor.ts state.ts types.ts utils.ts; do
  [[ ! -e "$TARGET_DIR/src/$removed" ]] || fail "legacy tracing source remains: src/$removed"
done
if grep -RInE \
  "from [\"']braintrust[\"']|initLogger|startSpan|updateSpan|BRAINTRUST_API_KEY|api_key|state_dir|parent_span_id|root_span_id|fetch\(|https?://api\." \
  "$TARGET_DIR/src"; then
  fail "Pi source still contains local tracing, credential, or API logic"
fi
if grep -Eq '(^|[ /])braintrust@|^[[:space:]]+braintrust:' "$TARGET_DIR/pnpm-lock.yaml"; then
  fail "Braintrust SDK remains in the Pi lockfile"
fi

echo "validate: Pi npm package OK ($TARGET_DIR)"
