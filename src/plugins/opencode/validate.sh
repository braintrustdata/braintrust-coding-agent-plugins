#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:?usage: validate.sh <TARGET_DIR>}"
fail() { echo "validate: $*" >&2; exit 1; }

[[ -f "$TARGET_DIR/package.json" ]] || fail "missing package.json"
[[ -f "$TARGET_DIR/dist/index.js" ]] || fail "missing production entrypoint"
tarball="$(find "$TARGET_DIR" -maxdepth 1 -name 'braintrust-trace-opencode-*.tgz' -print -quit)"
[[ -n "$tarball" ]] || fail "missing npm tarball"

(cd "$TARGET_DIR" && bun install --frozen-lockfile && bun run check && bun run typecheck && bun test && bun run build)
node -e "import(process.argv[1])" "$(cd "$TARGET_DIR" && pwd)/dist/index.js"
(cd "$TARGET_DIR" && npm pack --dry-run >/dev/null)

contents="$(tar -tzf "$tarball")"
grep -q '^package/dist/index.js$' <<<"$contents" || fail "tarball omits dist/index.js"
grep -q '^package/README.md$' <<<"$contents" || fail "tarball omits README.md"
grep -q '^package/LICENSE$' <<<"$contents" || fail "tarball omits LICENSE"
for removed in client.ts tracing.ts event-processor.ts replay.ts span-queue.ts span-sink.ts; do
  [[ ! -e "$TARGET_DIR/src/$removed" ]] || fail "old JavaScript tracing runtime remains: src/$removed"
done
if grep -R -n -E "fetch\(|/v1/|/btql|apikey/login|from [\"']\.\./tools" "$TARGET_DIR/src/tracing"; then
  fail "daemon tracing performs API access or imports the tools runtime"
fi
if grep -R -n -E \
  "fetch\(|https?://api\.|/v1/|/btql|apikey/login|BRAINTRUST_API_(KEY|URL)|BRAINTRUST_APP_URL" \
  "$TARGET_DIR/src"; then
  fail "OpenCode package contains direct Braintrust API or credential handling"
fi
grep -q 'BtCliToolsClient' "$TARGET_DIR/src/tools/index.ts" \
  || fail "data-access tools no longer delegate to bt"
grep -q '"--prefer-profile"' "$TARGET_DIR/src/tools/bt-cli.ts" \
  || fail "bt tool delegation does not prefer managed profiles"
if grep -q '"braintrust"[[:space:]]*:' "$TARGET_DIR/package.json"; then
  fail "OpenCode package still depends on the Braintrust JavaScript SDK"
fi

echo "validate: OpenCode npm package OK ($TARGET_DIR)"
