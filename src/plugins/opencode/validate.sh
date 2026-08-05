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
if grep -R -n -E "from [\"']braintrust[\"']|from [\"']\.\./(client|span-)" "$TARGET_DIR/src/tracing"; then
  fail "daemon tracing imports the Braintrust SDK or legacy span runtime"
fi
grep -q -E "from [\"']braintrust[\"']|from [\"']\.\./client[\"']" "$TARGET_DIR/src/tools/index.ts" \
  || fail "data-access tools no longer use their intentional Braintrust client"

echo "validate: OpenCode npm package OK ($TARGET_DIR)"
