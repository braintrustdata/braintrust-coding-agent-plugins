#!/usr/bin/env bash
#
# build.sh — Assemble the Codex plugin distribution tree into $1.
#
# The Codex marketplace consumes a repo whose ROOT is the marketplace:
#   .agents/plugins/marketplace.json   marketplace manifest
#   plugins/braintrust-codex-plugin/   skills plugin (MCP + skills)
#   plugins/trace-codex/               tracing plugin (TS event server + hooks)
#
# By default this is a content assembly (a straight copy of content/). The seam
# for injecting shared code later lives here.
#
# trace-codex ships its compiled hook binaries (bin/codex-hook-<os>-<arch>)
# committed in the dist repo; the launcher execs the host's binary directly (no
# download). Compiling those binaries is gated behind CODEX_BUILD_BINARIES=1 so
# plain content builds (make test) stay fast and need no JS toolchain. publish.sh
# sets the flag, so every real deploy ships fresh binaries.
#
# Env:
#   CODEX_BUILD_BINARIES=1   compile + embed the trace-codex binaries (needs
#                            node + pnpm; pkg downloads base runtimes, so it can
#                            cross-compile all targets from one host).
#
# Usage: build.sh <TARGET_DIR>   (TARGET_DIR is created if missing)

set -euo pipefail

TARGET_DIR="${1:?usage: build.sh <TARGET_DIR>}"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTENT_DIR="$SRC_DIR/content"

mkdir -p "$TARGET_DIR"
rsync -a --delete --exclude '.git' --exclude 'node_modules' "$CONTENT_DIR/" "$TARGET_DIR/"

if [[ "${CODEX_BUILD_BINARIES:-}" == "1" ]]; then
  tc="$TARGET_DIR/plugins/trace-codex"
  echo "Compiling trace-codex hook binaries (all platforms)..."
  ( cd "$tc" && pnpm install && pnpm run build )

  # Ad-hoc sign the macOS binaries: unsigned arm64 Mach-O executables are
  # SIGKILLed by the kernel on Apple Silicon. `codesign` exists only on macOS;
  # `rcodesign` is the cross-platform fallback (Linux). Linux binaries need no
  # signature. This is why the codex build runs on a macOS CI runner.
  for f in "$tc"/bin/codex-hook-darwin-*; do
    [ -f "$f" ] || continue
    if command -v codesign >/dev/null 2>&1 && codesign -s - -f "$f" >/dev/null 2>&1; then
      continue
    fi
    if command -v rcodesign >/dev/null 2>&1 && rcodesign sign "$f" >/dev/null 2>&1; then
      continue
    fi
    echo "warning: could not sign $f (no codesign/rcodesign); it will be killed on Apple Silicon" >&2
  done

  # Keep only the compiled binaries; drop build intermediates.
  rm -rf "$tc/node_modules" "$tc/dist"
  # The plugin .gitignore excludes the binaries (correct for the monorepo, wrong
  # for the dist repo). Un-ignore them so the deploy actually commits them.
  if [[ -f "$tc/.gitignore" ]]; then
    grep -v -E '^bin/codex-hook' "$tc/.gitignore" > "$tc/.gitignore.tmp"
    mv "$tc/.gitignore.tmp" "$tc/.gitignore"
  fi
fi

echo "Built codex dist into $TARGET_DIR (content from $CONTENT_DIR)."
