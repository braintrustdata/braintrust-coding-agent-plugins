#!/usr/bin/env python3
"""Set a release version on every distributed version surface for an agent.

Versioning is per distribution: portable root manifests and compatibility
manifests share one version. Claude's marketplace catalog version follows its
plugins; other marketplace manifests are NOT touched.

Only version values are rewritten, so surrounding files keep their formatting.

Usage: set-plugin-version.py <agent> <version>   (leading 'v' is stripped)
"""
import glob
import os
import re
import sys

# agent -> globs of its per-plugin manifests (relative to repo root)
MANIFEST_GLOBS = {
    "claude": ["src/plugins/claude/content/plugins/*/.claude-plugin/plugin.json"],
    "codex": [
        "src/plugins/codex/content/plugins/*/.codex-plugin/plugin.json",
        "src/plugins/codex/content/plugins/*/plugin.json",
    ],
    "grok": ["src/plugins/grok/content/.grok-plugin/plugin.json"],
    "cursor": ["src/plugins/cursor/content/.cursor-plugin/plugin.json"],
}

VERSION_RE = re.compile(r'("version"\s*:\s*")[^"]*(")')
def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("usage: set-plugin-version.py <agent> <version>")
    agent, version = sys.argv[1], sys.argv[2].lstrip("v")
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        sys.exit("version must be MAJOR.MINOR.PATCH")
    patterns = MANIFEST_GLOBS.get(agent)
    if not patterns:
        sys.exit(f"unknown agent '{agent}' (known: {', '.join(MANIFEST_GLOBS)})")

    manifests = []
    for pattern in patterns:
        matches = sorted(glob.glob(pattern))
        if not matches:
            sys.exit(f"no plugin manifests found for '{agent}' ({pattern})")
        manifests.extend(matches)

    surfaces = [(path, VERSION_RE, "version") for path in manifests]
    if agent == "claude":
        # Top-level catalog version; plugin entries carry no version of their own.
        surfaces.append(
            ("src/plugins/claude/content/.claude-plugin/marketplace.json", VERSION_RE, "catalog version")
        )
    if agent == "cursor":
        content = "src/plugins/cursor/content"
        surfaces.extend([
            (f"{content}/package.json", VERSION_RE, "extension version"),
            (f"{content}/package-lock.json", VERSION_RE, "lockfile versions"),
        ])

    changes = []
    for path, version_re, label in surfaces:
        with open(path) as f:
            text = f.read()
        expected = 2 if label == "lockfile versions" else 1
        new_text, n = version_re.subn(
            lambda match: f"{match.group(1)}{version}{match.group(2)}",
            text,
            count=expected,
        )
        if n != expected:
            sys.exit(f"expected {expected} {label} field(s) in {path}, found {n}")
        changes.append((path, label, new_text))

    for path, label, new_text in changes:
        with open(path, "w") as f:
            f.write(new_text)
        print(f"set {os.path.relpath(path)} {label} -> {version}")


if __name__ == "__main__":
    main()
