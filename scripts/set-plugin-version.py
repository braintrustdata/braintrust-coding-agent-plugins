#!/usr/bin/env python3
"""Set a release version on every distributed version surface for an agent.

Versioning is per-plugin: every plugin under an agent carries its own
.<agent>-plugin/plugin.json with a `version` field. Grok's hook adapter also
embeds the plugin version forwarded to the daemon, so its manifest and adapter
constant are stamped together. The marketplace manifest is NOT touched.

Only version values are rewritten, so surrounding files keep their formatting.

Usage: set-plugin-version.py <agent> <version>   (leading 'v' is stripped)
"""
import glob
import os
import re
import sys

# agent -> glob of its per-plugin manifests (relative to repo root)
MANIFEST_GLOBS = {
    "claude": "src/plugins/claude/content/plugins/*/.claude-plugin/plugin.json",
    "codex": "src/plugins/codex/content/plugins/*/.codex-plugin/plugin.json",
    "grok": "src/plugins/grok/content/.grok-plugin/plugin.json",
}

VERSION_RE = re.compile(r'("version"\s*:\s*")[^"]*(")')
GROK_ADAPTER = "src/plugins/grok/content/hooks/forward.sh"
GROK_PLUGIN_VERSION_RE = re.compile(r'^(PLUGIN_VERSION=")[^"]*(")$', re.MULTILINE)


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("usage: set-plugin-version.py <agent> <version>")
    agent, version = sys.argv[1], sys.argv[2].lstrip("v")
    pattern = MANIFEST_GLOBS.get(agent)
    if not pattern:
        sys.exit(f"unknown agent '{agent}' (known: {', '.join(MANIFEST_GLOBS)})")

    manifests = sorted(glob.glob(pattern))
    if not manifests:
        sys.exit(f"no plugin manifests found for '{agent}' ({pattern})")

    surfaces = [(path, VERSION_RE, "version") for path in manifests]
    if agent == "grok":
        surfaces.append((GROK_ADAPTER, GROK_PLUGIN_VERSION_RE, "PLUGIN_VERSION"))

    changes = []
    for path, version_re, label in surfaces:
        with open(path) as f:
            text = f.read()
        new_text, n = version_re.subn(
            lambda match: f"{match.group(1)}{version}{match.group(2)}",
            text,
        )
        if n != 1:
            sys.exit(f"expected one {label} field in {path}, found {n}")
        changes.append((path, label, new_text))

    for path, label, new_text in changes:
        with open(path, "w") as f:
            f.write(new_text)
        print(f"set {os.path.relpath(path)} {label} -> {version}")


if __name__ == "__main__":
    main()
