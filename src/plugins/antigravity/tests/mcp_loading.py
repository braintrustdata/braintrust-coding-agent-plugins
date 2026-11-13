"""Verify native MCP discovery without OAuth, inference, or user profile edits."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def check(package, executable):
    package = Path(package).resolve()
    with tempfile.TemporaryDirectory(prefix="braintrust-antigravity-mcp-") as profile:
        env = dict(os.environ, HOME=profile, USERPROFILE=profile)

        def run(*args):
            return subprocess.run(
                [executable, *args], env=env, text=True, capture_output=True,
                check=True, timeout=30,
            ).stdout

        report = json.loads(run("plugin", "validate", "--strict", "--json", str(package)))
        assert report["findings"] == [], report
        assert report["components"]["mcpServers"] == 1, report
        assert report["components"]["hooks"] == 0, report
        tracing = package.parent.parent
        tracing_manifest = tracing / "plugin.json"
        coexist = tracing_manifest.exists() and json.loads(tracing_manifest.read_text()).get("name") == "braintrust-antigravity-tracing"
        if coexist:
            run("plugin", "install", str(tracing))
        run("plugin", "install", str(package))

        def installed():
            return next(p for p in json.loads(run("plugin", "list", "--json")) if p["name"] == "braintrust")

        plugin = installed()
        assert plugin["enabled"] is True, plugin
        assert plugin["mcpServers"] == ["braintrust"], plugin
        assert not plugin.get("hooks"), plugin
        target = Path(profile) / ".gemini/config/plugins/braintrust"
        for name in ("plugin.json", "mcp_config.json", "assets/icon.png"):
            assert (target / name).read_bytes() == (package / name).read_bytes(), name
        run("plugin", "disable", "braintrust")
        assert installed()["enabled"] is False
        run("plugin", "enable", "braintrust")
        assert installed()["enabled"] is True
        run("plugin", "uninstall", "braintrust")
        assert not target.exists(), target
        remaining = json.loads(run("plugin", "list", "--json"))
        assert not any(p["name"] == "braintrust" for p in remaining)
        if coexist:
            trace = next(p for p in remaining if p["name"] == "braintrust-antigravity-tracing")
            assert trace["enabled"] and not trace.get("mcpServers"), trace
            assert trace["hooks"] == ["braintrust-antigravity-tracing"], trace
            run("plugin", "uninstall", "braintrust-antigravity-tracing")
    print("Antigravity: strict validation, MCP discovery, enable/disable, and uninstall passed")


if __name__ == "__main__":
    check(sys.argv[1], os.environ.get("ANTIGRAVITY_BIN") or shutil.which("agy") or "agy")
