#!/usr/bin/env python3
"""Offline distribution and MCP-only submission validation/ZIP creation.

This is a narrow package contract, not a general OpenAI schema validator.
Template settings are portal configuration, not speculative JSON fields.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import struct
import tempfile
from urllib.parse import urlsplit
import zipfile


SCHEMA_BASE = "https://agent-plugins.org/schemas/1.0.0/"
PUBLIC_FILES = frozenset({"plugin.json", "mcp.json", "README.md", "LICENSE", "assets/icon.png"})
EVENTS = {
    "Interrupt", "PermissionRequest", "PostCompact", "PostToolUse", "PreCompact",
    "PreToolUse", "SessionEnd", "SessionStart", "Stop", "SubagentStart", "SubagentStop",
    "UserPromptSubmit",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_json(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate JSON key: {key}")
            result[key] = value
        return result
    return json.loads(path.read_text(), object_pairs_hook=unique)


def keys(value, allowed):
    require(isinstance(value, dict), "expected an object")
    require(set(value) == set(allowed), f"unexpected/missing fields: {set(value) ^ set(allowed)}")


def version(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value), "expected MAJOR.MINOR.PATCH version")


def https_url(value):
    require(isinstance(value, str), "URL must be a string")
    url = urlsplit(value)
    require(url.scheme == "https" and url.hostname and not url.username and not url.password
            and not url.query and not url.fragment and not re.search(r"[{}\s]", value),
            f"expected concrete credential-free HTTPS URL: {value}")


def inventory(root):
    require(root.is_dir() and not root.is_symlink(), f"missing or symlinked package: {root}")
    files = set()
    for path in root.rglob("*"):
        require(not path.is_symlink(), f"symlink not allowed: {path}")
        require(path.is_dir() or path.is_file(), f"special file not allowed: {path}")
        if path.is_file():
            files.add(path.relative_to(root).as_posix())
    return files


def submission_mcp_config(config):
    """Add portal auth metadata without changing the Codex-compatible source.

    Codex currently rejects server-level extensions, even though OpenAI's
    submission format documents them. Keep this projection in the ZIP builder.
    """
    # Round trip to leave the caller's validated distribution config untouched.
    config = json.loads(json.dumps(config))
    config["mcpServers"]["braintrust"]["extensions"] = {
        "com.openai": {"auth": {"type": "oauth", "client": {"mode": "dcr"}}},
    }
    return config


def validate_mcp_config(config, *, submission):
    # Exact contracts reject headers, credentials, processes, speculative
    # template fields, and submission-only metadata in the Codex distribution.
    expected = {
        "$schema": SCHEMA_BASE + "mcp.schema.json",
        "mcpServers": {"braintrust": {
            "type": "streamable-http", "url": "https://api.braintrust.dev/mcp",
        }},
    }
    if submission:
        expected = submission_mcp_config(expected)
        message = "Submission MCP must use the concrete NA URL, Streamable HTTP, and discovery-based OAuth DCR"
    else:
        message = "Codex MCP must use the concrete NA URL and Streamable HTTP without server extensions or credentials"
    require(config == expected, message)


def validate_mcp_package(root, *, submission=False):
    require(inventory(root) == PUBLIC_FILES, "public package files must match MCP-only allowlist (no hooks, apps, skills, scripts, or marketplace)")
    require({p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_dir()} == {"assets"}, "unexpected public package directory")
    manifest = read_json(root / "plugin.json")
    keys(manifest, {"$schema", "name", "version", "description", "author", "homepage", "repository", "license", "keywords", "extensions"})
    require(manifest["$schema"] == SCHEMA_BASE + "plugin.schema.json", "wrong portable plugin schema")
    require(manifest["name"] == "braintrust", "MCP identity must be braintrust")
    version(manifest["version"])
    require(manifest["license"] == "MIT", "expected MIT package license")
    keys(manifest["author"], {"name", "email", "url"})
    require(manifest["author"]["name"] == "Braintrust" and manifest["author"]["email"] == "support@braintrust.dev", "unexpected publisher")
    for value in [manifest["homepage"], manifest["repository"], manifest["author"]["url"]]:
        https_url(value)
    require(isinstance(manifest["description"], str) and manifest["description"].strip(), "missing description")
    require(isinstance(manifest["keywords"], list) and all(isinstance(k, str) and k.strip() for k in manifest["keywords"]), "invalid keywords")
    keys(manifest["extensions"], {"com.openai"})
    overlay = manifest["extensions"]["com.openai"]
    keys(overlay, {"interface"})  # No hooks/apps/mcpServers/desktop requirements.
    interface = overlay["interface"]
    keys(interface, {"displayName", "shortDescription", "longDescription", "developerName", "category", "capabilities", "websiteURL", "supportURL", "privacyPolicyURL", "termsOfServiceURL", "defaultPrompt", "brandColor", "composerIcon", "logo"})
    require(interface["displayName"] == interface["developerName"] == "Braintrust", "unexpected display name/publisher")
    require(interface["category"] == "Developer Tools", "unexpected listing category")
    for field, limit in [("displayName", 30), ("shortDescription", 30), ("longDescription", 4000), ("developerName", 80)]:
        value = interface[field]
        require(isinstance(value, str) and value.strip() and len(value) <= limit and not re.search(r"[\x00-\x1f]", value), f"invalid {field}")
    for field in ["websiteURL", "supportURL", "privacyPolicyURL", "termsOfServiceURL"]:
        https_url(interface[field])
        require(len(interface[field]) <= 1024, f"{field} too long")
    require(interface["capabilities"] == ["Read", "Write"], "unexpected capabilities")
    prompts = interface["defaultPrompt"]
    require(isinstance(prompts, list) and 1 <= len(prompts) <= 3 and all(isinstance(p, str) and p.strip() and len(p) <= 128 and "@" not in p for p in prompts), "invalid starter prompts")
    require(len(set(prompts)) == len(prompts), "duplicate starter prompts")
    require(interface["brandColor"] == "#2C1FEB", "unexpected brand color")
    for field in ["composerIcon", "logo"]:
        require(interface[field] == "./assets/icon.png", f"unexpected {field} path")
    data = (root / "assets/icon.png").read_bytes()
    require(24 <= len(data) <= 5 * 1024 * 1024 and data[:8] == b"\x89PNG\r\n\x1a\n" and data[12:16] == b"IHDR", "invalid PNG asset")
    width, height = struct.unpack(">II", data[16:24])
    require(width == height and 48 <= width <= 4096, "listing icon must be square, 48..4096px")
    validate_mcp_config(read_json(root / "mcp.json"), submission=submission)
    return manifest


def validate_distribution(root):
    inventory(root)
    marketplace = read_json(root / ".agents/plugins/marketplace.json")
    keys(marketplace, {"name", "interface", "plugins"})
    require(marketplace["name"] == "braintrust-codex-plugins", "marketplace identity changed")
    require(marketplace["interface"] == {"displayName": "Braintrust Codex Plugin"}, "marketplace display identity changed")
    entries = marketplace["plugins"]
    require(isinstance(entries, list) and len(entries) == 2 and {p["name"] for p in entries} == {"braintrust", "trace-codex"}, "expected exactly MCP and tracing entries")
    require({p.name for p in (root / "plugins").iterdir()} == {"braintrust", "trace-codex"}, "unexpected plugin paths")
    for entry in entries:
        keys(entry, {"name", "source", "policy", "category"})
        name = entry["name"]
        require(entry["source"] == {"source": "local", "path": f"./plugins/{name}"}, "unexpected marketplace source/path")
        require(entry["policy"] == {"installation": "AVAILABLE", "authentication": "ON_INSTALL"}, "plugins must be separately opt-in")
        require(entry["category"] == ("Developer Tools" if name == "braintrust" else "Observability"), "wrong marketplace category")
    public = validate_mcp_package(root / "plugins/braintrust")
    tracing = root / "plugins/trace-codex"
    manifest = read_json(tracing / ".codex-plugin/plugin.json")
    require(manifest["name"] == "trace-codex" and manifest["hooks"] == "./hooks/hooks.json", "tracing identity/hooks changed")
    version(manifest["version"])
    require(manifest["version"] == public["version"], "Codex packages must share a release version")
    require(not ({"apps", "mcpServers", "skills"} & set(manifest)), "unexpected tracing components")
    require("plugin.json" not in inventory(tracing), "conflicting tracing manifest")
    require(not any(p.name in {"mcp.json", ".mcp.json", ".app.json", "package.json", "pnpm-lock.yaml", "tsconfig.json"}
                    or p.name.startswith("codex-hook") for p in tracing.rglob("*")), "legacy tracing runtime or MCP/app configuration")
    hooks = read_json(tracing / "hooks/hooks.json")
    keys(hooks, {"hooks"})
    require(set(hooks["hooks"]) == EVENTS, "unexpected tracing events")
    for definitions in hooks["hooks"].values():
        require(isinstance(definitions, list) and definitions, "empty tracing hook definitions")
        for definition in definitions:
            require(isinstance(definition["hooks"], list) and definition["hooks"], "empty tracing hooks")
            for hook in definition["hooks"]:
                require(hook["type"] == "command" and hook["command"] == hook["commandWindows"] == "bt trace hook --source codex", "hooks must use daemon forwarders")
    for path in root.rglob("*"):
        if path.name in {"mcp.json", ".mcp.json", ".app.json"}:
            require(path == root / "plugins/braintrust/mcp.json", "unexpected distribution MCP/app configuration")
    return public


def validate_zip(path):
    with zipfile.ZipFile(path) as archive, tempfile.TemporaryDirectory() as temp:
        infos = archive.infolist()
        require(len(infos) == len(PUBLIC_FILES) and {i.filename for i in infos} == PUBLIC_FILES,
                "ZIP must contain only the MCP package with plugin.json at archive root")
        for info in infos:
            require(stat.S_IFMT(info.external_attr >> 16) == stat.S_IFREG, "ZIP entry must be a regular file")
            require(info.file_size <= 5 * 1024 * 1024, "oversized ZIP entry")
        archive.extractall(temp)  # Paths were checked against exact allowlist.
        return validate_mcp_package(Path(temp), submission=True)


def build_zip(root, output):
    manifest = validate_distribution(root)
    package = root / "plugins/braintrust"
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"braintrust-{manifest['version']}.zip"
    # Fixed order, timestamps, permissions, no compression-version dependency.
    with tempfile.TemporaryDirectory(dir=output) as temp:
        staged = Path(temp) / path.name
        with zipfile.ZipFile(staged, "w", compression=zipfile.ZIP_STORED) as archive:
            for name in sorted(PUBLIC_FILES):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.create_system = 3
                info.external_attr = (stat.S_IFREG | 0o644) << 16
                data = (package / name).read_bytes()
                if name == "mcp.json":
                    data = (json.dumps(submission_mcp_config(read_json(package / name)), indent=2) + "\n").encode()
                archive.writestr(info, data)
        validate_zip(staged)
        os.replace(staged, path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_suffix(".zip.sha256").write_text(f"{digest}  {path.name}\n")
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["validate", "validate-public", "validate-zip", "zip"])
    parser.add_argument("path", type=Path)
    parser.add_argument("output", type=Path, nargs="?")
    args = parser.parse_args()
    try:
        if args.command == "zip":
            require(args.output is not None, "zip requires output directory")
            print(build_zip(args.path, args.output))
        else:
            {"validate": validate_distribution, "validate-public": lambda path: validate_mcp_package(path, submission=True), "validate-zip": validate_zip}[args.command](args.path)
            print(f"validate: {args.command} OK ({args.path})")
    except (ValueError, KeyError, TypeError, OSError, zipfile.BadZipFile) as error:
        parser.exit(1, f"validate: {error}\n")


if __name__ == "__main__":
    main()
