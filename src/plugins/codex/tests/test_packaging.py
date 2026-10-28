"""Failure cases for package isolation and the reviewed-release boundary."""
import glob
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
import zipfile


REPO = Path(__file__).resolve().parents[4]
CODEX = REPO / "src/plugins/codex"
spec = importlib.util.spec_from_file_location("codex_package", CODEX / "package.py")
package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package)


def run_step(workflow, name):
    """Extract the actual workflow shell body so tests exercise release code."""
    text = (REPO / ".github/workflows" / workflow).read_text()
    tail = text.split(f"- name: {name}\n", 1)[1].split("        run: |\n", 1)[1]
    lines = []
    for line in tail.splitlines():
        if line and not line.startswith("          "):
            break
        lines.append(line[10:])
    return "\n".join(lines)


class PackagingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.dist = self.root / "dist"
        shutil.copytree(CODEX / "content", self.dist)
        self.public = self.dist / "plugins/braintrust"

    def edit(self, path, fn):
        data = json.loads(path.read_text())
        fn(data)
        path.write_text(json.dumps(data))

    def test_valid_distribution_and_reproducible_zip(self):
        first = package.build_zip(self.dist, self.root / "one")
        # A timestamp change must not change the archive bytes.
        for path in self.public.rglob("*"):
            os.utime(path, (1700000000, 1700000000))
        second = package.build_zip(self.dist, self.root / "two")
        self.assertEqual(first.read_bytes(), second.read_bytes())
        manifest = package.validate_zip(first)
        self.assertEqual(first.name, f"braintrust-{manifest['version']}.zip")
        with zipfile.ZipFile(first) as archive:
            self.assertEqual(set(archive.namelist()), package.PUBLIC_FILES)
            self.assertEqual(json.loads(archive.read("plugin.json"))["name"], "braintrust")

    def test_accidental_public_components_rejected_before_packaging(self):
        for name in ["hooks/hooks.json", ".app.json", ".codex-plugin/plugin.json", "skills/workflow/SKILL.md", "trace-codex/README.md", ".agents/plugins/marketplace.json", "credentials.json"]:
            with self.subTest(name=name):
                target = self.public / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("{}")
                with self.assertRaisesRegex(ValueError, "allowlist"):
                    package.build_zip(self.dist, self.root / "output")
                target.unlink()
                parent = target.parent
                while parent != self.public:
                    parent.rmdir()
                    parent = parent.parent

    def test_symlink_rejected(self):
        icon = self.public / "assets/icon.png"
        icon.unlink()
        icon.symlink_to(CODEX / "content/plugins/braintrust/assets/icon.png")
        with self.assertRaisesRegex(ValueError, "symlink"):
            package.validate_mcp_package(self.public)

    def test_zip_injection_rejected(self):
        original = package.build_zip(self.dist, self.root / "valid")
        for name in ["hooks/hooks.json", "../plugin.json", "braintrust/plugin.json", "plugin.json"]:
            with self.subTest(name=name):
                bad = self.root / "bad.zip"
                shutil.copyfile(original, bad)
                with zipfile.ZipFile(bad, "a") as archive:
                    import warnings
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        archive.writestr(name, "{}")
                with self.assertRaisesRegex(ValueError, "archive root"):
                    package.validate_zip(bad)

    def test_whole_marketplace_zip_rejected(self):
        bad = self.root / "marketplace.zip"
        with zipfile.ZipFile(bad, "w") as archive:
            for path in self.dist.rglob("*"):
                if path.is_file():
                    archive.write(path, path.relative_to(self.dist))
        with self.assertRaisesRegex(ValueError, "archive root"):
            package.validate_zip(bad)

    def test_zip_symlink_rejected(self):
        original = package.build_zip(self.dist, self.root / "valid")
        bad = self.root / "symlink.zip"
        with zipfile.ZipFile(original) as source, zipfile.ZipFile(bad, "w") as target:
            for info in source.infolist():
                if info.filename == "assets/icon.png":
                    info.external_attr = (stat.S_IFLNK | 0o777) << 16
                target.writestr(info, source.read(info.filename))
        with self.assertRaisesRegex(ValueError, "regular file"):
            package.validate_zip(bad)

    def test_codex_transport_and_template_misconfiguration_rejected(self):
        path = self.public / "mcp.json"
        original = path.read_text()
        mutations = [
            lambda s: s.update(url="https://{data_plane_host}/mcp"),
            lambda s: s.update(url="https://api-eu.braintrust.dev/mcp"),
            lambda s: s.update(type="sse"),
            lambda s: s.update(type="stdio", command="bt"),
            lambda s: s.update(headers={"Authorization": "Bearer fake-test-secret"}),
            lambda s: s.update(default="api.braintrust.dev"),
        ]
        for index, mutation in enumerate(mutations):
            with self.subTest(index=index):
                path.write_text(original)
                self.edit(path, lambda d: mutation(d["mcpServers"]["braintrust"]))
                with self.assertRaisesRegex(ValueError, "Codex MCP"):
                    package.validate_mcp_package(self.public)

    def test_zip_adds_submission_auth_without_mutating_distribution(self):
        path = self.public / "mcp.json"
        original = path.read_bytes()
        archive = package.build_zip(self.dist, self.root / "output")
        with zipfile.ZipFile(archive) as zipped:
            config = json.loads(zipped.read("mcp.json"))
        self.assertEqual(config["mcpServers"]["braintrust"], {
            "type": "streamable-http",
            "url": "https://api.braintrust.dev/mcp",
            "extensions": {"com.openai": {"auth": {"type": "oauth", "client": {"mode": "dcr"}}}},
        })
        self.assertEqual(path.read_bytes(), original)
        self.assertNotIn("extensions", json.loads(original)["mcpServers"]["braintrust"])
        package.validate_distribution(self.dist)

    def test_codex_rejects_submission_auth_metadata(self):
        path = self.public / "mcp.json"
        path.write_text(json.dumps(package.submission_mcp_config(json.loads(path.read_text()))))
        with self.assertRaisesRegex(ValueError, "without server extensions"):
            package.validate_distribution(self.dist)

    def test_submission_rejects_missing_or_invalid_auth(self):
        archive = package.build_zip(self.dist, self.root / "valid")
        mutations = [
            lambda s: s.pop("extensions"),
            lambda s: s["extensions"]["com.openai"]["auth"].update(clientSecret="fake-test-secret"),
            lambda s: s["extensions"]["com.openai"]["auth"].update(type="none"),
            lambda s: s["extensions"]["com.openai"]["auth"]["client"].update(mode="provided", clientId="test"),
            lambda s: s.update(headers={"Authorization": "Bearer fake-test-secret"}),
            lambda s: s.update(url="https://{data_plane_host}/mcp"),
        ]
        for index, mutation in enumerate(mutations):
            with self.subTest(index=index):
                bad = self.root / "bad.zip"
                with zipfile.ZipFile(archive) as source, zipfile.ZipFile(bad, "w") as target:
                    for info in source.infolist():
                        data = source.read(info.filename)
                        if info.filename == "mcp.json":
                            config = json.loads(data)
                            mutation(config["mcpServers"]["braintrust"])
                            data = json.dumps(config).encode()
                        target.writestr(info, data)
                with self.assertRaisesRegex(ValueError, "OAuth DCR"):
                    package.validate_zip(bad)

    def test_manifest_hook_app_and_template_fields_rejected(self):
        path = self.public / "plugin.json"
        original = path.read_text()
        for field in ["hooks", "apps", "mcpServers", "template", "desktopRequired"]:
            with self.subTest(field=field):
                path.write_text(original)
                self.edit(path, lambda d: d["extensions"]["com.openai"].update({field: "bad"}))
                with self.assertRaisesRegex(ValueError, "fields"):
                    package.validate_mcp_package(self.public)

    def test_asset_reference_and_missing_asset_rejected(self):
        self.edit(self.public / "plugin.json", lambda d: d["extensions"]["com.openai"]["interface"].update(logo="../trace-codex/icon.png"))
        with self.assertRaisesRegex(ValueError, "logo path"):
            package.validate_mcp_package(self.public)
        (self.public / "assets/icon.png").unlink()
        with self.assertRaisesRegex(ValueError, "allowlist"):
            package.validate_mcp_package(self.public)

    def test_duplicate_json_keys_rejected(self):
        path = self.public / "mcp.json"
        path.write_text(path.read_text().replace('"type": "streamable-http"', '"type": "stdio", "type": "streamable-http"'))
        with self.assertRaisesRegex(ValueError, "duplicate JSON"):
            package.validate_mcp_package(self.public)

    def test_marketplace_escape_identity_and_implicit_install_rejected(self):
        path = self.dist / ".agents/plugins/marketplace.json"
        original = path.read_text()
        for mutation in [
            lambda d: d.update(name="renamed"),
            lambda d: d["plugins"][0]["source"].update(path="../outside"),
            lambda d: d["plugins"][0]["policy"].update(installation="INSTALLED_BY_DEFAULT"),
            lambda d: d["plugins"].append(d["plugins"][0]),
        ]:
            path.write_text(original)
            self.edit(path, mutation)
            with self.assertRaises(ValueError):
                package.validate_distribution(self.dist)

    def test_tracing_hooks_remain_daemon_only(self):
        path = self.dist / "plugins/trace-codex/hooks/hooks.json"
        self.edit(path, lambda d: d["hooks"]["SessionStart"][0]["hooks"][0].update(command="other-runtime"))
        with self.assertRaisesRegex(ValueError, "daemon forwarders"):
            package.validate_distribution(self.dist)

    def test_mismatched_package_versions_rejected(self):
        self.edit(self.public / "plugin.json", lambda d: d.update(version="999.0.0"))
        with self.assertRaisesRegex(ValueError, "share a release version"):
            package.validate_distribution(self.dist)

    def prepare_source(self):
        target = self.root / "src/plugins/codex/content"
        target.parent.mkdir(parents=True)
        shutil.copytree(self.dist, target)
        (self.root / "scripts").mkdir()
        shutil.copyfile(REPO / "scripts/set-plugin-version.py", self.root / "scripts/set-plugin-version.py")
        return target

    def test_stamping_and_preparation_paths_cover_both_packages(self):
        source = self.prepare_source()
        result = subprocess.run([sys.executable, "scripts/set-plugin-version.py", "codex", "v9.8.7"], cwd=self.root, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        package.validate_distribution(source)
        output = self.root / "outputs"
        env = dict(os.environ, REQUESTED_VERSION="v9.8.7", PLUGIN="codex", SOURCE_REF="refs/heads/main", GITHUB_OUTPUT=str(output))
        subprocess.run(["bash", "-euc", run_step("release.yml", "Validate release request")], env=env, cwd=self.root, check=True, capture_output=True)
        patterns = output.read_text().split("paths<<EOF\n", 1)[1].split("EOF\n")[0].splitlines()
        selected = {str(Path(p).relative_to(self.root)) for pattern in patterns for p in glob.glob(str(self.root / pattern))}
        changed = {str(p.relative_to(self.root)) for p in source.rglob("plugin.json")}
        self.assertEqual(selected, changed)
        self.assertEqual(json.loads((source / "plugins/braintrust/plugin.json").read_text())["version"], "9.8.7")

    def test_missing_portable_manifest_fails_stamping_atomically(self):
        source = self.prepare_source()
        tracing = source / "plugins/trace-codex/.codex-plugin/plugin.json"
        original = tracing.read_bytes()
        (source / "plugins/braintrust/plugin.json").unlink()
        result = subprocess.run([sys.executable, "scripts/set-plugin-version.py", "codex", "9.8.7"], cwd=self.root, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(tracing.read_bytes(), original)

    def test_publication_rejects_unreviewed_versions(self):
        source = self.prepare_source()
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        # diff against index is sufficient for the workflow's git diff check.
        current = json.loads((source / "plugins/braintrust/plugin.json").read_text())["version"]
        requested = f"{int(current.split('.')[0]) + 1}.0.0"
        env = dict(os.environ, PLUGIN="codex", VERSION=requested, RECORD="true")
        result = subprocess.run(["bash", "-euc", run_step("_release.yml", "Stamp or verify reviewed release versions")], cwd=self.root, env=env, capture_output=True)
        self.assertNotEqual(result.returncode, 0)

    def test_publication_accepts_reviewed_shared_version_and_marker(self):
        source = self.prepare_source()
        current = json.loads((source / "plugins/braintrust/plugin.json").read_text())["version"]
        marker = self.root / ".github/release-versions/codex"
        marker.parent.mkdir(parents=True)
        marker.write_text(current + "\n")
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        env = dict(os.environ, PLUGIN="codex", VERSION=current, RECORD="true")
        result = subprocess.run(["bash", "-euc", run_step("_release.yml", "Stamp or verify reviewed release versions")], cwd=self.root, env=env, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_submission_attachment_retry_reuses_saved_asset(self):
        script = run_step("_release.yml", "Attach MCP-only ZIP to distribution release")
        # Exercise the real attachment step twice with a local gh double. Neither
        # invocation can deploy or create a release; both upload the same ZIP.
        archive = package.build_zip(self.dist, self.root / "upload")
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        gh = bin_dir / "gh"
        gh.write_text('#!/bin/bash\nset -eu\ntest "$1 $2" = "release upload"\ntest "$3" = "v$VERSION"\ntest "$8" = "--clobber"\nprintf "%s\\n" "$*" >> "$ATTACH_LOG"\n')
        gh.chmod(0o755)
        env = dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ["PATH"], VERSION=package.validate_zip(archive)["version"], DIST_REPO="braintrustdata/braintrust-codex-plugin", ATTACH_LOG=str(self.root / "log"))
        for _ in range(2):
            subprocess.run(["bash", "-euc", script], cwd=archive.parent, env=env, check=True, capture_output=True)
        self.assertEqual(len((self.root / "log").read_text().splitlines()), 2)
        archive.write_bytes(b"tampered")
        result = subprocess.run(["bash", "-euc", script], cwd=archive.parent, env=env, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len((self.root / "log").read_text().splitlines()), 2)


if __name__ == "__main__":
    unittest.main()
