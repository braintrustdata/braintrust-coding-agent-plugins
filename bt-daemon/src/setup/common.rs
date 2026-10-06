//! Helpers shared by the per-agent setup adapters: running agent CLIs,
//! editing JSON configuration files, and comparing plugin versions.

use crate::paths;
use anyhow::{bail, Context};
use serde_json::{Map, Value};
use std::io::Write;
use std::path::Path;

/// Embed a file from the monorepo's plugin sources, relative to `src/plugins/`.
macro_rules! plugin_source {
    ($path:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../src/plugins/",
            $path
        ))
    };
}
pub(super) use plugin_source;

/// Runs agent CLIs. Tests substitute a recording fake.
pub(super) trait CommandRunner {
    fn json(&mut self, program: &str, args: &[&str]) -> anyhow::Result<Value>;
    fn json_in_home(&mut self, program: &str, args: &[&str], home: &Path) -> anyhow::Result<Value>;
    fn run(&mut self, program: &str, args: &[&str]) -> anyhow::Result<()>;
    fn run_in_home(&mut self, program: &str, args: &[&str], home: &Path) -> anyhow::Result<()>;
}

pub(super) struct SystemCommandRunner;

impl SystemCommandRunner {
    /// Run a background command, capturing its output and failing on a
    /// non-zero exit status.
    fn capture(program: &str, args: &[&str], home: Option<&Path>) -> anyhow::Result<Vec<u8>> {
        let mut command = crate::subprocess::background_command(program);
        command.args(args);
        if let Some(home) = home {
            command.env("HOME", home);
        }
        let output = command.output().with_context(|| {
            format!("failed to run `{program}`; install {program} and ensure it is on PATH")
        })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("`{program} {}` failed: {}", args.join(" "), stderr.trim());
        }
        Ok(output.stdout)
    }

    fn capture_json(program: &str, args: &[&str], home: Option<&Path>) -> anyhow::Result<Value> {
        let stdout = Self::capture(program, args, home)?;
        serde_json::from_slice(&stdout)
            .with_context(|| format!("`{program} {}` returned invalid JSON", args.join(" ")))
    }
}

impl CommandRunner for SystemCommandRunner {
    fn json(&mut self, program: &str, args: &[&str]) -> anyhow::Result<Value> {
        Self::capture_json(program, args, None)
    }

    fn json_in_home(&mut self, program: &str, args: &[&str], home: &Path) -> anyhow::Result<Value> {
        Self::capture_json(program, args, Some(home))
    }

    /// Runs in the foreground so the agent CLI can show progress or prompt.
    fn run(&mut self, program: &str, args: &[&str]) -> anyhow::Result<()> {
        let status = crate::subprocess::interactive_command(program)
            .args(args)
            .status()
            .with_context(|| {
                format!("failed to run `{program}`; install {program} and ensure it is on PATH")
            })?;
        if !status.success() {
            bail!("`{program} {}` failed with {status}", args.join(" "));
        }
        Ok(())
    }

    fn run_in_home(&mut self, program: &str, args: &[&str], home: &Path) -> anyhow::Result<()> {
        Self::capture(program, args, Some(home)).map(drop)
    }
}

/// An agent marketplace that distributes a Braintrust plugin. Codex and
/// Claude Code share this flow; they differ in verbs and listing shapes.
pub(super) struct Marketplace {
    pub program: &'static str,
    pub name: &'static str,
    pub source: &'static str,
    /// The subcommand that refreshes a configured marketplace.
    pub refresh: &'static str,
    /// Find this marketplace in `<program> plugin marketplace list --json`.
    pub find: fn(&Value) -> Option<&Value>,
    /// Whether a configured marketplace points at the published source.
    pub is_published: fn(&Value) -> bool,
}

impl Marketplace {
    fn list(&self, runner: &mut impl CommandRunner) -> anyhow::Result<Value> {
        runner.json(self.program, &["plugin", "marketplace", "list", "--json"])
    }

    /// Configure the published marketplace, refreshing it when it is already
    /// present. A same-name marketplace from another source is replaced;
    /// returns whether that happened.
    pub fn reconcile(&self, runner: &mut impl CommandRunner) -> anyhow::Result<bool> {
        let marketplaces = self.list(runner)?;
        let replaced = match (self.find)(&marketplaces) {
            Some(marketplace) if (self.is_published)(marketplace) => {
                runner.run(
                    self.program,
                    &["plugin", "marketplace", self.refresh, self.name],
                )?;
                false
            }
            Some(_) => {
                runner.run(
                    self.program,
                    &["plugin", "marketplace", "remove", self.name],
                )?;
                runner.run(self.program, &["plugin", "marketplace", "add", self.source])?;
                true
            }
            None => {
                runner.run(self.program, &["plugin", "marketplace", "add", self.source])?;
                false
            }
        };
        Ok(replaced)
    }

    /// Refresh the marketplace, which must already be the published one.
    /// `display_name` and `agent` phrase the error that tells the user to
    /// run `bt trace enable <agent>` instead.
    pub fn refresh_published(
        &self,
        runner: &mut impl CommandRunner,
        display_name: &str,
        agent: &str,
    ) -> anyhow::Result<()> {
        let marketplaces = self.list(runner)?;
        let marketplace = (self.find)(&marketplaces).ok_or_else(|| {
            anyhow::anyhow!(
                "{display_name} tracing marketplace is not installed; run `bt trace enable {agent}`"
            )
        })?;
        if !(self.is_published)(marketplace) {
            bail!(
                "{display_name} tracing marketplace is not the published Braintrust marketplace; run `bt trace enable {agent}`"
            );
        }
        runner.run(
            self.program,
            &["plugin", "marketplace", self.refresh, self.name],
        )
    }
}

pub(super) fn github_repo_matches(source: &str, expected: &str) -> bool {
    let source = source.trim().trim_end_matches('/');
    let source = source.strip_suffix(".git").unwrap_or(source);
    let source = source
        .strip_prefix("https://github.com/")
        .or_else(|| source.strip_prefix("git@github.com:"))
        .unwrap_or(source);
    source == expected
}

pub(super) fn package_version(manifest: &str) -> anyhow::Result<String> {
    let manifest = serde_json::from_str::<Value>(manifest)?;
    manifest
        .get("version")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("package manifest has no string version"))
}

pub(super) fn npm_major_spec(package: &str, manifest: &str) -> anyhow::Result<String> {
    let version = package_version(manifest)?;
    let major = version
        .split_once('.')
        .map(|(major, _)| major)
        .ok_or_else(|| anyhow::anyhow!("package version is not semver: {version}"))?
        .parse::<u64>()
        .with_context(|| format!("package version is not semver: {version}"))?;
    Ok(format!("{package}@^{major}"))
}

pub(super) fn version_is_older(installed: &str, expected: &str) -> bool {
    let parse = |version: &str| {
        let mut parts = version.split(['.', '-', '+']);
        Some((
            parts.next()?.parse::<u64>().ok()?,
            parts.next().unwrap_or("0").parse::<u64>().ok()?,
            parts.next().unwrap_or("0").parse::<u64>().ok()?,
        ))
    };
    parse(installed)
        .zip(parse(expected))
        .is_some_and(|(installed, expected)| installed < expected)
}

/// Whether the plugin version an agent CLI reports as JSON is older than the
/// version in `manifest`. Any failure to determine it reports `false`.
pub(super) fn installed_json_version_is_older(
    program: &str,
    args: &[&str],
    find_version: impl Fn(&Value) -> Option<&str>,
    manifest: &str,
) -> bool {
    let Ok(expected) = package_version(manifest) else {
        return false;
    };
    let Ok(output) = crate::subprocess::background_command(program)
        .args(args)
        .output()
    else {
        return false;
    };
    if !output.status.success() {
        return false;
    }
    serde_json::from_slice::<Value>(&output.stdout)
        .ok()
        .and_then(|value| find_version(&value).map(str::to_owned))
        .is_some_and(|installed| version_is_older(&installed, &expected))
}

/// Read a JSON object file; a missing file reads as an empty object.
pub(super) fn load_object(path: &Path) -> anyhow::Result<Map<String, Value>> {
    match std::fs::read(path) {
        Ok(raw) => {
            let value: Value = serde_json::from_slice(&raw)
                .with_context(|| format!("invalid JSON configuration: {}", path.display()))?;
            value.as_object().cloned().ok_or_else(|| {
                anyhow::anyhow!("configuration must be a JSON object: {}", path.display())
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Map::new()),
        Err(error) => {
            Err(error).with_context(|| format!("failed to read configuration: {}", path.display()))
        }
    }
}

/// Edit a JSON object file in place under the settings lock, writing it only
/// when `edit` changed it. `edit` is told whether the file existed.
///
/// A missing file the edit would leave empty is neither created nor locked,
/// so removing configuration from an agent that was never configured does
/// not create its directory.
pub(super) fn edit_object(
    path: &Path,
    access: FileAccess,
    mut edit: impl FnMut(&mut Map<String, Value>, bool) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    if !path.exists() {
        let mut probe = Map::new();
        edit(&mut probe, false)?;
        if probe.is_empty() {
            return Ok(());
        }
    }
    crate::settings::with_settings_lock(path, || {
        let existed = path.exists();
        let mut object = load_object(path)?;
        let original = object.clone();
        edit(&mut object, existed)?;
        if object != original {
            write_object_atomic_unlocked(path, object, access)?;
        }
        Ok(())
    })
}

/// Whether an atomic replacement keeps the directory's inherited access or is
/// restricted to the current user before it is written and published.
#[derive(Clone, Copy)]
pub(super) enum FileAccess {
    Inherited,
    OwnerOnly,
}

pub(super) fn write_object_atomic_with(
    path: &Path,
    object: Map<String, Value>,
    access: FileAccess,
) -> anyhow::Result<()> {
    crate::settings::with_settings_lock(path, || write_object_atomic_unlocked(path, object, access))
}

fn write_object_atomic_unlocked(
    path: &Path,
    object: Map<String, Value>,
    access: FileAccess,
) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("configuration path has no parent: {}", path.display()))?;
    std::fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create configuration directory: {}",
            parent.display()
        )
    })?;
    let mut encoded = serde_json::to_string_pretty(&Value::Object(object))?;
    encoded.push('\n');
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("failed to create temporary file in {}", parent.display()))?;
    if let FileAccess::OwnerOnly = access {
        // Protect the replacement before it holds content or takes the
        // target's place; on failure the previous file stays untouched.
        paths::restrict_file_to_owner(temporary.path())
            .with_context(|| format!("failed to protect {}", path.display()))?;
    }
    temporary.write_all(encoded.as_bytes()).with_context(|| {
        format!(
            "failed to write temporary configuration for {}",
            path.display()
        )
    })?;
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("failed to replace configuration: {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
pub(super) mod test_support {
    use super::CommandRunner;
    use serde_json::Value;
    use std::collections::VecDeque;
    use std::path::{Path, PathBuf};

    /// Records every command and answers JSON queries from a queue.
    pub(in crate::setup) struct FakeRunner {
        responses: VecDeque<Value>,
        pub calls: Vec<String>,
        pub home_calls: Vec<(String, PathBuf)>,
    }

    impl FakeRunner {
        pub fn new(responses: impl IntoIterator<Item = Value>) -> Self {
            Self {
                responses: responses.into_iter().collect(),
                calls: Vec::new(),
                home_calls: Vec::new(),
            }
        }

        pub fn called(&self, command: &str) -> bool {
            self.calls.iter().any(|call| call == command)
        }

        fn record(&mut self, program: &str, args: &[&str], home: Option<&Path>) {
            let call = format!("{program} {}", args.join(" "));
            if let Some(home) = home {
                self.home_calls.push((call.clone(), home.to_path_buf()));
            }
            self.calls.push(call);
        }

        fn respond(&mut self) -> anyhow::Result<Value> {
            self.responses
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("missing fake JSON response"))
        }
    }

    impl CommandRunner for FakeRunner {
        fn json(&mut self, program: &str, args: &[&str]) -> anyhow::Result<Value> {
            self.record(program, args, None);
            self.respond()
        }

        fn json_in_home(
            &mut self,
            program: &str,
            args: &[&str],
            home: &Path,
        ) -> anyhow::Result<Value> {
            self.record(program, args, Some(home));
            self.respond()
        }

        fn run(&mut self, program: &str, args: &[&str]) -> anyhow::Result<()> {
            self.record(program, args, None);
            Ok(())
        }

        fn run_in_home(&mut self, program: &str, args: &[&str], home: &Path) -> anyhow::Result<()> {
            self.record(program, args, Some(home));
            Ok(())
        }
    }
}
