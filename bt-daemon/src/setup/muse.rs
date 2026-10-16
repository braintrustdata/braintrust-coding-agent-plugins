//! Muse Code managed command hooks.
use super::common::{load_object, write_object_atomic_unlocked, CommandRunner, FileAccess};
use super::Setup;
use crate::agents::Muse;
use crate::paths;
use anyhow::{bail, Context};
use fs2::FileExt;
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};

const MUSE_HOOK_EVENTS: &[&str] = &[
    "SessionStart",
    "UserPromptSubmit",
    "PreLLMCall",
    "PostLLMCall",
    "Stop",
    "SessionEnd",
];
const LEGACY_MUSE_HOOK_EVENTS: &[&str] = &[
    "SessionStart",
    "UserPromptSubmit",
    "PreToolUse",
    "PermissionRequest",
    "PostToolUse",
    "PreLLMCall",
    "PostLLMCall",
    "PreCompact",
    "PostCompact",
    "SubagentStart",
    "SubagentStop",
    "Stop",
    "SessionEnd",
];

fn muse_hook_path(config_dir: &Path) -> PathBuf {
    config_dir.join("braintrust-hooks.json")
}

fn muse_hook_config() -> Map<String, Value> {
    // Muse treats command-hook failures as non-fatal to the agent turn.
    muse_hook_config_for(
        MUSE_HOOK_EVENTS,
        "bt trace hook --source muse --flush-on-turn-end",
    )
}

fn muse_hook_config_for(events: &[&str], command: &str) -> Map<String, Value> {
    let hooks = events
        .iter()
        .map(|event| {
            (
                (*event).to_string(),
                serde_json::json!([{
                    "matcher": "*",
                    "hooks": [{"type": "command", "command": command}],
                }]),
            )
        })
        .collect::<Map<_, _>>();
    Map::from_iter([
        ("schema_version".into(), Value::from(1)),
        ("hooks".into(), Value::Object(hooks)),
    ])
}

/// Build an invocation-local managed file without changing Muse's settings.
/// Preserve handlers administered by someone else, but never run two Muse
/// Braintrust forwarders with different destinations in the same session.
pub(crate) fn muse_managed_hook_config(config_dir: &Path, command: &str) -> anyhow::Result<Value> {
    let settings = load_object(&config_dir.join("settings.json"))?;
    if settings
        .get("hooks")
        .is_some_and(muse_contains_braintrust_hook)
    {
        bail!("Muse user hooks already include Braintrust tracing; remove that duplicate before `bt trace run muse`");
    }
    let existing = match settings.get("managed_hooks_path").and_then(Value::as_str) {
        Some(path) => {
            let path = Path::new(path);
            let path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                config_dir.join(path)
            };
            if !path.exists() || muse_hook_is_owned(&path)? {
                Map::from_iter([("schema_version".into(), Value::from(1))])
            } else {
                let raw = std::fs::read(&path)
                    .with_context(|| format!("read Muse managed hooks {}", path.display()))?;
                let value: Value = serde_json::from_slice(&raw)?;
                if muse_contains_braintrust_hook(&value) {
                    bail!("Muse managed hooks already include an unrecognized Braintrust forwarder; cannot safely replace it");
                }
                if muse_has_strict_handler_fields(&value) {
                    bail!("Muse managed hooks use the strict handler schema; cannot safely merge invocation hooks");
                }
                value
                    .as_object()
                    .cloned()
                    .context("Muse managed hooks must be a JSON object")?
            }
        }
        None => Map::from_iter([("schema_version".into(), Value::from(1))]),
    };
    let mut result = existing;
    let mut hooks = result
        .remove("hooks")
        .unwrap_or_else(|| Value::Object(Map::new()));
    let hooks = hooks
        .as_object_mut()
        .context("Muse managed hooks must be an object")?;
    let generated = muse_hook_config_for(MUSE_HOOK_EVENTS, command);
    for (event, handlers) in generated["hooks"]
        .as_object()
        .context("generated Muse hooks must be an object")?
    {
        let existing = hooks
            .entry(event.clone())
            .or_insert_with(|| Value::Array(Vec::new()));
        existing
            .as_array_mut()
            .with_context(|| format!("Muse managed hook event {event} must be an array"))?
            .extend(
                handlers
                    .as_array()
                    .context("generated Muse handlers must be an array")?
                    .iter()
                    .cloned(),
            );
    }
    result.insert("hooks".into(), hooks.clone().into());
    Ok(Value::Object(result))
}

pub(crate) fn check_muse_project_hooks(start: &Path) -> anyhow::Result<()> {
    for dir in start.ancestors() {
        let path = dir.join(".muse").join("hooks.json");
        if !path.exists() {
            continue;
        }
        let hooks: Value = serde_json::from_slice(&std::fs::read(&path)?)
            .with_context(|| format!("read Muse project hooks {}", path.display()))?;
        if muse_contains_braintrust_hook(&hooks) {
            bail!("Muse project hooks {} already include Braintrust tracing; cannot safely run a managed trace", path.display());
        }
        break;
    }
    Ok(())
}

fn muse_contains_braintrust_hook(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            (key == "command"
                && value.as_str().is_some_and(|command| {
                    command.contains("--source")
                        && command.contains("muse")
                        && command.contains("hook")
                }))
                || muse_contains_braintrust_hook(value)
        }),
        Value::Array(items) => items.iter().any(muse_contains_braintrust_hook),
        _ => false,
    }
}

fn muse_has_strict_handler_fields(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key("name")
                || object.contains_key("timeout_ms")
                || object.values().any(muse_has_strict_handler_fields)
        }
        Value::Array(items) => items.iter().any(muse_has_strict_handler_fields),
        _ => false,
    }
}

fn legacy_muse_hook_config() -> Map<String, Value> {
    muse_hook_config_for(
        LEGACY_MUSE_HOOK_EVENTS,
        "bt trace hook --source muse --flush-on-turn-end || true",
    )
}

pub(crate) fn muse_version(output: &[u8]) -> anyhow::Result<semver::Version> {
    let text = std::str::from_utf8(output).context("Muse version is not UTF-8")?;
    text.split_whitespace()
        .find_map(|word| semver::Version::parse(word).ok())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Muse Code returned an unrecognized version: {}",
                text.trim()
            )
        })
}

/// Muse serializes changes to settings.json with this native lock file. Hold
/// it across the read, hook update, and settings update so we cannot discard a
/// concurrent change made by Muse itself.
fn with_muse_settings_lock<T>(
    config_dir: &Path,
    action: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    std::fs::create_dir_all(config_dir)
        .with_context(|| format!("create Muse config directory {}", config_dir.display()))?;
    let lock_path = config_dir.join(".settings.json.lock");
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .with_context(|| format!("open Muse settings lock {}", lock_path.display()))?;
    lock.lock_exclusive()
        .with_context(|| format!("lock Muse settings {}", lock_path.display()))?;
    let result = action();
    FileExt::unlock(&lock)?;
    result
}

fn muse_hook_is_owned(path: &Path) -> anyhow::Result<bool> {
    match std::fs::read(path) {
        Ok(raw) => {
            let value: Value = serde_json::from_slice(&raw)
                .with_context(|| format!("invalid Muse hook configuration: {}", path.display()))?;
            Ok(value == Value::Object(muse_hook_config())
                || value == Value::Object(legacy_muse_hook_config()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => {
            Err(error).with_context(|| format!("read Muse hook configuration: {}", path.display()))
        }
    }
}

fn check_muse_hook_ownership(path: &Path) -> anyhow::Result<()> {
    if path.exists() && !muse_hook_is_owned(path)? {
        bail!("Muse hook file {} differs from Braintrust's generated hooks; refusing to overwrite or remove it", path.display());
    }
    Ok(())
}

fn setup_muse_at(config_dir: &Path) -> anyhow::Result<()> {
    with_muse_settings_lock(config_dir, || {
        let settings_path = config_dir.join("settings.json");
        let hook_path = muse_hook_path(config_dir);
        let mut settings = load_object(&settings_path)?;
        let owned_path = hook_path.to_string_lossy();
        if let Some(existing) = settings.get("managed_hooks_path") {
            if existing.as_str() != Some(owned_path.as_ref()) {
                bail!(
                    "Muse already uses managed_hooks_path {}; refusing to overwrite it",
                    existing
                );
            }
        }
        check_muse_hook_ownership(&hook_path)?;
        settings.entry("schema_version").or_insert(Value::from(1));
        settings.insert(
            "managed_hooks_path".into(),
            Value::String(owned_path.into_owned()),
        );
        write_muse_object(&hook_path, muse_hook_config())?;
        write_muse_object(&settings_path, settings)
    })
}

fn disable_muse_at(config_dir: &Path) -> anyhow::Result<()> {
    with_muse_settings_lock(config_dir, || {
        let settings_path = config_dir.join("settings.json");
        let hook_path = muse_hook_path(config_dir);
        let mut settings = load_object(&settings_path)?;
        check_muse_hook_ownership(&hook_path)?;
        if settings.get("managed_hooks_path").and_then(Value::as_str)
            == Some(hook_path.to_string_lossy().as_ref())
        {
            settings.remove("managed_hooks_path");
            write_muse_object(&settings_path, settings)?;
        }
        match std::fs::remove_file(&hook_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| {
                format!(
                    "failed to remove Muse hook configuration: {}",
                    hook_path.display()
                )
            }),
        }
    })
}

fn update_muse_at(config_dir: &Path) -> anyhow::Result<()> {
    with_muse_settings_lock(config_dir, || {
        let settings = load_object(&config_dir.join("settings.json"))?;
        let hook_path = muse_hook_path(config_dir);
        if settings.get("managed_hooks_path").and_then(Value::as_str)
            != Some(hook_path.to_string_lossy().as_ref())
        {
            bail!("Muse tracing hooks are not installed; run `bt trace enable muse`");
        }
        if !hook_path.exists() {
            bail!("Muse tracing hook file is missing; run `bt trace enable muse`");
        }
        check_muse_hook_ownership(&hook_path)?;
        write_muse_object(&hook_path, muse_hook_config())
    })
}

pub(crate) fn muse_doctor_warnings(config_dir: &Path) -> Vec<String> {
    let mut warnings = Vec::new();
    match crate::subprocess::background_command("muse")
        .arg("--version")
        .output()
    {
        Ok(output) if output.status.success() => match muse_version(&output.stdout) {
            Ok(version) if version < semver::Version::new(1, 1, 1) => warnings
                .push("Muse Code 1.1.1 or newer is required for verified command hooks".into()),
            Ok(_) => {}
            Err(error) => warnings.push(error.to_string()),
        },
        _ => warnings.push(
            "Muse Code executable is unavailable; install Muse Code before enabling tracing".into(),
        ),
    }
    if !crate::subprocess::background_command("bt")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
    {
        warnings.push(
            "Braintrust `bt` executable is unavailable on PATH; Muse command hooks cannot forward events".into(),
        );
    }
    let settings = match load_object(&config_dir.join("settings.json")) {
        Ok(settings) => settings,
        Err(error) => {
            warnings.push(format!("Muse settings could not be read: {error}"));
            return warnings;
        }
    };
    let hook_path = muse_hook_path(config_dir);
    if settings.get("managed_hooks_path").and_then(Value::as_str)
        != Some(hook_path.to_string_lossy().as_ref())
    {
        warnings.push("Muse managed_hooks_path does not point to Braintrust's hook file; run `bt trace enable muse`".into());
    }
    match std::fs::read(&hook_path) {
        Ok(raw) => match serde_json::from_slice::<Value>(&raw) {
            Ok(hooks) if hooks == Value::Object(muse_hook_config()) => {}
            Ok(hooks) if hooks == Value::Object(legacy_muse_hook_config()) => warnings.push(
                "Muse Braintrust hooks use an older command; run `bt trace update muse`".into(),
            ),
            Ok(_) => warnings.push("Muse Braintrust hook file differs from expected event definitions; restore it or run `bt trace enable muse`".into()),
            Err(error) => warnings.push(format!("Muse Braintrust hook file is invalid JSON: {error}")),
        },
        Err(error) => warnings.push(format!("Muse Braintrust hook file could not be read: {error}")),
    }
    warnings
}

fn setup_muse() -> anyhow::Result<()> {
    let output = crate::subprocess::background_command("muse")
        .arg("--version")
        .output()
        .context("Muse Code is not installed; install Muse Code before enabling tracing")?;
    if !output.status.success() {
        bail!("Muse Code could not report its version; install Muse Code before enabling tracing");
    }
    let installed = muse_version(&output.stdout)?;
    if installed < semver::Version::new(1, 1, 1) {
        bail!("Muse Code 1.1.1 or newer is required for verified command hooks");
    }
    setup_muse_at(&paths::muse_config_dir())
}
fn disable_muse() -> anyhow::Result<()> {
    disable_muse_at(&paths::muse_config_dir())
}

fn update_muse() -> anyhow::Result<()> {
    update_muse_at(&paths::muse_config_dir())
}

fn write_muse_object(path: &Path, object: Map<String, Value>) -> anyhow::Result<()> {
    write_object_atomic_unlocked(path, object, FileAccess::Inherited)
}

impl Setup for Muse {
    fn enable(&self, _runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        setup_muse()
    }
    fn disable(&self, _runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        disable_muse()
    }
    fn update(&self, _runner: &mut dyn CommandRunner) -> anyhow::Result<()> {
        update_muse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn muse_setup_owns_only_its_managed_hook_file() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("muse");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            config_dir.join("settings.json"),
            r#"{"schema_version":1,"theme":"dark"}"#,
        )
        .unwrap();

        setup_muse_at(&config_dir).unwrap();
        setup_muse_at(&config_dir).unwrap();
        let settings: Value =
            serde_json::from_slice(&std::fs::read(config_dir.join("settings.json")).unwrap())
                .unwrap();
        assert_eq!(settings["theme"], "dark");
        assert_eq!(
            settings["managed_hooks_path"],
            muse_hook_path(&config_dir).to_string_lossy().as_ref()
        );
        let hooks: Value =
            serde_json::from_slice(&std::fs::read(muse_hook_path(&config_dir)).unwrap()).unwrap();
        assert_eq!(hooks["schema_version"], 1);
        assert!(hooks["hooks"]["PreLLMCall"].is_array());
        assert_eq!(hooks["hooks"].as_object().unwrap().len(), 6);
        assert!(hooks["hooks"].get("PreToolUse").is_none());
        assert_eq!(
            hooks["hooks"]["PreLLMCall"][0]["hooks"][0]["command"],
            "bt trace hook --source muse --flush-on-turn-end"
        );
        assert!(config_dir.join(".settings.json.lock").exists());

        disable_muse_at(&config_dir).unwrap();
        let settings: Value =
            serde_json::from_slice(&std::fs::read(config_dir.join("settings.json")).unwrap())
                .unwrap();
        assert_eq!(settings["theme"], "dark");
        assert!(settings.get("managed_hooks_path").is_none());
        assert!(!muse_hook_path(&config_dir).exists());
    }

    #[test]
    fn muse_managed_run_replaces_only_owned_braintrust_hooks() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("muse");
        setup_muse_at(&config_dir).unwrap();
        let saved = std::fs::read(muse_hook_path(&config_dir)).unwrap();
        let generated =
            muse_managed_hook_config(&config_dir, "'bt' 'hook' '--source' 'muse'").unwrap();
        assert_eq!(
            generated["hooks"].as_object().unwrap().len(),
            MUSE_HOOK_EVENTS.len()
        );
        assert_eq!(
            generated["hooks"]["SessionStart"][0]["hooks"][0]["command"],
            "'bt' 'hook' '--source' 'muse'"
        );
        assert_eq!(std::fs::read(muse_hook_path(&config_dir)).unwrap(), saved);
    }

    #[test]
    fn muse_managed_run_keeps_foreign_handlers_and_rejects_duplicate_user_hooks() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("muse");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            config_dir.join("settings.json"),
            r#"{"schema_version":1,"managed_hooks_path":"other.json"}"#,
        )
        .unwrap();
        let original = r#"{"schema_version":1,"hooks":{"Stop":[{"hooks":[{"type":"command","command":"true"}]}]}}"#;
        std::fs::write(config_dir.join("other.json"), original).unwrap();
        let generated = muse_managed_hook_config(&config_dir, "bt hook --source muse").unwrap();
        assert_eq!(generated["hooks"]["Stop"].as_array().unwrap().len(), 2);
        assert_eq!(generated["hooks"]["Stop"][0]["hooks"][0]["command"], "true");
        assert_eq!(
            std::fs::read_to_string(config_dir.join("other.json")).unwrap(),
            original
        );

        std::fs::write(
            config_dir.join("settings.json"),
            r#"{"schema_version":1,"hooks":{"Stop":[{"hooks":[{"type":"command","command":"bt trace hook --source muse"}]}]}}"#,
        )
        .unwrap();
        assert!(
            muse_managed_hook_config(&config_dir, "bt hook --source muse")
                .unwrap_err()
                .to_string()
                .contains("user hooks already include Braintrust")
        );

        std::fs::write(
            config_dir.join("settings.json"),
            r#"{"schema_version":1,"managed_hooks_path":"other.json"}"#,
        )
        .unwrap();
        std::fs::write(
            config_dir.join("other.json"),
            r#"{"schema_version":1,"hooks":{"Stop":[{"hooks":[{"type":"command","name":"foreign","timeout_ms":1000,"command":"true"}]}]}}"#,
        )
        .unwrap();
        assert!(
            muse_managed_hook_config(&config_dir, "bt hook --source muse")
                .unwrap_err()
                .to_string()
                .contains("strict handler schema")
        );
    }

    #[test]
    fn muse_managed_run_rejects_duplicate_project_hooks() {
        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("project");
        let nested = project.join("nested");
        std::fs::create_dir_all(project.join(".muse")).unwrap();
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(
            project.join(".muse/hooks.json"),
            r#"{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"'bt' 'trace' 'hook' '--source' 'muse'"}]}]}}"#,
        )
        .unwrap();
        assert!(check_muse_project_hooks(&nested)
            .unwrap_err()
            .to_string()
            .contains("project hooks"));
    }

    #[test]
    fn muse_setup_refuses_another_managed_hook_owner() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("muse");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            config_dir.join("settings.json"),
            r#"{"schema_version":1,"managed_hooks_path":"/other/hooks.json"}"#,
        )
        .unwrap();
        assert!(setup_muse_at(&config_dir)
            .unwrap_err()
            .to_string()
            .contains("refusing to overwrite"));
    }

    #[test]
    fn muse_setup_and_disable_preserve_foreign_hook_file() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("muse");
        std::fs::create_dir_all(&config_dir).unwrap();
        let hook_path = muse_hook_path(&config_dir);
        let custom = r#"{"schema_version":1,"hooks":{"Stop":[]}}"#;
        std::fs::write(&hook_path, custom).unwrap();

        assert!(setup_muse_at(&config_dir)
            .unwrap_err()
            .to_string()
            .contains("refusing to overwrite"));
        assert!(disable_muse_at(&config_dir)
            .unwrap_err()
            .to_string()
            .contains("refusing to overwrite"));
        assert_eq!(std::fs::read_to_string(&hook_path).unwrap(), custom);
    }

    #[test]
    fn muse_update_requires_an_owned_install_and_upgrades_legacy_command() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join("muse");
        assert!(update_muse_at(&config_dir)
            .unwrap_err()
            .to_string()
            .contains("not installed"));

        setup_muse_at(&config_dir).unwrap();
        let hook_path = muse_hook_path(&config_dir);
        write_muse_object(&hook_path, legacy_muse_hook_config()).unwrap();
        assert!(muse_doctor_warnings(&config_dir)
            .iter()
            .any(|warning| warning.contains("older command")));
        update_muse_at(&config_dir).unwrap();
        let installed = load_object(&hook_path).unwrap();
        assert_eq!(installed, muse_hook_config());

        std::fs::write(&hook_path, r#"{"hooks":{}}"#).unwrap();
        assert!(update_muse_at(&config_dir)
            .unwrap_err()
            .to_string()
            .contains("refusing to overwrite"));
    }

    #[test]
    fn muse_version_requires_a_parseable_native_version() {
        assert_eq!(
            muse_version(b"Muse Code 1.1.1 (1.1.1-R2514.1)\n").unwrap(),
            semver::Version::new(1, 1, 1)
        );
        assert!(muse_version(b"unexpected output").is_err());
    }
}
