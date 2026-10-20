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
    "PreToolUse",
    "PermissionRequest",
    "PostToolUse",
    "PostToolUseFailure",
    "PostToolBatch",
    "PreLLMCall",
    "PostLLMCall",
    "PreCompact",
    "PostCompact",
    "SubagentStart",
    "SubagentStop",
    "Stop",
    "StopFailure",
    "SessionEnd",
    "Notification",
];
const PREVIOUS_MUSE_HOOK_EVENTS: &[&str] = &[
    "SessionStart",
    "UserPromptSubmit",
    "PreLLMCall",
    "PostLLMCall",
    "Stop",
    "SessionEnd",
];
const MUSE_HOOK_COMMAND: &str = "bt trace hook --source muse --flush-on-turn-end";
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
    muse_hook_config_for(MUSE_HOOK_EVENTS, MUSE_HOOK_COMMAND)
}

fn muse_hook_config_for(events: &[&str], command: &str) -> Map<String, Value> {
    let hooks = events
        .iter()
        .map(|event| {
            (
                (*event).to_string(),
                serde_json::json!([{
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

/// Build a per-run user hook overlay. Keeping the managed source separate preserves
/// enterprise handlers, including its stricter file-wide validation schema.
pub(crate) fn muse_managed_hook_config(config_dir: &Path, command: &str) -> anyhow::Result<Value> {
    let settings = load_object(&config_dir.join("settings.json"))?;
    check_foreign_managed_hooks(config_dir, &settings)?;
    let mut hooks = settings
        .get("hooks")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    remove_owned_hooks(&mut hooks)?;
    if muse_contains_braintrust_hook(&hooks) {
        bail!("Muse user hooks already include an unrecognized Braintrust forwarder; cannot safely replace it");
    }
    append_hooks(&mut hooks, command)?;
    Ok(hooks)
}

fn managed_hook_path(config_dir: &Path, settings: &Map<String, Value>) -> Option<PathBuf> {
    settings
        .get("managed_hooks_path")
        .and_then(Value::as_str)
        .map(|path| {
            let path = Path::new(path);
            if path.is_absolute() {
                path.to_owned()
            } else {
                config_dir.join(path)
            }
        })
}

pub(crate) fn muse_has_owned_managed_hooks(config_dir: &Path) -> anyhow::Result<bool> {
    let settings = load_object(&config_dir.join("settings.json"))?;
    managed_hook_path(config_dir, &settings)
        .map(|path| {
            if path == muse_hook_path(config_dir) {
                muse_hook_is_owned(&path)
            } else {
                Ok(false)
            }
        })
        .transpose()
        .map(|owned| owned.unwrap_or(false))
}

fn check_foreign_managed_hooks(
    config_dir: &Path,
    settings: &Map<String, Value>,
) -> anyhow::Result<()> {
    if let Some(path) = managed_hook_path(config_dir, settings) {
        if path.exists() && (path != muse_hook_path(config_dir) || !muse_hook_is_owned(&path)?) {
            let hooks: Value = serde_json::from_slice(&std::fs::read(path)?)?;
            if muse_contains_braintrust_hook(&hooks) {
                bail!("Muse managed hooks already include an unrecognized Braintrust forwarder; cannot safely replace it");
            }
        }
    }
    Ok(())
}

fn owned_command(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("command")
        && matches!(
            value.get("command").and_then(Value::as_str),
            Some(MUSE_HOOK_COMMAND)
                | Some("bt trace hook --source muse --flush-on-turn-end || true")
        )
        && value.as_object().is_some_and(|object| object.len() == 2)
}

fn remove_owned_hooks(hooks: &mut Value) -> anyhow::Result<()> {
    let events = hooks
        .as_object_mut()
        .context("Muse user hooks must be an object")?;
    let mut emptied_events = Vec::new();
    for (event, groups) in events.iter_mut() {
        let groups = groups
            .as_array_mut()
            .context("Muse hook event must be an array")?;
        let original_len = groups.len();
        for index in (0..groups.len()).rev() {
            let handlers = groups[index]
                .get_mut("hooks")
                .and_then(Value::as_array_mut)
                .context("Muse hook group must contain a hooks array")?;
            let original_handlers = handlers.len();
            handlers.retain(|handler| !owned_command(handler));
            if original_handlers != handlers.len() && handlers.is_empty() {
                groups.remove(index);
            }
        }
        if original_len != groups.len() && groups.is_empty() {
            emptied_events.push(event.clone());
        }
    }
    for event in emptied_events {
        events.remove(&event);
    }
    Ok(())
}

fn append_hooks(hooks: &mut Value, command: &str) -> anyhow::Result<()> {
    let events = hooks
        .as_object_mut()
        .context("Muse hooks must be an object")?;
    let generated = muse_hook_config_for(MUSE_HOOK_EVENTS, command);
    for (event, groups) in generated["hooks"]
        .as_object()
        .context("generated Muse hooks must be an object")?
    {
        events
            .entry(event.clone())
            .or_insert_with(|| serde_json::json!([]))
            .as_array_mut()
            .context("Muse hook event must be an array")?
            .extend(
                groups
                    .as_array()
                    .context("generated Muse groups must be an array")?
                    .iter()
                    .cloned(),
            );
    }
    Ok(())
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

/// Resolve the running executable rather than stamping an install-time version:
/// Muse's launcher can upgrade independently of Braintrust's hook definitions.
pub(crate) fn installed_muse_version() -> anyhow::Result<semver::Version> {
    let executable = crate::env::var_os("MUSE_BIN").unwrap_or_else(|| "muse".into());
    let output = crate::subprocess::background_command(executable)
        .arg("--version")
        .output()
        .context("check Muse Code version")?;
    if !output.status.success() {
        bail!("Muse Code could not report its version");
    }
    muse_version(&output.stdout)
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
            let mut previous = muse_hook_config_for(PREVIOUS_MUSE_HOOK_EVENTS, MUSE_HOOK_COMMAND);
            let mut legacy = legacy_muse_hook_config();
            for config in [&mut previous, &mut legacy] {
                for groups in config["hooks"]
                    .as_object_mut()
                    .context("generated Muse hooks")?
                    .values_mut()
                {
                    groups[0]["matcher"] = Value::from("*");
                }
            }
            Ok(value == Value::Object(muse_hook_config())
                || value == Value::Object(previous)
                || value == Value::Object(legacy)
                || value == Value::Object(legacy_muse_hook_config()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => {
            Err(error).with_context(|| format!("read Muse hook configuration: {}", path.display()))
        }
    }
}

fn setup_muse_at(config_dir: &Path) -> anyhow::Result<()> {
    with_muse_settings_lock(config_dir, || {
        let settings_path = config_dir.join("settings.json");
        let mut settings = load_object(&settings_path)?;
        check_foreign_managed_hooks(config_dir, &settings)?;
        let mut hooks = settings
            .remove("hooks")
            .unwrap_or_else(|| serde_json::json!({}));
        remove_owned_hooks(&mut hooks)?;
        if muse_contains_braintrust_hook(&hooks) {
            bail!("Muse user hooks include an unrecognized Braintrust forwarder; refusing to duplicate it");
        }
        append_hooks(&mut hooks, MUSE_HOOK_COMMAND)?;
        settings.insert("hooks".into(), hooks);
        settings.entry("schema_version").or_insert(Value::from(1));
        // Migrate our old dedicated managed file; never edit an enterprise file.
        let old_path = managed_hook_path(config_dir, &settings)
            .filter(|path| path == &muse_hook_path(config_dir));
        let migrate = old_path
            .as_ref()
            .map(|path| muse_hook_is_owned(path))
            .transpose()?
            .unwrap_or(false);
        if migrate {
            settings.remove("managed_hooks_path");
        }
        write_muse_object(&settings_path, settings)?;
        if migrate {
            std::fs::remove_file(old_path.context("owned Muse hook path")?)?;
        }
        Ok(())
    })
}

fn disable_muse_at(config_dir: &Path) -> anyhow::Result<()> {
    with_muse_settings_lock(config_dir, || {
        let settings_path = config_dir.join("settings.json");
        if !settings_path.exists() {
            return Ok(());
        }
        let mut settings = load_object(&settings_path)?;
        if let Some(hooks) = settings.get_mut("hooks") {
            remove_owned_hooks(hooks)?;
            if hooks.as_object().is_some_and(Map::is_empty) {
                settings.remove("hooks");
            }
        }
        let old_path = managed_hook_path(config_dir, &settings)
            .filter(|path| path == &muse_hook_path(config_dir));
        let remove_old = old_path
            .as_ref()
            .map(|path| muse_hook_is_owned(path))
            .transpose()?
            .unwrap_or(false);
        if remove_old {
            settings.remove("managed_hooks_path");
        }
        write_muse_object(&settings_path, settings)?;
        if remove_old {
            std::fs::remove_file(old_path.context("owned Muse hook path")?)?;
        }
        Ok(())
    })
}

fn update_muse_at(config_dir: &Path) -> anyhow::Result<()> {
    let settings = load_object(&config_dir.join("settings.json"))?;
    let installed = settings
        .get("hooks")
        .is_some_and(muse_contains_braintrust_hook)
        || muse_has_owned_managed_hooks(config_dir)?;
    if !installed {
        bail!("Muse tracing hooks are not installed; run `bt trace enable muse`");
    }
    setup_muse_at(config_dir)
}

pub(crate) fn muse_doctor_warnings(config_dir: &Path) -> Vec<String> {
    let mut warnings = Vec::new();
    match crate::subprocess::background_command("muse")
        .arg("--version")
        .output()
    {
        Ok(output) if output.status.success() => match muse_version(&output.stdout) {
            Ok(version) if version < semver::Version::new(1, 3, 0) => warnings
                .push("Muse Code 1.3.0 or newer is required for verified command hooks".into()),
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
    if let Err(error) = check_foreign_managed_hooks(config_dir, &settings) {
        warnings.push(error.to_string());
    }
    let current = settings
        .get("hooks")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let mut remaining = current.clone();
    if remove_owned_hooks(&mut remaining).is_err() {
        warnings.push("Muse user hooks are malformed".into());
    } else {
        let mut upgraded = remaining;
        if append_hooks(&mut upgraded, MUSE_HOOK_COMMAND).is_err() || current != upgraded {
            warnings.push("Muse Braintrust hooks are missing or outdated; run `bt trace update muse` or `bt trace enable muse`".into());
        }
    }
    warnings
}

fn setup_muse() -> anyhow::Result<()> {
    let installed =
        installed_muse_version().context("install Muse Code before enabling tracing")?;
    if installed < semver::Version::new(1, 3, 0) {
        bail!("Muse Code 1.3.0 or newer is required for verified command hooks");
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
    fn setup_update_and_disable_preserve_enterprise_and_user_hooks() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path();
        let enterprise = serde_json::json!({"hooks":{"SessionStart":[{"hooks":[{"name":"policy","timeout_ms":1000,"command":"true"}]}]}});
        std::fs::write(config.join("enterprise.json"), enterprise.to_string()).unwrap();
        let foreign = serde_json::json!({"hooks":[{"type":"command","command":"echo unrelated"}]});
        let settings = serde_json::json!({"schema_version":1,"theme":"dark","managed_hooks_path":"enterprise.json","hooks":{"Stop":[foreign.clone()]}});
        std::fs::write(config.join("settings.json"), settings.to_string()).unwrap();
        setup_muse_at(config).unwrap();
        let first = std::fs::read(config.join("settings.json")).unwrap();
        setup_muse_at(config).unwrap();
        update_muse_at(config).unwrap();
        assert_eq!(first, std::fs::read(config.join("settings.json")).unwrap());
        let installed: Value = serde_json::from_slice(&first).unwrap();
        assert_eq!(installed["hooks"].as_object().unwrap().len(), 17);
        assert_eq!(installed["hooks"]["Stop"][0], foreign);
        // Unmatchable lifecycle events reject a matcher on current Muse.
        assert!(installed["hooks"]["Stop"][1].get("matcher").is_none());
        disable_muse_at(config).unwrap();
        assert_eq!(
            Value::Object(load_object(&config.join("settings.json")).unwrap()),
            settings
        );
        assert_eq!(
            std::fs::read_to_string(config.join("enterprise.json")).unwrap(),
            enterprise.to_string()
        );
    }

    #[test]
    fn migrate_old_owned_managed_file_and_preserve_unknown_files() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path();
        let hook_path = muse_hook_path(config);
        let mut old = muse_hook_config_for(PREVIOUS_MUSE_HOOK_EVENTS, MUSE_HOOK_COMMAND);
        for groups in old["hooks"].as_object_mut().unwrap().values_mut() {
            groups[0]["matcher"] = Value::from("*");
        }
        write_muse_object(&hook_path, old).unwrap();
        write_muse_object(
            &config.join("settings.json"),
            serde_json::json!({"managed_hooks_path":hook_path})
                .as_object()
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert!(muse_has_owned_managed_hooks(config).unwrap());
        update_muse_at(config).unwrap();
        assert!(!hook_path.exists());
        assert!(load_object(&config.join("settings.json"))
            .unwrap()
            .get("managed_hooks_path")
            .is_none());
        let custom = r#"{"hooks":{"Stop":[]}}"#;
        std::fs::write(&hook_path, custom).unwrap();
        setup_muse_at(config).unwrap();
        disable_muse_at(config).unwrap();
        assert_eq!(std::fs::read_to_string(&hook_path).unwrap(), custom);
    }

    #[test]
    fn invocation_overlay_replaces_owned_hooks_and_preserves_strict_enterprise() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path();
        let enterprise = r#"{"hooks":{"SessionStart":[{"hooks":[{"name":"policy","timeout_ms":1000,"command":"true"}]}]}}"#;
        std::fs::write(config.join("enterprise.json"), enterprise).unwrap();
        std::fs::write(config.join("settings.json"), r#"{"managed_hooks_path":"enterprise.json","hooks":{"Stop":[{"hooks":[{"type":"command","command":"echo unrelated"}]}]}}"#).unwrap();
        setup_muse_at(config).unwrap();
        let saved = std::fs::read(config.join("settings.json")).unwrap();
        let overlay = muse_managed_hook_config(config, "run-forwarder").unwrap();
        assert_eq!(overlay["Stop"].as_array().unwrap().len(), 2);
        assert_eq!(overlay["Stop"][0]["hooks"][0]["command"], "echo unrelated");
        assert_eq!(overlay["Stop"][1]["hooks"][0]["command"], "run-forwarder");
        assert!(!overlay.to_string().contains(MUSE_HOOK_COMMAND));
        assert_eq!(std::fs::read(config.join("settings.json")).unwrap(), saved);
        assert_eq!(
            std::fs::read_to_string(config.join("enterprise.json")).unwrap(),
            enterprise
        );
    }

    #[test]
    fn reject_unrecognized_braintrust_forwarders_and_project_duplicates() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("settings.json"), r#"{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"custom-bt trace hook --source muse"}]}]}}"#).unwrap();
        assert!(setup_muse_at(temp.path()).is_err());
        assert!(muse_managed_hook_config(temp.path(), "managed").is_err());
        std::fs::create_dir(temp.path().join(".muse")).unwrap();
        std::fs::write(temp.path().join(".muse/hooks.json"), r#"{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"bt trace hook --source muse"}]}]}}"#).unwrap();
        assert!(check_muse_project_hooks(temp.path()).is_err());
    }

    #[test]
    fn disable_before_install_does_not_create_invalid_native_settings() {
        let temp = tempfile::tempdir().unwrap();
        disable_muse_at(temp.path()).unwrap();
        assert!(!temp.path().join("settings.json").exists());
    }

    #[test]
    fn version_and_update_diagnostics() {
        assert_eq!(
            muse_version(b"Muse Code 1.4.10 (1.4.10-R6427.1)\n").unwrap(),
            semver::Version::new(1, 4, 10)
        );
        assert!(muse_version(b"unexpected output").is_err());
        let temp = tempfile::tempdir().unwrap();
        assert!(update_muse_at(temp.path())
            .unwrap_err()
            .to_string()
            .contains("not installed"));
    }
}
