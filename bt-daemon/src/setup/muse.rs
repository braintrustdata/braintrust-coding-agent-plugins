//! Muse Code setup orchestration. Native settings and hook schemas live in
//! crate::muse::config; this module owns locks, migration, and installation.
use super::common::{write_json_atomic_unlocked, CommandRunner, FileAccess};
use super::Setup;
use crate::agents::Muse;
use crate::muse::config::{
    inspect_hook_file, HookFileInspection, HookOverlay, MuseSettings,
    HOOK_COMMAND as MUSE_HOOK_COMMAND,
};
use crate::muse::field::Field;
use crate::paths;
use anyhow::{bail, Context};
use fs2::FileExt;
use std::path::{Path, PathBuf};

fn muse_hook_path(config_dir: &Path) -> PathBuf {
    config_dir.join("braintrust-hooks.json")
}

pub(crate) fn muse_managed_hook_config(
    config_dir: &Path,
    command: &str,
) -> anyhow::Result<HookOverlay> {
    let settings = MuseSettings::load(&config_dir.join("settings.json"))?;
    check_foreign_managed_hooks(config_dir, &settings)?;
    let mut hooks = settings.hook_overlay()?;
    hooks.remove_owned();
    if hooks.contains_braintrust_hook() {
        bail!("Muse user hooks already include an unrecognized Braintrust forwarder; cannot safely replace it");
    }
    hooks.append_owned(command);
    Ok(hooks)
}

pub(crate) fn muse_has_owned_managed_hooks(config_dir: &Path) -> anyhow::Result<bool> {
    let settings = MuseSettings::load(&config_dir.join("settings.json"))?;
    Ok(owned_managed_path(config_dir, &settings)?.is_some())
}
fn owned_managed_path(
    config_dir: &Path,
    settings: &MuseSettings,
) -> anyhow::Result<Option<PathBuf>> {
    match settings.managed_path(config_dir) {
        Some(path)
            if path == muse_hook_path(config_dir) && inspect_hook_file(&path)?.is_owned() =>
        {
            Ok(Some(path))
        }
        _ => Ok(None),
    }
}
fn check_foreign_managed_hooks(config_dir: &Path, settings: &MuseSettings) -> anyhow::Result<()> {
    if let Some(path) = settings.managed_path(config_dir) {
        let inspection = inspect_hook_file(&path)?;
        if inspection.contains_braintrust()
            && !(path == muse_hook_path(config_dir) && inspection.is_owned())
        {
            bail!("Muse managed hooks already include an unrecognized Braintrust forwarder; cannot safely replace it");
        }
    }
    Ok(())
}
pub(crate) fn check_muse_project_hooks(start: &Path) -> anyhow::Result<()> {
    for dir in start.ancestors() {
        let path = dir.join(".muse/hooks.json");
        let inspection = inspect_hook_file(&path)?;
        if inspection == HookFileInspection::Missing {
            continue;
        }
        if inspection.contains_braintrust() {
            bail!("Muse project hooks {} already include Braintrust tracing; cannot safely run a managed trace", path.display());
        }
        break;
    }
    Ok(())
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

fn setup_muse_at(config_dir: &Path) -> anyhow::Result<()> {
    with_muse_settings_lock(config_dir, || {
        let settings_path = config_dir.join("settings.json");
        let mut settings = MuseSettings::load(&settings_path)?;
        check_foreign_managed_hooks(config_dir, &settings)?;
        let mut hooks = settings.hook_overlay()?;
        hooks.remove_owned();
        if hooks.contains_braintrust_hook() {
            bail!("Muse user hooks include an unrecognized Braintrust forwarder; refusing to duplicate it");
        }
        hooks.append_owned(MUSE_HOOK_COMMAND);
        settings.hooks = Field::Value(hooks);
        settings.ensure_schema_version();
        let old_path = owned_managed_path(config_dir, &settings)?;
        if old_path.is_some() {
            settings.managed_hooks_path = Field::Missing;
        }
        write_muse_settings(&settings_path, &settings)?;
        if let Some(path) = old_path {
            std::fs::remove_file(path)?;
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
        let mut settings = MuseSettings::load(&settings_path)?;
        if !settings.hooks.is_missing() {
            let mut hooks = settings.hook_overlay()?;
            hooks.remove_owned();
            settings.hooks = if hooks.is_empty() {
                Field::Missing
            } else {
                Field::Value(hooks)
            };
        }
        let old_path = owned_managed_path(config_dir, &settings)?;
        if old_path.is_some() {
            settings.managed_hooks_path = Field::Missing;
        }
        write_muse_settings(&settings_path, &settings)?;
        if let Some(path) = old_path {
            std::fs::remove_file(path)?;
        }
        Ok(())
    })
}
fn update_muse_at(config_dir: &Path) -> anyhow::Result<()> {
    let settings = MuseSettings::load(&config_dir.join("settings.json"))?;
    if !settings.hook_overlay()?.contains_braintrust_hook()
        && owned_managed_path(config_dir, &settings)?.is_none()
    {
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
    let settings = match MuseSettings::load(&config_dir.join("settings.json")) {
        Ok(settings) => settings,
        Err(error) => {
            warnings.push(format!("Muse settings could not be read: {error}"));
            return warnings;
        }
    };
    if let Err(error) = check_foreign_managed_hooks(config_dir, &settings) {
        warnings.push(error.to_string());
    }
    match settings.hook_overlay() {
        Err(_) => warnings.push("Muse user hooks are malformed".into()),
        Ok(current) => {
            let mut upgraded = current.clone();
            upgraded.remove_owned();
            upgraded.append_owned(MUSE_HOOK_COMMAND);
            if current != upgraded {
                warnings.push("Muse Braintrust hooks are missing or outdated; run `bt trace update muse` or `bt trace enable muse`".into());
            }
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

fn write_muse_settings(path: &Path, settings: &MuseSettings) -> anyhow::Result<()> {
    write_json_atomic_unlocked(path, settings, FileAccess::Inherited)
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
    use crate::muse::config::OwnedHookFile;
    use crate::setup::common::load_object;
    use serde_json::Value;

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
        std::fs::write(
            &hook_path,
            serde_json::to_vec(&OwnedHookFile::previous()).unwrap(),
        )
        .unwrap();
        std::fs::write(
            config.join("settings.json"),
            serde_json::to_vec(&serde_json::json!({"managed_hooks_path":hook_path})).unwrap(),
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
        let overlay = serde_json::to_value(overlay).unwrap();
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
    fn malformed_known_fields_do_not_replace_saved_settings() {
        for invalid in [
            serde_json::json!({"schema_version":"wrong"}),
            serde_json::json!({"managed_hooks_path":42}),
            serde_json::json!({"hooks":null}),
            serde_json::json!({"hooks":{"Stop":[{"hooks":[{"type":"command","command":null}]}]}}),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("settings.json");
            let saved = serde_json::to_vec(&invalid).unwrap();
            std::fs::write(&path, &saved).unwrap();
            assert!(setup_muse_at(temp.path()).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), saved);
            assert!(disable_muse_at(temp.path()).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), saved);
        }
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
