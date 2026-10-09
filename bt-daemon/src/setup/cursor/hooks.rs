//! Capture and managed-run discovery entries in Cursor's user `hooks.json`.
//!
//! Setup registers fail-open capture entries for every plugin event. Managed
//! runs add no-op discovery entries
//! marked with their process identity, which later runs remove once that
//! process has exited.

use crate::setup::common::{edit_object, load_object, FileAccess};
use crate::wire::ProcessIdentity;
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};

const PERSISTENT_MARKER: &str = "braintrust-cursor-discovery";
const MANAGED_MARKER_PREFIX: &str = "braintrust-cursor-managed-";

/// `(event, command)` pairs for the discovery entries tagged with `marker`.
fn specs(marker: &str, windows: bool) -> Vec<(String, String)> {
    [
        ("beforeSubmitPrompt", r#"'{"continue":true}'"#),
        ("afterAgentResponse", "'{}'"),
        ("stop", "'{}'"),
    ]
    .into_iter()
    .map(|(event, response)| {
        let command = if windows {
            let response = if event == "beforeSubmitPrompt" {
                r#"{"continue":true}"#
            } else {
                "{}"
            };
            powershell_encoded_command(&format!(
                "[Console]::Out.WriteLine('{}') # {marker}",
                response.replace('\'', "''")
            ))
        } else {
            format!("printf '%s\\n' {response} # {marker}")
        };
        (event.to_owned(), command)
    })
    .collect()
}

pub(crate) const USER_HOOK_ENV: &str = "BT_TRACE_CURSOR_USER_HOOK";
const CAPTURE_MARKER: &str = "braintrust-cursor-capture";

pub(crate) fn persistent_specs() -> &'static [(String, String)] {
    static SPECS: std::sync::OnceLock<Vec<(String, String)>> = std::sync::OnceLock::new();
    SPECS.get_or_init(|| capture_specs(cfg!(windows)))
}

fn capture_specs(windows: bool) -> Vec<(String, String)> {
    let manifest: Value = serde_json::from_str(super::HOOKS_MANIFEST)
        .expect("embedded Cursor hooks manifest must be valid");
    manifest["hooks"].as_object().unwrap().iter().map(|(event, entries)| {
        let command = entries[0]["command"].as_str().unwrap();
        let response = if event == "beforeSubmitPrompt" { r#"{"continue":true}"# } else { "{}" };
        let command = if windows {
            powershell_encoded_command(&format!(
                "$env:{USER_HOOK_ENV} = '1'; try {{ & {command} *> $null }} catch {{}}; [Console]::Out.WriteLine('{response}') # {CAPTURE_MARKER}"
            ))
        } else {
            format!("{USER_HOOK_ENV}=1 {command} >/dev/null 2>/dev/null || true; printf '%s\\n' '{response}' # {CAPTURE_MARKER}")
        };
        (event.clone(), command)
    }).collect()
}

/// Suppress a plugin registration only when this event has a user capture hook.
pub(crate) fn user_capture_hook_is_installed_at(config_dir: &Path, event: &str) -> bool {
    let Ok(config) = load_object(&config_dir.join("hooks.json")) else {
        return false;
    };
    persistent_specs()
        .iter()
        .find(|(name, _)| name == event)
        .is_some_and(|(event, command)| capture_entry_is_registered(&config, event, command))
}

fn capture_entry_is_registered(config: &Map<String, Value>, event: &str, command: &str) -> bool {
    config
        .get("hooks")
        .and_then(|hooks| hooks.get(event))
        .and_then(Value::as_array)
        .is_some_and(|entries| {
            entries
                .iter()
                .any(|entry| entry.get("command").and_then(Value::as_str) == Some(command))
        })
}

fn powershell_encoded_command(script: &str) -> String {
    use base64::Engine;
    let utf16 = script
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    format!(
        "powershell.exe -NoProfile -EncodedCommand {}",
        base64::engine::general_purpose::STANDARD.encode(utf16)
    )
}

/// The process that registered a managed entry, from its marker.
fn managed_process_identity(command: &str) -> Option<ProcessIdentity> {
    let decoded = command
        .strip_prefix("powershell.exe -NoProfile -EncodedCommand ")
        .and_then(|encoded| {
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .ok()?;
            let (chunks, remainder) = bytes.as_chunks::<2>();
            if !remainder.is_empty() {
                return None;
            }
            let words = chunks
                .iter()
                .map(|pair| u16::from_le_bytes(*pair))
                .collect::<Vec<_>>();
            String::from_utf16(&words).ok()
        });
    let command = decoded.as_deref().unwrap_or(command);
    let marker = command.split(MANAGED_MARKER_PREFIX).nth(1)?;
    let mut parts = marker.splitn(3, '-');
    Some(ProcessIdentity {
        pid: parts.next()?.parse().ok()?,
        start_time_secs: parts.next()?.parse().ok()?,
    })
}

fn remove_stale_managed_hooks(
    config: &mut Map<String, Value>,
    mut process_is_alive: impl FnMut(&ProcessIdentity) -> bool,
) {
    let Some(hooks) = config.get_mut("hooks").and_then(Value::as_object_mut) else {
        return;
    };
    for entries in hooks.values_mut().filter_map(Value::as_array_mut) {
        entries.retain(|entry| {
            managed_process_identity(
                entry
                    .get("command")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )
            .is_none_or(|identity| process_is_alive(&identity))
        });
    }
}

fn hook_entry(command: &str) -> Value {
    serde_json::json!({
        "command": command,
        "timeout": 10,
        "failClosed": false
    })
}

/// Add (`enable`) or remove the entries for `specs`, preserving every other
/// hook. Adding replaces an existing entry with the same command.
fn apply_specs(
    config: &mut Map<String, Value>,
    path: &Path,
    specs: &[(String, String)],
    enable: bool,
) -> anyhow::Result<()> {
    if enable {
        config
            .entry("version")
            .or_insert_with(|| Value::Number(1.into()));
    }

    if enable || config.contains_key("hooks") {
        let hooks = config
            .entry("hooks")
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .ok_or_else(|| {
                anyhow::anyhow!("Cursor hooks must be a JSON object: {}", path.display())
            })?;

        let persistent = specs == persistent_specs();
        if persistent {
            for (event, command) in self::specs(PERSISTENT_MARKER, cfg!(windows)) {
                if let Some(entries) = hooks.get_mut(&event).and_then(Value::as_array_mut) {
                    entries.retain(|entry| {
                        entry.get("command").and_then(Value::as_str) != Some(command.as_str())
                    });
                }
            }
        }

        for (event, command) in specs {
            let Some(entries) = hooks.get_mut(event) else {
                if enable {
                    hooks.insert(event.clone(), Value::Array(vec![hook_entry(command)]));
                }
                continue;
            };
            let entries = entries.as_array_mut().ok_or_else(|| {
                anyhow::anyhow!(
                    "Cursor hook event `{event}` must be an array: {}",
                    path.display()
                )
            })?;
            entries.retain(|entry| {
                entry.get("command").and_then(Value::as_str) != Some(command.as_str())
            });
            if enable {
                entries.push(hook_entry(command));
            }
        }
    }

    Ok(())
}

/// Check that `apply` would succeed, without writing.
pub(super) fn validate(
    config_dir: &Path,
    specs: &[(String, String)],
    enable: bool,
) -> anyhow::Result<()> {
    let path = config_dir.join("hooks.json");
    let mut config = load_object(&path)?;
    apply_specs(&mut config, &path, specs, enable)
}

pub(super) fn apply(
    config_dir: &Path,
    specs: &[(String, String)],
    enable: bool,
) -> anyhow::Result<()> {
    let path = config_dir.join("hooks.json");
    edit_object(&path, FileAccess::Inherited, |config, _| {
        apply_specs(config, &path, specs, enable)
    })
}

/// Whether every persistent capture entry is registered.
pub(crate) fn capture_hooks_are_installed_at(config_dir: &Path) -> bool {
    let path = config_dir.join("hooks.json");
    let Ok(config) = load_object(&path) else {
        return false;
    };
    persistent_specs()
        .iter()
        .all(|(event, command)| capture_entry_is_registered(&config, event, command))
}

/// Discovery entries registered for the lifetime of one managed run and
/// removed when dropped.
pub(crate) struct CursorManagedHooks {
    config_dir: PathBuf,
    specs: Vec<(String, String)>,
}

impl CursorManagedHooks {
    pub(crate) fn install(config_dir: &Path, run_id: &str) -> anyhow::Result<Self> {
        let context = crate::process::capture_process_context(std::process::id());
        let identity = context
            .process_chain
            .first()
            .filter(|identity| identity.start_time_secs > 0);
        let identity = identity.ok_or_else(|| {
            anyhow::anyhow!("cannot safely install invocation-local Cursor hooks: process identity is unavailable")
        })?;
        let marker = format!(
            "{MANAGED_MARKER_PREFIX}{}-{}-{run_id}",
            identity.pid, identity.start_time_secs
        );
        let specs = specs(&marker, cfg!(windows));
        let path = config_dir.join("hooks.json");
        edit_object(&path, FileAccess::Inherited, |config, _| {
            remove_stale_managed_hooks(config, crate::process::process_is_alive);
            apply_specs(config, &path, &specs, true)
        })?;
        Ok(Self {
            config_dir: config_dir.to_path_buf(),
            specs,
        })
    }
}

impl Drop for CursorManagedHooks {
    fn drop(&mut self) {
        if let Err(error) = apply(&self.config_dir, &self.specs, false) {
            tracing::warn!(%error, "failed to remove managed Cursor hook discovery entries");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup::common::write_object_atomic_with;

    #[test]
    fn capture_setup_migrates_discovery_preserves_neighbors_and_disables_cleanly() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".cursor");
        let legacy = specs(PERSISTENT_MARKER, cfg!(windows));
        apply(&config_dir, &legacy, true).unwrap();
        let path = config_dir.join("hooks.json");
        let mut config = load_object(&path).unwrap();
        config["hooks"]["stop"]
            .as_array_mut()
            .unwrap()
            .push(hook_entry("other-hook"));
        write_object_atomic_with(&path, config, FileAccess::Inherited).unwrap();

        let capture = persistent_specs();
        apply(&config_dir, capture, true).unwrap();
        apply(&config_dir, capture, true).unwrap();
        assert!(capture_hooks_are_installed_at(&config_dir));
        for (event, _) in capture {
            assert!(user_capture_hook_is_installed_at(&config_dir, event));
        }
        assert!(!user_capture_hook_is_installed_at(&config_dir, "unknown"));
        let installed = load_object(&path).unwrap();
        assert_eq!(installed["hooks"]["stop"].as_array().unwrap().len(), 2);
        for (event, command) in legacy {
            assert!(installed["hooks"][event]
                .as_array()
                .unwrap()
                .iter()
                .all(|entry| entry["command"] != command));
        }
        apply(&config_dir, capture, false).unwrap();
        assert!(!capture_hooks_are_installed_at(&config_dir));
        let disabled = load_object(&path).unwrap();
        assert_eq!(
            disabled["hooks"]["stop"],
            serde_json::json!([hook_entry("other-hook")])
        );
    }

    #[cfg(unix)]
    #[test]
    fn user_capture_hooks_forward_payload_and_fail_open_on_cli_errors() {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let bt = temp.path().join("bt");
        // Simulate a CLI parse failure that would otherwise block Cursor.
        std::fs::write(&bt, "#!/bin/sh\ncat > \"$CAPTURE_PAYLOAD\"\nprintf '%s\\n' \"$BT_TRACE_CURSOR_USER_HOOK $*\" > \"$CAPTURE_ARGS\"\nprintf 'CLI help'\nexit 2\n").unwrap();
        std::fs::set_permissions(&bt, std::fs::Permissions::from_mode(0o755)).unwrap();
        let payload = br#"{"conversation_id":"session","hook_event_name":"stop"}"#;
        for (event, command) in capture_specs(false) {
            let mut child = crate::subprocess::interactive_command("/bin/sh")
                .args(["-c", &command])
                .env("PATH", format!("{}:/usr/bin:/bin", temp.path().display()))
                .env("CAPTURE_PAYLOAD", temp.path().join("payload"))
                .env("CAPTURE_ARGS", temp.path().join("args"))
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(payload).unwrap();
            let result = child.wait_with_output().unwrap();
            assert!(result.status.success());
            let response = if event == "beforeSubmitPrompt" {
                "{\"continue\":true}\n"
            } else {
                "{}\n"
            };
            assert_eq!(result.stdout, response.as_bytes());
            assert!(result.stderr.is_empty());
            assert_eq!(std::fs::read(temp.path().join("payload")).unwrap(), payload);
            let args = std::fs::read_to_string(temp.path().join("args")).unwrap();
            assert!(args.starts_with("1 trace hook --source cursor "));
            assert!(args.contains(&format!("--event {event}")));
        }
    }

    #[test]
    fn capture_commands_cover_all_events_on_both_platforms() {
        use base64::Engine;
        let manifest: Value = serde_json::from_str(super::super::HOOKS_MANIFEST).unwrap();
        for windows in [false, true] {
            let specs = capture_specs(windows);
            assert_eq!(specs.len(), manifest["hooks"].as_object().unwrap().len());
            for (event, command) in specs {
                let script = if windows {
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(
                            command
                                .strip_prefix("powershell.exe -NoProfile -EncodedCommand ")
                                .unwrap(),
                        )
                        .unwrap();
                    let words = bytes
                        .as_chunks::<2>()
                        .0
                        .iter()
                        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                        .collect::<Vec<_>>();
                    String::from_utf16(&words).unwrap()
                } else {
                    command
                };
                assert!(script.contains(&format!("--event {event}")));
                assert!(script.contains(USER_HOOK_ENV));
                assert!(script.contains(CAPTURE_MARKER));
                assert!(script.contains(if event == "beforeSubmitPrompt" {
                    r#"{"continue":true}"#
                } else {
                    "{}"
                }));
            }
        }
    }

    #[test]
    fn capture_health_requires_each_event() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".cursor");
        apply(&config_dir, persistent_specs(), true).unwrap();
        assert!(capture_hooks_are_installed_at(&config_dir));

        let hooks_path = config_dir.join("hooks.json");
        let mut config = load_object(&hooks_path).unwrap();
        config["hooks"].as_object_mut().unwrap().remove("stop");
        write_object_atomic_with(&hooks_path, config, FileAccess::Inherited).unwrap();

        assert!(!capture_hooks_are_installed_at(&config_dir));
    }

    #[test]
    fn discovery_hooks_generate_shell_independent_windows_commands() {
        use base64::Engine;
        let specs = specs("braintrust-cursor-managed-1234-42-run", true);
        assert_eq!(specs.len(), 3);
        for (event, command) in &specs {
            let encoded = command
                .strip_prefix("powershell.exe -NoProfile -EncodedCommand ")
                .unwrap();
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap();
            let (chunks, remainder) = bytes.as_chunks::<2>();
            assert!(remainder.is_empty());
            let words = chunks
                .iter()
                .map(|pair| u16::from_le_bytes(*pair))
                .collect::<Vec<_>>();
            let script = String::from_utf16(&words).unwrap();
            assert!(script.contains("braintrust-cursor-managed-1234-42-run"));
            assert_eq!(
                managed_process_identity(command),
                Some(ProcessIdentity {
                    pid: 1234,
                    start_time_secs: 42,
                })
            );
            if event == "beforeSubmitPrompt" {
                assert!(script.contains(r#"'{"continue":true}'"#));
            } else {
                assert!(script.contains("'{}'"));
            }
        }
    }

    #[test]
    fn managed_setup_removes_only_hooks_from_exited_runs() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".cursor");
        std::fs::create_dir_all(&config_dir).unwrap();
        let context = crate::process::capture_process_context(std::process::id());
        let current = context.process_chain.first().unwrap();
        let stale = specs("braintrust-cursor-managed-4294967295-1-stale", false);
        let active = specs(
            &format!(
                "braintrust-cursor-managed-{}-{}-active",
                current.pid, current.start_time_secs
            ),
            false,
        );
        let mut config = Map::new();
        config.insert("version".into(), serde_json::json!(1));
        let mut hooks = Map::new();
        for ((stale_event, stale_command), (active_event, active_command)) in
            stale.into_iter().zip(active)
        {
            assert_eq!(stale_event, active_event);
            hooks.insert(
                stale_event,
                serde_json::json!([hook_entry(&stale_command), hook_entry(&active_command)]),
            );
        }
        config.insert("hooks".into(), Value::Object(hooks));
        write_object_atomic_with(
            &config_dir.join("hooks.json"),
            config,
            FileAccess::Inherited,
        )
        .unwrap();

        let _managed = CursorManagedHooks::install(&config_dir, "new-run").unwrap();
        let config = load_object(&config_dir.join("hooks.json")).unwrap();

        for event in ["beforeSubmitPrompt", "afterAgentResponse", "stop"] {
            let commands = config["hooks"][event].as_array().unwrap();
            assert_eq!(commands.len(), 2);
            assert!(commands
                .iter()
                .any(|entry| entry["command"].as_str().unwrap().contains("-active")));
            assert!(commands
                .iter()
                .all(|entry| !entry["command"].as_str().unwrap().contains("-stale")));
        }
    }

    #[test]
    fn removing_hooks_does_not_create_a_missing_config_directory() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.path().join(".cursor");

        apply(&config_dir, persistent_specs(), false).unwrap();

        assert!(!config_dir.exists());
    }
}
