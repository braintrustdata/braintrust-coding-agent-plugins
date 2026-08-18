//! Socket and data-directory resolution. Both `serve` and `hook` must agree on
//! the defaults, so the logic lives here. See `docs/protocol.md`.

use crate::{AgentId, AgentSettingsLocation};
use std::path::{Path, PathBuf};

/// Env override for the socket path (also settable via `--socket`).
pub const SOCKET_ENV: &str = "BT_DAEMON_SOCKET";
/// Env override for the data/journal directory.
pub const DATA_DIR_ENV: &str = "BT_DAEMON_DATA_DIR";
/// Env override for the current agent's non-credential tracing settings file.
pub const SETTINGS_ENV: &str = "BT_DAEMON_CONFIG";

fn home() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Resolve the socket path: explicit `override` → `$BT_DAEMON_SOCKET` →
/// `$XDG_RUNTIME_DIR/braintrust/daemon.sock` → `~/.braintrust/run/daemon.sock`.
pub fn socket_path(explicit: Option<&Path>) -> PathBuf {
    if let Some(p) = explicit {
        return p.to_path_buf();
    }
    if let Some(p) = std::env::var_os(SOCKET_ENV) {
        return PathBuf::from(p);
    }
    #[cfg(windows)]
    {
        use sha2::{Digest, Sha256};
        let identity = format!(
            "{}\\{}",
            std::env::var("USERDOMAIN").unwrap_or_default(),
            std::env::var("USERNAME").unwrap_or_default()
        );
        let digest = Sha256::digest(identity.as_bytes());
        let suffix: String = digest[..8]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        PathBuf::from(format!(r"\\.\pipe\braintrust-bt-daemon-{suffix}"))
    }
    #[cfg(unix)]
    if let Some(rt) = std::env::var_os("XDG_RUNTIME_DIR") {
        if !rt.is_empty() {
            return PathBuf::from(rt).join("braintrust").join("daemon.sock");
        }
    }
    #[cfg(unix)]
    {
        home().join(".braintrust").join("run").join("daemon.sock")
    }
}

/// Resolve the data dir: explicit `override` → `$BT_DAEMON_DATA_DIR` →
/// `$XDG_STATE_HOME/braintrust/bt-daemon` → `~/.braintrust/state/bt-daemon`.
pub fn data_dir(explicit: Option<&Path>) -> PathBuf {
    if let Some(p) = explicit {
        return p.to_path_buf();
    }
    if let Some(p) = std::env::var_os(DATA_DIR_ENV) {
        return PathBuf::from(p);
    }
    #[cfg(windows)]
    if let Some(local) = std::env::var_os("LOCALAPPDATA") {
        if !local.is_empty() {
            return PathBuf::from(local).join("Braintrust").join("bt-daemon");
        }
    }
    #[cfg(unix)]
    if let Some(s) = std::env::var_os("XDG_STATE_HOME") {
        if !s.is_empty() {
            return PathBuf::from(s).join("braintrust").join("bt-daemon");
        }
    }
    home().join(".braintrust").join("state").join("bt-daemon")
}

/// Resolve one coding agent's persistent tracing settings file.
///
/// An explicit override and `$BT_DAEMON_CONFIG` remain useful for isolated
/// tests and managed environments. Normal setup keeps every agent independent
/// by writing into that agent's native configuration directory.
pub fn agent_settings_path(source: &str, explicit: Option<&Path>) -> PathBuf {
    if let Some(path) = explicit {
        return path.to_path_buf();
    }
    if let Some(path) = std::env::var_os(SETTINGS_ENV) {
        return PathBuf::from(path);
    }
    match AgentId::parse(source).map(|agent| agent.spec().settings_location) {
        Some(AgentSettingsLocation::Codex) => home().join(".codex").join("braintrust.json"),
        Some(AgentSettingsLocation::Claude) => home().join(".claude").join("braintrust.json"),
        Some(AgentSettingsLocation::OpenCode) => std::env::var_os("XDG_CONFIG_HOME")
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| home().join(".config"))
            .join("opencode")
            .join("braintrust.json"),
        Some(AgentSettingsLocation::Pi) => home().join(".pi").join("agent").join("braintrust.json"),
        None => data_dir(None).join("agents").join(format!("{source}.json")),
    }
}

/// Create `dir` (and parents) mode 0700 on unix.
pub fn ensure_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o700);
        std::fs::set_permissions(dir, perms)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_paths_take_precedence() {
        let socket = Path::new("custom-endpoint");
        let data = Path::new("custom-data");
        assert_eq!(socket_path(Some(socket)), socket);
        assert_eq!(data_dir(Some(data)), data);
        assert_eq!(
            agent_settings_path("codex", Some(Path::new("config.json"))),
            Path::new("config.json")
        );
    }

    #[test]
    fn aliases_share_their_canonical_settings_location() {
        assert_eq!(
            agent_settings_path("claude", None),
            agent_settings_path("claude-code", None)
        );
        assert_eq!(
            agent_settings_path("open-code", None),
            agent_settings_path("opencode", None)
        );
    }

    #[test]
    fn private_directory_is_created() {
        let temp = tempfile::tempdir().unwrap();
        let nested = temp.path().join("one/two");
        ensure_private_dir(&nested).unwrap();
        assert!(nested.is_dir());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&nested).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
    }
}
