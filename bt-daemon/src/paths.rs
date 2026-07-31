//! Socket and data-directory resolution. Both `serve` and `hook` must agree on
//! the defaults, so the logic lives here. See `docs/protocol.md`.

use std::path::{Path, PathBuf};

/// Env override for the socket path (also settable via `--socket`).
pub const SOCKET_ENV: &str = "BT_DAEMON_SOCKET";
/// Env override for the data/journal directory.
pub const DATA_DIR_ENV: &str = "BT_DAEMON_DATA_DIR";
/// Env override for the shared non-credential daemon settings file.
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

/// Resolve the shared settings file: explicit override → `$BT_DAEMON_CONFIG`
/// → `<data_dir>/config.json`.
pub fn settings_path(explicit: Option<&Path>) -> PathBuf {
    if let Some(path) = explicit {
        return path.to_path_buf();
    }
    std::env::var_os(SETTINGS_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| data_dir(None).join("config.json"))
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
            settings_path(Some(Path::new("config.json"))),
            Path::new("config.json")
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
