//! `serve.log`, where a spawned daemon's standard streams are redirected.
//!
//! A daemon may run for days, so the log is bounded where it is written: once
//! it outgrows its limit its newest bytes are kept as `serve.log.1` and it
//! starts again. The limit is [`DEFAULT_MAX_MB`] MiB unless
//! `BRAINTRUST_DAEMON_LOG_MAX_MB` sets another whole number of MiB. The running daemon writes through an inherited handle that
//! always appends at the end of the file, so the file is truncated in place
//! rather than renamed away from that handle.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Rotate once the log grows past this many MiB. One earlier generation of
/// the same size is kept.
pub(crate) const DEFAULT_MAX_MB: u64 = 10;

const MAX_MB_ENV: &str = "BRAINTRUST_DAEMON_LOG_MAX_MB";

/// The configured limit in bytes. An unset, unparsable or zero value uses the
/// default, so the log stays bounded.
pub(crate) fn max_bytes() -> u64 {
    limit(crate::env::var(MAX_MB_ENV).as_deref())
}

fn limit(mb: Option<&str>) -> u64 {
    mb.and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|mb| *mb > 0)
        .unwrap_or(DEFAULT_MAX_MB)
        .saturating_mul(1024 * 1024)
}

fn path(data_dir: &Path) -> PathBuf {
    data_dir.join("serve.log")
}

/// Open the log for a new daemon's standard streams, rotating it first.
pub(crate) fn open(data_dir: &Path) -> std::io::Result<File> {
    if let Err(error) = rotate(data_dir, max_bytes()) {
        eprintln!("bt-daemon: serve.log rotation failed: {error}");
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path(data_dir))
}

/// Keep the newest `max_bytes` of an oversized log as `serve.log.1` and empty
/// the log.
pub(crate) fn rotate(data_dir: &Path, max_bytes: u64) -> std::io::Result<()> {
    let path = path(data_dir);
    let mut log = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(log) => log,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let len = log.metadata()?.len();
    if len <= max_bytes {
        return Ok(());
    }
    log.seek(SeekFrom::Start(len - max_bytes))?;
    std::io::copy(&mut log, &mut File::create(path.with_extension("log.1"))?)?;
    log.set_len(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const LIMIT: u64 = 1024;

    #[test]
    fn an_oversized_log_keeps_its_newest_bytes_and_starts_again() {
        let dir = tempfile::tempdir().unwrap();
        // The daemon's handle, opened before rotation.
        let mut daemon = open(dir.path()).unwrap();
        daemon.write_all(&vec![b'.'; LIMIT as usize]).unwrap();
        daemon.write_all(b"newest\n").unwrap();
        rotate(dir.path(), LIMIT).unwrap();

        let previous = std::fs::read(dir.path().join("serve.log.1")).unwrap();
        assert_eq!(previous.len() as u64, LIMIT);
        assert!(previous.ends_with(b"newest\n"));
        daemon.write_all(b"after\n").unwrap();
        assert_eq!(
            std::fs::read(dir.path().join("serve.log")).unwrap(),
            b"after\n",
            "the running daemon keeps appending to the emptied log"
        );
    }

    #[test]
    fn the_limit_is_a_positive_number_of_mib() {
        assert_eq!(limit(Some(" 50 ")), 50 * 1024 * 1024);
        for unusable in [None, Some(""), Some("0"), Some("-1"), Some("10MB")] {
            assert_eq!(
                limit(unusable),
                DEFAULT_MAX_MB * 1024 * 1024,
                "{unusable:?}"
            );
        }
    }

    #[test]
    fn a_log_within_its_bound_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        rotate(dir.path(), LIMIT).unwrap();
        open(dir.path()).unwrap().write_all(b"line\n").unwrap();
        rotate(dir.path(), LIMIT).unwrap();
        assert_eq!(
            std::fs::read(dir.path().join("serve.log")).unwrap(),
            b"line\n"
        );
        assert!(!dir.path().join("serve.log.1").exists());
    }
}
