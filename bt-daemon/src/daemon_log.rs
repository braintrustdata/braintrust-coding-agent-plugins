//! The daemon's logging. Everything it logs goes through one `tracing`
//! subscriber with two outputs:
//!
//! - `daemon.log` in the data directory: newline-delimited JSON at `debug`
//!   (`BT_TRACE_LOG_LEVEL`), for diagnosis after the fact.
//! - standard error: plain text at `warn`, or `debug` with `--verbose`
//!   (`BT_TRACE_STDERR_LEVEL`). A spawned daemon's standard streams go to
//!   `serve.log`, which therefore only holds warnings and anything printed
//!   before logging starts.
//!
//! Both files are rotated by `file-rotate` once they pass
//! `BT_TRACE_LOG_MAX_MB` MiB (default [`DEFAULT_MAX_MB`]), keeping one earlier
//! generation as `<name>.1`. `daemon.log` rotates as it is written; `serve.log`
//! rotates before a daemon is spawned, since the daemon writes it through an
//! inherited handle.
//!
//! The global subscriber can be set once per process, so a host that installs
//! its own must not do so before running the daemon command.

use file_rotate::{compression::Compression, suffix::AppendCount, ContentLimit, FileRotate};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::Mutex;
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt, EnvFilter, Layer};

/// Rotate once a log would grow past this many MiB. One earlier generation of
/// the same size is kept.
pub(crate) const DEFAULT_MAX_MB: u64 = 10;
pub(crate) const MAX_MB_ENV: &str = "BT_TRACE_LOG_MAX_MB";
/// Filter for `daemon.log`, in `RUST_LOG` syntax.
pub(crate) const LEVEL_ENV: &str = "BT_TRACE_LOG_LEVEL";
/// Filter for standard error, in `RUST_LOG` syntax.
pub(crate) const STDERR_LEVEL_ENV: &str = "BT_TRACE_STDERR_LEVEL";

/// The configured size limit in bytes. An unset, unparsable or zero value
/// uses the default, so logs stay bounded.
pub(crate) fn max_bytes() -> u64 {
    limit(crate::env::var(MAX_MB_ENV).as_deref())
}

fn limit(mb: Option<&str>) -> u64 {
    mb.and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|mb| *mb > 0)
        .unwrap_or(DEFAULT_MAX_MB)
        .saturating_mul(1024 * 1024)
}

/// Install the daemon's subscriber. Returns `false` if the process already
/// has one, which then keeps receiving the daemon's events.
pub(crate) fn init(data_dir: &Path, verbose: bool) -> bool {
    let (subscriber, errors) = subscriber(
        Mutex::new(rotating(&data_dir.join("daemon.log"))),
        std::io::stderr,
        // A spawned daemon's stderr is `serve.log`, which must stay plain.
        std::io::IsTerminal::is_terminal(&std::io::stderr()),
        verbose,
    );
    if tracing::subscriber::set_global_default(subscriber).is_err() {
        return false;
    }
    for error in errors {
        tracing::warn!("{error}");
    }
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        tracing::error!(panic = %info, "daemon panicked");
        previous(info);
    }));
    true
}

/// The daemon's subscriber, and any filter settings it had to reject.
fn subscriber<F, E>(
    file: F,
    stderr: E,
    ansi: bool,
    verbose: bool,
) -> (impl tracing::Subscriber + Send + Sync, Vec<String>)
where
    F: for<'a> MakeWriter<'a> + Send + Sync + 'static,
    E: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let (file_filter, file_error) = filter(LEVEL_ENV, "debug");
    let (stderr_filter, stderr_error) =
        filter(STDERR_LEVEL_ENV, if verbose { "debug" } else { "warn" });
    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(file)
                .with_filter(file_filter),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(ansi)
                .with_writer(stderr)
                .with_filter(stderr_filter),
        );
    (
        subscriber,
        [file_error, stderr_error].into_iter().flatten().collect(),
    )
}

/// The filter named by `env`, or `default` with the reason it was rejected.
fn filter(env: &str, default: &str) -> (EnvFilter, Option<String>) {
    parse_filter(env, crate::env::var(env), default)
}

fn parse_filter(env: &str, value: Option<String>, default: &str) -> (EnvFilter, Option<String>) {
    match value {
        Some(value) => match EnvFilter::try_new(&value) {
            Ok(filter) => (filter, None),
            Err(error) => (
                EnvFilter::new(default),
                Some(format!("ignoring {env}={value:?}: {error}")),
            ),
        },
        None => (EnvFilter::new(default), None),
    }
}

/// A log at `path` that rotates once it passes the configured size, keeping
/// one earlier generation as `<path>.1`.
fn rotating(path: &Path) -> FileRotate<AppendCount> {
    FileRotate::new(
        path,
        AppendCount::new(1),
        ContentLimit::BytesSurpassed(usize::try_from(max_bytes()).unwrap_or(usize::MAX)),
        Compression::None,
        None,
    )
}

/// Open `serve.log` for a new daemon's standard streams, rotating it first.
/// A daemon still exiting keeps writing into the rotated generation.
pub(crate) fn open_serve_log(data_dir: &Path) -> std::io::Result<File> {
    std::fs::create_dir_all(data_dir)?;
    let path = data_dir.join("serve.log");
    let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    if len > max_bytes() {
        if let Err(error) = rotating(&path).rotate() {
            tracing::warn!(%error, "serve.log rotation failed");
        }
    }
    OpenOptions::new().create(true).append(true).open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path).unwrap_or_default()
    }

    #[test]
    fn daemon_log_keeps_one_rotated_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.log");
        let mut log = rotating(&path);
        let line = vec![b'.'; 1024 * 1024];
        for _ in 0..(DEFAULT_MAX_MB * 3) {
            log.write_all(&line).unwrap();
        }
        log.flush().unwrap();
        let generations = std::fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(generations, 2, "the current log and one rotated generation");
        assert!(dir.path().join("daemon.log.1").exists());
        assert!(std::fs::metadata(&path).unwrap().len() <= (DEFAULT_MAX_MB + 1) * 1024 * 1024);
    }

    #[test]
    fn serve_log_rotates_before_a_daemon_is_spawned() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.log");
        std::fs::write(
            &path,
            vec![b'.'; (DEFAULT_MAX_MB * 1024 * 1024 + 1) as usize],
        )
        .unwrap();
        let mut log = open_serve_log(dir.path()).unwrap();
        log.write_all(b"new daemon\n").unwrap();
        assert_eq!(read(&path), "new daemon\n");
        assert_eq!(
            std::fs::metadata(dir.path().join("serve.log.1"))
                .unwrap()
                .len(),
            DEFAULT_MAX_MB * 1024 * 1024 + 1
        );
        // Within the limit, the log is appended to as before.
        drop(log);
        open_serve_log(dir.path())
            .unwrap()
            .write_all(b"again\n")
            .unwrap();
        assert_eq!(read(&path), "new daemon\nagain\n");
    }

    #[test]
    fn each_output_has_its_own_level_and_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.log");
        let stderr = Mutex::new(File::create(dir.path().join("stderr")).unwrap());
        let (subscriber, errors) = subscriber(Mutex::new(rotating(&path)), stderr, false, false);
        assert!(errors.is_empty());
        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(session_id = "s", "event received");
            tracing::warn!("delivery failed");
        });
        let lines: Vec<serde_json::Value> = read(&path)
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines.len(), 2, "the file records debug and above");
        assert_eq!(lines[0]["fields"]["message"], "event received");
        assert_eq!(lines[0]["fields"]["session_id"], "s");
        let stderr = read(&dir.path().join("stderr"));
        assert!(stderr.contains("delivery failed"), "{stderr}");
        assert!(
            !stderr.contains("event received"),
            "stderr records warn and above"
        );
        assert!(serde_json::from_str::<serde_json::Value>(stderr.trim()).is_err());
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
    fn an_unusable_filter_falls_back_to_its_default() {
        let (filter, error) = parse_filter(LEVEL_ENV, Some("bt_daemon=trace,warn".into()), "debug");
        assert_eq!(filter.to_string(), "bt_daemon=trace,warn");
        assert!(error.is_none());
        let (filter, error) = parse_filter(LEVEL_ENV, Some("bt_daemon=[".into()), "debug");
        assert_eq!(filter.to_string(), "debug");
        assert!(error.unwrap().starts_with("ignoring BT_TRACE_LOG_LEVEL"));
        let (filter, error) = parse_filter(LEVEL_ENV, None, "debug");
        assert_eq!(filter.to_string(), "debug");
        assert!(error.is_none());
    }
}
