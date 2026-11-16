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
//! Both files are bounded by rename-and-reopen rotation: once a file would
//! outgrow its limit (`BT_TRACE_LOG_MAX_MB` MiB, default [`DEFAULT_MAX_MB`]),
//! it is renamed to `<name>.1`, replacing the previous generation, and a fresh
//! file is started. Every line lands whole in one generation or the next.
//! `daemon.log` rotates as it is written; `serve.log` rotates before a daemon
//! is spawned, since the daemon writes it through an inherited handle.
//!
//! The global subscriber can be set once per process, so a host that installs
//! its own must not do so before running the daemon command.

use parking_lot::{Mutex, MutexGuard};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
        RotatingFile::new(data_dir.join("daemon.log"), max_bytes()),
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
fn subscriber<E>(
    file: RotatingFile,
    stderr: E,
    ansi: bool,
    verbose: bool,
) -> (impl tracing::Subscriber + Send + Sync, Vec<String>)
where
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

/// Open `serve.log` for a new daemon's standard streams, rotating it first.
/// No writer of this process holds it, so renaming it away is safe; a daemon
/// still exiting keeps writing into the rotated generation.
pub(crate) fn open_serve_log(data_dir: &Path) -> std::io::Result<File> {
    let path = data_dir.join("serve.log");
    let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    if len > max_bytes() {
        if let Err(error) = rotate(&path) {
            tracing::warn!(%error, "serve.log rotation failed");
        }
    }
    append(&path)
}

fn append(path: &Path) -> std::io::Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    OpenOptions::new().create(true).append(true).open(path)
}

/// Rename `path` to `<path>.1`, replacing the previous generation. A virus
/// scanner or indexer can briefly hold a file on Windows, so a failed rename
/// is retried a few times before giving up.
fn rotate(path: &Path) -> std::io::Result<()> {
    let mut rotated = path.as_os_str().to_owned();
    rotated.push(".1");
    let mut attempt = 0;
    loop {
        match std::fs::rename(path, &rotated) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) if attempt < 3 => {
                attempt += 1;
                std::thread::sleep(std::time::Duration::from_millis(10 * attempt));
            }
            Err(error) => return Err(error),
        }
    }
}

/// A log file that rotates by renaming itself once a write would take it past
/// its limit. `tracing` writes each event with one call, so rotation happens
/// between whole lines.
#[derive(Clone)]
pub(crate) struct RotatingFile(Arc<Mutex<State>>);

struct State {
    path: PathBuf,
    max_bytes: u64,
    file: Option<File>,
    written: u64,
}

impl RotatingFile {
    pub(crate) fn new(path: PathBuf, max_bytes: u64) -> Self {
        Self(Arc::new(Mutex::new(State {
            path,
            max_bytes,
            file: None,
            written: 0,
        })))
    }
}

impl State {
    fn file(&mut self) -> std::io::Result<&mut File> {
        let file = match self.file.take() {
            Some(file) => file,
            None => {
                let file = append(&self.path)?;
                self.written = file.metadata()?.len();
                file
            }
        };
        Ok(self.file.insert(file))
    }

    fn write_line(&mut self, line: &[u8]) -> std::io::Result<()> {
        self.file()?;
        if self.written > 0 && self.written + line.len() as u64 > self.max_bytes {
            self.file = None;
            // Keep writing to the current generation if it cannot move.
            let _ = rotate(&self.path);
        }
        self.file()?.write_all(line)?;
        self.written += line.len() as u64;
        Ok(())
    }
}

pub(crate) struct Writer<'a>(MutexGuard<'a, State>);

impl Write for Writer<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write_line(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self.0.file.as_mut() {
            Some(file) => file.flush(),
            None => Ok(()),
        }
    }
}

impl<'a> MakeWriter<'a> for RotatingFile {
    type Writer = Writer<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        Writer(self.0.lock())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(file: &RotatingFile, line: &str) {
        file.make_writer().write_all(line.as_bytes()).unwrap();
    }

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path).unwrap_or_default()
    }

    #[test]
    fn rotation_keeps_every_line_across_generations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.log");
        let rotated = dir.path().join("daemon.log.1");
        let file = RotatingFile::new(path.clone(), 30);
        let lines: Vec<String> = (0..6).map(|i| format!("line {i:02} ......\n")).collect();
        for line in &lines[..3] {
            write(&file, line);
        }
        // Two 15-byte lines fit in 30 bytes, so the third starts a new file.
        assert_eq!(read(&rotated), lines[..2].concat());
        assert_eq!(read(&path), lines[2]);
        for line in &lines[3..] {
            write(&file, line);
        }
        // A later rotation replaces the earlier generation; the two files
        // still read as one continuous log.
        assert_eq!(read(&rotated) + &read(&path), lines[2..].concat());
    }

    #[test]
    fn a_line_longer_than_the_limit_is_still_written() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.log");
        let file = RotatingFile::new(path.clone(), 4);
        write(&file, "longer than the limit\n");
        assert_eq!(read(&path), "longer than the limit\n");
    }

    #[test]
    fn writing_resumes_an_existing_log_and_counts_its_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.log");
        std::fs::write(&path, "earlier daemon\n").unwrap();
        let file = RotatingFile::new(path.clone(), 20);
        write(&file, "this daemon\n");
        assert_eq!(read(&dir.path().join("daemon.log.1")), "earlier daemon\n");
        assert_eq!(read(&path), "this daemon\n");
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
        let stderr = RotatingFile::new(dir.path().join("stderr"), u64::MAX);
        let (subscriber, errors) = subscriber(
            RotatingFile::new(path.clone(), u64::MAX),
            stderr,
            false,
            false,
        );
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
