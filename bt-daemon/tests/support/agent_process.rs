use crate::support::ingest::{IngestMock, IngestScenario};
use crate::support::server::TestServer;
use bt_daemon::{run_status, StatusArgs};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tempfile::TempDir;
use tokio::process::{Child, Command};
#[cfg(windows)]
use uuid::Uuid;

const INFERENCE_MODE_ENV: &str = "BT_AGENT_INFERENCE_MODE";
const INGEST_MODE_ENV: &str = "BT_AGENT_INGEST_MODE";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TestBackendMode {
    Mock,
    Live,
}

impl TestBackendMode {
    fn from_env(name: &str) -> Self {
        match std::env::var(name).as_deref() {
            Ok("live") => Self::Live,
            Ok("mock" | "deterministic") | Err(std::env::VarError::NotPresent) => Self::Mock,
            Ok(value) => panic!("{name} must be `mock` or `live`, got {value:?}"),
            Err(error) => panic!("could not read {name}: {error}"),
        }
    }
}

pub struct AgentTestWorld {
    inference_mode: TestBackendMode,
    ingest_mode: TestBackendMode,
    root: TempDir,
    collector: IngestMock,
    collector_server: TestServer,
    daemon: Child,
    wrapper_dir: PathBuf,
    socket: PathBuf,
    data_dir: PathBuf,
    config_path: PathBuf,
}

impl AgentTestWorld {
    pub async fn start() -> Self {
        let inference_mode = TestBackendMode::from_env(INFERENCE_MODE_ENV);
        let ingest_mode = TestBackendMode::from_env(INGEST_MODE_ENV);
        let root = tempfile::tempdir().expect("create agent test root");
        let collector = IngestMock::new();
        let collector_server = TestServer::start(collector.router()).await;
        let wrapper_dir = root.path().join("bin");
        let data_dir = root.path().join("daemon");
        let socket = test_endpoint(root.path());
        let config_path = data_dir.join("config.json");
        std::fs::create_dir_all(&wrapper_dir).expect("create wrapper directory");
        std::fs::create_dir_all(&data_dir).expect("create daemon data directory");
        std::fs::write(
            &config_path,
            serde_json::to_vec_pretty(&json!({
                "traceToBraintrust": true,
                "project": "agent-e2e",
                "flushOnTurnEnd": true,
                "additionalMetadata": {"test_harness": true}
            }))
            .unwrap(),
        )
        .expect("write daemon config");

        let daemon_binary = Path::new(env!("CARGO_BIN_EXE_bt-daemon"));
        write_bt_wrapper(&wrapper_dir, daemon_binary);

        let mut command = Command::new(daemon_binary);
        command
            .arg("serve")
            .arg("--socket")
            .arg(&socket)
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--idle-timeout-secs")
            .arg("0")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if ingest_mode == TestBackendMode::Mock {
            command
                .env("BRAINTRUST_API_URL", collector_server.uri())
                .env("BRAINTRUST_APP_URL", collector_server.uri());
        }
        let daemon = command.spawn().expect("start daemon");

        wait_for_daemon(daemon_binary, &socket).await;
        Self {
            inference_mode,
            ingest_mode,
            root,
            collector,
            collector_server,
            daemon,
            wrapper_dir,
            socket,
            data_dir,
            config_path,
        }
    }

    pub fn uses_mock_inference(&self) -> bool {
        self.inference_mode == TestBackendMode::Mock
    }

    pub fn uses_live_inference(&self) -> bool {
        self.inference_mode == TestBackendMode::Live
    }

    pub fn uses_mock_ingest(&self) -> bool {
        self.ingest_mode == TestBackendMode::Mock
    }

    pub fn workspace(&self) -> PathBuf {
        let workspace = self.root.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("create agent workspace");
        workspace
    }

    pub fn temp_path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    pub fn configure(&self, command: &mut Command) {
        let path = std::env::var_os("PATH").unwrap_or_default();
        let mut entries = vec![self.wrapper_dir.clone()];
        entries.extend(std::env::split_paths(&path));
        let combined = std::env::join_paths(entries).expect("construct test PATH");
        command
            .env("PATH", combined)
            .env("BT_DAEMON_SOCKET", &self.socket)
            .env("BT_DAEMON_DATA_DIR", &self.data_dir)
            .env("BT_DAEMON_CONFIG", &self.config_path)
            .env("BRAINTRUST_FLUSH_ON_TURN_END", "true")
            .stdin(Stdio::null());
        if self.uses_mock_ingest() {
            command
                .env("BRAINTRUST_API_KEY", "test-key")
                .env("BRAINTRUST_API_URL", self.collector_server.uri())
                .env("BRAINTRUST_APP_URL", self.collector_server.uri())
                .env("BRAINTRUST_PROJECT", "agent-e2e");
        }
    }

    pub async fn output(&self, command: &mut Command) -> std::process::Output {
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let child = command.spawn().expect("spawn agent command");
        tokio::time::timeout(Duration::from_secs(90), child.wait_with_output())
            .await
            .expect("agent command timed out")
            .expect("wait for agent command")
    }

    pub async fn wait_for_trace_rows(&self) -> Vec<Value> {
        self.wait_for_trace_rows_matching(|rows| !rows.is_empty())
            .await
    }

    pub async fn wait_for_trace_rows_matching(
        &self,
        predicate: impl Fn(&[Value]) -> bool,
    ) -> Vec<Value> {
        for _ in 0..100 {
            let rows = self.collector.rows();
            if predicate(&rows) {
                return rows;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "daemon delivered no trace rows; {}; daemon files:\n{}",
            self.collector.diagnostics(),
            directory_contents(&self.data_dir)
        );
    }

    /// Wait for the stable trace-delivery invariant in every backend mode.
    /// Mock ingest returns captured rows for ordinary assertions; live ingest
    /// verifies daemon emission and sink health.
    pub async fn wait_for_trace_delivery(&self) -> Vec<Value> {
        if self.uses_mock_ingest() {
            self.wait_for_trace_rows().await
        } else {
            self.wait_for_live_ingest().await
        }
    }

    pub async fn wait_for_mock_ingest_scenario(&self, scenario: &IngestScenario) -> Vec<Value> {
        assert!(
            self.uses_mock_ingest(),
            "ingest scenarios require mock ingest"
        );
        let mut last_error = String::new();
        for _ in 0..100 {
            match self.collector.evaluate(scenario) {
                Ok(rows) => return rows,
                Err(error) => last_error = error,
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "ingest scenario did not complete: {last_error}; {}; daemon files:\n{}",
            self.collector.diagnostics(),
            directory_contents(&self.data_dir)
        );
    }

    async fn wait_for_live_ingest(&self) -> Vec<Value> {
        let mut last_status = String::new();
        for _ in 0..100 {
            match run_status(StatusArgs {
                socket: Some(self.socket.clone()),
                session_id: None,
            })
            .await
            {
                Ok(Some(status)) => {
                    last_status = format!("{:?}", status.sessions);
                    let emitted = status
                        .sessions
                        .iter()
                        .any(|session| session.spans_emitted > 0);
                    let errors = status
                        .sessions
                        .iter()
                        .filter_map(|session| session.last_error.as_deref())
                        .collect::<Vec<_>>();
                    assert!(
                        errors.is_empty(),
                        "live ingest reported daemon sink errors: {errors:?}"
                    );
                    if emitted {
                        return Vec::new();
                    }
                }
                Ok(None) => last_status = "daemon not running".into(),
                Err(error) => last_status = error.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("live ingest emitted no spans; last daemon status: {last_status}");
    }
}

impl Drop for AgentTestWorld {
    fn drop(&mut self) {
        let _ = self.daemon.start_kill();
    }
}

#[cfg(unix)]
fn write_bt_wrapper(directory: &Path, daemon_binary: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let path = directory.join("bt");
    let script = format!(
        "#!/bin/sh\ncase \"$1\" in trace) shift;; esac\nexec '{}' \"$@\"\n",
        daemon_binary.display()
    );
    std::fs::write(&path, script).expect("write bt test wrapper");
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).expect("make bt wrapper executable");
}

#[cfg(windows)]
fn write_bt_wrapper(directory: &Path, daemon_binary: &Path) {
    let powershell = directory.join("bt-wrapper.ps1");
    let script = format!(
        "$forward = @($args)\n\
         if ($forward.Count -gt 0 -and $forward[0] -eq 'trace') {{\n\
           if ($forward.Count -eq 1) {{ $forward = @() }} else {{ $forward = @($forward[1..($forward.Count - 1)]) }}\n\
         }}\n\
         & '{}' @forward\n\
         exit $LASTEXITCODE\n",
        daemon_binary.display()
    );
    std::fs::write(&powershell, script).expect("write bt PowerShell wrapper");
    std::fs::write(
        directory.join("bt.cmd"),
        "@echo off\r\npowershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File \"%~dp0bt-wrapper.ps1\" %*\r\n",
    )
    .expect("write bt command wrapper");

    // Claude Code, and some Codex releases, launch the portable `command`
    // hook through Git Bash even on Windows. Git Bash does not resolve
    // PATHEXT, so expose an extensionless shim in addition to bt.cmd.
    let shell_binary = daemon_binary.to_string_lossy().replace('\\', "/");
    let shell = format!(
        "#!/bin/sh\ncase \"$1\" in trace) shift;; esac\nexec '{}' \"$@\"\n",
        shell_binary
    );
    std::fs::write(directory.join("bt"), shell).expect("write bt Git Bash wrapper");
}

#[cfg(unix)]
fn test_endpoint(root: &Path) -> PathBuf {
    root.join("daemon.sock")
}

#[cfg(windows)]
fn test_endpoint(_root: &Path) -> PathBuf {
    PathBuf::from(format!(
        r"\\.\pipe\braintrust-bt-daemon-test-{}",
        Uuid::new_v4()
    ))
}

async fn wait_for_daemon(daemon_binary: &Path, endpoint: &Path) {
    for _ in 0..100 {
        let output = Command::new(daemon_binary)
            .arg("status")
            .arg("--socket")
            .arg(endpoint)
            .output()
            .await;
        if let Ok(output) = output {
            if output.status.success()
                && !String::from_utf8_lossy(&output.stdout).contains("not running")
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("daemon endpoint was not ready at {}", endpoint.display());
}

fn directory_contents(root: &Path) -> String {
    fn visit(path: &Path, output: &mut String) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                visit(&path, output);
            } else {
                let body = std::fs::read_to_string(&path).unwrap_or_else(|_| "<binary>".into());
                output.push_str(&format!("{}:\n{}\n", path.display(), body));
            }
        }
    }
    let mut output = String::new();
    visit(root, &mut output);
    output
}
