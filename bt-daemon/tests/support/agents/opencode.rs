use super::{AgentOutput, ProcessOptions};
use crate::support::agent_process::AgentTestWorld;
use serde_json::json;
use std::ffi::OsString;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Child;

pub struct OpenCodeAgent {
    home: PathBuf,
    config_home: PathBuf,
    data_home: PathBuf,
    cache_home: PathBuf,
    plugin: PathBuf,
}

pub struct OpenCodeRun {
    prompt: OsString,
    mock_inference: Option<String>,
    options: ProcessOptions,
}

impl OpenCodeRun {
    pub fn new(prompt: impl Into<OsString>) -> Self {
        Self {
            prompt: prompt.into(),
            mock_inference: None,
            options: ProcessOptions::default(),
        }
    }

    pub fn mock_inference(mut self, base_url: impl Into<String>) -> Self {
        self.mock_inference = Some(base_url.into());
        self
    }

    pub fn arg(mut self, value: impl Into<OsString>) -> Self {
        self.options.arg(value);
        self
    }

    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.options.env(key, value);
        self
    }
}

impl OpenCodeAgent {
    pub fn new(world: &AgentTestWorld) -> Self {
        let home = world.temp_path("opencode-home");
        let config_home = world.temp_path("opencode-config");
        let data_home = world.temp_path("opencode-data");
        let cache_home = world.temp_path("opencode-cache");
        for directory in [&home, &config_home, &data_home, &cache_home] {
            std::fs::create_dir_all(directory).expect("create OpenCode test directory");
        }
        let plugin = std::env::var_os("OPENCODE_PLUGIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .expect("repository root")
                    .join("dist/opencode/dist/index.mjs")
            });
        assert!(
            plugin.is_file(),
            "install the packed OpenCode plugin and set OPENCODE_PLUGIN before running integration tests: {}",
            plugin.display()
        );
        Self {
            home,
            config_home,
            data_home,
            cache_home,
            plugin,
        }
    }

    pub async fn run(&self, world: &AgentTestWorld, run: OpenCodeRun) -> AgentOutput {
        let workspace = world.workspace();
        let mut config = json!({
            "$schema": "https://opencode.ai/config.json",
            "permission": {"*": "allow"}
        });
        if world.uses_mock_inference() {
            let base_url = run
                .mock_inference
                .as_ref()
                .expect("mock OpenCode runs require a mock inference endpoint");
            config["model"] = json!("mock/mock-model");
            config["provider"] = json!({
                "mock": {
                    "npm": "@ai-sdk/openai",
                    "name": "Harness Mock",
                    "options": {
                        "baseURL": format!("{base_url}/v1"),
                        "apiKey": "test-key"
                    },
                    "models": {"mock-model": {"name": "Mock Model"}}
                }
            });
        }
        std::fs::write(
            workspace.join("opencode.json"),
            serde_json::to_vec_pretty(&config).unwrap(),
        )
        .expect("write OpenCode config");

        // Keep the plugin-hosting process alive until OpenCode has published
        // the final message and session.idle events. The one-shot `run` server
        // tears itself down as soon as it has printed the final response, which
        // can drop those last plugin events before the daemon receives them.
        let port = available_port();
        let server_url = format!("http://127.0.0.1:{port}");
        let server_log = world.temp_path("opencode-server.log");
        let server_log_file =
            std::fs::File::create(&server_log).expect("create OpenCode server diagnostic log");
        let mut server = tokio::process::Command::new(env!("CARGO_BIN_EXE_bt-daemon"));
        server
            .args([
                "run",
                "--project",
                "agent-e2e",
                "opencode",
                "serve",
                "--hostname",
                "127.0.0.1",
                "--port",
                &port.to_string(),
            ])
            .current_dir(&workspace)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(server_log_file))
            .kill_on_drop(true);
        configure_environment(&mut server, self);
        server.env(
            "BT_TRACE_OPENCODE_PLUGIN_SPEC",
            path_to_file_url(&self.plugin),
        );
        world.configure(&mut server);
        run.options.apply_env(&mut server);
        let mut server = server.spawn().expect("start OpenCode server");
        if let Err(error) = wait_for_server(&server_url, &mut server, &server_log).await {
            let _ = server.kill().await;
            return AgentOutput::from_http_result(Err(error));
        }

        let output = run_session(&server_url, &workspace, &run.prompt)
            .await
            .map_err(|error| format!("{error}\n{}", server_diagnostics(&mut server, &server_log)));

        // The synchronous message endpoint returns once the final assistant
        // message is persisted. Give the server event bus a bounded window to
        // deliver completion and idle callbacks before stopping the plugin host.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = server.kill().await;
        AgentOutput::from_http_result(output)
    }
}

fn configure_environment(command: &mut tokio::process::Command, agent: &OpenCodeAgent) {
    command
        .env("HOME", &agent.home)
        .env("USERPROFILE", &agent.home)
        .env("XDG_CONFIG_HOME", &agent.config_home)
        .env("XDG_DATA_HOME", &agent.data_home)
        .env("XDG_CACHE_HOME", &agent.cache_home)
        .env("BRAINTRUST_OPENCODE_ENABLE_TOOLS", "false")
        .env("OPENCODE_DISABLE_MODELS_FETCH", "true");
    for key in [
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "GOOGLE_GENERATIVE_AI_API_KEY",
    ] {
        command.env_remove(key);
    }
}

fn available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve OpenCode server port")
        .local_addr()
        .expect("read OpenCode server port")
        .port()
}

async fn wait_for_server(url: &str, server: &mut Child, log: &Path) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_millis(500))
        .build()
        .map_err(|error| format!("build OpenCode health client: {error}"))?;
    for _ in 0..60 {
        if let Some(status) = server
            .try_wait()
            .map_err(|error| format!("query OpenCode server process: {error}"))?
        {
            return Err(format!(
                "OpenCode server exited before becoming healthy with {status}\n{}",
                server_diagnostics(server, log)
            ));
        }
        if let Ok(response) = client.get(format!("{url}/global/health")).send().await {
            if let Ok(response) = response.error_for_status() {
                if response
                    .json::<serde_json::Value>()
                    .await
                    .is_ok_and(|body| body["healthy"] == true)
                {
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!(
        "OpenCode server did not become healthy at {url}/global/health\n{}",
        server_diagnostics(server, log)
    ))
}

async fn run_session(url: &str, workspace: &Path, prompt: &OsString) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(90))
        .build()
        .map_err(|error| format!("build OpenCode HTTP client: {error}"))?;
    let directory = workspace.to_string_lossy().into_owned();
    let session = client
        .post(format!("{url}/session"))
        .query(&[("directory", &directory)])
        .json(&json!({}))
        .send()
        .await
        .map_err(|error| format!("create OpenCode session: {error}"))?
        .error_for_status()
        .map_err(|error| format!("create OpenCode session: {error}"))?
        .json::<serde_json::Value>()
        .await
        .map_err(|error| format!("decode OpenCode session: {error}"))?;
    let session_id = session["id"]
        .as_str()
        .ok_or_else(|| format!("OpenCode session response omitted id: {session}"))?;
    let response = client
        .post(format!("{url}/session/{session_id}/message"))
        .query(&[("directory", &directory)])
        .json(&json!({
            "parts": [{"type": "text", "text": prompt.to_string_lossy()}]
        }))
        .send()
        .await
        .map_err(|error| format!("run OpenCode session: {error}"))?
        .error_for_status()
        .map_err(|error| format!("run OpenCode session: {error}"))?
        .text()
        .await
        .map_err(|error| format!("read OpenCode response: {error}"))?;
    Ok(response)
}

fn server_diagnostics(server: &mut Child, log: &Path) -> String {
    let status = match server.try_wait() {
        Ok(Some(status)) => format!("exited with {status}"),
        Ok(None) => "still running".into(),
        Err(error) => format!("status unavailable: {error}"),
    };
    let log = std::fs::read_to_string(log).unwrap_or_else(|error| format!("<unreadable: {error}>"));
    format!("OpenCode server is {status}; stderr:\n{log}")
}

fn path_to_file_url(path: &Path) -> String {
    let path = path.to_string_lossy().replace('\\', "/");
    if path.starts_with('/') {
        format!("file://{path}")
    } else {
        format!("file:///{path}")
    }
}
