use super::{command_from_env, AgentOutput, ProcessOptions};
use crate::support::agent_process::AgentTestWorld;
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

pub struct PiAgent {
    config_dir: PathBuf,
    session_dir: PathBuf,
    extension: PathBuf,
}

pub struct PiRun {
    prompt: OsString,
    mock_inference: Option<String>,
    options: ProcessOptions,
}

impl PiRun {
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

impl PiAgent {
    pub fn new(world: &AgentTestWorld) -> Self {
        let config_dir = world.temp_path("pi-config");
        let session_dir = world.temp_path("pi-sessions");
        for directory in [&config_dir, &session_dir] {
            std::fs::create_dir_all(directory).expect("create Pi test directory");
        }
        let extension = std::env::var_os("PI_EXTENSION_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .expect("repository root")
                    .join("dist/pi/dist/index.mjs")
            });
        assert!(
            extension.is_file(),
            "install the packed Pi extension and set PI_EXTENSION_PATH before running integration tests: {}",
            extension.display()
        );
        Self {
            config_dir,
            session_dir,
            extension,
        }
    }

    pub async fn run(&self, world: &AgentTestWorld, run: PiRun) -> AgentOutput {
        let mut command = command_from_env("PI_BIN", "pi");
        if world.uses_mock_inference() {
            let base_url = run
                .mock_inference
                .as_ref()
                .expect("mock Pi runs require a mock inference endpoint");
            std::fs::write(
                self.config_dir.join("models.json"),
                serde_json::to_vec_pretty(&json!({
                    "providers": {
                        "mock": {
                            "baseUrl": format!("{base_url}/v1"),
                            "api": "openai-responses",
                            "apiKey": "test-key",
                            "models": [{"id": "mock-model", "name": "Mock Model"}]
                        }
                    }
                }))
                .unwrap(),
            )
            .expect("write Pi mock model config");
        }
        command
            .args([
                "--print",
                "--mode",
                "json",
                "--provider",
                "mock",
                "--model",
                "mock-model",
                "--api-key",
                "test-key",
                "--no-extensions",
                "--no-skills",
                "--no-context-files",
                "--tools",
                "bash",
                "--extension",
            ])
            .arg(&self.extension)
            .arg("--session-dir")
            .arg(&self.session_dir)
            .current_dir(world.workspace())
            .env("PI_CODING_AGENT_DIR", &self.config_dir)
            .env("TRACE_TO_BRAINTRUST", "true")
            .env("BRAINTRUST_PROJECT", "agent-e2e")
            .env("PI_OFFLINE", "true");
        world.configure(&mut command);
        run.options.apply(&mut command);
        command.arg(run.prompt);
        world.output(&mut command).await.into()
    }
}
