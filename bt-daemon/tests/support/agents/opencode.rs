use super::{command_from_env, AgentOutput, ProcessOptions};
use crate::support::agent_process::AgentTestWorld;
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

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
        let plugin = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("repository root")
            .join("dist/opencode/dist/index.js");
        assert!(
            plugin.is_file(),
            "build the OpenCode plugin before running integration tests: {}",
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
            "plugin": [path_to_file_url(&self.plugin)],
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

        let mut command = command_from_env("OPENCODE_BIN", "opencode");
        command
            .args(["run", "--format", "json", "--auto"])
            .arg("--dir")
            .arg(&workspace)
            .current_dir(&workspace)
            .env("HOME", &self.home)
            .env("USERPROFILE", &self.home)
            .env("XDG_CONFIG_HOME", &self.config_home)
            .env("XDG_DATA_HOME", &self.data_home)
            .env("XDG_CACHE_HOME", &self.cache_home)
            .env("TRACE_TO_BRAINTRUST", "true")
            .env("BRAINTRUST_OPENCODE_ENABLE_TOOLS", "false")
            .env("OPENCODE_DISABLE_MODELS_FETCH", "true");
        if world.uses_mock_inference() {
            command.args(["--model", "mock/mock-model"]);
        }
        for key in [
            "OPENAI_API_KEY",
            "ANTHROPIC_API_KEY",
            "ANTHROPIC_AUTH_TOKEN",
            "GOOGLE_GENERATIVE_AI_API_KEY",
        ] {
            command.env_remove(key);
        }
        world.configure(&mut command);
        run.options.apply(&mut command);
        command.arg(run.prompt);
        world.output(&mut command).await.into()
    }
}

fn path_to_file_url(path: &Path) -> String {
    let path = path.to_string_lossy().replace('\\', "/");
    if path.starts_with('/') {
        format!("file://{path}")
    } else {
        format!("file:///{path}")
    }
}
