use super::{command_from_env, repository_root, AgentOutput, ProcessOptions};
use crate::support::agent_process::AgentTestWorld;
use std::ffi::OsString;
use std::path::PathBuf;
use uuid::Uuid;

pub struct ClaudeAgent<'a> {
    world: &'a AgentTestWorld,
    isolated_home: PathBuf,
    isolated_config: PathBuf,
}

pub struct ClaudeRun {
    prompt: OsString,
    inference: Option<ClaudeInference>,
    options: ProcessOptions,
}

struct ClaudeInference {
    base_url: String,
    model: String,
    api_key: String,
}

impl ClaudeRun {
    pub fn live(prompt: impl Into<OsString>) -> Self {
        Self {
            prompt: prompt.into(),
            inference: None,
            options: ProcessOptions::default(),
        }
    }

    pub fn mock(prompt: impl Into<OsString>, base_url: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            inference: Some(ClaudeInference {
                base_url: base_url.into(),
                model: "mock-model".into(),
                api_key: "test-key".into(),
            }),
            options: ProcessOptions::default(),
        }
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

impl<'a> ClaudeAgent<'a> {
    pub fn new(world: &'a AgentTestWorld) -> Self {
        let isolated_home = world.temp_path("claude-home");
        let isolated_config = world.temp_path("claude-config");
        std::fs::create_dir_all(&isolated_home).expect("create Claude home");
        std::fs::create_dir_all(&isolated_config).expect("create Claude config");
        Self {
            world,
            isolated_home,
            isolated_config,
        }
    }

    pub async fn run(&self, run: ClaudeRun) -> AgentOutput {
        let session_id = Uuid::new_v4().to_string();
        let plugin = repository_root().join("src/plugins/claude/content/plugins/trace-claude-code");
        let mut command = command_from_env("CLAUDE_BIN", "claude");
        command
            .args([
                "-p",
                "--output-format",
                "json",
                "--dangerously-skip-permissions",
                "--session-id",
                &session_id,
                "--plugin-dir",
            ])
            .arg(plugin)
            .current_dir(self.world.workspace())
            .env("ANTHROPIC_MAX_RETRIES", "0")
            .env("DISABLE_AUTOUPDATER", "1")
            .env("DISABLE_TELEMETRY", "1")
            .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1");
        self.world.configure(&mut command);

        if let Some(inference) = &run.inference {
            command
                .args(["--model", &inference.model])
                .env("HOME", &self.isolated_home)
                .env("CLAUDE_CONFIG_DIR", &self.isolated_config)
                .env("ANTHROPIC_BASE_URL", &inference.base_url)
                .env("ANTHROPIC_API_KEY", &inference.api_key)
                .env("ANTHROPIC_AUTH_TOKEN", &inference.api_key)
                .env("ANTHROPIC_DEFAULT_OPUS_MODEL", &inference.model)
                .env("ANTHROPIC_DEFAULT_SONNET_MODEL", &inference.model)
                .env("ANTHROPIC_DEFAULT_HAIKU_MODEL", &inference.model);
        }
        run.options.apply(&mut command);
        command.arg(run.prompt);
        self.world.output(&mut command).await.into()
    }
}
