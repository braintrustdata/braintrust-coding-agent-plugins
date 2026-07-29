use super::{command_from_env, configured_home, repository_root, AgentOutput, ProcessOptions};
use crate::support::agent_process::AgentTestWorld;
use std::ffi::OsString;
use std::path::PathBuf;
use tokio::process::Command;

const TEST_MODE_ENV: &str = "BT_AGENT_TEST_MODE";

pub struct CodexAgent<'a> {
    world: &'a AgentTestWorld,
    home: PathBuf,
}

pub struct CodexRun {
    prompt: OsString,
    inference: Option<CodexInference>,
    options: ProcessOptions,
}

struct CodexInference {
    base_url: String,
    model: String,
    api_key: String,
}

impl CodexRun {
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
            inference: Some(CodexInference {
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

impl<'a> CodexAgent<'a> {
    pub async fn install(world: &'a AgentTestWorld) -> Self {
        let home = world.temp_path("codex-home");
        std::fs::create_dir_all(&home).expect("create Codex home");

        let marketplace = repository_root().join("src/plugins/codex/content");
        let mut add_marketplace = command_from_env("CODEX_BIN", "codex");
        add_marketplace
            .arg("plugin")
            .arg("marketplace")
            .arg("add")
            .arg(&marketplace)
            .env("CODEX_HOME", &home);
        world.configure(&mut add_marketplace);
        AgentOutput::from(world.output(&mut add_marketplace).await).assert_success();

        let mut add_plugin = command_from_env("CODEX_BIN", "codex");
        add_plugin
            .args(["plugin", "add", "trace-codex@braintrust-codex-plugins"])
            .env("CODEX_HOME", &home);
        world.configure(&mut add_plugin);
        AgentOutput::from(world.output(&mut add_plugin).await).assert_success();

        Self { world, home }
    }

    pub fn seed_live_auth(&self) {
        if std::env::var_os("OPENAI_API_KEY").is_some() {
            return;
        }
        let source = configured_home("CODEX_HOME", ".codex")
            .map(|home| home.join("auth.json"))
            .filter(|path| path.is_file())
            .unwrap_or_else(|| {
                panic!(
                    "{TEST_MODE_ENV}=live requires OPENAI_API_KEY or auth.json in the configured Codex home"
                )
            });
        std::fs::copy(source, self.home.join("auth.json")).expect("copy Codex live credentials");
    }

    pub async fn run(&self, run: CodexRun) -> AgentOutput {
        let mut command = self.command();
        if let Some(inference) = &run.inference {
            configure_mock_inference(&mut command, inference);
        }
        run.options.apply(&mut command);
        command.arg(run.prompt);
        self.world.output(&mut command).await.into()
    }

    fn command(&self) -> Command {
        let mut command = command_from_env("CODEX_BIN", "codex");
        command
            .args([
                "exec",
                "--skip-git-repo-check",
                "--dangerously-bypass-hook-trust",
                "--sandbox",
                "read-only",
                "-c",
                r#"approval_policy="never""#,
            ])
            .current_dir(self.world.workspace())
            .env("CODEX_HOME", &self.home);
        self.world.configure(&mut command);
        command
    }
}

fn configure_mock_inference(command: &mut Command, inference: &CodexInference) {
    let provider = format!(
        r#"model_providers.mock={{name="Mock",base_url="{}/v1",wire_api="responses",env_key="MOCK_API_KEY",request_max_retries=0,stream_max_retries=0,stream_idle_timeout_ms=5000}}"#,
        inference.base_url
    );
    let chatgpt_base_url = format!(r#"chatgpt_base_url="{}/backend-api""#, inference.base_url);
    command
        .args([
            "-c",
            &format!(r#"model="{}""#, inference.model),
            "-c",
            r#"model_provider="mock""#,
            "-c",
            &provider,
            "-c",
            &chatgpt_base_url,
        ])
        .env("MOCK_API_KEY", &inference.api_key);
}
