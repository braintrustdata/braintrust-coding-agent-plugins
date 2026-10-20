use super::AgentOutput;
use crate::support::agent_process::AgentTestWorld;
use serde_json::json;
use std::path::PathBuf;

/// Native hooks ship with bt; the CLI installs them for this invocation.
/// The seeded endpoint/auth shape follows Muse's official sdk-quickstart fake
/// provider, with dummy credentials and isolated configuration/data directories.
pub struct MuseAgent {
    config_home: PathBuf,
    data_home: PathBuf,
}

impl MuseAgent {
    pub fn new(world: &AgentTestWorld, base_url: &str) -> Self {
        let config_home = world.temp_path("muse-config");
        let config = config_home.join("muse");
        std::fs::create_dir_all(&config).expect("create Muse config");
        std::fs::create_dir_all(world.temp_path("muse-home")).expect("create Muse home");
        std::fs::write(
            config.join("settings.json"),
            json!({
                "schema_version": 1,
                "endpoint_transport": {"base_url": base_url, "auth": "bearer"}
            })
            .to_string(),
        )
        .expect("write Muse settings");
        std::fs::write(
            config.join("auth.json"),
            json!({
                "schema_version": 1,
                "providers": {"meta": {"api_key": "test-key"}}
            })
            .to_string(),
        )
        .expect("write dummy Muse auth");
        Self {
            config_home,
            data_home: world.temp_path("muse-data"),
        }
    }

    pub async fn export_latest(&self, world: &AgentTestWorld) -> serde_json::Value {
        let path = world.temp_path("muse-export.json");
        let mut command = super::command_from_env("MUSE_BIN", "muse");
        command
            .args(["export", "--last", "--out"])
            .arg(&path)
            .current_dir(world.workspace())
            .env_remove("CODEX_HOME")
            .env("HOME", world.temp_path("muse-home"))
            .env("TBH_CREDENTIAL_BACKEND", "file")
            .env("TBH_DISABLE_TELEMETRY", "1")
            .env("META_API_KEY", "test-key")
            .env("XDG_CONFIG_HOME", &self.config_home)
            .env("XDG_DATA_HOME", &self.data_home);
        let output = world.output(&mut command).await;
        assert!(
            output.status.success(),
            "Muse export: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&std::fs::read(path).expect("read native export"))
            .expect("parse native export")
    }

    pub async fn run(&self, world: &AgentTestWorld, prompt: &str) -> AgentOutput {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_bt-daemon"));
        command
            .args([
                "run",
                "--project",
                "agent-e2e",
                "muse",
                "exec",
                "--provider",
                "meta",
                "--model",
                "mock-model",
                "--no-foreign-personal-context",
                "--disable-reminders",
                "--disable-web-tools",
                "--disable-request-user-input",
                "--enable-shell-tool",
                "--max-model-steps",
                "3",
                "--disable-approval",
                "--disable-sandbox",
                prompt,
            ])
            .current_dir(world.workspace())
            .env_remove("CODEX_HOME")
            .env("HOME", world.temp_path("muse-home"))
            .env("TBH_CREDENTIAL_BACKEND", "file")
            .env("TBH_DISABLE_TELEMETRY", "1")
            .env("META_API_KEY", "test-key")
            .env("XDG_CONFIG_HOME", &self.config_home)
            .env("XDG_DATA_HOME", &self.data_home);
        // Do not import the parent coding agent's injected tool runtime.
        for (key, _) in std::env::vars_os() {
            let name = key.to_string_lossy();
            if [
                "CODEX_",
                "NODE_REPL_",
                "CUA_REPL_",
                "BROWSER_USE_",
                "SKY_CUA_",
            ]
            .iter()
            .any(|prefix| name.starts_with(prefix))
            {
                command.env_remove(key);
            }
        }
        command.env_remove("ELECTRON_RUN_AS_NODE");
        world.configure(&mut command);
        world.output(&mut command).await.into()
    }
}
