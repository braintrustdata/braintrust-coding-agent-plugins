mod claude;
mod codex;
mod opencode;
mod test_plugin;

#[allow(unused_imports)]
pub use claude::{ClaudeAgent, ClaudeRun};
#[allow(unused_imports)]
pub use codex::{CodexAgent, CodexRun};
#[allow(unused_imports)]
pub use opencode::{OpenCodeAgent, OpenCodeRun};

use std::ffi::OsString;
use std::path::PathBuf;
use tokio::process::Command;

pub struct AgentOutput {
    output: std::process::Output,
}

impl AgentOutput {
    pub fn success(&self) -> bool {
        self.output.status.success()
    }

    pub fn text(&self) -> String {
        format!(
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&self.output.stdout),
            String::from_utf8_lossy(&self.output.stderr)
        )
    }

    pub fn assert_success(&self) {
        assert!(self.success(), "{}", self.text());
    }

    pub fn assert_failure(&self) {
        assert!(!self.success(), "{}", self.text());
    }

    pub fn assert_contains(&self, expected: &str) {
        assert!(
            self.text().contains(expected),
            "agent output did not contain {expected:?}:\n{}",
            self.text()
        );
    }
}

impl From<std::process::Output> for AgentOutput {
    fn from(output: std::process::Output) -> Self {
        Self { output }
    }
}

#[derive(Default)]
struct ProcessOptions {
    args: Vec<OsString>,
    env: Vec<(OsString, OsString)>,
}

impl ProcessOptions {
    fn arg(&mut self, value: impl Into<OsString>) {
        self.args.push(value.into());
    }

    fn env(&mut self, key: impl Into<OsString>, value: impl Into<OsString>) {
        self.env.push((key.into(), value.into()));
    }

    fn apply(&self, command: &mut Command) {
        command
            .args(&self.args)
            .envs(self.env.iter().map(|(k, v)| (k, v)));
    }
}

fn command_from_env(name: &str, fallback: &str) -> Command {
    if let Some(command) = std::env::var_os(name) {
        return Command::new(command);
    }
    #[cfg(windows)]
    let fallback = format!("{fallback}.cmd");
    Command::new(fallback)
}

fn configured_home(config_env: &str, directory: &str) -> Option<PathBuf> {
    std::env::var_os(config_env).map(PathBuf::from).or_else(|| {
        std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from)
            .map(|home| home.join(directory))
    })
}
