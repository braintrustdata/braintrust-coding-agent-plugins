//! Invocation-local Muse hooks and their route context.
use super::{Injection, ManagedRun, Terminal};
use crate::agents::Muse;
use crate::args::RunHookCommand;
use crate::wire::SessionRoute;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct MuseManagedContext {
    pub route: SessionRoute,
    pub managed_run_id: String,
    pub socket: PathBuf,
    #[serde(default)]
    pub source_version: Option<String>,
}

impl ManagedRun for Muse {
    fn executable(&self) -> (&'static str, &'static str) {
        ("MUSE_BIN", "muse")
    }

    fn check(&self, args: &[OsString], _terminal: Terminal) -> anyhow::Result<()> {
        crate::setup::muse::check_muse_project_hooks(&std::env::current_dir()?)?;
        for (index, arg) in args.iter().enumerate() {
            let Some(arg) = arg.to_str() else { continue };
            let workspace = if arg == "--workspace" {
                args.get(index + 1).map(PathBuf::from)
            } else {
                arg.strip_prefix("--workspace=").map(PathBuf::from)
            };
            if let Some(workspace) = workspace {
                crate::setup::muse::check_muse_project_hooks(&workspace)?;
            }
        }
        if crate::setup::muse::installed_muse_version()? < semver::Version::new(1, 3, 0) {
            anyhow::bail!(
                "Muse Code 1.3.0 or newer is required for invocation-local hook overrides"
            );
        }
        Ok(())
    }

    fn invocation_args(
        &self,
        injection_args: Vec<OsString>,
        mut agent_args: Vec<OsString>,
    ) -> Vec<OsString> {
        // Muse dispatches subcommands before parsing options. Placing --config
        // before `exec` accidentally launches its interactive parser instead.
        if agent_args
            .first()
            .and_then(|arg| arg.to_str())
            .is_some_and(|arg| matches!(arg, "exec" | "resume"))
        {
            let subcommand = agent_args.remove(0);
            std::iter::once(subcommand)
                .chain(injection_args)
                .chain(agent_args)
                .collect()
        } else {
            injection_args.into_iter().chain(agent_args).collect()
        }
    }

    fn needs_daemon_host(&self) -> bool {
        true
    }

    fn inject(&self, _hook: &RunHookCommand, _id: &str) -> anyhow::Result<Injection> {
        anyhow::bail!("Muse managed hooks require an invocation route and socket")
    }

    fn inject_with_context(
        &self,
        hook: &RunHookCommand,
        id: &str,
        route: &SessionRoute,
        socket: &Path,
    ) -> anyhow::Result<Injection> {
        let directory = tempfile::Builder::new()
            .prefix("bt-trace-run-muse-")
            .tempdir()?;
        crate::paths::ensure_private_dir(directory.path())?;
        let context_path = directory.path().join("context.json");
        let context = MuseManagedContext {
            route: route.clone(),
            managed_run_id: id.into(),
            socket: socket.into(),
            source_version: Some(crate::setup::muse::installed_muse_version()?.to_string()),
        };
        std::fs::write(&context_path, serde_json::to_vec(&context)?)?;
        crate::paths::restrict_file_to_owner(&context_path)?;
        let mut hook = hook.clone();
        hook.args.extend([
            "--flush-on-turn-end".into(),
            "--managed-context".into(),
            context_path.into_os_string(),
        ]);
        let command = super::managed_hook_shell_command(&hook, "muse", cfg!(windows))?;
        let hooks = crate::setup::muse::muse_managed_hook_config(
            &crate::paths::muse_config_dir(),
            &command,
        )?;
        let mut args = vec![
            OsString::from("--config"),
            OsString::from(format!("hooks={}", serde_json::to_string(&hooks)?)),
        ];
        // Old persistent installs used a dedicated managed file. Its handlers
        // must be suppressed alongside the replaced user hooks for this run.
        if crate::setup::muse::muse_has_owned_managed_hooks(&crate::paths::muse_config_dir())? {
            args.extend([
                OsString::from("--config"),
                OsString::from("managed_hooks_path=\"\""),
            ]);
        }
        Ok(Injection {
            args,
            guards: vec![Box::new(directory)],
            ..Injection::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_context_carries_runtime_version_and_reads_old_contexts() {
        let context = MuseManagedContext {
            route: SessionRoute::default(),
            managed_run_id: "run".into(),
            socket: PathBuf::from("daemon.sock"),
            source_version: Some("1.4.11".into()),
        };
        let mut value = serde_json::to_value(&context).unwrap();
        assert_eq!(
            serde_json::from_value::<MuseManagedContext>(value.clone())
                .unwrap()
                .source_version
                .as_deref(),
            Some("1.4.11")
        );
        value.as_object_mut().unwrap().remove("source_version");
        assert!(serde_json::from_value::<MuseManagedContext>(value)
            .unwrap()
            .source_version
            .is_none());
    }

    #[test]
    fn invocation_options_follow_native_subcommands() {
        for subcommand in ["exec", "resume"] {
            let args = Muse.invocation_args(
                vec!["--config".into(), "hooks={}".into()],
                vec![
                    subcommand.into(),
                    "--workspace".into(),
                    "path with spaces".into(),
                    "prompt".into(),
                ],
            );
            assert_eq!(
                args,
                [
                    subcommand,
                    "--config",
                    "hooks={}",
                    "--workspace",
                    "path with spaces",
                    "prompt"
                ]
                .map(OsString::from)
            );
        }
        let args = Muse.invocation_args(
            vec!["--config".into(), "hooks={}".into()],
            vec!["prompt".into()],
        );
        assert_eq!(args, ["--config", "hooks={}", "prompt"].map(OsString::from));
    }
}
