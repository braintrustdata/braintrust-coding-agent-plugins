//! Invocation-local Muse hooks and their route context.
use super::{Injection, ManagedRun, Terminal};
use crate::agents::Muse;
use crate::args::RunHookCommand;
use crate::wire::SessionRoute;
use anyhow::Context;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct MuseManagedContext {
    pub route: SessionRoute,
    pub managed_run_id: String,
    pub socket: PathBuf,
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
        let executable = crate::env::var_os("MUSE_BIN").unwrap_or_else(|| "muse".into());
        let output = crate::subprocess::background_command(executable)
            .arg("--version")
            .output()
            .context("check Muse Code version for managed hooks")?;
        if !output.status.success()
            || crate::setup::muse::muse_version(&output.stdout)? != semver::Version::new(1, 1, 1)
        {
            anyhow::bail!("Muse managed runs require the verified Muse Code 1.1.1 hook override");
        }
        Ok(())
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
        let hooks_path = directory.path().join("hooks.json");
        std::fs::write(&hooks_path, serde_json::to_vec(&hooks)?)?;
        Ok(Injection {
            env: vec![(
                "TBH_MANAGED_HOOKS_PATH",
                hooks_path.to_string_lossy().into_owned(),
            )],
            guards: vec![Box::new(directory)],
            ..Injection::default()
        })
    }
}
