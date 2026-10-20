//! Select the hook executable before consuming input or resolving configuration.

use anyhow::Context;
use std::path::Path;

const TARGET_ENV: &str = "BRAINTRUST_BT_BIN";
const DELEGATED_ENV: &str = "BRAINTRUST_HOOK_DELEGATED";

/// Delegate a CLI hook invocation to an explicitly selected executable.
///
/// This is process-level dispatch: on success it replaces or exits the calling
/// process. Embedders should call it only at the CLI hook entry point.
pub fn delegate_hook_executable() -> anyhow::Result<()> {
    let Some(target) = crate::env::var_os(TARGET_ENV) else {
        return Ok(());
    };
    let target = Path::new(&target);
    anyhow::ensure!(
        target.is_absolute(),
        "{TARGET_ENV} must be an absolute executable path, not a PATH lookup or version-manager shim"
    );
    let target = target.canonicalize().with_context(|| {
        format!(
            "cannot resolve {TARGET_ENV} executable {}",
            target.display()
        )
    })?;
    let current = std::env::current_exe()?.canonicalize()?;
    if target == current {
        return Ok(());
    }
    anyhow::ensure!(
        crate::env::var_os(DELEGATED_ENV).is_none(),
        "{TARGET_ENV} delegation loop: point it at the actual bt executable, not a wrapper or version-manager shim"
    );
    let mut command = crate::subprocess::background_command(&target);
    command
        .args(std::env::args_os().skip(1))
        .env(DELEGATED_ENV, "1");
    // Standard streams are inherited untouched, including the unread payload.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let error = command.exec();
        Err(error)
            .with_context(|| format!("cannot execute {TARGET_ENV} target {}", target.display()))
    }
    #[cfg(not(unix))]
    {
        let status = command
            .status()
            .with_context(|| format!("cannot execute {TARGET_ENV} target {}", target.display()))?;
        std::process::exit(status.code().unwrap_or(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::process::Stdio;

    const CHILD_ENV: &str = "BRAINTRUST_TEST_DELEGATE_CHILD";

    #[test]
    fn child_entry() {
        if crate::env::var_os(CHILD_ENV).is_none() {
            return;
        }
        if let Err(error) = delegate_hook_executable() {
            eprintln!("{error:#}");
            std::process::exit(1);
        }
        let mut payload = String::new();
        std::io::stdin().read_to_string(&mut payload).unwrap();
        println!("payload={payload}");
        println!("args={:?}", std::env::args_os().skip(1).collect::<Vec<_>>());
        eprintln!("target stderr");
        std::process::exit(23);
    }

    fn child(target: &Path, loop_guard: bool) -> std::process::Output {
        let mut command = crate::subprocess::background_command(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "hook_executable::tests::child_entry",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .env(TARGET_ENV, target)
            .env_remove(DELEGATED_ENV)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if loop_guard {
            command.env(DELEGATED_ENV, "1");
        }
        let mut child = command.spawn().unwrap();
        let _ = child
            .stdin
            .take()
            .unwrap()
            .write_all(b"{\"raw\": [1, 2]}\n");
        child.wait_with_output().unwrap()
    }

    #[test]
    fn delegation_preserves_input_arguments_streams_and_status() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp
            .path()
            .join(format!("selected bt{}", std::env::consts::EXE_SUFFIX));
        std::fs::copy(std::env::current_exe().unwrap(), &target).unwrap();
        let output = child(&target, false);
        assert_eq!(output.status.code(), Some(23), "{output:?}");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("payload={\"raw\": [1, 2]}\n"), "{stdout}");
        assert!(stdout.contains("--exact"), "{stdout}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("target stderr"));
        let output = child(&target, true);
        assert_eq!(output.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&output.stderr).contains("delegation loop"));
    }

    #[test]
    fn current_executable_continues_and_invalid_targets_fail_clearly() {
        assert_eq!(
            child(&std::env::current_exe().unwrap(), true).status.code(),
            Some(23)
        );
        for (target, message) in [
            (std::path::PathBuf::from("bt"), "absolute executable path"),
            (
                tempfile::tempdir().unwrap().path().join("missing-bt"),
                "cannot resolve",
            ),
        ] {
            let output = child(&target, false);
            assert_eq!(output.status.code(), Some(1));
            assert!(
                String::from_utf8_lossy(&output.stderr).contains(message),
                "{output:?}"
            );
        }
    }
}
