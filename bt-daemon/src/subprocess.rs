//! Process creation policy: background work never allocates a Windows console.
//!
//! Interactive commands inherit the caller's terminal. Daemon startup has its
//! own detachment policy so it can outlive the hook that starts it.

#![allow(
    clippy::disallowed_methods,
    reason = "This module owns subprocess creation policy."
)]

use std::ffi::OsStr;
use std::process::Command;

pub(crate) fn background_command(program: impl AsRef<OsStr>) -> Command {
    let command = Command::new(program);
    #[cfg(windows)]
    let command = {
        use std::os::windows::process::CommandExt;

        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        let mut command = command;
        command.creation_flags(CREATE_NO_WINDOW);
        command
    };
    command
}

/// Only foreground agent runs and interactive setup may inherit a terminal.
pub(crate) fn interactive_command(program: impl AsRef<OsStr>) -> Command {
    Command::new(program)
}

/// Only daemon startup may detach; ordinary background children must use
/// `background_command` so redirected pipes keep their normal semantics.
pub(crate) fn detached_daemon_command(program: impl AsRef<OsStr>) -> Command {
    let mut command = Command::new(program);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;

        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }
    command
}

#[cfg(all(test, windows))]
mod tests {
    use super::{background_command, detached_daemon_command, interactive_command};
    use std::io::{Read, Write};
    use std::process::Stdio;

    fn has_console() -> bool {
        #[link(name = "kernel32")]
        extern "system" {
            fn GetConsoleWindow() -> *mut std::ffi::c_void;
        }
        // SAFETY: GetConsoleWindow takes no arguments and owns its result.
        !unsafe { GetConsoleWindow() }.is_null()
    }

    #[test]
    fn detached_background_command_preserves_pipes_without_console() {
        const ROLE: &str = "_BT_BACKGROUND_COMMAND_TEST_ROLE";
        const TEST: &str =
            "subprocess::tests::detached_background_command_preserves_pipes_without_console";

        match std::env::var(ROLE).as_deref() {
            Ok("child") => {
                assert!(!has_console());
                let mut input = String::new();
                std::io::stdin().read_to_string(&mut input).unwrap();
                assert_eq!(input, "metadata input");
                println!("metadata output");
                eprintln!("metadata diagnostic");
                std::process::exit(23);
            }
            Ok("unsuppressed") => {
                // A plain launch from a detached parent would allocate a console.
                assert!(has_console(), "negative control must detect a console");
            }
            Ok("detached") => {
                assert!(
                    !has_console(),
                    "parent must reproduce daemon launch conditions"
                );
                let control = interactive_command(std::env::current_exe().unwrap())
                    .args(["--exact", TEST, "--nocapture"])
                    .env(ROLE, "unsuppressed")
                    .output()
                    .unwrap();
                assert!(control.status.success(), "{control:?}");

                let mut child = background_command(std::env::current_exe().unwrap())
                    .args(["--exact", TEST, "--nocapture"])
                    .env(ROLE, "child")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap();
                child
                    .stdin
                    .take()
                    .unwrap()
                    .write_all(b"metadata input")
                    .unwrap();
                let output = child.wait_with_output().unwrap();
                assert_eq!(output.status.code(), Some(23), "{output:?}");
                assert!(String::from_utf8_lossy(&output.stdout).contains("metadata output"));
                assert!(String::from_utf8_lossy(&output.stderr).contains("metadata diagnostic"));
            }
            _ => {
                let output = detached_daemon_command(std::env::current_exe().unwrap())
                    .args(["--exact", TEST, "--nocapture"])
                    .env(ROLE, "detached")
                    .output()
                    .unwrap();
                assert!(output.status.success(), "{output:?}");
            }
        }
    }
}
