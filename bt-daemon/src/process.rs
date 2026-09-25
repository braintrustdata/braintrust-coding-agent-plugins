//! Best-effort local process identity capture for cross-agent correlation.
//!
//! The daemon snapshots a connecting client's ancestry while that process is
//! still alive. Only executable and script basenames are retained for
//! disambiguation; full command lines, environments, and cwd are not stored.

use crate::wire::{CaptureContext, ProcessIdentity};
use std::collections::HashSet;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};

/// Capture `pid` and its ancestors, nearest process first.
///
/// Process inspection can race with process exit and can be restricted by the
/// host. In either case this returns the useful prefix collected so far rather
/// than failing event capture.
pub(crate) fn capture_process_context(pid: u32) -> CaptureContext {
    let mut system = System::new();
    let (process_chain, process_labels, truncated) = build_process_chain(pid, |pid| {
        let sysinfo_pid = Pid::from_u32(pid);
        system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[sysinfo_pid]),
            true,
            ProcessRefreshKind::nothing()
                .with_cmd(UpdateKind::OnlyIfNotSet)
                .with_exe(UpdateKind::OnlyIfNotSet),
        );
        let process = system.process(sysinfo_pid)?;
        Some(ProcessSnapshot {
            identity: ProcessIdentity {
                pid,
                start_time_secs: process.start_time(),
            },
            parent_pid: process.parent().map(Pid::as_u32),
            labels: process_labels(process),
        })
    });
    CaptureContext {
        process_chain,
        process_labels,
        truncated,
    }
}

#[derive(Debug, Clone)]
struct ProcessSnapshot {
    identity: ProcessIdentity,
    parent_pid: Option<u32>,
    labels: Vec<String>,
}

fn process_labels(process: &sysinfo::Process) -> Vec<String> {
    let mut labels = Vec::new();
    if let Some(exe) = process.exe().and_then(|path| path.file_name()) {
        labels.push(exe.to_string_lossy().into_owned());
    } else {
        labels.push(process.name().to_string_lossy().into_owned());
    }
    // A shell's executable is usually uninformative. Its script argument is
    // useful, but arbitrary arguments may contain prompts or credentials.
    for arg in process.cmd().iter().skip(1) {
        let arg = arg.to_string_lossy();
        let arg = std::path::Path::new(arg.trim_matches(['"', '\'']));
        let Some(name) = arg.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.len() > 128
            || !name
                .chars()
                .all(|ch| ch.is_alphanumeric() || matches!(ch, '.' | '_' | '-'))
        {
            continue;
        }
        if ["sh", "bash", "zsh", "ps1", "cmd", "bat", "py", "js", "mjs"]
            .iter()
            .any(|extension| {
                name.to_ascii_lowercase()
                    .ends_with(&format!(".{extension}"))
            })
        {
            labels.push(name.to_owned());
        }
    }
    labels
}

fn build_process_chain(
    start_pid: u32,
    mut inspect: impl FnMut(u32) -> Option<ProcessSnapshot>,
) -> (Vec<ProcessIdentity>, Vec<Vec<String>>, bool) {
    if start_pid == 0 {
        return (Vec::new(), Vec::new(), true);
    }

    let mut chain = Vec::new();
    let mut labels = Vec::new();
    let mut seen = HashSet::new();
    let mut current = start_pid;

    // A fixed depth would misclassify agents launched through deep wrapper
    // chains. A PID seen twice would belong to different observations, not a
    // valid lineage; stop before crossing that inconsistent boundary.
    while seen.insert(current) {
        let Some(snapshot) = inspect(current) else {
            return (chain, labels, true);
        };
        if chain.last().is_some_and(|child: &ProcessIdentity| {
            child.start_time_secs != 0
                && snapshot.identity.start_time_secs != 0
                && snapshot.identity.start_time_secs > child.start_time_secs
        }) {
            // A real parent cannot have started after its child. The PID was
            // likely reused between our per-process reads.
            return (chain, labels, true);
        }
        chain.push(snapshot.identity);
        labels.push(snapshot.labels);
        let Some(parent) = snapshot.parent_pid.filter(|parent| *parent != 0) else {
            return (chain, labels, false);
        };
        current = parent;
    }

    (chain, labels, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::process::Command;
    use std::time::Duration;

    fn snapshot(pid: u32, parent_pid: Option<u32>) -> ProcessSnapshot {
        ProcessSnapshot {
            identity: ProcessIdentity {
                pid,
                start_time_secs: u64::from(pid) * 10,
            },
            parent_pid,
            labels: Vec::new(),
        }
    }

    #[test]
    fn builds_nearest_first_chain() {
        let processes = HashMap::from([
            (30, snapshot(30, Some(20))),
            (20, snapshot(20, Some(10))),
            (10, snapshot(10, None)),
        ]);

        let (chain, _, truncated) = build_process_chain(30, |pid| processes.get(&pid).cloned());
        assert!(!truncated);

        assert_eq!(
            chain
                .iter()
                .map(|identity| identity.pid)
                .collect::<Vec<_>>(),
            vec![30, 20, 10]
        );
    }

    #[test]
    fn returns_available_prefix_when_an_ancestor_disappears() {
        let processes = HashMap::from([(30, snapshot(30, Some(20)))]);

        let (chain, _, truncated) = build_process_chain(30, |pid| processes.get(&pid).cloned());
        assert!(truncated);

        assert_eq!(
            chain
                .iter()
                .map(|identity| identity.pid)
                .collect::<Vec<_>>(),
            vec![30]
        );
    }

    #[test]
    fn stops_at_inconsistent_parentage_and_walks_past_arbitrary_wrapper_depth() {
        let cycle = HashMap::from([(30, snapshot(30, Some(20))), (20, snapshot(20, Some(30)))]);
        let (chain, _, truncated) = build_process_chain(30, |pid| cycle.get(&pid).cloned());
        assert!(truncated);
        assert_eq!(
            chain
                .iter()
                .map(|identity| identity.pid)
                .collect::<Vec<_>>(),
            vec![30, 20]
        );

        let replaced_parent = HashMap::from([
            (30, snapshot(30, Some(20))),
            (
                20,
                ProcessSnapshot {
                    identity: ProcessIdentity {
                        pid: 20,
                        start_time_secs: 400,
                    },
                    parent_pid: Some(10),
                    labels: Vec::new(),
                },
            ),
        ]);
        let (chain, _, truncated) =
            build_process_chain(30, |pid| replaced_parent.get(&pid).cloned());
        assert!(truncated);
        assert_eq!(chain.len(), 1, "do not traverse a newer reused parent PID");

        let (chain, _, truncated) = build_process_chain(1, |pid| {
            (pid <= 128).then(|| ProcessSnapshot {
                identity: ProcessIdentity {
                    pid,
                    start_time_secs: 1_000 - u64::from(pid),
                },
                parent_pid: (pid < 128).then_some(pid + 1),
                labels: Vec::new(),
            })
        });
        assert!(!truncated);
        assert_eq!(chain.len(), 128);
    }

    #[test]
    fn captures_current_process_with_stable_identity() {
        let first = capture_process_context(std::process::id());
        let second = capture_process_context(std::process::id());
        assert!(first
            .process_labels
            .first()
            .is_some_and(|labels| !labels.is_empty()));

        assert_eq!(
            first.process_chain.first().map(|process| process.pid),
            Some(std::process::id())
        );
        assert_eq!(
            first
                .process_chain
                .first()
                .map(|process| process.start_time_secs),
            second
                .process_chain
                .first()
                .map(|process| process.start_time_secs)
        );
    }

    #[test]
    fn captures_a_live_script_basename_without_its_command_line() {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(windows)]
        let script = dir.path().join("trace-child.cmd");
        #[cfg(not(windows))]
        let script = dir.path().join("trace-child.sh");
        #[cfg(windows)]
        std::fs::write(&script, "@echo off\r\nping -n 4 127.0.0.1 >NUL\r\n").unwrap();
        #[cfg(not(windows))]
        std::fs::write(&script, "sleep 3\n").unwrap();

        #[cfg(windows)]
        let mut child = Command::new("cmd").arg("/C").arg(&script).spawn().unwrap();
        #[cfg(not(windows))]
        let mut child = Command::new("sh").arg(&script).spawn().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        let capture = capture_process_context(child.id());
        let labels = &capture.process_labels[0];
        let expected = script.file_name().unwrap().to_string_lossy();
        assert!(
            labels.iter().any(|label| label == expected.as_ref()),
            "{labels:?}"
        );
        assert!(labels
            .iter()
            .all(|label| !label.contains("ping -n") && !label.contains("sleep 3")));
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn captures_spawned_child_ancestry() {
        const CHILD_ENV: &str = "_BT_PROCESS_CAPTURE_TEST_CHILD";
        if std::env::var_os(CHILD_ENV).is_some() {
            std::thread::sleep(Duration::from_secs(5));
            return;
        }

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process::tests::captures_spawned_child_ancestry",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .spawn()
            .unwrap();
        let child_pid = child.id();
        let mut captured = CaptureContext::default();
        for _ in 0..50 {
            captured = capture_process_context(child_pid);
            if captured
                .process_chain
                .iter()
                .any(|process| process.pid == std::process::id())
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = child.kill();
        let _ = child.wait();

        assert_eq!(
            captured.process_chain.first().map(|process| process.pid),
            Some(child_pid)
        );
        assert!(captured
            .process_chain
            .iter()
            .any(|process| process.pid == std::process::id()));
    }

    #[test]
    fn zero_pid_has_no_process_context() {
        assert!(capture_process_context(0).process_chain.is_empty());
    }
}
