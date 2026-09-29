//! Best-effort local process identity capture for cross-agent correlation.
//!
//! The daemon snapshots a connecting client's ancestry while that process is
//! still alive. Only process identities and parent relationships are inspected;
//! executable paths, command arguments, environments, and cwd are not read.

use crate::wire::{CaptureContext, ProcessIdentity};
use std::collections::HashSet;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

/// Capture `pid` and its ancestors, nearest process first.
///
/// Process inspection can race with process exit and can be restricted by the
/// host. In either case this returns the useful prefix collected so far rather
/// than failing event capture.
pub(crate) fn capture_process_context(pid: u32) -> CaptureContext {
    let mut system = System::new();
    let (process_chain, truncated) = build_process_chain(pid, |pid| {
        let sysinfo_pid = Pid::from_u32(pid);
        system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[sysinfo_pid]),
            true,
            ProcessRefreshKind::nothing(),
        );
        let process = system.process(sysinfo_pid)?;
        Some(ProcessSnapshot {
            identity: ProcessIdentity {
                pid,
                start_time_secs: process.start_time(),
            },
            parent_pid: process.parent().map(Pid::as_u32),
        })
    });
    CaptureContext {
        process_chain,
        truncated,
    }
}

/// Whether `identity` still names a running process. A reused PID has a
/// different start time and is reported as exited.
pub(crate) fn process_is_alive(identity: &ProcessIdentity) -> bool {
    let mut system = System::new();
    let pid = Pid::from_u32(identity.pid);
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing(),
    );
    system
        .process(pid)
        .is_some_and(|process| process.start_time() == identity.start_time_secs)
}

#[derive(Debug, Clone)]
struct ProcessSnapshot {
    identity: ProcessIdentity,
    parent_pid: Option<u32>,
}

fn build_process_chain(
    start_pid: u32,
    mut inspect: impl FnMut(u32) -> Option<ProcessSnapshot>,
) -> (Vec<ProcessIdentity>, bool) {
    if start_pid == 0 {
        return (Vec::new(), true);
    }

    let mut chain = Vec::new();
    let mut seen = HashSet::new();
    let mut current = start_pid;

    // A fixed depth would misclassify agents launched through deep wrapper
    // chains. A PID seen twice would belong to different observations, not a
    // valid lineage; stop before crossing that inconsistent boundary.
    while seen.insert(current) {
        let Some(snapshot) = inspect(current) else {
            return (chain, true);
        };
        if chain.last().is_some_and(|child: &ProcessIdentity| {
            child.start_time_secs != 0
                && snapshot.identity.start_time_secs != 0
                && snapshot.identity.start_time_secs > child.start_time_secs
        }) {
            // A real parent cannot have started after its child. The PID was
            // likely reused between our per-process reads.
            return (chain, true);
        }
        chain.push(snapshot.identity);
        let Some(parent) = snapshot.parent_pid.filter(|parent| *parent != 0) else {
            return (chain, false);
        };
        current = parent;
    }

    (chain, true)
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
        }
    }

    #[test]
    fn builds_nearest_first_chain() {
        let processes = HashMap::from([
            (30, snapshot(30, Some(20))),
            (20, snapshot(20, Some(10))),
            (10, snapshot(10, None)),
        ]);

        let (chain, truncated) = build_process_chain(30, |pid| processes.get(&pid).cloned());
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

        let (chain, truncated) = build_process_chain(30, |pid| processes.get(&pid).cloned());
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
        let (chain, truncated) = build_process_chain(30, |pid| cycle.get(&pid).cloned());
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
                },
            ),
        ]);
        let (chain, truncated) = build_process_chain(30, |pid| replaced_parent.get(&pid).cloned());
        assert!(truncated);
        assert_eq!(chain.len(), 1, "do not traverse a newer reused parent PID");

        let (chain, truncated) = build_process_chain(1, |pid| {
            (pid <= 128).then(|| ProcessSnapshot {
                identity: ProcessIdentity {
                    pid,
                    start_time_secs: 1_000 - u64::from(pid),
                },
                parent_pid: (pid < 128).then_some(pid + 1),
            })
        });
        assert!(!truncated);
        assert_eq!(chain.len(), 128);
    }

    #[test]
    fn captures_current_process_with_stable_identity() {
        let first = capture_process_context(std::process::id());
        let second = capture_process_context(std::process::id());

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
