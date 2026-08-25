//! Best-effort local process identity capture for cross-agent correlation.
//!
//! The daemon snapshots a connecting client's ancestry while that process is
//! still alive. Collection is deliberately metadata-only: no command lines,
//! environment variables, or working directories are read.

use crate::wire::{CaptureContext, ProcessIdentity};
use std::collections::HashSet;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

const MAX_PROCESS_CHAIN_DEPTH: usize = 64;

/// Capture `pid` and its ancestors, nearest process first.
///
/// Process inspection can race with process exit and can be restricted by the
/// host. In either case this returns the useful prefix collected so far rather
/// than failing event capture.
pub(crate) fn capture_process_context(pid: u32) -> CaptureContext {
    let mut system = System::new();
    let process_chain = build_process_chain(pid, |pid| {
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
    CaptureContext { process_chain }
}

#[derive(Debug, Clone)]
struct ProcessSnapshot {
    identity: ProcessIdentity,
    parent_pid: Option<u32>,
}

fn build_process_chain(
    start_pid: u32,
    mut inspect: impl FnMut(u32) -> Option<ProcessSnapshot>,
) -> Vec<ProcessIdentity> {
    if start_pid == 0 {
        return Vec::new();
    }

    let mut chain = Vec::new();
    let mut seen = HashSet::new();
    let mut current = start_pid;

    while chain.len() < MAX_PROCESS_CHAIN_DEPTH && seen.insert(current) {
        let Some(snapshot) = inspect(current) else {
            break;
        };
        chain.push(snapshot.identity);
        let Some(parent) = snapshot.parent_pid.filter(|parent| *parent != 0) else {
            break;
        };
        current = parent;
    }

    chain
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

        let chain = build_process_chain(30, |pid| processes.get(&pid).cloned());

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

        let chain = build_process_chain(30, |pid| processes.get(&pid).cloned());

        assert_eq!(
            chain
                .iter()
                .map(|identity| identity.pid)
                .collect::<Vec<_>>(),
            vec![30]
        );
    }

    #[test]
    fn stops_at_cycles_and_depth_limit() {
        let cycle = HashMap::from([(30, snapshot(30, Some(20))), (20, snapshot(20, Some(30)))]);
        let chain = build_process_chain(30, |pid| cycle.get(&pid).cloned());
        assert_eq!(
            chain
                .iter()
                .map(|identity| identity.pid)
                .collect::<Vec<_>>(),
            vec![30, 20]
        );

        let chain = build_process_chain(1, |pid| snapshot(pid, Some(pid + 1)).into());
        assert_eq!(chain.len(), MAX_PROCESS_CHAIN_DEPTH);
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
