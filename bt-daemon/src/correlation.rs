//! Daemon-wide local correlation between active tool calls and child sessions.
//!
//! Agent translators remain source-specific, but their emitted tool rows are a
//! common contract. This registry observes those rows, indexes the active tools
//! by the coding agent process that owns their session, and resolves a child's
//! requested route to the exact spawning tool span.

use crate::translate::{SpanOp, SpanRow, SpanType};
use crate::wire::{CaptureContext, ProcessIdentity, SessionConfig, SessionRoute, TraceDestination};
use braintrust_sdk_rust::{SpanComponents, SpanObjectType};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub(crate) struct ParentLink {
    pub route: SessionRoute,
}

#[derive(Debug, Clone)]
pub(crate) enum Resolution {
    Standalone,
    Parent(Box<ParentLink>),
    Ambiguous(Vec<String>),
}

#[derive(Default)]
pub(crate) struct CorrelationRegistry {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    process_sessions: HashMap<ProcessIdentity, HashSet<String>>,
    session_processes: HashMap<String, HashSet<ProcessIdentity>>,
    unconfirmed_processes: HashMap<String, Vec<ProcessIdentity>>,
    active_tools: HashMap<String, HashMap<String, ActiveTool>>,
    live_sessions: HashSet<String>,
}

#[derive(Clone)]
struct ActiveTool {
    components: SpanComponents,
    route: SessionRoute,
    fingerprints: HashSet<[u8; 32]>,
    active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ActiveParentSnapshot {
    version: u32,
    #[serde(default)]
    dirty: bool,
    correlation_key: String,
    processes: Vec<ProcessIdentity>,
    tools: Vec<ActiveToolSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActiveToolSnapshot {
    components: SpanComponents,
    route: SessionRoute,
    fingerprints: Vec<[u8; 32]>,
}

impl CorrelationRegistry {
    pub(crate) fn observe_session(
        &self,
        key: &str,
        source: &str,
        capture: Option<&CaptureContext>,
    ) {
        let mut state = self.state.lock().unwrap();
        state.live_sessions.insert(key.to_string());
        let Some(capture) = capture else { return };
        if state.session_processes.contains_key(key) {
            return;
        }
        let processes: Vec<_> = capture
            .process_chain
            .iter()
            .skip(usize::from(uses_command_hook(source)))
            .cloned()
            .collect();
        let agent = if !uses_command_hook(source) {
            processes
                .first()
                .filter(|process| process.start_time_secs != 0)
                .cloned()
        } else if let Some(previous) = state.unconfirmed_processes.get(key) {
            // A command hook gets a fresh CLI process for each event. The first
            // process shared by successive hooks is the closest stable process
            // owned by this session, even when the launcher adds extra shells.
            processes
                .iter()
                .find(|process| previous.contains(process))
                .filter(|process| process.start_time_secs != 0)
                .cloned()
        } else {
            state
                .unconfirmed_processes
                .insert(key.to_string(), processes);
            return;
        };
        let Some(agent) = agent else {
            if uses_command_hook(source) {
                // Process inspection may stop before the agent. Compare the
                // next hook against this newer capture instead.
                state
                    .unconfirmed_processes
                    .insert(key.to_string(), processes);
            }
            return;
        };
        state.unconfirmed_processes.remove(key);
        state
            .session_processes
            .entry(key.to_string())
            .or_default()
            .insert(agent.clone());
        state
            .process_sessions
            .entry(agent)
            .or_default()
            .insert(key.to_string());
    }

    pub(crate) fn active_parent_snapshot(&self, key: &str) -> Option<ActiveParentSnapshot> {
        let state = self.state.lock().unwrap();
        let tools = state.active_tools.get(key)?;
        let tools: Vec<_> = tools
            .values()
            .filter(|tool| tool.active)
            .map(|tool| ActiveToolSnapshot {
                components: tool.components.clone(),
                route: tool.route.clone(),
                fingerprints: tool.fingerprints.iter().copied().collect(),
            })
            .collect();
        if tools.is_empty() {
            return None;
        }
        Some(ActiveParentSnapshot {
            version: 2,
            dirty: false,
            correlation_key: key.to_string(),
            processes: state
                .session_processes
                .get(key)
                .map(|processes| processes.iter().cloned().collect())
                .unwrap_or_default(),
            tools,
        })
    }

    pub(crate) fn dirty_active_parent_snapshot(&self, key: &str) -> Option<ActiveParentSnapshot> {
        self.active_parent_snapshot(key).map(|mut snapshot| {
            snapshot.dirty = true;
            snapshot
        })
    }

    pub(crate) fn restore_active_parent(&self, snapshot: ActiveParentSnapshot) -> bool {
        let restored_tools: Vec<_> = snapshot
            .tools
            .into_iter()
            .filter(|tool| tool.components.span_id.is_some())
            .collect();
        if snapshot.version != 2
            || snapshot.dirty
            || snapshot.processes.len() != 1
            || restored_tools.is_empty()
        {
            return false;
        }
        let mut state = self.state.lock().unwrap();
        let key = snapshot.correlation_key;
        for process in snapshot
            .processes
            .into_iter()
            .filter(|process| process.start_time_secs != 0)
        {
            state
                .session_processes
                .entry(key.clone())
                .or_default()
                .insert(process.clone());
            state
                .process_sessions
                .entry(process)
                .or_default()
                .insert(key.clone());
        }
        if !state.session_processes.contains_key(&key) {
            return false;
        }
        let tools = state.active_tools.entry(key).or_default();
        for snapshot in restored_tools {
            let span_id = snapshot.components.span_id.clone().unwrap();
            tools.insert(
                span_id,
                ActiveTool {
                    components: snapshot.components,
                    route: snapshot.route,
                    fingerprints: snapshot.fingerprints.into_iter().collect(),
                    active: true,
                },
            );
        }
        !tools.is_empty()
    }

    pub(crate) fn observe_ops(
        &self,
        key: &str,
        route: &SessionRoute,
        config: &SessionConfig,
        ops: &[SpanOp],
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        let mut changed = false;
        for op in ops {
            let row = match op {
                SpanOp::Insert(row) | SpanOp::Merge(row) => row,
            };
            if row.span_id.is_empty() {
                continue;
            }
            if row.end_ms.is_some() {
                if let Some(tools) = state.active_tools.get_mut(key) {
                    if let Some(tool) = tools.get_mut(&row.span_id) {
                        if let Some(output) = &row.output {
                            tool.fingerprints.extend(fingerprints(output));
                        }
                        tool.active = false;
                        changed = true;
                    }
                }
                continue;
            }
            if row.span_type != SpanType::Tool {
                continue;
            }
            if !matches!(op, SpanOp::Insert(_)) {
                continue;
            }
            let components = span_components(config, row);
            let fingerprints = row.input.as_ref().map(fingerprints).unwrap_or_default();
            state
                .active_tools
                .entry(key.to_string())
                .or_default()
                .insert(
                    row.span_id.clone(),
                    ActiveTool {
                        components,
                        route: route.clone(),
                        fingerprints,
                        active: true,
                    },
                );
            changed = true;
        }
        changed
    }

    pub(crate) fn resolve(
        &self,
        child_session_key: Option<&str>,
        capture: Option<&CaptureContext>,
        child_agent: Option<&ProcessIdentity>,
        evidence: &Value,
    ) -> Resolution {
        let Some(capture) = capture else {
            return Resolution::Standalone;
        };
        let Some(processes) = ancestors(capture, child_agent) else {
            return Resolution::Standalone;
        };
        let state = self.state.lock().unwrap();
        for process in processes.filter(|process| process.start_time_secs != 0) {
            let Some(sessions) = state.process_sessions.get(process) else {
                continue;
            };
            let mut candidates = Vec::new();
            for session in sessions {
                if child_session_key.is_some_and(|child| child == session) {
                    continue;
                }
                if let Some(tools) = state.active_tools.get(session) {
                    candidates.extend(tools.values().filter(|tool| tool.active).cloned());
                }
            }
            if candidates.is_empty() {
                return Resolution::Standalone;
            }
            if candidates.len() == 1 {
                return Resolution::Parent(Box::new(to_link(candidates.pop().unwrap())));
            }

            let candidate_span_ids = candidates
                .iter()
                .filter_map(|candidate| candidate.components.span_id.clone())
                .collect();
            let evidence = fingerprints(evidence);
            let mut scored: Vec<(usize, ActiveTool)> = candidates
                .into_iter()
                .map(|candidate| {
                    let score = candidate.fingerprints.intersection(&evidence).count();
                    (score, candidate)
                })
                .filter(|(score, _)| *score > 0)
                .collect();
            scored.sort_by_key(|(score, _)| std::cmp::Reverse(*score));
            if let Some((best_score, best)) = scored.first().cloned() {
                let tied = scored
                    .get(1)
                    .is_some_and(|(second, _)| *second == best_score);
                if !tied {
                    return Resolution::Parent(Box::new(to_link(best)));
                }
            }
            return Resolution::Ambiguous(candidate_span_ids);
        }
        Resolution::Standalone
    }

    pub(crate) fn resolve_pending(
        &self,
        capture: Option<&CaptureContext>,
        child_agent: Option<&ProcessIdentity>,
        evidence: &Value,
        candidate_span_ids: &[String],
    ) -> Resolution {
        let Some(capture) = capture else {
            return Resolution::Standalone;
        };
        let Some(child_agent) = child_agent else {
            return Resolution::Standalone;
        };
        let Some(processes) = ancestors(capture, Some(child_agent)) else {
            return Resolution::Standalone;
        };
        let wanted: HashSet<&str> = candidate_span_ids.iter().map(String::as_str).collect();
        let evidence = fingerprints(evidence);
        let state = self.state.lock().unwrap();
        for process in processes.filter(|process| process.start_time_secs != 0) {
            let Some(sessions) = state.process_sessions.get(process) else {
                continue;
            };
            let mut scored = Vec::new();
            let mut found = 0usize;
            let mut any_active = false;
            for session in sessions {
                let Some(tools) = state.active_tools.get(session) else {
                    continue;
                };
                for (span_id, tool) in tools {
                    if wanted.contains(span_id.as_str()) {
                        found += 1;
                        any_active |= tool.active;
                        let score = tool.fingerprints.intersection(&evidence).count();
                        if score > 0 {
                            scored.push((score, tool.clone()));
                        }
                    }
                }
            }
            if found == 0 {
                // A nearer agent owns this process. Older candidates cannot
                // be reached by stepping over that agent.
                return Resolution::Standalone;
            }
            scored.sort_by_key(|(score, _)| std::cmp::Reverse(*score));
            if let Some((best_score, best)) = scored.first().cloned() {
                let tied = scored
                    .get(1)
                    .is_some_and(|(second, _)| *second == best_score);
                if !tied {
                    return Resolution::Parent(Box::new(to_link(best)));
                }
            }
            if found == wanted.len() && !any_active {
                return Resolution::Standalone;
            }
            return Resolution::Ambiguous(candidate_span_ids.to_vec());
        }
        Resolution::Standalone
    }

    pub(crate) fn remove_session(&self, key: &str) {
        let mut state = self.state.lock().unwrap();
        state.live_sessions.remove(key);
        state.unconfirmed_processes.remove(key);
        state.active_tools.remove(key);
        if let Some(processes) = state.session_processes.remove(key) {
            for process in processes {
                if let Some(sessions) = state.process_sessions.get_mut(&process) {
                    sessions.remove(key);
                    if sessions.is_empty() {
                        state.process_sessions.remove(&process);
                    }
                }
            }
        }
    }

    pub(crate) fn has_active_tools(&self, key: &str) -> bool {
        self.state
            .lock()
            .unwrap()
            .active_tools
            .get(key)
            .is_some_and(|tools| tools.values().any(|tool| tool.active))
    }

    pub(crate) fn has_any_active_tools(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.live_sessions.iter().any(|key| {
            state
                .active_tools
                .get(key)
                .is_some_and(|tools| tools.values().any(|tool| tool.active))
        })
    }
}

pub(crate) fn uses_command_hook(source: &str) -> bool {
    // Only the connecting CLI process is known to be outside the agent. The
    // number of shell or launcher processes between it and the agent varies.
    matches!(source, "claude-code" | "codex")
}

pub(crate) fn session_agent_process(
    source: &str,
    previous: &CaptureContext,
    latest: Option<&CaptureContext>,
) -> Option<ProcessIdentity> {
    if !uses_command_hook(source) {
        return previous
            .process_chain
            .first()
            .filter(|process| process.start_time_secs != 0)
            .cloned();
    }
    let latest = latest?;
    latest
        .process_chain
        .iter()
        .skip(1)
        .find(|process| {
            previous
                .process_chain
                .iter()
                .skip(1)
                .any(|prior| prior == *process)
        })
        .filter(|process| process.start_time_secs != 0)
        .cloned()
}

fn ancestors<'a>(
    capture: &'a CaptureContext,
    child_agent: Option<&ProcessIdentity>,
) -> Option<impl Iterator<Item = &'a ProcessIdentity>> {
    let start = if let Some(child_agent) = child_agent {
        capture
            .process_chain
            .iter()
            .position(|process| process == child_agent)?
            + 1
    } else {
        0
    };
    Some(capture.process_chain.iter().skip(start))
}

fn to_link(tool: ActiveTool) -> ParentLink {
    let mut route = tool.route;
    route.destination = Some(TraceDestination::ParentSpan {
        components: tool.components.clone(),
    });
    ParentLink { route }
}

fn span_components(config: &SessionConfig, row: &SpanRow) -> SpanComponents {
    let mut object_type = SpanObjectType::ProjectLogs;
    let mut object_id = None;
    let mut compute_object_metadata_args = None;
    let mut propagated_event = None;
    let mut effective_root = row.root_span_id.clone();
    match config.destination.as_ref() {
        Some(TraceDestination::ProjectLogs {
            project_id,
            project_name,
        }) => {
            object_id = project_id.clone();
            let mut args = Map::new();
            if let Some(project_id) = project_id {
                args.insert("project_id".into(), Value::String(project_id.clone()));
            }
            if let Some(project_name) = project_name {
                args.insert("project_name".into(), Value::String(project_name.clone()));
            }
            compute_object_metadata_args = (!args.is_empty()).then_some(args);
        }
        Some(TraceDestination::Experiment { experiment_id }) => {
            object_type = SpanObjectType::Experiment;
            object_id = Some(experiment_id.clone());
        }
        Some(TraceDestination::ParentSpan { components }) => {
            object_type = components.object_type;
            object_id = components.object_id.clone();
            compute_object_metadata_args = components.compute_object_metadata_args.clone();
            propagated_event = components.propagated_event.clone();
            if let Some(root) = &components.root_span_id {
                effective_root = root.clone();
            }
        }
        None => {}
    }
    SpanComponents {
        object_type,
        object_id,
        compute_object_metadata_args,
        row_id: Some(row.span_id.clone()),
        span_id: Some(row.span_id.clone()),
        root_span_id: Some(effective_root),
        span_parents: (!row.parent_span_ids.is_empty()).then(|| row.parent_span_ids.clone()),
        propagated_event,
    }
}

fn fingerprints(value: &Value) -> HashSet<[u8; 32]> {
    let mut strings = Vec::new();
    collect_strings(value, &mut strings);
    let mut result = HashSet::new();
    for value in strings {
        let normalized = normalize(&value);
        if normalized.len() >= 8 {
            result.insert(hash(normalized.as_bytes()));
        }
        let tokens: Vec<&str> = normalized.split_whitespace().collect();
        for width in 3..=tokens.len().min(8) {
            for window in tokens.windows(width) {
                result.insert(hash(window.join(" ").as_bytes()));
            }
        }
    }
    result
}

fn collect_strings(value: &Value, strings: &mut Vec<String>) {
    match value {
        Value::String(value) => strings.push(value.clone()),
        Value::Array(values) => values
            .iter()
            .for_each(|value| collect_strings(value, strings)),
        Value::Object(values) => values
            .values()
            .for_each(|value| collect_strings(value, strings)),
        _ => {}
    }
}

fn normalize(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn hash(value: &[u8]) -> [u8; 32] {
    Sha256::digest(value).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprints_match_prompt_subsets_without_parsing_shell() {
        let parent = fingerprints(&serde_json::json!({
            "command": "agent --prompt 'inspect the distributed tracing linkage carefully please'"
        }));
        let child = fingerprints(&serde_json::json!({
            "prompt": "inspect the distributed tracing linkage carefully please"
        }));
        assert!(parent.intersection(&child).count() > 0);
    }
}
