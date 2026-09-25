//! Daemon-wide registry of coding-agent sessions, their processes, and spans.
//!
//! Agent translators remain source-specific, but they emit ordinary session
//! and tool spans into this common registry. A child's nearest registered
//! coding-agent ancestor is the only possible parent process; a sibling agent
//! under the same shell cannot be mistaken for its parent. One open tool under
//! that process is sufficient to attach directly. Concurrent tools require a
//! unique executable/script-name match on the branch between the two agents.
//! When tools remain ambiguous but all belong to one session, the child is
//! attached to that session span. Across multiple possible sessions, it stays
//! standalone. The registry never guesses from process timing or child text.

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
    session_spans: HashMap<String, ActiveTool>,
    live_sessions: HashSet<String>,
}

#[derive(Clone)]
struct ActiveTool {
    components: SpanComponents,
    route: SessionRoute,
    /// Hashes of executable/script basenames in the tool input. These are
    /// compared only to the captured process path when tools are concurrent.
    command_terms: HashSet<[u8; 32]>,
    active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ActiveParentSnapshot {
    version: u32,
    #[serde(default)]
    dirty: bool,
    correlation_key: String,
    processes: Vec<ProcessIdentity>,
    #[serde(default)]
    session_span: Option<ActiveToolSnapshot>,
    tools: Vec<ActiveToolSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActiveToolSnapshot {
    components: SpanComponents,
    route: SessionRoute,
    #[serde(default)]
    command_terms: Vec<[u8; 32]>,
}

impl CorrelationRegistry {
    pub(crate) fn has_registered_agent_process(&self) -> bool {
        !self.state.lock().unwrap().process_sessions.is_empty()
    }

    pub(crate) fn needs_process_capture(&self, source: &str, session_id: &str) -> bool {
        let prefix = format!("{source}\u{1f}{session_id}\u{1f}");
        let state = self.state.lock().unwrap();
        // Command hooks may need a second request to distinguish the agent
        // from their one-shot `bt` process. Once its mapping is established,
        // later hooks need no process walk until another SessionStart/resume.
        state
            .unconfirmed_processes
            .keys()
            .any(|key| key.starts_with(&prefix))
            || !state
                .session_processes
                .keys()
                .any(|key| key.starts_with(&prefix))
    }

    pub(crate) fn observe_session(
        &self,
        key: &str,
        source: &str,
        capture: Option<&CaptureContext>,
    ) {
        let mut state = self.state.lock().unwrap();
        state.live_sessions.insert(key.to_string());
        let Some(capture) = capture else { return };
        let processes: Vec<_> = capture
            .process_chain
            .iter()
            .skip(usize::from(uses_command_hook(source)))
            .cloned()
            .collect();
        let known = state.session_processes.get(key).cloned();
        let agent = if !uses_command_hook(source) {
            processes
                .first()
                .filter(|process| process.start_time_secs != 0)
                .cloned()
        } else if let Some(previous) = state
            .unconfirmed_processes
            .insert(key.to_string(), processes.clone())
        {
            // A command hook gets a fresh CLI process for each event. The first
            // process shared by successive hooks is the closest stable process
            // owned by this session, even when the launcher adds extra shells.
            let crossed_known_boundary = known.as_ref().is_some_and(|known| {
                let previous_known = previous.iter().any(|process| known.contains(process));
                let current_known = processes.iter().any(|process| known.contains(process));
                previous_known != current_known
            });
            (!crossed_known_boundary)
                .then(|| {
                    processes
                        .iter()
                        .find(|process| previous.contains(process))
                        .filter(|process| process.start_time_secs != 0)
                        .cloned()
                })
                .flatten()
        } else {
            return;
        };
        let Some(agent) = agent else { return };
        state.unconfirmed_processes.remove(key);
        if known.as_ref().is_some_and(|known| known.contains(&agent)) {
            return;
        }
        // A native session may resume in a different agent process. Keep the
        // nearest current process while leaving its parentage decision intact.
        if let Some(previous) = state.session_processes.remove(key) {
            for process in previous {
                if let Some(sessions) = state.process_sessions.get_mut(&process) {
                    sessions.remove(key);
                    if sessions.is_empty() {
                        state.process_sessions.remove(&process);
                    }
                }
            }
        }
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
        let tools: Vec<_> = state
            .active_tools
            .get(key)
            .into_iter()
            .flat_map(|tools| tools.values())
            .filter(|tool| tool.active)
            .map(|tool| ActiveToolSnapshot {
                components: tool.components.clone(),
                route: tool.route.clone(),
                command_terms: tool.command_terms.iter().copied().collect(),
            })
            .collect();
        let session_span = state
            .session_spans
            .get(key)
            .filter(|span| span.active)
            .map(|span| ActiveToolSnapshot {
                components: span.components.clone(),
                route: span.route.clone(),
                command_terms: Vec::new(),
            });
        if tools.is_empty() && session_span.is_none() {
            return None;
        }
        Some(ActiveParentSnapshot {
            version: 3,
            dirty: false,
            correlation_key: key.to_string(),
            processes: state
                .session_processes
                .get(key)
                .map(|processes| processes.iter().cloned().collect())
                .unwrap_or_default(),
            session_span,
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
        if !matches!(snapshot.version, 2 | 3)
            || snapshot.dirty
            || snapshot.processes.len() != 1
            || (restored_tools.is_empty() && snapshot.session_span.is_none())
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
        let tools = state.active_tools.entry(key.clone()).or_default();
        for snapshot in restored_tools {
            let span_id = snapshot.components.span_id.clone().unwrap();
            tools.insert(
                span_id,
                ActiveTool {
                    components: snapshot.components,
                    route: snapshot.route,
                    command_terms: snapshot.command_terms.into_iter().collect(),
                    active: true,
                },
            );
        }
        let has_tools = !tools.is_empty();
        if let Some(span) = snapshot.session_span {
            state.session_spans.insert(
                key.clone(),
                ActiveTool {
                    components: span.components,
                    route: span.route,
                    command_terms: HashSet::new(),
                    active: true,
                },
            );
        }
        state.session_spans.contains_key(&key) || has_tools
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
            if let Some(span) = state.session_spans.get_mut(key) {
                if row.span_id == span.components.span_id.as_deref().unwrap_or_default() {
                    // Terminal root merges close the session as a possible
                    // parent. A later root update on resume can reopen it.
                    let active = row.end_ms.is_none();
                    if span.active != active {
                        span.active = active;
                        changed = true;
                    }
                }
            }
            // The first top-level task belongs to the session itself. Its
            // parent is either empty or the external span that already owns
            // this session. A later turn/task has the session span as parent.
            // Retain this span so ambiguous tools can still attach a child to
            // the *right session* without inventing a tool relationship.
            let external_parent = config.attached_span_ids().0;
            let is_session_span = row.span_type == SpanType::Task
                && row.parent_span_ids == external_parent.into_iter().collect::<Vec<_>>();
            if matches!(op, SpanOp::Insert(_))
                && is_session_span
                && !state.session_spans.contains_key(key)
            {
                state.session_spans.insert(
                    key.to_string(),
                    ActiveTool {
                        components: span_components(config, row),
                        route: route.clone(),
                        command_terms: HashSet::new(),
                        active: true,
                    },
                );
                changed = true;
            }
            if row.end_ms.is_some() {
                if let Some(tools) = state.active_tools.get_mut(key) {
                    if let Some(tool) = tools.get_mut(&row.span_id) {
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
            let command_terms = row.input.as_ref().map(command_terms).unwrap_or_default();
            state
                .active_tools
                .entry(key.to_string())
                .or_default()
                .insert(
                    row.span_id.clone(),
                    ActiveTool {
                        components,
                        route: route.clone(),
                        command_terms,
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
    ) -> Resolution {
        self.resolve_internal(child_session_key, capture, child_agent, None)
    }

    pub(crate) fn resolve_pending(
        &self,
        capture: Option<&CaptureContext>,
        child_agent: Option<&ProcessIdentity>,
        candidate_span_ids: &[String],
    ) -> Resolution {
        self.resolve_internal(None, capture, child_agent, Some(candidate_span_ids))
    }

    fn resolve_internal(
        &self,
        child_session_key: Option<&str>,
        capture: Option<&CaptureContext>,
        child_agent: Option<&ProcessIdentity>,
        candidate_span_ids: Option<&[String]>,
    ) -> Resolution {
        let Some(capture) = capture else {
            return Resolution::Standalone;
        };
        let Some(processes) = ancestors(capture, child_agent) else {
            return Resolution::Standalone;
        };
        let wanted: Option<HashSet<&str>> =
            candidate_span_ids.map(|ids| ids.iter().map(String::as_str).collect());
        let state = self.state.lock().unwrap();
        for process in processes.filter(|process| process.start_time_secs != 0) {
            let Some(sessions) = state.process_sessions.get(process) else {
                continue;
            };
            let mut candidates: Vec<(&str, &ActiveTool)> = Vec::new();
            for session in sessions {
                if child_session_key.is_some_and(|child| child == session) {
                    continue;
                }
                if let Some(tools) = state.active_tools.get(session) {
                    for (span_id, tool) in tools {
                        if wanted
                            .as_ref()
                            .is_some_and(|ids| !ids.contains(span_id.as_str()))
                        {
                            continue;
                        }
                        if tool.active || wanted.is_some() {
                            candidates.push((session, tool));
                        }
                    }
                }
            }
            if candidates.len() == 1 {
                return Resolution::Parent(Box::new(to_link(candidates[0].1.clone())));
            }

            if candidates.len() > 1 {
                // Start times distinguish reused PIDs, not simultaneous calls.
                // Only names from the branch between the two agent processes
                // can distinguish concurrent tool calls. Generic shells and
                // the child agent executable itself are excluded.
                let terms = branch_terms(capture, child_agent, process);
                let mut scored: Vec<_> = candidates
                    .iter()
                    .map(|(_, tool)| tool.command_terms.intersection(&terms).count())
                    .enumerate()
                    .collect();
                scored.sort_by_key(|(_, score)| std::cmp::Reverse(*score));
                if let Some(&(index, best)) = scored.first() {
                    if best > 0 && scored.get(1).is_none_or(|(_, next)| *next < best) {
                        return Resolution::Parent(Box::new(to_link(candidates[index].1.clone())));
                    }
                }
            }

            if child_agent.is_none() && candidates.len() > 1 {
                // A command hook initially exposes only its own CLI process.
                // Wait for a second hook to identify the stable child agent;
                // only then can we inspect the branch below this parent.
                return Resolution::Ambiguous(
                    candidates
                        .iter()
                        .filter_map(|(_, tool)| tool.components.span_id.clone())
                        .collect(),
                );
            }

            // Tool attribution may be ambiguous while session attribution is
            // certain. Attach to that session's span, preserving the trace
            // tree without claiming a particular tool launched the child.
            let candidate_sessions: HashSet<&str> = if candidates.is_empty() && wanted.is_none() {
                sessions
                    .iter()
                    .filter(|session| child_session_key != Some(session.as_str()))
                    .map(String::as_str)
                    .collect()
            } else {
                candidates.iter().map(|(session, _)| *session).collect()
            };
            if candidate_sessions.len() == 1 {
                if let Some(span) = state
                    .session_spans
                    .get(*candidate_sessions.iter().next().unwrap())
                    .filter(|span| span.active)
                {
                    return Resolution::Parent(Box::new(to_link(span.clone())));
                }
            }

            // A nearer registered agent is a hard boundary. Even if its
            // sessions are ambiguous, never search beyond it for an older
            // agent process that happens to have an open tool.
            // No later tool result can prove which concurrent launch owned
            // this child. Once the process branch has been captured, keeping
            // the child pending would only delay an unavoidable decision.
            return Resolution::Standalone;
        }
        Resolution::Standalone
    }

    pub(crate) fn remove_session(&self, key: &str) {
        let mut state = self.state.lock().unwrap();
        state.live_sessions.remove(key);
        state.unconfirmed_processes.remove(key);
        state.active_tools.remove(key);
        state.session_spans.remove(key);
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
    matches!(source, "antigravity" | "claude-code" | "codex" | "grok")
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

fn branch_terms(
    capture: &CaptureContext,
    child_agent: Option<&ProcessIdentity>,
    parent_agent: &ProcessIdentity,
) -> HashSet<[u8; 32]> {
    let Some(child_agent) = child_agent else {
        return HashSet::new();
    };
    let Some(start) = capture.process_chain.iter().position(|p| p == child_agent) else {
        return HashSet::new();
    };
    let Some(end) = capture.process_chain.iter().position(|p| p == parent_agent) else {
        return HashSet::new();
    };
    capture
        .process_labels
        .get(start + 1..end)
        .unwrap_or_default()
        .iter()
        .flatten()
        .filter_map(|label| normalized_command_name(label))
        .map(|name| hash(name.as_bytes()))
        .collect()
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

fn command_terms(value: &Value) -> HashSet<[u8; 32]> {
    let mut strings = Vec::new();
    collect_strings(value, &mut strings);
    strings
        .into_iter()
        .flat_map(|text| {
            text.split(|ch: char| ch.is_whitespace() || "'\";|&()<>".contains(ch))
                .filter_map(normalized_command_name)
                .map(|name| hash(name.as_bytes()))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn normalized_command_name(text: &str) -> Option<String> {
    // Compare whole executable/script names, never arbitrary substrings:
    // `app` must not match `other-app`. Normalize both Unix and Windows paths
    // regardless of the host running a fixture or replay.
    let name = text
        .rsplit(['/', '\\'])
        .next()?
        .trim_matches(|ch: char| !ch.is_alphanumeric() && ch != '.' && ch != '_' && ch != '-');
    let name = name.to_ascii_lowercase();
    let name = name.strip_suffix(".exe").unwrap_or(&name);
    if name.len() < 4
        || matches!(
            name,
            "bash"
                | "zsh"
                | "fish"
                | "cmd"
                | "pwsh"
                | "python"
                | "node"
                | "claude"
                | "codex"
                | "grok"
                | "opencode"
        )
    {
        return None;
    }
    Some(name.to_owned())
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

fn hash(value: &[u8]) -> [u8; 32] {
    Sha256::digest(value).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_span(id: &str, text: &str) -> ActiveTool {
        let mut components = SpanComponents::new(SpanObjectType::ProjectLogs);
        components.span_id = Some(id.into());
        components.root_span_id = Some("trace-root".into());
        ActiveTool {
            components,
            route: SessionRoute::default(),
            command_terms: command_terms(&Value::String(text.into())),
            active: true,
        }
    }

    #[test]
    fn concurrent_tools_use_process_text_then_fall_back_to_session_span() {
        let registry = CorrelationRegistry::default();
        let parent = process(20);
        let child = process(10);
        let wrapper = process(15);
        {
            let mut state = registry.state.lock().unwrap();
            state
                .process_sessions
                .insert(parent.clone(), HashSet::from(["one".into()]));
            state
                .session_spans
                .insert("one".into(), test_span("session", ""));
            state.active_tools.insert(
                "one".into(),
                HashMap::from([
                    ("a".into(), test_span("a", "bash something.sh")),
                    ("b".into(), test_span("b", "./my-app")),
                ]),
            );
        }
        let mut capture = CaptureContext {
            process_chain: vec![child.clone(), wrapper, parent],
            process_labels: vec![
                vec!["codex".into()],
                vec!["bash".into(), "something.sh".into()],
                vec!["claude".into()],
            ],
            truncated: false,
        };
        let parent_id =
            |capture: &CaptureContext| match registry.resolve(None, Some(capture), Some(&child)) {
                Resolution::Parent(link) => match link.route.destination {
                    Some(TraceDestination::ParentSpan { components }) => {
                        components.span_id.unwrap()
                    }
                    _ => panic!("missing span destination"),
                },
                _ => panic!("expected parent"),
            };
        assert_eq!(parent_id(&capture), "a");
        capture.process_labels[1] = vec!["my-app".into()];
        assert_eq!(parent_id(&capture), "b");
        capture.process_labels[1] = vec!["bash".into()];
        assert_eq!(parent_id(&capture), "session");
    }

    #[test]
    fn shared_process_requires_one_session_when_text_cannot_distinguish_calls() {
        let registry = CorrelationRegistry::default();
        let parent = process(20);
        let child = process(10);
        let wrapper = process(15);
        {
            let mut state = registry.state.lock().unwrap();
            state
                .process_sessions
                .insert(parent.clone(), HashSet::from(["one".into(), "two".into()]));
            for (session, id) in [("one", "a"), ("two", "b")] {
                state
                    .session_spans
                    .insert(session.into(), test_span(session, ""));
                state.active_tools.insert(
                    session.into(),
                    HashMap::from([(id.into(), test_span(id, &format!("./{session}.sh")))]),
                );
            }
        }
        let mut capture = CaptureContext {
            process_chain: vec![child.clone(), wrapper, parent],
            process_labels: vec![
                vec!["codex".into()],
                vec!["bash".into()],
                vec!["claude".into()],
            ],
            truncated: false,
        };
        assert!(matches!(
            registry.resolve(None, Some(&capture), Some(&child)),
            Resolution::Standalone
        ));
        capture.process_labels[1].push("two.sh".into());
        assert!(matches!(
            registry.resolve(None, Some(&capture), Some(&child)),
            Resolution::Parent(_)
        ));
    }

    #[test]
    fn completed_session_is_not_a_fallback_parent() {
        let registry = CorrelationRegistry::default();
        let parent = process(20);
        let child = process(10);
        {
            let mut state = registry.state.lock().unwrap();
            state
                .process_sessions
                .insert(parent.clone(), HashSet::from(["one".into()]));
            state
                .session_spans
                .insert("one".into(), test_span("session", ""));
        }
        let capture = CaptureContext {
            process_chain: vec![child.clone(), parent],
            process_labels: Vec::new(),
            truncated: false,
        };
        let config = SessionConfig {
            auth: crate::wire::BackendAuth {
                token: String::new(),
                api_url: None,
                app_url: None,
                org_name: None,
                org_id: None,
            },
            destination: None,
            flush_mode: crate::wire::FlushMode::default(),
            additional_metadata: None,
            tags: Vec::new(),
            span_plugins: Vec::new(),
        };
        assert!(matches!(
            registry.resolve(None, Some(&capture), Some(&child)),
            Resolution::Parent(_)
        ));
        registry.observe_ops(
            "one",
            &SessionRoute::default(),
            &config,
            &[SpanOp::Merge(SpanRow {
                span_id: "session".into(),
                end_ms: Some(1),
                ..Default::default()
            })],
        );
        assert!(matches!(
            registry.resolve(None, Some(&capture), Some(&child)),
            Resolution::Standalone
        ));
    }

    fn process(pid: u32) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            start_time_secs: u64::from(pid),
        }
    }

    #[test]
    fn every_command_hook_agent_is_identified_before_scanning_all_ancestors() {
        let agent = process(10);
        let parent_agent = process(20);
        let first = CaptureContext {
            process_chain: vec![
                process(1),
                process(2),
                agent.clone(),
                process(11),
                process(12),
                parent_agent.clone(),
            ],
            process_labels: Vec::new(),
            truncated: false,
        };
        let latest = CaptureContext {
            process_chain: vec![
                process(3),
                process(4),
                agent.clone(),
                process(11),
                process(12),
                parent_agent.clone(),
            ],
            process_labels: Vec::new(),
            truncated: false,
        };
        for source in ["antigravity", "claude-code", "codex", "grok"] {
            assert!(uses_command_hook(source), "{source} uses a CLI hook");
            assert_eq!(
                session_agent_process(source, &first, Some(&latest)),
                Some(agent.clone())
            );
            let registry = CorrelationRegistry::default();
            registry.observe_session(source, source, Some(&first));
            registry.observe_session(source, source, Some(&latest));
            let state = registry.state.lock().unwrap();
            assert_eq!(
                state.session_processes[source],
                HashSet::from([agent.clone()])
            );
            assert!(!state.process_sessions.contains_key(&parent_agent));
            let ancestors: Vec<_> = ancestors(&latest, Some(&agent)).unwrap().cloned().collect();
            assert_eq!(
                ancestors,
                vec![process(11), process(12), parent_agent.clone()]
            );
        }
        for source in ["pi", "opencode"] {
            assert!(!uses_command_hook(source));
            let in_process = CaptureContext {
                process_chain: vec![
                    agent.clone(),
                    process(11),
                    process(12),
                    parent_agent.clone(),
                ],
                process_labels: Vec::new(),
                truncated: false,
            };
            assert_eq!(
                session_agent_process(source, &in_process, None),
                Some(agent.clone())
            );
        }
    }

    #[test]
    fn every_agent_source_can_parent_every_other_source_through_wrappers() {
        let sources = [
            "antigravity",
            "claude-code",
            "codex",
            "grok",
            "pi",
            "opencode",
        ];
        let parent_agent = process(20);
        let child_agent = process(10);
        for parent_source in sources {
            for child_source in sources {
                let registry = CorrelationRegistry::default();
                let parent_first = CaptureContext {
                    process_chain: if uses_command_hook(parent_source) {
                        vec![process(30), parent_agent.clone()]
                    } else {
                        vec![parent_agent.clone()]
                    },
                    process_labels: Vec::new(),
                    truncated: false,
                };
                registry.observe_session("parent", parent_source, Some(&parent_first));
                if uses_command_hook(parent_source) {
                    let parent_next = CaptureContext {
                        process_chain: vec![process(31), parent_agent.clone()],
                        process_labels: Vec::new(),
                        truncated: false,
                    };
                    registry.observe_session("parent", parent_source, Some(&parent_next));
                }
                let mut components = SpanComponents::new(SpanObjectType::ProjectLogs);
                components.span_id = Some("tool".into());
                registry.state.lock().unwrap().active_tools.insert(
                    "parent".into(),
                    HashMap::from([(
                        "tool".into(),
                        ActiveTool {
                            components,
                            route: SessionRoute::default(),
                            command_terms: HashSet::new(),
                            active: true,
                        },
                    )]),
                );

                let ancestry = vec![
                    child_agent.clone(),
                    process(11),
                    process(12),
                    process(13),
                    parent_agent.clone(),
                ];
                let child_first = CaptureContext {
                    process_chain: if uses_command_hook(child_source) {
                        [vec![process(40), process(41)], ancestry.clone()].concat()
                    } else {
                        ancestry.clone()
                    },
                    process_labels: Vec::new(),
                    truncated: false,
                };
                let child_next = CaptureContext {
                    process_chain: if uses_command_hook(child_source) {
                        [vec![process(42), process(43)], ancestry].concat()
                    } else {
                        child_first.process_chain.clone()
                    },
                    process_labels: Vec::new(),
                    truncated: false,
                };
                let child_agent =
                    session_agent_process(child_source, &child_first, Some(&child_next));
                assert_eq!(
                    child_agent,
                    Some(process(10)),
                    "{parent_source} -> {child_source}"
                );
                assert!(
                    matches!(
                        registry.resolve(None, Some(&child_next), child_agent.as_ref(),),
                        Resolution::Parent(_)
                    ),
                    "{parent_source} -> {child_source} failed through wrapper processes"
                );
            }
        }
    }

    #[test]
    fn resumed_session_replaces_its_agent_process_without_indexing_shared_shell() {
        let registry = CorrelationRegistry::default();
        let old_agent = process(100);
        let new_agent = process(200);
        let shell = process(300);
        let capture = |hook, agent: &ProcessIdentity| CaptureContext {
            process_chain: vec![process(hook), agent.clone(), shell.clone()],
            process_labels: Vec::new(),
            truncated: false,
        };
        registry.observe_session("resumed", "claude-code", Some(&capture(1, &old_agent)));
        registry.observe_session("resumed", "claude-code", Some(&capture(2, &old_agent)));
        assert!(registry
            .state
            .lock()
            .unwrap()
            .process_sessions
            .contains_key(&old_agent));

        // The first new hook shares only the shell with the old process. It
        // cannot establish a new agent identity until another new hook agrees.
        registry.observe_session("resumed", "claude-code", Some(&capture(3, &new_agent)));
        assert!(!registry
            .state
            .lock()
            .unwrap()
            .process_sessions
            .contains_key(&shell));
        registry.observe_session("resumed", "claude-code", Some(&capture(4, &new_agent)));
        let state = registry.state.lock().unwrap();
        assert_eq!(
            state.session_processes["resumed"],
            HashSet::from([new_agent.clone()])
        );
        assert!(!state.process_sessions.contains_key(&old_agent));
        assert!(!state.process_sessions.contains_key(&shell));
        assert!(state.process_sessions[&new_agent].contains("resumed"));
    }
}
