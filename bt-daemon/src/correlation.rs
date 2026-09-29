//! Daemon-wide registry of coding-agent sessions, their processes, and spans.
//!
//! Agent translators remain source-specific, but they emit ordinary session
//! and tool spans into this common registry. A child's nearest registered
//! coding-agent ancestor is the only possible parent process; a sibling agent
//! under the same shell cannot be mistaken for its parent. Exactly one open
//! native session and one compatible delivery route must own that process.
//! A single open tool becomes the parent; otherwise its active session span is used.
//! Unknown child processes hold a frozen candidate
//! until strict ancestry is confirmed. Tool text and later tool activity never
//! change that initial choice.

use crate::translate::{SpanOp, SpanRow, SpanType};
use crate::wire::{CaptureContext, ProcessIdentity, SessionConfig, SessionRoute, TraceDestination};
use braintrust_sdk_rust::{SpanComponents, SpanObjectType};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub(crate) struct ParentLink {
    pub route: SessionRoute,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ParentCandidate {
    pub(crate) process: ProcessIdentity,
    pub(crate) route: SessionRoute,
}

#[derive(Debug, Clone)]
pub(crate) enum Resolution {
    Standalone,
    Parent(Box<ParentLink>),
    Pending(Box<ParentCandidate>),
}

#[derive(Default)]
pub(crate) struct CorrelationRegistry {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    process_sessions: HashMap<ProcessIdentity, HashSet<String>>,
    session_processes: HashMap<String, HashSet<ProcessIdentity>>,
    unconfirmed_processes: HashMap<String, CaptureContext>,
    // A native start opened a new process generation that has not yet
    // confirmed its agent. The previous agent remains registered meanwhile.
    open_generations: HashSet<String>,
    // Idle sessions whose delivery pipeline was retired but whose agent may
    // still run a turn. Their process stays registered until it exits.
    retired: HashSet<String>,
    active_tools: HashMap<String, HashMap<String, ActiveTool>>,
    session_spans: HashMap<String, ActiveTool>,
    live_sessions: HashSet<String>,
}

#[derive(Clone)]
struct ActiveTool {
    components: SpanComponents,
    route: SessionRoute,
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
}

impl CorrelationRegistry {
    pub(crate) fn needs_process_capture(&self, source: &str, session_id: &str) -> bool {
        let prefix = format!("{source}\u{1f}{session_id}\u{1f}");
        let state = self.state.lock().unwrap();
        // Command hooks may need a second request to distinguish the agent
        // from their one-shot `bt` process. Once its mapping is established,
        // later hooks need no process walk until another SessionStart/resume.
        state
            .unconfirmed_processes
            .keys()
            .chain(state.open_generations.iter())
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
        session_start: bool,
    ) {
        let mut state = self.state.lock().unwrap();
        state.live_sessions.insert(key.to_string());
        state.retired.remove(key);
        if session_start {
            // A start may be a resume in another process, or a compaction in
            // the same one. Keep the known agent until its replacement is
            // confirmed so children launched meanwhile still find it.
            state.unconfirmed_processes.remove(key);
            state.open_generations.insert(key.to_string());
        } else if state.session_processes.contains_key(key) && !state.open_generations.contains(key)
        {
            // Once confirmed, ordinary hooks cannot replace an agent with a
            // shared shell. Only an explicit native start opens a generation.
            return;
        }
        let Some(capture) = capture else { return };
        let agent = if uses_command_hook(source) {
            let Some(previous) = state.unconfirmed_processes.get(key) else {
                state
                    .unconfirmed_processes
                    .insert(key.to_string(), capture.clone());
                return;
            };
            session_agent_process(source, previous, Some(capture))
        } else {
            session_agent_process(source, capture, None)
        };
        let Some(agent) = agent else {
            if uses_command_hook(source) {
                state
                    .unconfirmed_processes
                    .insert(key.to_string(), capture.clone());
            }
            return;
        };
        state.unconfirmed_processes.remove(key);
        state.open_generations.remove(key);
        remove_process_mapping(&mut state, key);
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
            })
            .collect();
        let session_span = state
            .session_spans
            .get(key)
            .filter(|span| span.active)
            .map(|span| ActiveToolSnapshot {
                components: span.components.clone(),
                route: span.route.clone(),
            });
        if tools.is_empty() && session_span.is_none() {
            return None;
        }
        Some(ActiveParentSnapshot {
            version: 4,
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
        if snapshot.version != 4
            || snapshot.dirty
            || snapshot.processes.len() != 1
            || (restored_tools.is_empty() && snapshot.session_span.is_none())
        {
            return false;
        }
        let mut state = self.state.lock().unwrap();
        let key = snapshot.correlation_key;
        for process in snapshot.processes.into_iter().filter(valid_process) {
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
            state
                .active_tools
                .entry(key.to_string())
                .or_default()
                .insert(
                    row.span_id.clone(),
                    ActiveTool {
                        components,
                        route: route.clone(),
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
        parent_route: impl Fn(&SessionRoute) -> Option<SessionRoute>,
    ) -> Resolution {
        let Some(capture) = capture else {
            return Resolution::Standalone;
        };
        let Some(processes) = ancestors(capture, child_agent) else {
            return Resolution::Standalone;
        };
        let state = self.state.lock().unwrap();
        for process in processes.filter(|process| valid_process(process)) {
            let Some(sessions) = state.process_sessions.get(process) else {
                continue;
            };
            // The nearest registered agent is a hard boundary. Count native
            // sessions, not their independent delivery routes, before filtering
            // routes so an incompatible session cannot hide ambiguity.
            // Ended or retired sessions cannot parent and do not count.
            let mut open = sessions
                .iter()
                .filter(|session| session_is_open(&state, session));
            let Some(session) = open.next() else {
                return Resolution::Standalone;
            };
            if open
                .clone()
                .any(|other| native_session_key(other) != native_session_key(session))
            {
                return Resolution::Standalone;
            }
            // Keys are `source␟session␟route`; a session never parents itself
            // under any route.
            if child_session_key.is_some_and(|child| session.starts_with(child)) {
                return Resolution::Standalone;
            }
            let mut selected = None;
            for session in std::iter::once(session).chain(open) {
                let mut tools = state
                    .active_tools
                    .get(session)
                    .into_iter()
                    .flat_map(|tools| tools.values())
                    .filter(|tool| tool.active);
                let first = tools.next();
                let parent = if first.is_some() && tools.next().is_none() {
                    first
                } else {
                    state.session_spans.get(session).filter(|span| span.active)
                };
                let Some(route) = parent.and_then(|parent| parent_route(&to_link(parent).route))
                else {
                    continue;
                };
                // Multiple compatible pipelines can disagree about the active
                // parent span. Never let HashSet iteration choose one.
                if selected.is_some() {
                    return Resolution::Standalone;
                }
                selected = Some(route);
            }
            let Some(route) = selected else {
                return Resolution::Standalone;
            };
            return if child_agent.is_some() {
                Resolution::Parent(Box::new(ParentLink { route }))
            } else {
                Resolution::Pending(Box::new(ParentCandidate {
                    process: process.clone(),
                    route,
                }))
            };
        }
        Resolution::Standalone
    }

    pub(crate) fn confirm_parent(
        candidate: &ParentCandidate,
        capture: &CaptureContext,
        child_agent: &ProcessIdentity,
    ) -> Resolution {
        if valid_process(&candidate.process)
            && ancestors(capture, Some(child_agent))
                .is_some_and(|mut ancestors| ancestors.any(|process| process == &candidate.process))
        {
            Resolution::Parent(Box::new(ParentLink {
                route: candidate.route.clone(),
            }))
        } else {
            Resolution::Standalone
        }
    }

    #[cfg(test)]
    pub(crate) fn remove_session(&self, key: &str) {
        let mut state = self.state.lock().unwrap();
        forget_spans(&mut state, key);
        state.retired.remove(key);
        remove_process_mapping(&mut state, key);
    }

    /// Release an idle session's spans but keep its confirmed agent process.
    /// Command-hook agents need two hooks to be relearned; a turn resuming
    /// after a long pause must still parent a child launched by its next tool.
    pub(crate) fn retire_session(&self, key: &str) {
        let mut state = self.state.lock().unwrap();
        forget_spans(&mut state, key);
        if state.session_processes.contains_key(key) {
            state.retired.insert(key.to_string());
        }
    }

    /// Forget retired sessions whose agent processes have all exited.
    pub(crate) fn prune_retired(&self, alive: impl Fn(&ProcessIdentity) -> bool) {
        let retired: Vec<_> = {
            let state = self.state.lock().unwrap();
            state
                .retired
                .iter()
                .map(|key| {
                    let processes: Vec<_> = state
                        .session_processes
                        .get(key)
                        .into_iter()
                        .flatten()
                        .cloned()
                        .collect();
                    (key.clone(), processes)
                })
                .collect()
        };
        let exited: Vec<_> = retired
            .into_iter()
            .filter(|(_, processes)| !processes.iter().any(&alive))
            .map(|(key, _)| key)
            .collect();
        let mut state = self.state.lock().unwrap();
        for key in exited {
            // A late event may have revived the session while we checked.
            if state.retired.remove(&key) {
                remove_process_mapping(&mut state, &key);
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

fn forget_spans(state: &mut State, key: &str) {
    state.live_sessions.remove(key);
    state.unconfirmed_processes.remove(key);
    state.open_generations.remove(key);
    state.active_tools.remove(key);
    state.session_spans.remove(key);
}

fn native_session_key(key: &str) -> &str {
    // Delivery keys are `source␟session␟route`.
    key.rsplit_once('\u{1f}').map_or(key, |(native, _)| native)
}

fn session_is_open(state: &State, key: &str) -> bool {
    state
        .active_tools
        .get(key)
        .is_some_and(|tools| tools.values().any(|tool| tool.active))
        || state.session_spans.get(key).is_some_and(|span| span.active)
}

fn remove_process_mapping(state: &mut State, key: &str) {
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

fn valid_process(process: &ProcessIdentity) -> bool {
    process.pid != 0 && process.start_time_secs != 0
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
            .filter(|process| valid_process(process))
            .cloned();
    }
    let latest = latest?;
    let previous_hook = previous.process_chain.first()?;
    let latest_hook = latest.process_chain.first()?;
    if !valid_process(previous_hook) || !valid_process(latest_hook) {
        return None;
    }
    if previous_hook == latest_hook {
        // Windows reuses PIDs quickly, so two one-shot hooks can share an
        // identity within one second. They are distinct only if neither
        // hook's parent appears in the other chain; otherwise this is one hook
        // seen twice, possibly after being reparented to a shared ancestor.
        let distinct = match (previous.process_chain.get(1), latest.process_chain.get(1)) {
            (Some(prior), Some(parent)) => {
                !previous.process_chain.contains(parent) && !latest.process_chain.contains(prior)
            }
            _ => false,
        };
        if !distinct {
            return None;
        }
    }
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
        .filter(|process| valid_process(process))
        .cloned()
}

fn ancestors<'a>(
    capture: &'a CaptureContext,
    child_agent: Option<&ProcessIdentity>,
) -> Option<impl Iterator<Item = &'a ProcessIdentity>> {
    let start = if let Some(child_agent) = child_agent {
        if !valid_process(child_agent) {
            return None;
        }
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

fn to_link(tool: &ActiveTool) -> ParentLink {
    let mut route = tool.route.clone();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn process(pid: u32) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            start_time_secs: u64::from(pid),
        }
    }

    fn capture(pids: &[u32]) -> CaptureContext {
        CaptureContext {
            process_chain: pids.iter().copied().map(process).collect(),
            truncated: false,
        }
    }

    fn config() -> SessionConfig {
        SessionRoute::default().with_auth(crate::wire::BackendAuth {
            token: String::new(),
            api_url: None,
            app_url: None,
            org_name: None,
            org_id: None,
        })
    }

    fn open_span(registry: &CorrelationRegistry, key: &str, id: &str, span_type: SpanType) {
        registry.observe_ops(
            key,
            &SessionRoute::default(),
            &config(),
            &[SpanOp::Insert(SpanRow {
                span_id: id.into(),
                root_span_id: "trace-root".into(),
                span_type,
                ..Default::default()
            })],
        );
    }

    fn close_span(registry: &CorrelationRegistry, key: &str, id: &str) {
        registry.observe_ops(
            key,
            &SessionRoute::default(),
            &config(),
            &[SpanOp::Merge(SpanRow {
                span_id: id.into(),
                end_ms: Some(1),
                ..Default::default()
            })],
        );
    }

    fn parent_registry() -> CorrelationRegistry {
        let registry = CorrelationRegistry::default();
        registry.observe_session("parent", "pi", Some(&capture(&[20])), true);
        open_span(&registry, "parent", "session", SpanType::Task);
        registry
    }

    fn parent_id(resolution: Resolution) -> String {
        let Resolution::Parent(link) = resolution else {
            panic!("expected confirmed parent, got {resolution:?}");
        };
        let Some(TraceDestination::ParentSpan { components }) = link.route.destination else {
            panic!("missing parent destination");
        };
        components.span_id.unwrap()
    }

    #[test]
    fn one_tool_wins_but_concurrent_tools_and_no_tools_use_active_session() {
        let registry = parent_registry();
        let child = process(10);
        let lineage = capture(&[10, 15, 20]);
        let resolve = || {
            parent_id(
                registry.resolve(None, Some(&lineage), Some(&child), |route| {
                    Some(route.clone())
                }),
            )
        };
        assert_eq!(resolve(), "session");
        open_span(&registry, "parent", "a", SpanType::Tool);
        assert_eq!(resolve(), "a");
        open_span(&registry, "parent", "b", SpanType::Tool);
        assert_eq!(resolve(), "session");
        close_span(&registry, "parent", "a");
        assert_eq!(resolve(), "b");
        close_span(&registry, "parent", "b");
        assert_eq!(resolve(), "session");
        close_span(&registry, "parent", "session");
        assert!(matches!(
            registry.resolve(None, Some(&lineage), Some(&child), |route| Some(
                route.clone()
            )),
            Resolution::Standalone
        ));
    }

    #[test]
    fn pending_parent_is_frozen_across_tool_churn_and_parent_removal() {
        for concurrent in [false, true] {
            let registry = parent_registry();
            open_span(&registry, "parent", "original", SpanType::Tool);
            if concurrent {
                open_span(&registry, "parent", "concurrent", SpanType::Tool);
            }
            let Resolution::Pending(candidate) =
                registry.resolve(None, Some(&capture(&[1, 10, 20])), None, |route| {
                    Some(route.clone())
                })
            else {
                panic!("unknown hook child must await process confirmation");
            };
            close_span(&registry, "parent", "original");
            open_span(&registry, "parent", "later", SpanType::Tool);
            registry.remove_session("parent");
            assert_eq!(
                parent_id(CorrelationRegistry::confirm_parent(
                    &candidate,
                    &capture(&[2, 10, 20]),
                    &process(10),
                )),
                if concurrent { "session" } else { "original" }
            );
        }
    }

    #[test]
    fn multiple_sessions_are_a_hard_boundary_even_with_only_one_open_tool() {
        let registry = parent_registry();
        registry.observe_session("other", "pi", Some(&capture(&[20])), true);
        open_span(&registry, "other", "other-session", SpanType::Task);
        open_span(&registry, "parent", "tool", SpanType::Tool);
        registry.observe_session("grandparent", "pi", Some(&capture(&[30])), true);
        open_span(&registry, "grandparent", "outer", SpanType::Tool);
        let lineage = capture(&[10, 20, 30]);
        for child_key in [None, Some("other")] {
            assert!(matches!(
                registry.resolve(child_key, Some(&lineage), Some(&process(10)), |route| Some(
                    route.clone()
                )),
                Resolution::Standalone
            ));
        }
        // Even if only one session has a compatible route, the process still
        // belongs to two native sessions and cannot identify the parent.
        assert!(matches!(
            registry.resolve(None, Some(&lineage), Some(&process(10)), |route| {
                matches!(
                    &route.destination,
                    Some(TraceDestination::ParentSpan { components })
                        if components.span_id.as_deref() == Some("tool")
                )
                .then(|| route.clone())
            }),
            Resolution::Standalone
        ));
        // An ended session in the same process (`/clear`, `/new`) cannot
        // parent, so it no longer hides the one open session.
        close_span(&registry, "other", "other-session");
        assert_eq!(
            parent_id(
                registry.resolve(None, Some(&lineage), Some(&process(10)), |route| Some(
                    route.clone()
                ))
            ),
            "tool"
        );
        registry.remove_session("other");
        assert_eq!(
            parent_id(
                registry.resolve(None, Some(&lineage), Some(&process(10)), |route| Some(
                    route.clone()
                ))
            ),
            "tool"
        );
        close_span(&registry, "parent", "tool");
        close_span(&registry, "parent", "session");
        assert!(matches!(
            registry.resolve(None, Some(&lineage), Some(&process(10)), |route| Some(
                route.clone()
            )),
            Resolution::Standalone
        ));
    }

    #[test]
    fn pending_confirmation_requires_original_parent_strictly_above_child() {
        let registry = parent_registry();
        let Resolution::Pending(candidate) =
            registry.resolve(None, Some(&capture(&[1, 20, 30])), None, |route| {
                Some(route.clone())
            })
        else {
            panic!("expected candidate");
        };
        for (lineage, child) in [
            (capture(&[2, 20, 30]), process(20)),
            (capture(&[2, 10, 30]), process(10)),
            (capture(&[2, 20, 30]), process(10)),
        ] {
            assert!(matches!(
                CorrelationRegistry::confirm_parent(&candidate, &lineage, &child),
                Resolution::Standalone
            ));
        }
        assert_eq!(
            parent_id(CorrelationRegistry::confirm_parent(
                &candidate,
                &capture(&[2, 10, 20, 30]),
                &process(10),
            )),
            "session"
        );
        // A sibling shares the shell, but not the registered parent process.
        assert!(matches!(
            registry.resolve(
                None,
                Some(&capture(&[10, 30])),
                Some(&process(10)),
                |route| Some(route.clone())
            ),
            Resolution::Standalone
        ));
    }

    #[test]
    fn reused_and_unknown_process_identities_never_prove_parentage() {
        let registry = parent_registry();
        let Resolution::Pending(candidate) =
            registry.resolve(None, Some(&capture(&[1, 10, 20])), None, |route| {
                Some(route.clone())
            })
        else {
            panic!("expected candidate");
        };
        for start_time_secs in [0, 999] {
            let mut lineage = capture(&[2, 10, 20]);
            lineage.process_chain[2].start_time_secs = start_time_secs;
            assert!(matches!(
                registry.resolve(None, Some(&lineage), Some(&process(10)), |route| Some(
                    route.clone()
                )),
                Resolution::Standalone
            ));
            assert!(matches!(
                CorrelationRegistry::confirm_parent(&candidate, &lineage, &process(10)),
                Resolution::Standalone
            ));
        }
        let mut unknown = process(10);
        unknown.start_time_secs = 0;
        let lineage = CaptureContext {
            process_chain: vec![unknown.clone(), process(20)],
            truncated: false,
        };
        assert!(matches!(
            CorrelationRegistry::confirm_parent(&candidate, &lineage, &unknown),
            Resolution::Standalone
        ));
    }

    #[test]
    fn repeated_hook_capture_cannot_confirm_a_session() {
        let registry = CorrelationRegistry::default();
        let first = capture(&[1, 2, 20]);
        registry.observe_session("parent", "claude-code", Some(&first), true);
        registry.observe_session("parent", "claude-code", Some(&first), false);
        open_span(&registry, "parent", "tool", SpanType::Tool);
        assert_eq!(
            session_agent_process("claude-code", &first, Some(&first)),
            None
        );
        assert!(matches!(
            registry.resolve(
                None,
                Some(&capture(&[10, 2, 20])),
                Some(&process(10)),
                |route| Some(route.clone())
            ),
            Resolution::Standalone
        ));
        registry.observe_session("parent", "claude-code", Some(&capture(&[3, 4, 20])), false);
        assert_eq!(
            parent_id(registry.resolve(
                None,
                Some(&capture(&[10, 20])),
                Some(&process(10)),
                |route| Some(route.clone())
            )),
            "tool"
        );
    }

    #[test]
    fn resume_keeps_known_agent_until_new_generation_confirms_without_shared_shell() {
        for old_confirmed in [false, true] {
            let registry = CorrelationRegistry::default();
            registry.observe_session("parent", "claude-code", Some(&capture(&[1, 20, 30])), true);
            if old_confirmed {
                registry.observe_session(
                    "parent",
                    "claude-code",
                    Some(&capture(&[2, 20, 30])),
                    false,
                );
            }
            open_span(&registry, "parent", "session", SpanType::Task);
            registry.observe_session("parent", "claude-code", Some(&capture(&[3, 40, 30])), true);
            let old_agent = registry.resolve(
                None,
                Some(&capture(&[10, 20, 30])),
                Some(&process(10)),
                |route| Some(route.clone()),
            );
            if old_confirmed {
                assert_eq!(parent_id(old_agent), "session");
            } else {
                assert!(matches!(old_agent, Resolution::Standalone));
            }
            for lineage in [capture(&[10, 40, 30]), capture(&[10, 30])] {
                assert!(matches!(
                    registry.resolve(None, Some(&lineage), Some(&process(10)), |route| Some(
                        route.clone()
                    )),
                    Resolution::Standalone
                ));
            }
            registry.observe_session("parent", "claude-code", Some(&capture(&[4, 40, 30])), false);
            assert_eq!(
                parent_id(registry.resolve(
                    None,
                    Some(&capture(&[10, 40, 30])),
                    Some(&process(10)),
                    |route| Some(route.clone())
                )),
                "session"
            );
            // The confirmed replacement supersedes the previous agent.
            assert!(matches!(
                registry.resolve(
                    None,
                    Some(&capture(&[10, 20, 30])),
                    Some(&process(10)),
                    |route| Some(route.clone())
                ),
                Resolution::Standalone
            ));
            // Missing/truncated ordinary events must not discard a known agent.
            registry.observe_session("parent", "claude-code", Some(&capture(&[5, 30])), false);
            registry.observe_session("parent", "claude-code", None, false);
            assert_eq!(
                parent_id(registry.resolve(
                    None,
                    Some(&capture(&[10, 40, 30])),
                    Some(&process(10)),
                    |route| Some(route.clone())
                )),
                "session"
            );
            // A start without evidence (e.g. compaction) keeps the agent.
            registry.observe_session("parent", "claude-code", None, true);
            assert_eq!(
                parent_id(registry.resolve(
                    None,
                    Some(&capture(&[10, 40, 30])),
                    Some(&process(10)),
                    |route| Some(route.clone())
                )),
                "session"
            );
        }
    }

    #[test]
    fn compaction_start_in_the_same_process_keeps_parenting_children() {
        let registry = CorrelationRegistry::default();
        registry.observe_session("parent", "claude-code", Some(&capture(&[1, 20, 30])), true);
        registry.observe_session("parent", "claude-code", Some(&capture(&[2, 20, 30])), false);
        open_span(&registry, "parent", "session", SpanType::Task);
        registry.observe_session("parent", "claude-code", Some(&capture(&[3, 20, 30])), true);
        open_span(&registry, "parent", "tool", SpanType::Tool);
        assert_eq!(
            parent_id(registry.resolve(
                None,
                Some(&capture(&[10, 20, 30])),
                Some(&process(10)),
                |route| Some(route.clone())
            )),
            "tool"
        );
        registry.observe_session("parent", "claude-code", Some(&capture(&[4, 20, 30])), false);
        assert_eq!(
            parent_id(registry.resolve(
                None,
                Some(&capture(&[10, 20, 30])),
                Some(&process(10)),
                |route| Some(route.clone())
            )),
            "tool"
        );
    }

    #[test]
    fn retired_session_keeps_its_agent_until_the_process_exits() {
        let registry = CorrelationRegistry::default();
        registry.observe_session("parent", "claude-code", Some(&capture(&[1, 20, 30])), true);
        registry.observe_session("parent", "claude-code", Some(&capture(&[2, 20, 30])), false);
        open_span(&registry, "parent", "session", SpanType::Task);
        registry.retire_session("parent");
        // A retired session has no open span, so it cannot parent by itself.
        assert!(matches!(
            registry.resolve(
                None,
                Some(&capture(&[10, 20, 30])),
                Some(&process(10)),
                |route| Some(route.clone())
            ),
            Resolution::Standalone
        ));
        // The next turn's first tool hook is enough; no reconfirmation needed.
        registry.observe_session("parent", "claude-code", Some(&capture(&[3, 20, 30])), false);
        open_span(&registry, "parent", "tool", SpanType::Tool);
        assert_eq!(
            parent_id(registry.resolve(
                None,
                Some(&capture(&[10, 20, 30])),
                Some(&process(10)),
                |route| Some(route.clone())
            )),
            "tool"
        );
        registry.retire_session("parent");
        registry.prune_retired(|process| process.pid != 20);
        registry.observe_session("parent", "claude-code", Some(&capture(&[4, 20, 30])), false);
        open_span(&registry, "parent", "tool", SpanType::Tool);
        assert!(matches!(
            registry.resolve(
                None,
                Some(&capture(&[10, 20, 30])),
                Some(&process(10)),
                |route| Some(route.clone())
            ),
            Resolution::Standalone
        ));
    }

    #[test]
    fn reused_hook_identity_confirms_only_when_parents_differ() {
        // Windows can hand two consecutive one-shot hooks the same PID and
        // start second; distinct hook shells still reveal the shared agent.
        let first = capture(&[1, 2, 20, 30]);
        let reused = capture(&[1, 3, 20, 30]);
        assert_eq!(
            session_agent_process("claude-code", &first, Some(&reused)),
            Some(process(20))
        );
        // One hook reparented to a shared ancestor is not a second hook.
        let reparented = capture(&[1, 30]);
        assert_eq!(
            session_agent_process("claude-code", &first, Some(&reparented)),
            None
        );
        assert_eq!(
            session_agent_process("claude-code", &reparented, Some(&first)),
            None
        );
    }

    #[test]
    fn every_source_can_parent_every_other_source_through_wrappers() {
        let sources = [
            "antigravity",
            "claude-code",
            "codex",
            "grok",
            "pi",
            "opencode",
        ];
        for parent_source in sources {
            for child_source in sources {
                let registry = CorrelationRegistry::default();
                let parent_first = if uses_command_hook(parent_source) {
                    capture(&[1, 2, 20])
                } else {
                    capture(&[20])
                };
                registry.observe_session("parent", parent_source, Some(&parent_first), true);
                registry.observe_session(
                    "parent",
                    parent_source,
                    Some(&capture(&[3, 4, 20])),
                    false,
                );
                open_span(&registry, "parent", "tool", SpanType::Tool);
                let child_first = if uses_command_hook(child_source) {
                    capture(&[5, 6, 10, 11, 12, 20])
                } else {
                    capture(&[10, 11, 12, 20])
                };
                let child_latest = capture(&[7, 8, 10, 11, 12, 20]);
                let child = session_agent_process(child_source, &child_first, Some(&child_latest));
                assert_eq!(
                    parent_id(registry.resolve(
                        None,
                        Some(&child_latest),
                        child.as_ref(),
                        |route| Some(route.clone())
                    )),
                    "tool",
                    "{parent_source} -> {child_source}"
                );
            }
        }
    }

    #[test]
    fn only_current_clean_snapshots_can_restore_parentage() {
        let registry = parent_registry();
        let snapshot = registry.active_parent_snapshot("parent").unwrap();
        for version in [2, 3] {
            let restored = CorrelationRegistry::default();
            let mut legacy = snapshot.clone();
            legacy.version = version;
            assert!(!restored.restore_active_parent(legacy));
            assert!(matches!(
                restored.resolve(
                    None,
                    Some(&capture(&[10, 20])),
                    Some(&process(10)),
                    |route| Some(route.clone())
                ),
                Resolution::Standalone
            ));
        }
        let restored = CorrelationRegistry::default();
        assert!(!restored
            .restore_active_parent(registry.dirty_active_parent_snapshot("parent").unwrap()));
        assert!(restored.restore_active_parent(snapshot));
        assert_eq!(
            parent_id(restored.resolve(
                None,
                Some(&capture(&[10, 20])),
                Some(&process(10)),
                |route| Some(route.clone())
            )),
            "session"
        );
    }
}
