//! Automatic parent correlation: holding a new session briefly while its
//! parent agent session is identified, and persisting that decision and the
//! active parent snapshots across daemon restarts.

use super::*;

// Correlation is a short, optional discovery step, never a delivery queue.
pub(super) const CORRELATION_VERSION: u32 = 1;

// Long enough for a slow cold start of the second hook (e.g. an antivirus scan
// of `bt.exe`); only sessions with a frozen parent candidate ever wait.
pub(super) const CORRELATION_WAIT: Duration = Duration::from_secs(5);

pub(super) const CORRELATION_EVENT_LIMIT: usize = 3;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct PendingSession {
    // Missing versions predate safe ancestry matching; see `restore`.
    #[serde(default)]
    pub(super) version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) linked_route: Option<SessionRoute>,
    #[serde(default)]
    pub(super) standalone: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) parent: Option<crate::correlation::ParentCandidate>,
    #[serde(default)]
    pub(super) first_seen_ms: i64,
    #[serde(skip)]
    pub(super) deadline: Option<Instant>,
    // Discovery expiry is not an authentication/delivery retry loop. A failed
    // handoff stays visible until a new event, explicit flush, or restart.
    #[serde(skip)]
    pub(super) delivery_failed: bool,
    #[serde(default)]
    pub(super) events: Vec<PendingEvent>,
}

impl Default for PendingSession {
    fn default() -> Self {
        Self {
            version: CORRELATION_VERSION,
            linked_route: None,
            standalone: false,
            parent: None,
            first_seen_ms: 0,
            deadline: None,
            delivery_failed: false,
            events: Vec::new(),
        }
    }
}

impl PendingSession {
    pub(super) fn restore(mut self) -> Self {
        if self.version != CORRELATION_VERSION {
            // A legacy link was decided before safe ancestry matching, but
            // its earlier rows already live in that parent's trace. Keep later
            // rows there too: splitting one session across two roots is worse.
            // Undecided legacy buffers were never delivered; send them alone.
            self.version = CORRELATION_VERSION;
            self.standalone = self.linked_route.is_none();
            self.parent = None;
        }
        let remaining = self
            .first_seen_ms
            .saturating_add(CORRELATION_WAIT.as_millis() as i64)
            .saturating_sub(now_ms());
        // A missing/future receipt timestamp is not permission to wait longer.
        let remaining = if (0..=CORRELATION_WAIT.as_millis() as i64).contains(&remaining) {
            remaining as u64
        } else {
            0
        };
        self.deadline = Some(Instant::now() + Duration::from_millis(remaining));
        self
    }

    pub(super) fn decided(&self) -> bool {
        self.standalone || self.linked_route.is_some()
    }

    pub(super) fn expired(&self) -> bool {
        self.deadline
            .is_none_or(|deadline| Instant::now() >= deadline)
    }
}

#[derive(Clone, Serialize)]
pub(super) struct PendingEvent {
    pub(super) env: Envelope,
    pub(super) replay_through: u64,
    pub(super) journal_through: u64,
}

impl<'de> Deserialize<'de> for PendingEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StoredPendingEvent {
            Current {
                env: Envelope,
                replay_through: u64,
                journal_through: u64,
            },
            Legacy(Envelope),
        }
        Ok(match StoredPendingEvent::deserialize(deserializer)? {
            StoredPendingEvent::Current {
                env,
                replay_through,
                journal_through,
            } => Self {
                env,
                replay_through,
                journal_through,
            },
            StoredPendingEvent::Legacy(env) => Self {
                env,
                replay_through: 0,
                journal_through: 0,
            },
        })
    }
}

pub(super) async fn accept_event(daemon: &Arc<Daemon>, event: PendingEvent) -> Result<(), String> {
    let key = automatic_link_key(&event.env);
    let lock = daemon.correlation_lock(&key);
    let _guard = lock.lock().await;
    let mut state = daemon.pending_sessions.lock().unwrap().get(&key).cloned();
    if state.is_none() {
        if daemon.standalone_links.lock().unwrap().contains(&key) {
            return accept_resolved_event(daemon, event).await;
        }
        let route = daemon.automatic_links.lock().unwrap().get(&key).cloned();
        if let Some(route) = route {
            let mut event = event;
            event.env.route = Some(route);
            return accept_resolved_event(daemon, event).await;
        }
        state = read_correlation_state(&daemon.data_dir, &key).await;
    }

    if let Some(mut state) = state {
        if state.decided() {
            // Queue behind any earlier events whose handoff failed so a
            // second failure keeps this event visible and retryable too.
            state.events.push(event);
            return finish_correlation(daemon, &key, state).await;
        }

        // A new native start is a different process generation, not a second
        // observation confirming the previous hook's ancestry.
        let new_generation = is_session_start(&event.env.source, &event.env.event);
        state.events.push(event);
        if !state.expired() && !new_generation {
            let mut captures = state
                .events
                .iter()
                .rev()
                .filter_map(|event| event.env.capture.as_ref());
            if let (Some(latest), Some(previous), Some(parent)) =
                (captures.next(), captures.next(), state.parent.as_ref())
            {
                if let Some(agent) = crate::correlation::session_agent_process(
                    &state.events[0].env.source,
                    previous,
                    Some(latest),
                ) {
                    match crate::correlation::CorrelationRegistry::confirm_parent(
                        parent, latest, &agent,
                    ) {
                        crate::correlation::Resolution::Parent(parent) => {
                            state.linked_route = Some(parent.route);
                        }
                        _ => state.standalone = true,
                    }
                }
            }
        }
        if !state.decided()
            && (new_generation || state.expired() || state.events.len() >= CORRELATION_EVENT_LIMIT)
        {
            state.standalone = true;
        }
        if state.decided() {
            return finish_correlation(daemon, &key, state).await;
        }
        return store_correlation(daemon, &key, &state).await;
    }

    let mut state = PendingSession::default();
    // An explicit parent is authoritative. Missing first-start/process evidence
    // also means standalone, permanently; a later start cannot reparent rows
    // already delivered.
    if is_session_start(&event.env.source, &event.env.event)
        && !matches!(
            event
                .env
                .route
                .as_ref()
                .and_then(|route| route.destination.as_ref()),
            Some(crate::wire::TraceDestination::ParentSpan { .. })
        )
    {
        let child_agent = event.env.capture.as_ref().and_then(|capture| {
            crate::correlation::session_agent_process(&event.env.source, capture, None)
        });
        let session_prefix = format!("{}\u{1f}{}\u{1f}", event.env.source, event.env.session_id);
        match daemon.correlation.resolve(
            Some(&session_prefix),
            event.env.capture.as_ref(),
            child_agent.as_ref(),
            |parent| automatic_parent_route(event.env.route.as_ref(), parent),
        ) {
            crate::correlation::Resolution::Parent(parent) => {
                state.linked_route = Some(parent.route);
            }
            crate::correlation::Resolution::Pending(parent) => {
                state.parent = Some(*parent);
                state.first_seen_ms = now_ms();
                state.deadline = Some(Instant::now() + CORRELATION_WAIT);
            }
            crate::correlation::Resolution::Standalone => {}
        }
    }
    state.standalone = state.linked_route.is_none() && state.parent.is_none();
    state.events.push(event);
    if state.decided() {
        finish_correlation(daemon, &key, state).await
    } else {
        store_correlation(daemon, &key, &state).await
    }
}

/// An inferred parent may change hierarchy, never the requested credential or
/// destination. Preserve the child's transforms, metadata, tags and flush mode.
pub(super) fn automatic_parent_route(
    child: Option<&SessionRoute>,
    parent: &SessionRoute,
) -> Option<SessionRoute> {
    use crate::wire::TraceDestination;
    use braintrust_sdk_rust::SpanObjectType;

    let child = child?;
    let same_profile = match (&child.auth.profile_id, &parent.auth.profile_id) {
        (Some(child), Some(parent)) => child == parent,
        _ => child.auth.profile == parent.auth.profile,
    };
    if child.auth.effective_source() != parent.auth.effective_source()
        || !same_profile
        || child.auth.org_name != parent.auth.org_name
    {
        return None;
    }
    let Some(TraceDestination::ParentSpan { components }) = &parent.destination else {
        return None;
    };
    let same_destination = match child.destination.as_ref()? {
        TraceDestination::ProjectLogs {
            project_id,
            project_name,
        } => {
            if components.object_type != SpanObjectType::ProjectLogs {
                false
            } else if let Some(id) = project_id {
                components.object_id.as_ref() == Some(id)
            } else {
                project_name.as_deref().is_some_and(|name| {
                    components
                        .compute_object_metadata_args
                        .as_ref()
                        .and_then(|args| args.get("project_name"))
                        .and_then(Value::as_str)
                        == Some(name)
                })
            }
        }
        TraceDestination::Experiment { experiment_id } => {
            components.object_type == SpanObjectType::Experiment
                && components.object_id.as_ref() == Some(experiment_id)
        }
        TraceDestination::ParentSpan { .. } => false,
    };
    if !same_destination {
        return None;
    }
    let mut route = child.clone();
    route.destination = parent.destination.clone();
    Some(route)
}

pub(super) async fn store_correlation(
    daemon: &Daemon,
    key: &str,
    state: &PendingSession,
) -> Result<(), String> {
    // Keep accepted events visible/retryable even if persisting the decision
    // fails. The immutable raw journal remains authoritative for recovery.
    if !state.events.is_empty() {
        daemon
            .pending_sessions
            .lock()
            .unwrap()
            .insert(key.to_string(), state.clone());
    }
    write_correlation_state(&daemon.data_dir, key, state).await?;
    if state.events.is_empty() {
        daemon.pending_sessions.lock().unwrap().remove(key);
        if let Some(route) = &state.linked_route {
            daemon
                .automatic_links
                .lock()
                .unwrap()
                .insert(key.to_string(), route.clone());
        } else if state.standalone {
            daemon
                .standalone_links
                .lock()
                .unwrap()
                .insert(key.to_string());
        }
    }
    Ok(())
}

pub(super) async fn finish_correlation(
    daemon: &Arc<Daemon>,
    key: &str,
    mut state: PendingSession,
) -> Result<(), String> {
    state.parent = None;
    state.delivery_failed = false;
    let result = async {
        // Persist the final route before delivering any events. A crash may
        // replay deterministic rows, but must never select a different parent.
        store_correlation(daemon, key, &state).await?;
        while let Some(event) = state.events.first() {
            let mut event = event.clone();
            if let Some(route) = &state.linked_route {
                event.env.route = Some(route.clone());
            }
            accept_resolved_event(daemon, event).await?;
            state.events.remove(0);
            store_correlation(daemon, key, &state).await?;
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        state.delivery_failed = true;
        daemon
            .pending_sessions
            .lock()
            .unwrap()
            .insert(key.to_string(), state);
    }
    result
}

pub(super) async fn retry_pending_sessions(daemon: &Arc<Daemon>) -> Result<(), String> {
    resolve_pending_sessions(daemon, |_| false).await
}

pub(super) async fn resolve_pending_sessions(
    daemon: &Arc<Daemon>,
    force: impl Fn(&Envelope) -> bool + Send + Sync,
) -> Result<(), String> {
    let _reconcile_guard = daemon.pending_reconcile_lock.lock().await;
    let keys: Vec<_> = daemon
        .pending_sessions
        .lock()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    let mut failure = None;
    for key in keys {
        let lock = daemon.correlation_lock(&key);
        let _guard = lock.lock().await;
        let Some(mut state) = daemon.pending_sessions.lock().unwrap().get(&key).cloned() else {
            continue;
        };
        let forced = state.events.first().is_some_and(|event| force(&event.env));
        if state.delivery_failed && !forced {
            continue;
        }
        if !state.decided() && (state.expired() || forced) {
            state.standalone = true;
        }
        if state.decided() {
            if let Err(error) = finish_correlation(daemon, &key, state).await {
                // One unavailable destination must not prevent other sessions
                // from leaving the discovery window.
                failure = Some(error);
            }
        }
    }
    failure.map_or(Ok(()), Err)
}

pub(super) fn correlation_state_path(data_dir: &std::path::Path, key: &str) -> PathBuf {
    let digest = Sha256::digest(key.as_bytes());
    data_dir
        .join("correlation")
        .join(format!("{digest:x}.json"))
}

pub(super) async fn read_correlation_state(
    data_dir: &std::path::Path,
    key: &str,
) -> Option<PendingSession> {
    let bytes = tokio::fs::read(correlation_state_path(data_dir, key))
        .await
        .ok()?;
    serde_json::from_slice::<PendingSession>(&bytes)
        .ok()
        .map(PendingSession::restore)
}

pub(super) async fn write_correlation_state(
    data_dir: &std::path::Path,
    key: &str,
    state: &PendingSession,
) -> Result<(), String> {
    let dir = data_dir.join("correlation");
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|error| format!("correlation journal directory failed: {error}"))?;
    let path = correlation_state_path(data_dir, key);
    let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let bytes = serde_json::to_vec(state)
        .map_err(|error| format!("correlation journal encoding failed: {error}"))?;
    tokio::fs::write(&temp, bytes)
        .await
        .map_err(|error| format!("correlation journal write failed: {error}"))?;
    if let Err(first_error) = tokio::fs::rename(&temp, &path).await {
        // Windows cannot replace an existing destination with rename.
        let _ = tokio::fs::remove_file(&path).await;
        tokio::fs::rename(&temp, &path).await.map_err(|error| {
            format!("correlation journal replace failed: {first_error}; retry failed: {error}")
        })?;
    }
    Ok(())
}

pub(super) fn active_parent_snapshot_path(data_dir: &std::path::Path, key: &str) -> PathBuf {
    let digest = Sha256::digest(key.as_bytes());
    data_dir
        .join("correlation")
        .join("parents")
        .join(format!("{digest:x}.json"))
}

pub(crate) async fn persist_active_parent_snapshot(
    data_dir: &std::path::Path,
    key: &str,
    correlation: &crate::correlation::CorrelationRegistry,
) -> Result<(), String> {
    let path = active_parent_snapshot_path(data_dir, key);
    let Some(snapshot) = correlation.active_parent_snapshot(key) else {
        let _ = tokio::fs::remove_file(path).await;
        return Ok(());
    };
    let bytes = serde_json::to_vec(&snapshot)
        .map_err(|error| format!("active parent encoding failed: {error}"))?;
    write_active_parent_snapshot(data_dir, path, bytes).await
}

pub(super) async fn mark_active_parent_snapshot_dirty(
    data_dir: &std::path::Path,
    key: &str,
    correlation: &crate::correlation::CorrelationRegistry,
) -> Result<(), String> {
    let Some(snapshot) = correlation.dirty_active_parent_snapshot(key) else {
        return Ok(());
    };
    let bytes = serde_json::to_vec(&snapshot)
        .map_err(|error| format!("active parent encoding failed: {error}"))?;
    write_active_parent_snapshot(data_dir, active_parent_snapshot_path(data_dir, key), bytes).await
}

pub(super) async fn write_active_parent_snapshot(
    data_dir: &std::path::Path,
    path: PathBuf,
    bytes: Vec<u8>,
) -> Result<(), String> {
    let dir = data_dir.join("correlation").join("parents");
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|error| format!("active parent directory failed: {error}"))?;
    let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    tokio::fs::write(&temp, bytes)
        .await
        .map_err(|error| format!("active parent write failed: {error}"))?;
    if let Err(first_error) = tokio::fs::rename(&temp, &path).await {
        let _ = tokio::fs::remove_file(&path).await;
        tokio::fs::rename(&temp, &path).await.map_err(|error| {
            format!("active parent replace failed: {first_error}; retry failed: {error}")
        })?;
    }
    Ok(())
}

pub(super) async fn restore_active_parent_snapshots(
    data_dir: &std::path::Path,
    correlation: &crate::correlation::CorrelationRegistry,
) {
    let Ok(mut entries) = tokio::fs::read_dir(data_dir.join("correlation").join("parents")).await
    else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let Ok(bytes) = tokio::fs::read(entry.path()).await else {
            continue;
        };
        let Ok(snapshot) =
            serde_json::from_slice::<crate::correlation::ActiveParentSnapshot>(&bytes)
        else {
            tracing::debug!(path = %entry.path().display(), "invalid active parent snapshot ignored");
            continue;
        };
        correlation.restore_active_parent(snapshot);
    }
}

pub(super) async fn restore_pending_sessions(daemon: &Arc<Daemon>) {
    let Ok(mut entries) = tokio::fs::read_dir(daemon.data_dir.join("correlation")).await else {
        return;
    };
    let mut restored = 0usize;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let Ok(bytes) = tokio::fs::read(&path).await else {
            continue;
        };
        let Ok(state) = serde_json::from_slice::<PendingSession>(&bytes) else {
            continue;
        };
        let state = state.restore();
        let Some(key) = state
            .events
            .first()
            .map(|event| automatic_link_key(&event.env))
        else {
            continue;
        };
        daemon.pending_sessions.lock().unwrap().insert(key, state);
        restored += 1;
    }
    if restored > 0 {
        tracing::info!(restored, "restored pending child sessions");
    }
}

pub(super) fn automatic_link_key(env: &Envelope) -> String {
    let route = env
        .route
        .as_ref()
        .and_then(|route| serde_json::to_string(route).ok())
        .unwrap_or_default();
    format!("{}\u{1f}{}\u{1f}{route}", env.source, env.session_id)
}

/// Correlated child events retain their setup route in the immutable journal,
/// while their delivery checkpoint belongs to the resolved parent route.
/// Recover against that effective route without rewriting the captured event.
pub(super) async fn recovered_delivery_route(
    daemon: &Arc<Daemon>,
    env: &Envelope,
) -> Option<SessionRoute> {
    let key = automatic_link_key(env);
    if let Some(route) = daemon.automatic_links.lock().unwrap().get(&key).cloned() {
        return Some(route);
    }
    if let Some(route) = daemon
        .pending_sessions
        .lock()
        .unwrap()
        .get(&key)
        .and_then(|state| state.linked_route.clone())
    {
        return Some(route);
    }
    read_correlation_state(&daemon.data_dir, &key)
        .await
        .and_then(|state| state.linked_route)
        .or_else(|| env.route.clone())
}

impl Daemon {
    pub(super) fn correlation_lock(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.correlation_locks.lock().unwrap();
        locks
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    pub(super) fn correlation_decided(&self, key: &str) -> bool {
        self.standalone_links.lock().unwrap().contains(key)
            || self.automatic_links.lock().unwrap().contains_key(key)
    }
}
