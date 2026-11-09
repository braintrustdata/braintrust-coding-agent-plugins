//! Recover completed OpenCode 2 session projections after the plugin exits.
use super::*;
use crate::wire::opencode_snapshot::{
    Snapshot, SnapshotAssistant, SnapshotCompaction, SnapshotMessage, SnapshotMessageKind,
    SnapshotUser,
};
use crate::wire::OpenCodeRecovery;
use rusqlite::{Connection, OpenFlags};

#[derive(serde::Deserialize)]
struct NativePayload {
    id: Option<String>,
    data: NativeSession,
}
#[derive(serde::Deserialize)]
struct NativeSession {
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "assistantMessageID")]
    assistant_message_id: Option<String>,
    #[serde(rename = "inboxID")]
    inbox_id: Option<String>,
    #[serde(rename = "inputID")]
    input_id: Option<String>,
}

#[derive(Clone)]
struct Observation {
    template: Envelope,
    last_message_id: Option<String>,
    closed_at_ms: Option<i64>,
    inferred_close: bool,
}

#[derive(Default)]
pub(super) struct OpenCodeObservations {
    sessions: HashMap<(String, String), Observation>,
}
impl OpenCodeObservations {
    pub(super) fn completed(&self, env: &Envelope) -> Option<Self> {
        if !matches!(
            env.event.as_str(),
            "session.execution.succeeded"
                | "session.execution.failed"
                | "session.execution.interrupted"
        ) {
            return None;
        }
        let payload = serde_json::from_value::<NativePayload>(env.payload.clone()).ok()?;
        let key = (env.session_id.clone(), payload.data.session_id);
        let mut observation = self.sessions.get(&key)?.clone();
        observation.closed_at_ms = Some(now_ms());
        observation.inferred_close = false;
        Some(Self {
            sessions: HashMap::from([(key, observation)]),
        })
    }
    pub(super) fn observe(&mut self, env: &Envelope) {
        if env.source != "opencode"
            || !env
                .source_version
                .as_deref()
                .is_some_and(|v| v.trim_start_matches('v').starts_with("2."))
        {
            return;
        }
        let Ok(payload) = serde_json::from_value::<NativePayload>(env.payload.clone()) else {
            return;
        };
        let mut template = env.clone();
        template.payload = Value::Null;
        template.capture = None;
        let observation = self
            .sessions
            .entry((env.session_id.clone(), payload.data.session_id))
            .or_insert_with(|| Observation {
                template: template.clone(),
                last_message_id: None,
                closed_at_ms: None,
                inferred_close: false,
            });
        observation.template = template;
        if env.event == "session.capture.closed" {
            observation.closed_at_ms = Some(env.ts_ms);
        }
        let compaction_id = (env.event == "session.compaction.started")
            .then(|| {
                payload
                    .id
                    .as_deref()
                    .and_then(|id| id.strip_prefix("evt_").map(|id| format!("msg_{id}")))
            })
            .flatten();
        if let Some(id) = payload
            .data
            .assistant_message_id
            .or(payload.data.inbox_id)
            .or(payload.data.input_id)
            .or(compaction_id)
        {
            observation.last_message_id = Some(id);
        }
    }

    pub(super) async fn recover(
        mut self,
        daemon: &Arc<Daemon>,
        config: OpenCodeRecovery,
        managed: bool,
    ) {
        if self.sessions.is_empty() {
            return;
        }
        if managed {
            // A host can exit before its last socket write reaches the daemon.
            // Only recover an inferred close when the native database confirms
            // the session had already reached an idle state at disconnection.
            let disconnected_at = now_ms();
            for observation in self.sessions.values_mut() {
                if observation.closed_at_ms.is_none() {
                    observation.closed_at_ms = Some(disconnected_at);
                    observation.inferred_close = true;
                }
            }
        }
        let result =
            tokio::task::spawn_blocking(move || read_snapshots(config, self.sessions)).await;
        let items = match result {
            Ok(Ok(items)) => items,
            Ok(Err(error)) => {
                tracing::warn!(%error, "could not read OpenCode session projection");
                return;
            }
            Err(error) => {
                tracing::warn!(%error, "OpenCode session recovery task failed");
                return;
            }
        };
        // Recreate parent turns before their children, then finish children
        // before their parents. This preserves native subagent ownership.
        let mut items = items;
        let parents: HashMap<String, Option<String>> = items
            .iter()
            .map(|(_, snapshot)| (snapshot.session_id.clone(), snapshot.parent_id.clone()))
            .collect();
        let depth = |session: &str| {
            let mut seen = HashSet::new();
            let mut current = session;
            let mut depth = 0;
            while seen.insert(current.to_owned()) {
                let Some(Some(parent)) = parents.get(current) else {
                    break;
                };
                if !parents.contains_key(parent) {
                    break;
                }
                depth += 1;
                current = parent;
            }
            depth
        };
        items.sort_by_key(|(envelope, snapshot)| {
            (envelope.session_id.clone(), depth(&snapshot.session_id))
        });
        let mut terminals = Vec::new();
        for (envelope, snapshot) in &mut items {
            if let Some(idle_ms) = snapshot.idle_ms.take() {
                terminals.push((
                    envelope.clone(),
                    Snapshot {
                        session_id: snapshot.session_id.clone(),
                        messages: Vec::new(),
                        parent_id: None,
                        title: None,
                        outcome: snapshot.outcome.take(),
                        idle_ms: Some(idle_ms),
                        terminal_error: snapshot.terminal_error.take(),
                    },
                ));
            }
        }
        for (mut envelope, snapshot) in items.into_iter().chain(terminals.into_iter().rev()) {
            let Ok(payload) = serde_json::to_value(snapshot) else {
                continue;
            };
            envelope.event = "session.snapshot".into();
            envelope.ts_ms = now_ms();
            envelope.payload = serde_json::json!({"type":"session.snapshot", "data":payload});
            if let Err(error) = daemon.capture_event(envelope).await {
                tracing::warn!(%error, "could not capture OpenCode session recovery");
            }
        }
    }
}

fn read_snapshots(
    config: OpenCodeRecovery,
    mut sessions: HashMap<(String, String), Observation>,
) -> anyhow::Result<Vec<(Envelope, Snapshot)>> {
    if !config.database.is_absolute() {
        anyhow::bail!("OpenCode recovery requires an absolute database path");
    }
    let db = Connection::open_with_flags(
        &config.database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    db.busy_timeout(Duration::ZERO)?;
    // A child may have been created after the subscriber stopped reading.
    // Follow native parent IDs so those sessions stay under the same trace.
    let roots: Vec<_> = sessions
        .iter()
        .filter(|(_, observation)| observation.closed_at_ms.is_some())
        .map(|((root, session), observation)| {
            (
                root.clone(),
                session.clone(),
                observation.template.clone(),
                observation.closed_at_ms.unwrap_or_default(),
                observation.inferred_close,
            )
        })
        .collect();
    for (root, session, template, closed_at_ms, inferred_close) in roots {
        if inferred_close && !session_was_idle(&db, &session, config.since_ms, closed_at_ms) {
            continue;
        }
        let mut statement = db.prepare("WITH RECURSIVE descendants(id) AS (SELECT id FROM session_v2 WHERE parent_id = ?1 UNION SELECT child.id FROM session_v2 child JOIN descendants parent ON child.parent_id = parent.id) SELECT descendants.id FROM descendants JOIN session_v2 s ON s.id = descendants.id WHERE s.time_idle >= ?2 OR EXISTS (SELECT 1 FROM session_message m WHERE m.session_id = descendants.id AND m.time_created >= ?2)")?;
        let children = statement.query_map(rusqlite::params![session, config.since_ms], |row| {
            row.get::<_, String>(0)
        })?;
        for child in children {
            sessions
                .entry((root.clone(), child?))
                .and_modify(|observation| {
                    observation.closed_at_ms.get_or_insert(closed_at_ms);
                })
                .or_insert_with(|| Observation {
                    template: template.clone(),
                    last_message_id: None,
                    closed_at_ms: Some(closed_at_ms),
                    inferred_close,
                });
        }
    }
    let mut result = Vec::new();
    for ((_, session_id), observation) in sessions {
        let Some(closed_at_ms) = observation.closed_at_ms else {
            continue;
        };
        if observation.inferred_close
            && !session_was_idle(&db, &session_id, config.since_ms, closed_at_ms)
        {
            continue;
        }
        let mut statement = db.prepare("SELECT id, type, data, seq, time_updated FROM session_message WHERE session_id = ?1 AND type IN ('user', 'assistant', 'compaction') AND (time_created >= ?2 OR id = ?3) AND time_created <= ?4 AND time_updated <= ?4 AND seq >= COALESCE((SELECT seq FROM session_message WHERE id = ?3 AND session_id = ?1), 0) ORDER BY seq")?;
        let rows = statement.query_map(
            rusqlite::params![
                session_id,
                config.since_ms,
                observation.last_message_id,
                closed_at_ms
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )?;
        let mut messages = Vec::new();
        for row in rows {
            let (id, kind, data, seq, updated_ms) = row?;
            let kind = match kind.as_str() {
                "user" => serde_json::from_str::<SnapshotUser>(&data)
                    .ok()
                    .map(SnapshotMessageKind::User),
                "assistant" => serde_json::from_str::<SnapshotAssistant>(&data)
                    .ok()
                    .map(SnapshotMessageKind::Assistant),
                "compaction" => serde_json::from_str::<SnapshotCompaction>(&data)
                    .ok()
                    .map(SnapshotMessageKind::Compaction),
                _ => None,
            };
            if let Some(kind) = kind {
                messages.push(SnapshotMessage {
                    id,
                    seq,
                    updated_ms,
                    kind,
                });
            }
        }
        let state = db.query_row(
            "SELECT idle_outcome, time_idle, time_suspended, parent_id, title FROM session_v2 WHERE id = ?1",
            [&session_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        );
        let (outcome, idle_ms, suspended, parent_id, title) = state.unwrap_or_default();
        let (outcome, idle_ms) = if suspended.is_none()
            && idle_ms.is_some_and(|time| time >= config.since_ms && time <= closed_at_ms)
        {
            (outcome, idle_ms)
        } else {
            (None, None)
        };
        let terminal_error = messages
            .iter()
            .rev()
            .find_map(|message| match &message.kind {
                SnapshotMessageKind::Assistant(data) => data.error.clone(),
                SnapshotMessageKind::Compaction(data) => data.error.clone(),
                SnapshotMessageKind::User(_) => None,
            });
        result.push((
            observation.template,
            Snapshot {
                session_id,
                messages,
                parent_id,
                title,
                outcome,
                idle_ms,
                terminal_error,
            },
        ));
    }
    Ok(result)
}

fn session_was_idle(db: &Connection, session: &str, since: i64, until: i64) -> bool {
    let state = db.query_row(
        "SELECT time_idle, time_suspended FROM session_v2 WHERE id = ?1",
        [session],
        |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?)),
    );
    matches!(state, Ok((Some(time), None)) if time >= since && time <= until)
}
