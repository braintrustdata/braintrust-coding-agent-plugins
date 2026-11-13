//! Background maintenance: retention GC, idle-session retirement, the idle
//! watchdog, and draining on shutdown.

use super::*;

/// The retention `serve` uses unless told otherwise.
#[cfg(test)]
pub(super) const RETENTION: Duration = crate::args::DEFAULT_RETENTION;

/// How often that retention is enforced while the daemon keeps running.
pub(super) const GC_INTERVAL: Duration = Duration::from_secs(60 * 60);

pub(super) async fn collect_garbage(daemon: &Daemon) {
    collect_sessions(daemon).await;
    // Delivery is acknowledged by SpanOp heads; ledgers from earlier daemons
    // hold nothing a current pipeline reads.
    match tokio::fs::remove_dir_all(daemon.data_dir.join("delivery-ledger")).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => tracing::warn!(%error, "legacy delivery ledger cleanup failed"),
    }
    journal::gc_old_managed_runs(&daemon.data_dir, daemon.retention).await;
    // Protect references held by every retained session, including sessions
    // still inside retention and sessions with no registered heads.
    let Ok(protected) = retained_references(&daemon.data_dir).await else {
        return;
    };
    crate::transcript_mirror::gc_old_mirrors(&daemon.data_dir, daemon.retention, &protected).await;
    gc_old_correlation_dir(
        &daemon.data_dir.join("correlation"),
        daemon.retention,
        &protected,
    )
    .await;
    gc_old_correlation_dir(
        &daemon.data_dir.join("correlation/parents"),
        daemon.retention,
        &protected,
    )
    .await;
}

async fn old(path: &std::path::Path, age: Duration) -> bool {
    tokio::fs::metadata(path)
        .await
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|elapsed| elapsed > age)
}

async fn collect_sessions(daemon: &Daemon) {
    let Ok(sessions) = crate::collections::sessions(&daemon.data_dir).await else {
        return;
    };
    for storage in sessions {
        if let Err(error) = collect_session(daemon, &storage).await {
            tracing::warn!(%error, path = %storage.path.display(), "session storage cleanup failed");
        }
    }
}

/// Remove the storage of a cold source session that owes nothing: every
/// captured event is translated and every route has delivered every SpanOp.
/// A session with live actors, queued captures or active tools is retained.
pub(super) async fn collect_session(
    daemon: &Daemon,
    storage: &crate::collections::SessionStorage,
) -> anyhow::Result<()> {
    // Inspection must never create storage for a session that has none.
    if !tokio::fs::try_exists(&storage.path).await? {
        return Ok(());
    }
    let events = storage.events().await?;
    let Some((source, session_id)) = events.owner().await? else {
        // Output storage that no captured input ever claimed has no owner to
        // complete it. Keep it only while it may still be in use.
        drop(events);
        if storage
            .modified_ago()
            .await
            .is_some_and(|idle| idle > daemon.retention)
        {
            crate::collections::remove_session(&storage.path).await?;
        }
        return Ok(());
    };
    // A route that has delivered nothing for the whole retention period, such
    // as one whose spans its destination keeps rejecting, is expired below.
    // Stop its delivery first; retirement takes the session lock itself.
    for route in storage.span_ops().await?.stalled(daemon.retention).await? {
        let Ok(key) = DeliveryKey::new(&source, &session_id, &route) else {
            continue;
        };
        if daemon.sessions.lock().contains_key(&key) {
            daemon.retire_session(&key).await;
        }
    }
    let namespace = crate::ids::session_namespace(&source, &session_id);
    let lock = daemon.session_lock(&source, &session_id);
    let _guard = lock.lock().await;
    let queued = daemon.pending_sessions.lock().values().any(|state| {
        state
            .events
            .iter()
            .any(|event| event.env.source == source && event.env.session_id == session_id)
    });
    if queued {
        return Ok(());
    }
    // A session that never ends keeps only what it still uses: expire stale
    // capture identities, finished span metadata and stuck routes, even
    // while the session is live.
    let live: Vec<String> = daemon
        .sessions
        .lock()
        .keys()
        .filter(|key| key.source == source && key.session_id == session_id)
        .map(|key| key.route.clone())
        .collect();
    events.expire(daemon.retention).await?;
    let ops = storage.span_ops().await?;
    ops.expire(daemon.retention, live).await?;
    storage.compact().await?;
    if daemon.journals.lock().contains_key(&namespace)
        || daemon.translations.lock().contains_key(&namespace)
    {
        return Ok(());
    }
    let routes = ops.routes().await?;
    if routes.iter().any(|route| {
        DeliveryKey::new(&source, &session_id, route)
            .is_ok_and(|key| daemon.correlation.has_active_tools(&key.correlation_key()))
    }) {
        return Ok(());
    }
    // Delivered payloads are already gone. What remains is the translator
    // continuation and span metadata a resumed session needs to extend its
    // trace instead of re-creating it, so keep it until the session goes cold.
    // Output that no destination ever subscribed to can never be delivered.
    let complete = routes.is_empty() || ops.complete(events.tail().await?).await?;
    if !complete || events.idle_for().await? <= daemon.retention {
        return Ok(());
    }
    drop((ops, events));
    if crate::collections::remove_session(&storage.path).await? {
        tracing::debug!(session_id = %session_id, "completed session storage removed");
    }
    Ok(())
}

async fn retained_references(data_dir: &std::path::Path) -> anyhow::Result<HashSet<PathBuf>> {
    fn mirrors(value: &Value, protected: &mut HashSet<PathBuf>) {
        match value {
            Value::Object(fields) => {
                for (key, value) in fields {
                    if key == "mirror" {
                        if let Some(path) = value.as_str() {
                            protected.insert(path.into());
                        }
                    }
                    mirrors(value, protected);
                }
            }
            Value::Array(values) => {
                for value in values {
                    mirrors(value, protected);
                }
            }
            _ => {}
        }
    }
    // Consumed event records no longer own transcript or correlation references.
    // Their durable continuation and source identity take over that ownership.
    fn checkpoint_paths(value: &Value, root: &std::path::Path, protected: &mut HashSet<PathBuf>) {
        match value {
            Value::String(value) if std::path::Path::new(value).starts_with(root) => {
                protected.insert(value.into());
            }
            Value::Object(fields) => {
                for (key, value) in fields {
                    if std::path::Path::new(key).starts_with(root) {
                        protected.insert(key.into());
                    }
                    checkpoint_paths(value, root, protected);
                }
            }
            Value::Array(values) => {
                for value in values {
                    checkpoint_paths(value, root, protected);
                }
            }
            _ => {}
        }
    }
    let mut protected = HashSet::new();
    for storage in crate::collections::sessions(data_dir).await? {
        let events = storage.events().await?;
        let Some(identity) = events.identity().await? else {
            continue;
        };
        mirrors(&identity.payload, &mut protected);
        let mut reader = events.read(0, events.tail().await?);
        while let Some(event) = reader.next_event().await? {
            mirrors(&event.payload, &mut protected);
        }
        let env = journal::envelope_from_redacted(identity);
        let key = automatic_link_key(&env);
        protected.insert(correlation_state_path(data_dir, &key));
        protected.insert(active_parent_snapshot_path(data_dir, &key));
        let ops = storage.span_ops().await?;
        if let Some((_, bytes)) = ops.checkpoint().await? {
            checkpoint_paths(
                &serde_json::from_slice(&bytes)?,
                &data_dir.join("transcripts"),
                &mut protected,
            );
        }
        for route in ops.routes().await? {
            let key = DeliveryKey::new(&env.source, &env.session_id, &route)?.correlation_key();
            protected.insert(correlation_state_path(data_dir, &key));
            protected.insert(active_parent_snapshot_path(data_dir, &key));
        }
    }
    Ok(protected)
}

async fn gc_old_correlation_dir(
    dir: &std::path::Path,
    age: Duration,
    protected: &HashSet<PathBuf>,
) {
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        if !protected.contains(&entry.path())
            && entry.file_type().await.is_ok_and(|kind| kind.is_file())
            && old(&entry.path(), age).await
        {
            let _ = tokio::fs::remove_file(entry.path()).await;
        }
    }
}

/// Retire delivery pipelines that have gone quiet. Without this, every session
/// the daemon ever saw keeps its translator state, sink handles, and journal
/// file handle alive until the process exits — which for a continuously busy
/// user is never, since the idle watchdog needs *all* sessions quiet.
pub(super) fn spawn_session_reaper(daemon: Arc<Daemon>, idle_timeout: Duration) {
    if idle_timeout.is_zero() {
        return; // 0 disables retirement (useful in tests)
    }
    tokio::spawn(async move {
        let tick = (idle_timeout / 4).max(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return,
                _ = tokio::time::sleep(tick) => {}
            }
            for key in daemon.idle_sessions(idle_timeout) {
                daemon.retire_session(&key).await;
            }
        }
    });
}

pub(super) fn spawn_gc(daemon: Arc<Daemon>) {
    tokio::spawn(async move {
        // Retention may scan old transcripts. Start it after recovery releases
        // live ingress, so it cannot delay resuming an active source session.
        collect_garbage(&daemon).await;
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return,
                _ = tokio::time::sleep(GC_INTERVAL) => {}
            }
            collect_garbage(&daemon).await;
            daemon
                .correlation
                .prune_retired(crate::process::process_is_alive);
        }
    });
}

pub(super) fn spawn_idle_watchdog(daemon: Arc<Daemon>, idle_timeout: Duration) {
    if idle_timeout.is_zero() {
        return; // 0 disables the watchdog (useful in tests)
    }
    tokio::spawn(async move {
        let tick = (idle_timeout / 4).max(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = daemon.shutdown.notified() => return,
                _ = tokio::time::sleep(tick) => {}
            }
            let idle_for = daemon.last_activity.lock().elapsed();
            if idle_for >= idle_timeout
                && daemon.total_queued() == 0
                && !daemon.correlation.has_any_active_tools()
            {
                tracing::info!("idle for {:?}; shutting down", idle_for);
                daemon.trigger_shutdown();
                return;
            }
        }
    });
}

pub(super) async fn drain_all(daemon: &Arc<Daemon>) {
    let mut drained = daemon.drained.lock().await;
    if *drained {
        return;
    }
    daemon.begin_quiesce();
    let _capture_guard = daemon.capture_gate.write().await;
    // Captures already acknowledged may still be waiting to register their
    // transcript paths in ingress. Observe only after those have dispatched.
    daemon.settle_ingress().await;
    daemon.observe_claude_transcripts(None, true).await;
    daemon.settle_ingress().await;
    if let Err(error) = resolve_pending_sessions(daemon, |_| true).await {
        tracing::warn!(%error, "pending correlation events retained for recovery during shutdown");
    }
    // Publish each source's retirement boundary once, then give every route
    // a chance to consume it before its actor stops.
    let sources: Vec<_> = daemon
        .translations
        .lock()
        .iter()
        .map(|(key, source)| (key.clone(), source.clone()))
        .collect();
    let mut targets = HashMap::new();
    for (key, source) in sources {
        if let Ok(tail) = source
            .control(crate::pipeline::input::ControlKind::Retire)
            .await
        {
            targets.insert(key, tail);
        }
    }
    let sessions: Vec<_> = daemon
        .sessions
        .lock()
        .iter()
        .map(|(key, session)| (key.clone(), session.clone()))
        .collect();
    let mut draining = tokio::task::JoinSet::new();
    for (key, session) in sessions {
        let target = targets
            .get(&crate::ids::session_namespace(&key.source, &key.session_id))
            .copied();
        draining.spawn(async move { session.shutdown_through(target).await });
    }
    while let Some(result) = draining.join_next().await {
        if let Err(error) = result {
            tracing::warn!(%error, "route shutdown failed");
        }
    }
    let sources: Vec<_> = daemon.translations.lock().values().cloned().collect();
    for source in sources {
        source.shutdown().await;
    }
    daemon.sessions.lock().clear();
    daemon.translations.lock().clear();
    daemon.journals.lock().clear();
    daemon.source_inputs.lock().clear();
    let (reply, done) = oneshot::channel();
    if daemon
        .ingress_tx
        .send(super::ingress::IngressMsg::Stop(reply))
        .await
        .is_ok()
    {
        let _ = done.await;
    }
    *drained = true;
}

/// Is a live daemon answering at the endpoint? Connect and expect any line
/// back from a well-formed `initialize`.
pub(super) async fn probe_alive(endpoint: &std::path::Path) -> bool {
    let Ok(stream) = transport::connect(endpoint).await else {
        return false;
    };
    let (read_half, mut write_half) = tokio::io::split(stream);
    let init = Request::new(
        crate::wire::RequestId::Int(0),
        method::INITIALIZE,
        serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "client": { "source": "probe" }
        }),
    );
    let Ok(mut line) = Message::Request(init).to_line() else {
        return false;
    };
    line.push('\n');
    if write_half.write_all(line.as_bytes()).await.is_err() {
        return false;
    }
    let mut lines = BufReader::new(read_half).lines();
    matches!(
        tokio::time::timeout(Duration::from_secs(1), lines.next_line()).await,
        Ok(Ok(Some(_)))
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::journal::Cursor;
    use crate::{
        translate::{SpanOp, SpanRow},
        wire::Envelope,
    };

    #[allow(dead_code)]
    mod distributed_fixture {
        use crate as bt_daemon;
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/distributed.rs"
        ));
    }

    use super::super::recovery::tests::TestAuth;

    fn event(session: &str, route: SessionRoute) -> Envelope {
        Envelope {
            source: "debug".into(),
            session_id: session.into(),
            event: "event".into(),
            ts_ms: 1,
            payload: serde_json::json!({}),
            route: Some(route),
            config: None,
            capture: None,
            managed_run_id: None,
            plugin_version: None,
            source_version: None,
        }
    }

    fn paused_ingress(root: &std::path::Path) -> (Arc<Daemon>, oneshot::Sender<()>) {
        let (ready, startup) = oneshot::channel();
        (
            Daemon::new_with_startup(
                debug_serve_options("test", root),
                root.into(),
                Some(startup),
                RETENTION,
            ),
            ready,
        )
    }

    #[tokio::test]
    async fn concurrent_recovery_and_dispatch_share_one_source_producer() {
        let f = crate::pipeline::translation::tests::SourceFixture::new("debug").await;
        f.source.shutdown().await;
        let (daemon, ready) = paused_ingress(&f.options.data_dir);
        let start = Arc::new(tokio::sync::Barrier::new(8));
        let mut opening = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let daemon = daemon.clone();
            let start = start.clone();
            opening.spawn(async move {
                start.wait().await;
                daemon
                    .translation_for(&event("checkpoint", SessionRoute::default()))
                    .await
                    .unwrap()
            });
        }
        let first = opening.join_next().await.unwrap().unwrap();
        while let Some(source) = opening.join_next().await {
            assert!(
                Arc::ptr_eq(&first, &source.unwrap()),
                "source initialization must have one owner"
            );
        }
        ready.send(()).unwrap();
        drain_all(&daemon).await;
    }

    #[tokio::test]
    async fn capture_is_durable_during_startup_and_recovery_stops_before_it() {
        let tmp = tempfile::tempdir().unwrap();
        let mut env = event("resuming", SessionRoute::default());
        let events =
            crate::collections::EventCollection::open(tmp.path(), &env.source, &env.session_id)
                .await
                .unwrap();
        let old_end = events.append(&env, None).await.unwrap().unwrap().through;
        let (daemon, ready) = paused_ingress(tmp.path());
        env.ts_ms = 2;
        env.event = "next".into();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), daemon.capture_event(env))
                .await
                .unwrap()
                .unwrap()
        );
        let mut reader = events.read(0, events.tail().await.unwrap());
        let mut captured = Vec::new();
        while let Some(event) = reader.next_event().await.unwrap() {
            captured.push(event.event);
        }
        assert!(captured.iter().any(|event| event == "next"));
        assert!(daemon.translations.lock().is_empty());
        assert!(
            daemon.source_inputs.lock().is_empty(),
            "capture must retain live payloads only in the bounded ingress queue"
        );
        let recovered = discover_unprocessed_journals(&daemon).await;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].journal_through, old_end);
        for event in recovered {
            dispatch_ingress_event(&daemon, event).await;
        }
        drain_capture_buffer(&daemon).await.unwrap();
        ready.send(()).unwrap();
        daemon.settle_ingress().await;
        let source = daemon.translations.lock().values().next().unwrap().clone();
        assert!(source.barrier().await.unwrap() > 0);
        let partition = crate::collections::SpanOps::for_events(&events)
            .await
            .unwrap();
        assert_eq!(
            partition.translated_through().await.unwrap(),
            events.tail().await.unwrap()
        );
        drain_all(&daemon).await;
    }

    #[tokio::test]
    async fn repeated_overflow_preserves_each_session_and_event_order() {
        let tmp = tempfile::tempdir().unwrap();
        let (daemon, ready) = paused_ingress(tmp.path());
        // Four journals interleave through two eviction ranges and a live tail.
        let mut envs = Vec::new();
        for source in ["debug", "pi"] {
            for session in ["first", "second"] {
                let mut env = event(session, SessionRoute::default());
                env.source = source.into();
                env.event = "context".into();
                envs.push(env);
            }
        }
        let mut first = None;
        let total = 2 * INGRESS_QUEUE_CAPACITY + 12;
        for index in 0..total {
            let mut env = envs[index % envs.len()].clone();
            env.ts_ms = index as i64;
            env.payload = serde_json::json!({"index":index,"event":{"messages":[{"role":"user","content":"history".repeat(1024)},{"role":"assistant","content":index}]}});
            daemon.capture_event(env).await.unwrap();
            if index + 1 == 2 * INGRESS_QUEUE_CAPACITY {
                let CaptureWork::Evicted(bounds) = daemon.next_capture_work() else {
                    panic!("missing first eviction");
                };
                assert_eq!(bounds.len(), envs.len());
                first = Some(bounds);
            }
        }
        {
            let buffer = daemon.capture_buffer.lock();
            assert_eq!(buffer.events.len(), INGRESS_QUEUE_CAPACITY);
            assert_eq!(
                buffer.events.front().unwrap().env.payload["index"],
                total - INGRESS_QUEUE_CAPACITY
            );
        }
        replay_evicted(&daemon, &first.unwrap()).await.unwrap();
        let CaptureWork::Evicted(second) = daemon.next_capture_work() else {
            panic!("new captures must extend recovery");
        };
        replay_evicted(&daemon, &second).await.unwrap();
        drain_capture_buffer(&daemon).await.unwrap();
        ready.send(()).unwrap();
        daemon.settle_ingress().await;
        for (slot, env) in envs.iter().enumerate() {
            // These sources may have no authenticated delivery actor. Wait for
            // translation itself before inspecting its durable head.
            daemon
                .translation_for(env)
                .await
                .unwrap()
                .barrier()
                .await
                .unwrap();
            let events = daemon
                .events_for(&env.source, &env.session_id)
                .await
                .unwrap();
            let partition = crate::collections::SpanOps::for_events(&events)
                .await
                .unwrap();
            assert_eq!(
                partition.translated_through().await.unwrap(),
                events.tail().await.unwrap()
            );
            let expected: Vec<_> = (slot..total).step_by(envs.len()).collect();
            let (_, state) = partition.checkpoint().await.unwrap().unwrap();
            let state: Value = serde_json::from_slice(&state).unwrap();
            if env.source == "debug" {
                assert_eq!(state["native"]["event_seq"], expected.len() as u64);
                let actual: Vec<_> = partition
                    .stored()
                    .await
                    .into_iter()
                    .filter_map(|op| {
                        let (SpanOp::Insert(row) | SpanOp::Merge(row)) = op;
                        row.input?.get("index")?.as_u64()
                    })
                    .collect();
                assert_eq!(
                    actual,
                    expected.into_iter().map(|i| i as u64).collect::<Vec<_>>()
                );
            }
        }
        drain_all(&daemon).await;
    }

    #[tokio::test]
    async fn shutdown_observes_transcript_registered_by_queued_capture() {
        let tmp = tempfile::tempdir().unwrap();
        let transcript = tmp.path().join("transcript.jsonl");
        tokio::fs::write(&transcript, b"{}\n").await.unwrap();
        let (daemon, ready) = paused_ingress(tmp.path());
        let mut env = event("late-transcript", SessionRoute::default());
        env.source = "claude-code".into();
        env.event = "Stop".into();
        env.payload = serde_json::json!({"transcript_path": transcript});
        daemon.capture_event(env.clone()).await.unwrap();
        tokio::fs::write(&transcript, b"{}\n{}\n").await.unwrap();

        let shutdown_daemon = daemon.clone();
        let shutdown = tokio::spawn(async move { drain_all(&shutdown_daemon).await });
        while !daemon.quiescing.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        drain_capture_buffer(&daemon).await.unwrap();
        ready.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), shutdown)
            .await
            .unwrap()
            .unwrap();
        let identity = daemon
            .events_for(&env.source, &env.session_id)
            .await
            .unwrap()
            .identity()
            .await
            .unwrap()
            .unwrap();
        assert!(
            identity.event == "TranscriptUpdate",
            "late transcript must be journaled before shutdown"
        );
    }

    #[tokio::test]
    async fn startup_overflow_preserves_parent_child_tool_order() {
        use distributed_fixture::{AgentKind, DistributedFixtures, ProcessTree};
        let tmp = tempfile::tempdir().unwrap();
        let (ready, startup) = oneshot::channel();
        let mut opts = debug_serve_options("test", tmp.path());
        opts.auth_provider = Some(Arc::new(TestAuth));
        let daemon = Daemon::new_with_startup(opts, tmp.path().into(), Some(startup), RETENTION);
        let mut fixtures = DistributedFixtures::new();
        let parent = ProcessTree::root(1000);
        let child = ProcessTree::child(1002, 1001, &parent);
        let mut events =
            fixtures.start_turn(AgentKind::OpenCode, "parent", &parent, "delegate", 100);
        events.push(fixtures.open_tool(
            AgentKind::OpenCode,
            "parent",
            &parent,
            "tool",
            "do work",
            102,
        ));
        let child_events =
            fixtures.start_turn(AgentKind::OpenCode, "child", &child, "do work", 103);
        let child_key = automatic_link_key(&child_events[0]);
        events.extend(child_events);
        events.push(fixtures.close_tool(
            AgentKind::OpenCode,
            "parent",
            &parent,
            "tool",
            "do work",
            "done",
            105,
        ));
        for env in events {
            daemon.capture_event(env).await.unwrap();
        }
        for i in 0..INGRESS_QUEUE_CAPACITY {
            let mut filler = event("filler", SessionRoute::default());
            filler.ts_ms = i as i64;
            daemon.capture_event(filler).await.unwrap();
        }
        drain_capture_buffer(&daemon).await.unwrap();
        ready.send(()).unwrap();
        daemon.settle_ingress().await;
        let route = daemon
            .automatic_links
            .lock()
            .get(&child_key)
            .cloned()
            .expect("child must be linked");
        let Some(crate::wire::TraceDestination::ParentSpan { components }) = route.destination
        else {
            panic!("child was not attached to the parent tool");
        };
        let tool_id = crate::ids::span_id(
            &crate::ids::session_namespace("opencode", "parent"),
            "tool:parent:tool",
        );
        assert_eq!(components.span_id.as_deref(), Some(tool_id.as_str()));
        drain_all(&daemon).await;
    }

    #[tokio::test]
    async fn completed_inactive_sessions_are_removed_and_every_current_obligation_retains_one() {
        for scenario in [
            "no_heads",
            "unresolved",
            "second_unresolved",
            "rejected",
            "held",
            "source_held",
            "untranslated",
            "active",
            "within_retention",
            "complete",
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let env = event(scenario, SessionRoute::default());
            let events =
                crate::collections::EventCollection::open(tmp.path(), &env.source, &env.session_id)
                    .await
                    .unwrap();
            let through = events.append(&env, None).await.unwrap().unwrap().through;
            let partition = crate::collections::SpanOps::for_events(&events)
                .await
                .unwrap();
            partition
                .commit_translation(crate::collections::Translated {
                    input: through,
                    observation: true,
                    ops: vec![SpanOp::Insert(SpanRow {
                        span_id: "stored".into(),
                        ..Default::default()
                    })],
                    roots: Default::default(),
                    state: Some(b"null".to_vec()),
                })
                .await
                .unwrap();
            if scenario == "untranslated" {
                let mut later = env.clone();
                later.ts_ms += 1;
                events.append(&later, None).await.unwrap().unwrap();
            }
            if scenario != "no_heads" {
                let mut head = Cursor::open(&partition, env.route.as_ref().unwrap())
                    .await
                    .unwrap();
                if scenario == "second_unresolved" {
                    let mut second = env.route.clone().unwrap();
                    second.tags.insert("second".into());
                    Cursor::open(&partition, &second).await.unwrap();
                }
                if !matches!(scenario, "unresolved" | "rejected") {
                    head.next_batch(8)
                        .await
                        .unwrap()
                        .unwrap()
                        .into_completed()
                        .await
                        .ok()
                        .unwrap();
                }
                if matches!(scenario, "held" | "rejected") {
                    partition
                        .set_failure(&head.route, Some("held".into()))
                        .await
                        .unwrap();
                }
            }
            if scenario == "source_held" {
                partition
                    .set_source_failure(Some("held".into()))
                    .await
                    .unwrap();
            }
            drop((partition, events));
            let daemon = Daemon::new(
                super::super::debug_serve_options("test", tmp.path()),
                tmp.path().into(),
            );
            if scenario == "active" {
                daemon
                    .events_for(&env.source, &env.session_id)
                    .await
                    .unwrap();
            }
            let storage =
                crate::collections::inspect::session_path(tmp.path(), &env.source, &env.session_id);
            if scenario != "within_retention" {
                crate::collections::inspect::Inspector::open(&storage)
                    .unwrap()
                    .age(RETENTION + Duration::from_secs(3600))
                    .unwrap();
            }
            collect_sessions(&daemon).await;
            // Output that no destination has accepted for the whole retention
            // period is expired with the route that owed it.
            let complete = matches!(
                scenario,
                "complete" | "no_heads" | "unresolved" | "second_unresolved" | "rejected"
            );
            assert_eq!(storage.exists(), !complete, "{scenario}");
            for sidecar in ["sqlite-wal", "sqlite-shm"] {
                assert!(
                    !complete || !storage.with_extension(sidecar).exists(),
                    "{scenario}: {sidecar}"
                );
            }
            drain_all(&daemon).await;
        }
    }

    #[tokio::test]
    async fn a_live_route_held_by_a_failure_expires_after_retention() {
        use crate::pipeline::recovery::Failure;
        let tmp = tempfile::tempdir().unwrap();
        let route = SessionRoute {
            destination: Some(crate::wire::TraceDestination::ProjectLogs {
                project_id: Some("test-project".into()),
                project_name: None,
            }),
            ..Default::default()
        };
        let env = event("rejected", route.clone());
        let events =
            crate::collections::EventCollection::open(tmp.path(), &env.source, &env.session_id)
                .await
                .unwrap();
        let through = events.append(&env, None).await.unwrap().unwrap().through;
        let spans = crate::collections::SpanOps::for_events(&events)
            .await
            .unwrap();
        drop(events);
        spans
            .translated(
                through,
                vec![SpanOp::Insert(SpanRow {
                    span_id: "rejected".into(),
                    root_span_id: "rejected".into(),
                    end_ms: Some(1),
                    ..Default::default()
                })],
            )
            .await
            .unwrap();
        Cursor::open(&spans, &route).await.unwrap();
        let lease = TestAuth
            .resolve(&route.auth, AuthResolveReason::Initial)
            .await
            .unwrap();
        // A rejection that no new settings correct, such as a span too large
        // for its destination.
        let failure = Failure::legacy(
            "413 Request Too Long".into(),
            1,
            &route.with_auth(lease.auth),
        );
        let key = serde_json::to_string(&route).unwrap();
        spans
            .set_failure(&key, Some(serde_json::to_string(&failure).unwrap()))
            .await
            .unwrap();

        let mut options = debug_serve_options("test", tmp.path());
        options.auth_provider = Some(Arc::new(TestAuth));
        let daemon = Daemon::new(options, tmp.path().into());
        super::super::recovery::recover_unprocessed_journals(&daemon).await;
        daemon.ingress_barrier().await;
        let delivery = DeliveryKey::new(&env.source, &env.session_id, &route).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !daemon.sessions.lock().contains_key(&delivery) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        let path =
            crate::collections::inspect::session_path(tmp.path(), &env.source, &env.session_id);
        let db = crate::collections::inspect::Inspector::open(&path).unwrap();
        collect_sessions(&daemon).await;
        assert!(daemon.sessions.lock().contains_key(&delivery));
        assert_eq!(db.heads().len(), 1, "a recent failure is kept for recovery");

        db.age(RETENTION + Duration::from_secs(3600)).unwrap();
        collect_sessions(&daemon).await;
        assert!(
            !daemon.sessions.lock().contains_key(&delivery),
            "the held delivery is stopped"
        );
        assert!(db.heads().is_empty());
        assert_eq!(db.count(crate::collections::inspect::Table::Ops), 0);
        drain_all(&daemon).await;
    }

    #[tokio::test]
    async fn retained_inputs_protect_only_their_reconstruction_references() {
        for routed in [true, false] {
            let tmp = tempfile::tempdir().unwrap();
            let dir = crate::transcript_mirror::mirror_dir(tmp.path());
            std::fs::create_dir_all(&dir).unwrap();
            let retained = dir.join("retained.jsonl");
            let obsolete = dir.join("obsolete.jsonl");
            std::fs::write(&retained, "retained").unwrap();
            std::fs::write(&obsolete, "obsolete").unwrap();
            let mut env = event("test", SessionRoute::default());
            if !routed {
                env.route = None;
            }
            env.payload =
                serde_json::json!({"_bt_transcript_mirror":{"mirror": retained,"through":8}});
            crate::collections::EventCollection::open(tmp.path(), "debug", "test")
                .await
                .unwrap()
                .append(&env, None)
                .await
                .unwrap()
                .unwrap();
            let key = automatic_link_key(&env);
            let state = PendingSession {
                events: vec![PendingEvent {
                    env,
                    replay_through: 0,
                    journal_through: 1,
                    settings_through: None,
                }],
                ..Default::default()
            };
            write_correlation_state(tmp.path(), &key, &state)
                .await
                .unwrap();
            let parent = active_parent_snapshot_path(tmp.path(), &key);
            std::fs::create_dir_all(parent.parent().unwrap()).unwrap();
            std::fs::write(&parent, "retained parent").unwrap();
            let protected = retained_references(tmp.path()).await.unwrap();
            crate::transcript_mirror::gc_old_mirrors(tmp.path(), Duration::ZERO, &protected).await;
            gc_old_correlation_dir(&tmp.path().join("correlation"), Duration::ZERO, &protected)
                .await;
            gc_old_correlation_dir(parent.parent().unwrap(), Duration::ZERO, &protected).await;
            assert!(retained.exists());
            assert!(parent.exists());
            assert!(read_correlation_state(tmp.path(), &key).await.is_some());
            assert!(!obsolete.exists());
        }
    }
}
