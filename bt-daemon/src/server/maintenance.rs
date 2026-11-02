//! Background maintenance: retention GC, idle-session retirement, the idle
//! watchdog, and draining on shutdown.

use super::*;

/// How long recovery state is kept on disk.
pub(super) const RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// How often that retention is enforced while the daemon keeps running.
pub(super) const GC_INTERVAL: Duration = Duration::from_secs(60 * 60);

pub(super) async fn collect_garbage(daemon: &Daemon) {
    collect_partitions(daemon, RETENTION).await;
    journal::gc_old_journals(&daemon.data_dir, RETENTION).await;
    journal::gc_old_managed_runs(&daemon.data_dir, RETENTION).await;
    // Protect references from every retained input log, including completed
    // partitions still inside retention and sessions with no registered heads.
    let Ok(protected) = retained_references(&daemon.data_dir).await else {
        return;
    };
    crate::transcript_mirror::gc_old_mirrors(&daemon.data_dir, RETENTION, &protected).await;
    gc_old_correlation_dir(&daemon.data_dir.join("correlation"), RETENTION, &protected).await;
    gc_old_correlation_dir(
        &daemon.data_dir.join("correlation/parents"),
        RETENTION,
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

async fn collect_partitions(daemon: &Daemon, age: Duration) {
    let Ok(mut entries) = tokio::fs::read_dir(journal::journal_dir(&daemon.data_dir)).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|v| v.to_str()) != Some("ndjson") || !old(&path, age).await {
            continue;
        }
        let recorded = journal::JournalReader::recorded_len(&path).await;
        let Ok(Some(mut reader)) = journal::JournalReader::open(&path, recorded).await else {
            continue;
        };
        let Ok(Some(first)) = reader.next_entry().await else {
            continue;
        };
        let id = crate::ids::session_storage_id(&first.source, &first.session_id);
        // Legacy mixed-source logs are handled by their own conservative reader.
        if path != journal::source_journal_path(&daemon.data_dir, &first.source, &first.session_id)
        {
            continue;
        }
        let storage = daemon
            .data_dir
            .join("pipeline")
            .join(format!("{id}.sqlite"));
        if !old(&storage, age).await {
            continue;
        }
        let wal = storage.with_extension("sqlite-wal");
        if tokio::fs::try_exists(&wal).await.unwrap_or(true) && !old(&wal, age).await {
            continue;
        }
        // Scanning a large journal must not hold the capture lock. The length
        // and route set are revalidated under that lock before deletion.
        let Ok(partition) = crate::pipeline::journal::SpanJournal::open(&storage).await else {
            continue;
        };
        let Ok(routes) = partition.routes().await else {
            continue;
        };
        if !captured_routes_registered(&daemon.data_dir, &path, recorded, &routes).await {
            continue;
        }
        let lock = daemon.session_lock(&first.source, &first.session_id);
        let _guard = lock.lock().await;
        let namespace = crate::ids::session_namespace(&first.source, &first.session_id);
        if daemon.journals.lock().contains_key(&path)
            || daemon.translations.lock().contains_key(&namespace)
        {
            continue;
        }
        if journal::JournalReader::recorded_len(&path).await != recorded {
            continue;
        }
        let Ok(current_routes) = partition.routes().await else {
            continue;
        };
        let (Ok(current_routes), Ok(scanned_routes)) = (
            serde_json::to_value(&current_routes),
            serde_json::to_value(&routes),
        ) else {
            continue;
        };
        if current_routes != scanned_routes {
            continue;
        }
        let pending = daemon.pending_sessions.lock().values().any(|state| {
            state.events.iter().any(|event| {
                event.env.source == first.source && event.env.session_id == first.session_id
            })
        });
        if pending {
            continue;
        }

        if routes.iter().any(|route| {
            DeliveryKey::new(&first.source, &first.session_id, route)
                .is_ok_and(|key| daemon.correlation.has_active_tools(&key.correlation_key()))
        }) {
            continue;
        }
        if !partition.collectible(recorded).await.unwrap_or(false)
            || partition.close_storage().await.is_err()
        {
            continue;
        }
        drop(partition);
        drop(reader);
        // Delete the output partition first. A failed input removal can only
        // cause idempotent replay; it cannot hide new captures behind an old receipt.
        if tokio::fs::remove_file(&storage).await.is_err() {
            continue;
        }
        daemon.ingress_history.lock().remove(&path);
        daemon
            .ingress_dispatched
            .lock()
            .retain(|key, _| key.source != first.source || key.session_id != first.session_id);
        for sidecar in [wal, storage.with_extension("sqlite-shm"), path] {
            if let Err(error) = tokio::fs::remove_file(&sidecar).await {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(%error,path=%sidecar.display(),"expired partition cleanup failed");
                }
            }
        }
    }
}

// Authentication/discovery may have prevented subscription registration. Such
// captured routes are obligations too, even if all existing heads are complete.
async fn captured_routes_registered(
    data_dir: &std::path::Path,
    path: &std::path::Path,
    recorded: u64,
    heads: &[SessionRoute],
) -> bool {
    let Ok(Some(mut reader)) = journal::JournalReader::open(path, recorded).await else {
        return false;
    };
    loop {
        match reader.next_record().await {
            Ok(Some(record)) => {
                if let journal::JournalRecord::Event(event) = record.record {
                    let env = journal::envelope_from_redacted(event);
                    let route = read_correlation_state(data_dir, &automatic_link_key(&env))
                        .await
                        .and_then(|state| state.linked_route)
                        .or(env.route);
                    if route
                        .as_ref()
                        .is_none_or(|route| !heads.iter().any(|head| head.same_route(route)))
                    {
                        return false;
                    }
                }
            }
            Ok(None) => return true,
            Err(_) => return false,
        }
    }
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
    let mut protected = HashSet::new();
    let mut sources = HashSet::new();
    let mut entries = match tokio::fs::read_dir(journal::journal_dir(data_dir)).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(protected),
        Err(error) => return Err(error.into()),
    };
    while let Some(entry) = entries.next_entry().await? {
        if entry.path().extension().and_then(|v| v.to_str()) != Some("ndjson") {
            continue;
        }
        let mut reader = journal::JournalReader::open(
            &entry.path(),
            journal::JournalReader::recorded_len(&entry.path()).await,
        )
        .await?;
        while let Some(record) = match reader.as_mut() {
            Some(reader) => reader.next_record().await?,
            None => None,
        } {
            if let journal::JournalRecord::Event(event) = record.record {
                mirrors(&event.payload, &mut protected);
                let env = journal::envelope_from_redacted(event);
                let key = automatic_link_key(&env);
                protected.insert(correlation_state_path(data_dir, &key));
                protected.insert(active_parent_snapshot_path(data_dir, &key));
                let storage = data_dir.join("pipeline").join(format!(
                    "{}.sqlite",
                    crate::ids::session_storage_id(&env.source, &env.session_id)
                ));
                if sources.insert((env.source.clone(), env.session_id.clone()))
                    && tokio::fs::try_exists(&storage).await?
                {
                    let partition = crate::pipeline::journal::SpanJournal::open(&storage).await?;
                    for route in partition.routes().await? {
                        let key = DeliveryKey::new(&env.source, &env.session_id, &route)?
                            .correlation_key();
                        protected.insert(correlation_state_path(data_dir, &key));
                        protected.insert(active_parent_snapshot_path(data_dir, &key));
                    }
                }
            }
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
        if let Ok(tail) = source.control(crate::journal::ControlKind::Retire).await {
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

    struct TestAuth;

    #[async_trait]
    impl AuthProvider for TestAuth {
        async fn resolve(
            &self,
            selection: &AuthSelection,
            _: AuthResolveReason,
        ) -> anyhow::Result<AuthLease> {
            Ok(AuthLease {
                selection: selection.clone().canonicalized()?,
                auth: BackendAuth {
                    token: "test".into(),
                    api_url: Some("http://127.0.0.1.invalid".into()),
                    app_url: None,
                    org_name: Some("test".into()),
                    org_id: Some("test".into()),
                },
                expires_at_ms: None,
            })
        }
    }

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

    #[tokio::test]
    async fn capture_is_durable_while_startup_dispatch_is_blocked() {
        let tmp = tempfile::tempdir().unwrap();
        let (ready, startup) = oneshot::channel();
        let daemon = Daemon::new_with_startup(
            debug_serve_options("test", tmp.path()),
            tmp.path().into(),
            Some(startup),
        );
        let env = event("during-startup", SessionRoute::default());
        assert!(
            tokio::time::timeout(Duration::from_secs(1), daemon.capture_event(env.clone()))
                .await
                .unwrap()
                .unwrap()
        );
        let path = journal::source_journal_path(tmp.path(), &env.source, &env.session_id);
        assert!(tokio::fs::read_to_string(path)
            .await
            .unwrap()
            .contains("during-startup"));
        assert!(daemon.translations.lock().is_empty());
        drain_startup_buffer(&daemon).await.unwrap();
        ready.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), daemon.ingress_barrier())
            .await
            .unwrap();
        assert!(!daemon.translations.lock().is_empty());
        drain_all(&daemon).await;
    }

    #[tokio::test]
    async fn shutdown_observes_transcript_registered_by_queued_capture() {
        let tmp = tempfile::tempdir().unwrap();
        let transcript = tmp.path().join("transcript.jsonl");
        tokio::fs::write(&transcript, b"{}\n").await.unwrap();
        let (ready, startup) = oneshot::channel();
        let daemon = Daemon::new_with_startup(
            debug_serve_options("test", tmp.path()),
            tmp.path().into(),
            Some(startup),
        );
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
        drain_startup_buffer(&daemon).await.unwrap();
        ready.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), shutdown)
            .await
            .unwrap()
            .unwrap();
        let path = journal::source_journal_path(tmp.path(), &env.source, &env.session_id);
        let content = tokio::fs::read_to_string(path).await.unwrap();
        assert!(
            content.contains("TranscriptUpdate"),
            "late transcript must be journaled before shutdown"
        );
    }

    #[tokio::test]
    async fn startup_recovery_dispatches_old_session_before_new_capture() {
        let tmp = tempfile::tempdir().unwrap();
        let env = event("resuming", SessionRoute::default());
        let path = journal::source_journal_path(tmp.path(), &env.source, &env.session_id);
        let mut writer = JournalWriter::open_path(&path).await.unwrap();
        let old_end = writer.append(&env).await.unwrap();
        drop(writer);
        let (ready, startup) = oneshot::channel();
        let daemon = Daemon::new_with_startup(
            debug_serve_options("test", tmp.path()),
            tmp.path().into(),
            Some(startup),
        );
        let mut next = env.clone();
        next.event = "next".into();
        next.ts_ms = 2;
        assert!(daemon.capture_event(next).await.unwrap());
        let recovered = discover_unprocessed_journals(&daemon).await;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].journal_through, old_end);
        for event in recovered {
            dispatch_ingress_event(&daemon, event).await;
        }
        drain_startup_buffer(&daemon).await.unwrap();
        ready.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), daemon.ingress_barrier())
            .await
            .unwrap();
        let source = daemon.translations.lock().values().next().unwrap().clone();
        source.barrier().await.unwrap();
        let storage = tmp.path().join("pipeline").join(format!(
            "{}.sqlite",
            crate::ids::session_storage_id(&env.source, &env.session_id)
        ));
        let partition = crate::pipeline::journal::SpanJournal::open(&storage)
            .await
            .unwrap();
        let through = partition.translated_through().await.unwrap();
        assert!(through > old_end);
        assert_eq!(through, journal::JournalReader::recorded_len(&path).await);
        drain_all(&daemon).await;
    }

    #[tokio::test]
    async fn startup_overflow_replays_oldest_events_before_the_buffered_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let (ready, startup) = oneshot::channel();
        let daemon = Daemon::new_with_startup(
            debug_serve_options("test", tmp.path()),
            tmp.path().into(),
            Some(startup),
        );
        let env = event("overflow", SessionRoute::default());
        let path = journal::source_journal_path(tmp.path(), &env.source, &env.session_id);
        for i in 0..(INGRESS_QUEUE_CAPACITY + 6) {
            let mut next = env.clone();
            next.ts_ms = i as i64;
            next.payload = serde_json::json!({"index": i});
            assert!(daemon.capture_event(next).await.unwrap());
        }
        {
            let state = daemon.startup_buffer.lock();
            let buffer = state.as_ref().unwrap();
            assert_eq!(buffer.events.len(), INGRESS_QUEUE_CAPACITY);
            assert_eq!(buffer.events.front().unwrap().env.payload["index"], 6);
        }
        drain_startup_buffer(&daemon).await.unwrap();
        ready.send(()).unwrap();
        daemon.ingress_barrier().await;
        let source = daemon.translations.lock().values().next().unwrap().clone();
        source.barrier().await.unwrap();
        let storage = tmp.path().join("pipeline").join(format!(
            "{}.sqlite",
            crate::ids::session_storage_id(&env.source, &env.session_id)
        ));
        let db = rusqlite::Connection::open(storage).unwrap();
        let count: u64 = db
            .query_row(
                "SELECT count(*) FROM batches WHERE committed=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, (INGRESS_QUEUE_CAPACITY + 6) as u64);
        let partition = crate::pipeline::journal::SpanJournal::open(
            &tmp.path().join("pipeline").join(format!(
                "{}.sqlite",
                crate::ids::session_storage_id(&env.source, &env.session_id)
            )),
        )
        .await
        .unwrap();
        assert_eq!(
            partition.translated_through().await.unwrap(),
            journal::JournalReader::recorded_len(&path).await
        );
        drop(db);
        drain_all(&daemon).await;
    }

    #[tokio::test]
    async fn startup_overflow_advances_again_while_evicted_records_are_catching_up() {
        let tmp = tempfile::tempdir().unwrap();
        let (ready, startup) = oneshot::channel();
        let daemon = Daemon::new_with_startup(
            debug_serve_options("test", tmp.path()),
            tmp.path().into(),
            Some(startup),
        );
        let env = event("chasing", SessionRoute::default());
        for i in 0..(INGRESS_QUEUE_CAPACITY + 2) {
            let mut next = env.clone();
            next.ts_ms = i as i64;
            next.payload = serde_json::json!({"index": i});
            assert!(daemon.capture_event(next).await.unwrap());
        }
        let StartupWork::Evicted(first) = daemon.next_startup_work() else {
            panic!("the oldest events were not evicted");
        };
        for i in INGRESS_QUEUE_CAPACITY + 2..INGRESS_QUEUE_CAPACITY + 7 {
            let mut next = env.clone();
            next.ts_ms = i as i64;
            next.payload = serde_json::json!({"index": i});
            assert!(daemon.capture_event(next).await.unwrap());
        }
        {
            let state = daemon.startup_buffer.lock();
            let buffer = state.as_ref().unwrap();
            assert_eq!(buffer.events.len(), INGRESS_QUEUE_CAPACITY);
            assert_eq!(buffer.events.front().unwrap().env.payload["index"], 7);
        }
        let mut cursors = HashMap::new();
        replay_evicted_startup(&daemon, &mut cursors, &first)
            .await
            .unwrap();
        let StartupWork::Evicted(second) = daemon.next_startup_work() else {
            panic!("new evictions did not advance recovery");
        };
        replay_evicted_startup(&daemon, &mut cursors, &second)
            .await
            .unwrap();
        drain_startup_buffer(&daemon).await.unwrap();
        ready.send(()).unwrap();
        daemon.ingress_barrier().await;
        let source = daemon.translations.lock().values().next().unwrap().clone();
        source.barrier().await.unwrap();
        let storage = tmp.path().join("pipeline").join(format!(
            "{}.sqlite",
            crate::ids::session_storage_id(&env.source, &env.session_id)
        ));
        let db = rusqlite::Connection::open(storage).unwrap();
        let count: u64 = db
            .query_row(
                "SELECT count(*) FROM batches WHERE committed=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, (INGRESS_QUEUE_CAPACITY + 7) as u64);
        drop(db);
        drain_all(&daemon).await;
    }

    #[tokio::test]
    async fn overflow_snapshot_excludes_unacknowledged_writes() {
        let tmp = tempfile::tempdir().unwrap();
        let (ready, startup) = oneshot::channel();
        let mut opts = debug_serve_options("test", tmp.path());
        opts.auth_provider = Some(Arc::new(TestAuth));
        let daemon = Daemon::new_with_startup(opts, tmp.path().into(), Some(startup));
        *daemon.startup_buffer.lock() = None;
        let mut env = event("durable-bound", SessionRoute::default());
        env.payload = serde_json::json!({"index":0});
        daemon.capture_event(env.clone()).await.unwrap();
        let path = journal::source_journal_path(tmp.path(), &env.source, &env.session_id);
        let acknowledged = tokio::fs::read(&path).await.unwrap();
        let through = acknowledged.len() as u64;
        // OS-visible bytes need not have completed fsync. Model a complete
        // record written after the writer's acknowledged durable position.
        env.payload = serde_json::json!({"index":1});
        let mut unacknowledged = acknowledged.clone();
        unacknowledged.extend(serde_json::to_vec(&env.redacted()).unwrap());
        unacknowledged.push(b'\n');
        tokio::fs::write(&path, unacknowledged).await.unwrap();
        recover_ingress_overflow(&daemon).await;
        let source = daemon.translations.lock().values().next().unwrap().clone();
        source.barrier().await.unwrap();
        let storage = tmp.path().join("pipeline").join(format!(
            "{}.sqlite",
            crate::ids::session_storage_id(&env.source, &env.session_id)
        ));
        let partition = crate::pipeline::journal::SpanJournal::open(&storage)
            .await
            .unwrap();
        assert_eq!(
            partition.translated_through().await.unwrap(),
            through,
            "recovery must not commit beyond the durable capture head"
        );
        tokio::fs::write(&path, acknowledged).await.unwrap();
        ready.send(()).unwrap();
        daemon.settle_ingress().await;
        drain_all(&daemon).await;
    }

    #[tokio::test]
    async fn startup_overflow_preserves_parent_child_tool_order() {
        use distributed_fixture::{AgentKind, DistributedFixtures, ProcessTree};
        let tmp = tempfile::tempdir().unwrap();
        let (ready, startup) = oneshot::channel();
        let mut opts = debug_serve_options("test", tmp.path());
        opts.auth_provider = Some(Arc::new(TestAuth));
        let daemon = Daemon::new_with_startup(opts, tmp.path().into(), Some(startup));
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
        drain_startup_buffer(&daemon).await.unwrap();
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
    async fn steady_overflow_preserves_parent_child_tool_order() {
        use distributed_fixture::{AgentKind, DistributedFixtures, ProcessTree};
        let tmp = tempfile::tempdir().unwrap();
        let (ready, startup) = oneshot::channel();
        let mut opts = debug_serve_options("test", tmp.path());
        opts.auth_provider = Some(Arc::new(TestAuth));
        let daemon = Daemon::new_with_startup(opts, tmp.path().into(), Some(startup));
        // Pause the steady ingress worker while captures saturate its queue.
        *daemon.startup_buffer.lock() = None;
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
        assert!(daemon.ingress_overflow.load(Ordering::Acquire));
        recover_ingress_overflow(&daemon).await;
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
    async fn startup_overflow_tracks_each_source_journal_independently() {
        let tmp = tempfile::tempdir().unwrap();
        let (ready, startup) = oneshot::channel();
        let daemon = Daemon::new_with_startup(
            debug_serve_options("test", tmp.path()),
            tmp.path().into(),
            Some(startup),
        );
        for i in 0..(INGRESS_QUEUE_CAPACITY + 6) {
            let session = if i % 2 == 0 { "first" } else { "second" };
            let mut env = event(session, SessionRoute::default());
            env.ts_ms = i as i64;
            env.payload = serde_json::json!({"index": i});
            assert!(daemon.capture_event(env).await.unwrap());
        }
        {
            let state = daemon.startup_buffer.lock();
            let buffer = state.as_ref().unwrap();
            assert_eq!(buffer.events.len(), INGRESS_QUEUE_CAPACITY);
        }
        drain_startup_buffer(&daemon).await.unwrap();
        ready.send(()).unwrap();
        daemon.ingress_barrier().await;
        let sources: Vec<_> = daemon.translations.lock().values().cloned().collect();
        for source in sources {
            source.barrier().await.unwrap();
        }
        for session in ["first", "second"] {
            let storage = tmp.path().join("pipeline").join(format!(
                "{}.sqlite",
                crate::ids::session_storage_id("debug", session)
            ));
            let db = rusqlite::Connection::open(storage).unwrap();
            let count: u64 = db
                .query_row(
                    "SELECT count(*) FROM batches WHERE committed=1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, ((INGRESS_QUEUE_CAPACITY + 6) / 2) as u64);
        }
        drain_all(&daemon).await;
    }

    #[tokio::test]
    async fn startup_overflow_keeps_pi_context_across_recovery_advances() {
        let tmp = tempfile::tempdir().unwrap();
        let (ready, startup) = oneshot::channel();
        let daemon = Daemon::new_with_startup(
            debug_serve_options("test", tmp.path()),
            tmp.path().into(),
            Some(startup),
        );
        let mut env = event("pi-overflow", SessionRoute::default());
        env.source = "pi".into();
        env.event = "context".into();
        for i in 0..(INGRESS_QUEUE_CAPACITY + 2) {
            env.ts_ms = i as i64;
            env.payload = serde_json::json!({"event":{"messages":[{"role":"user","content":"shared"},{"role":"assistant","content":i}]}});
            assert!(daemon.capture_event(env.clone()).await.unwrap());
        }
        let StartupWork::Evicted(first) = daemon.next_startup_work() else {
            panic!("missing first overflow")
        };
        let mut cursors = HashMap::new();
        replay_evicted_startup(&daemon, &mut cursors, &first)
            .await
            .unwrap();
        for i in INGRESS_QUEUE_CAPACITY + 2..INGRESS_QUEUE_CAPACITY + 4 {
            env.ts_ms = i as i64;
            env.payload = serde_json::json!({"event":{"messages":[{"role":"user","content":"shared"},{"role":"assistant","content":i}]}});
            assert!(daemon.capture_event(env.clone()).await.unwrap());
        }
        let StartupWork::Evicted(second) = daemon.next_startup_work() else {
            panic!("missing second overflow")
        };
        replay_evicted_startup(&daemon, &mut cursors, &second)
            .await
            .unwrap();
        drain_startup_buffer(&daemon).await.unwrap();
        ready.send(()).unwrap();
        daemon.ingress_barrier().await;
        let source = daemon.translations.lock().values().next().unwrap().clone();
        source.barrier().await.unwrap();
        let storage = tmp.path().join("pipeline").join(format!(
            "{}.sqlite",
            crate::ids::session_storage_id(&env.source, &env.session_id)
        ));
        let db = rusqlite::Connection::open(storage).unwrap();
        let count: u64 = db
            .query_row(
                "SELECT count(*) FROM batches WHERE committed=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, (INGRESS_QUEUE_CAPACITY + 4) as u64);
        drop(db);
        drain_all(&daemon).await;
    }
    #[tokio::test]
    async fn retention_requires_complete_inactive_partitions_and_nonempty_heads() {
        for scenario in [
            "no_heads",
            "unresolved",
            "unregistered",
            "held",
            "source_held",
            "untranslated",
            "invisible",
            "active",
            "within_retention",
            "complete",
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let env = event(scenario, SessionRoute::default());
            let raw = journal::source_journal_path(tmp.path(), &env.source, &env.session_id);
            let mut writer = JournalWriter::open_path(&raw).await.unwrap();
            let mut through = writer.append(&env).await.unwrap();
            if scenario == "unregistered" {
                let mut other = env.clone();
                other
                    .route
                    .as_mut()
                    .unwrap()
                    .tags
                    .push("not registered".into());
                through = writer.append(&other).await.unwrap();
            }
            drop(writer);
            let storage = tmp.path().join("pipeline").join(format!(
                "{}.sqlite",
                crate::ids::session_storage_id(&env.source, &env.session_id)
            ));
            let partition = crate::pipeline::journal::SpanJournal::open(&storage)
                .await
                .unwrap();
            let input = if scenario == "untranslated" {
                through - 1
            } else {
                through
            };
            partition.begin(input, true).await.unwrap();
            partition
                .append(
                    input,
                    &[SpanOp::Insert(SpanRow {
                        span_id: "stored".into(),
                        ..Default::default()
                    })],
                )
                .await
                .unwrap();
            partition.commit(input).await.unwrap();
            if scenario != "no_heads" {
                let mut head = partition
                    .subscribe(env.route.as_ref().unwrap(), 0)
                    .await
                    .unwrap();
                if scenario != "unresolved" {
                    head.next_batch(8)
                        .await
                        .unwrap()
                        .unwrap()
                        .into_completed()
                        .await
                        .ok()
                        .unwrap();
                }
                if scenario == "held" {
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
            if scenario == "invisible" {
                partition.begin(through + 1, true).await.unwrap();
            }
            drop(partition);
            let daemon = Daemon::new(
                super::super::debug_serve_options("test", tmp.path()),
                tmp.path().into(),
            );
            if scenario == "active" {
                daemon
                    .journal_writer_for(&env.source, &env.session_id)
                    .await
                    .unwrap();
            }
            let age = if scenario == "within_retention" {
                RETENTION
            } else {
                Duration::ZERO
            };
            collect_partitions(&daemon, age).await;
            assert_eq!(storage.exists(), scenario != "complete", "{scenario}");
            assert_eq!(raw.exists(), scenario != "complete", "{scenario}");
            drain_all(&daemon).await;
        }
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
            let raw = journal::source_journal_path(tmp.path(), "debug", "test");
            let mut writer = JournalWriter::open_path(&raw).await.unwrap();
            writer.append(&env).await.unwrap();
            drop(writer);
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
