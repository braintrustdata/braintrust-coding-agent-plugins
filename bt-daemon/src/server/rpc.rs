//! The JSON-RPC listener, request handling and status reporting.

use super::*;

pub(super) async fn accept_loop(daemon: Arc<Daemon>, mut listener: Listener) -> anyhow::Result<()> {
    loop {
        // `Notify::notify_waiters` does not retain a permit. If accepting a
        // connection wins the select at the same time shutdown is requested,
        // check the sticky flag before waiting again so the notification
        // cannot be lost.
        if daemon.shutting_down.load(Ordering::SeqCst) {
            tracing::info!("shutdown requested");
            return Ok(());
        }
        tokio::select! {
            _ = daemon.shutdown.notified() => {
                tracing::info!("shutdown requested");
                return Ok(());
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("interrupt received");
                return Ok(());
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok(stream) => {
                        let d = daemon.clone();
                        tokio::spawn(async move {
                            if let Err(e) = serve_connection(d, stream).await {
                                tracing::debug!("connection ended: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!("accept error: {e}");
                    }
                }
            }
        }
    }
}

pub(super) async fn serve_connection(
    daemon: Arc<Daemon>,
    stream: ServerStream,
) -> anyhow::Result<()> {
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut lines = BufReader::new(read_half).lines();
    let mut client = None;

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let mut shutdown_after_response = false;
        let response = match Message::from_line(&line) {
            Ok(Message::Request(req)) => {
                let request_id = req.id.clone();
                let method = req.method.clone();
                tracing::info!(
                    request_id = ?request_id,
                    method,
                    "request received"
                );
                let response = handle_request(&daemon, req, &mut client).await;
                shutdown_after_response = method == method::DAEMON_SHUTDOWN
                    && response.error.is_none()
                    && response
                        .result
                        .as_ref()
                        .and_then(|result| result.get("ok"))
                        .and_then(Value::as_bool)
                        == Some(true);
                if let Some(error) = &response.error {
                    tracing::warn!(
                        request_id = ?request_id,
                        method,
                        error_code = error.code,
                        error = %error.message,
                        "request failed"
                    );
                } else {
                    tracing::info!(
                        request_id = ?request_id,
                        method,
                        "request completed"
                    );
                }
                Some(response)
            }
            Ok(Message::Notification(note)) => {
                tracing::info!(method = %note.method, "notification received");
                // Legacy best-effort notifications cannot participate in the
                // acknowledged handover retry used by current clients.
                if note.method == method::EVENT_LOG {
                    if let Some(params) = note.params {
                        match serde_json::from_value::<Envelope>(params) {
                            Ok(mut env) => {
                                attach_process_capture(&daemon, &mut env, client.as_ref());
                                match daemon.capture_event(env).await {
                                    Ok(true) => {}
                                    Ok(false) => tracing::warn!(
                                        "event notification arrived while daemon was shutting down"
                                    ),
                                    Err(error) => {
                                        tracing::warn!(%error, "event notification was not captured")
                                    }
                                }
                            }
                            Err(error) => tracing::warn!(
                                method = %note.method,
                                error = %error,
                                "notification parameters rejected"
                            ),
                        }
                    }
                }
                None
            }
            Ok(Message::Response(_)) => None, // clients don't send us responses
            Err(e) => {
                tracing::warn!(error = %e, "request parse failed");
                Some(Response::err(
                    crate::wire::RequestId::Int(0),
                    RpcError::new(error_code::PARSE, format!("parse error: {e}")),
                ))
            }
        };

        if let Some(resp) = response {
            let mut buf = Message::Response(resp).to_line()?;
            buf.push('\n');
            let write_result = async {
                write_half.write_all(buf.as_bytes()).await?;
                write_half.flush().await
            }
            .await;
            if shutdown_after_response {
                daemon.trigger_shutdown();
            }
            write_result?;
        }
    }
    Ok(())
}

pub(super) fn attach_process_capture(
    daemon: &Daemon,
    env: &mut Envelope,
    client: Option<&crate::wire::ClientInfo>,
) {
    if env.capture.is_some() {
        return;
    }
    if !is_session_start(&env.source, &env.event)
        && !daemon
            .correlation
            .needs_process_capture(&env.source, &env.session_id)
    {
        return;
    }
    let Some(pid) = client.and_then(|client| client.pid) else {
        return;
    };
    let capture = crate::process::capture_process_context(pid);
    if !capture.process_chain.is_empty() || capture.truncated {
        env.capture = Some(capture);
    }
}

pub(super) async fn handle_request(
    daemon: &Arc<Daemon>,
    req: Request,
    client: &mut Option<crate::wire::ClientInfo>,
) -> Response {
    let id = req.id.clone();
    let params = req.params.unwrap_or(serde_json::Value::Null);

    macro_rules! parse {
        ($t:ty) => {
            match serde_json::from_value::<$t>(params) {
                Ok(v) => v,
                Err(e) => {
                    return Response::err(
                        id,
                        RpcError::new(error_code::INVALID_PARAMS, format!("invalid params: {e}")),
                    )
                }
            }
        };
    }

    match req.method.as_str() {
        method::INITIALIZE => {
            let p = parse!(InitializeParams);
            if p.protocol_version != PROTOCOL_VERSION {
                return Response::err(
                    id,
                    RpcError::new(
                        error_code::APP,
                        format!(
                            "protocol version mismatch: client {} daemon {}",
                            p.protocol_version, PROTOCOL_VERSION
                        ),
                    ),
                );
            }
            *client = Some(p.client);
            let result = InitializeResult {
                protocol_version: PROTOCOL_VERSION,
                daemon_version: daemon.version.clone(),
                capabilities: Capabilities {
                    sources: daemon.translators.sources(),
                },
            };
            Response::ok(id, serde_json::to_value(result).unwrap())
        }
        method::EVENT_LOG => {
            let mut env = parse!(Envelope);
            attach_process_capture(daemon, &mut env, client.as_ref());
            match daemon.capture_event(env).await {
                Ok(accepted) => Response::ok(
                    id,
                    serde_json::to_value(EventLogResult { accepted }).unwrap(),
                ),
                Err(error) => Response::err(id, RpcError::new(error_code::INTERNAL, error)),
            }
        }
        method::SESSION_FLUSH => {
            let p = parse!(FlushParams);
            // Explicit flushes wait for assembly to register every captured
            // route. Hook capture never waits on this.
            daemon.settle_assembly().await;
            if let Err(error) =
                resolve_pending_sessions(daemon, |env| env.session_id == p.session_id).await
            {
                return Response::err(id, RpcError::new(error_code::INTERNAL, error));
            }
            let result = daemon
                .flush_partitions(
                    daemon.partitions_for_session(&p.session_id).await,
                    None,
                    Duration::from_millis(p.timeout_ms),
                )
                .await;
            Response::ok(id, serde_json::to_value(result).unwrap())
        }
        method::MANAGED_RUN_FLUSH => {
            let params = parse!(ManagedRunFlushParams);
            daemon.settle_assembly().await;
            if let Err(error) = resolve_pending_sessions(daemon, |env| {
                env.managed_run_id.as_deref() == Some(params.managed_run_id.as_str())
            })
            .await
            {
                return Response::err(id, RpcError::new(error_code::INTERNAL, error));
            }
            let result = daemon.flush_managed_run(params).await;
            Response::ok(id, serde_json::to_value(result).unwrap())
        }
        method::STATUS_GET => {
            let p = parse!(StatusParams);
            daemon.settle_assembly().await;
            Response::ok(id, serde_json::to_value(daemon.status(p).await).unwrap())
        }
        method::AUTH_DIAGNOSE => {
            let p = parse!(AuthDiagnoseParams);
            Response::ok(
                id,
                serde_json::to_value(daemon.diagnose_auth(p.auth).await).unwrap(),
            )
        }
        method::DAEMON_SHUTDOWN => {
            if !client_may_shutdown(client.as_ref(), &daemon.version) {
                return Response::ok(
                    id,
                    serde_json::to_value(ShutdownResult { ok: false }).unwrap(),
                );
            }
            drain_all(daemon).await;
            Response::ok(
                id,
                serde_json::to_value(ShutdownResult { ok: true }).unwrap(),
            )
        }
        other => Response::err(
            id,
            RpcError::new(
                error_code::METHOD_NOT_FOUND,
                format!("unknown method: {other}"),
            ),
        ),
    }
}

pub(super) fn client_may_shutdown(client: Option<&ClientInfo>, daemon_version: &str) -> bool {
    let Some(client) = client else {
        return true; // Explicit stop commands do not initialize first.
    };
    let Some(client_version) = client.daemon_version.as_deref() else {
        return false; // Legacy initialized hooks must not downgrade the daemon.
    };
    crate::client::compare_daemon_versions(client_version, daemon_version)
        .is_none_or(|ordering| !ordering.is_lt())
}

fn recovery_status(
    stage: &str,
    record: &crate::pipeline::recovery::FailureRecord,
) -> crate::wire::RecoveryStatus {
    crate::wire::RecoveryStatus {
        stage: stage.to_string(),
        cause: record.failure.cause.label().to_string(),
        diagnostic: record.failure.diagnostic.clone(),
        guidance: record.failure.guidance.clone(),
        automatic: record.failure.automatic,
        occurrences: record.occurrences,
        first_seen_ms: record.first_seen_ms,
        last_seen_ms: record.last_seen_ms,
        next_attempt_ms: record.next_attempt_ms,
    }
}

impl Daemon {
    /// Every partition of a native session id, across sources, loading any
    /// that are on disk but idle.
    pub(super) async fn partitions_for_session(&self, session_id: &str) -> Vec<PartitionId> {
        let mut partitions: Vec<PartitionId> = self
            .pipeline
            .runtimes()
            .into_iter()
            .map(|runtime| runtime.partition.clone())
            .filter(|partition| partition.session_id == session_id)
            .collect();
        if let Ok(rows) = self
            .pipeline
            .store
            .read(crate::pipeline::store::partitions)
            .await
        {
            for row in rows {
                if row.partition.session_id == session_id && !partitions.contains(&row.partition) {
                    partitions.push(row.partition);
                }
            }
        }
        partitions
    }

    /// Append a barrier to each partition and wait for every branch to reach
    /// it. `finalize` selects a terminal finalization instead of a checkpoint.
    pub(super) async fn flush_partitions(
        &self,
        partitions: Vec<PartitionId>,
        finalize: Option<crate::pipeline::types::FinalizeKind>,
        timeout: Duration,
    ) -> FlushResult {
        let mut result = FlushResult {
            flushed: true,
            pending: 0,
            accepted_sessions: 0,
        };
        let mut waits = Vec::new();
        for partition in partitions {
            let runtime = match self.pipeline.partition(&partition).await {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::warn!(%partition, %error, "flush could not load partition");
                    result.flushed = false;
                    continue;
                }
            };
            result.accepted_sessions += runtime.branch_list().len() as u64;
            match self.pipeline.control(&partition, finalize).await {
                Ok(barrier) => waits.push((runtime, barrier)),
                Err(error) => {
                    tracing::warn!(%partition, %error, "flush barrier not captured");
                    result.flushed = false;
                }
            }
        }
        for (runtime, barrier) in waits {
            let (reached, pending) = self
                .pipeline
                .wait_barrier(&runtime, &barrier, timeout)
                .await;
            result.flushed &= reached;
            result.pending = result.pending.saturating_add(pending);
        }
        result
    }

    pub(super) async fn status(&self, p: StatusParams) -> StatusResult {
        let mut sessions = Vec::new();
        for runtime in self.pipeline.runtimes() {
            let partition = &runtime.partition;
            if p.session_id
                .as_ref()
                .is_some_and(|want| want != &partition.session_id)
            {
                continue;
            }
            let translation = runtime.translation.borrow().clone();
            let events_tail = self.pipeline.events.tail(partition).await.unwrap_or(0);
            let untranslated = events_tail.saturating_sub(translation.receipt);
            let spans_tail = *runtime.spans_changed.borrow();
            for branch in runtime.branch_list() {
                let view = branch.view.borrow().clone();
                let config = branch.config.borrow().clone();
                let backlog = if view.head == 0 {
                    0
                } else {
                    spans_tail.saturating_sub(view.delivered_through.max(view.head - 1))
                };
                let recovery = translation
                    .recovering
                    .as_ref()
                    .map(|record| recovery_status("translation", record))
                    .or_else(|| {
                        view.recovering.as_ref().map(|record| {
                            recovery_status(
                                if view.backfilling {
                                    "backfill"
                                } else {
                                    "delivery"
                                },
                                record,
                            )
                        })
                    });
                sessions.push(SessionStatus {
                    session_id: partition.session_id.clone(),
                    source: partition.source.clone(),
                    route: Some(config.session_route(&branch.key)),
                    queued: untranslated + backlog,
                    spans_emitted: view.spans_emitted,
                    permalink: view.permalink.clone(),
                    last_error: recovery
                        .as_ref()
                        .map(|recovery| format!("{}; {}", recovery.diagnostic, recovery.guidance)),
                    recovery,
                    held_root: view.held_root,
                });
            }
            if runtime.branch_list().is_empty() {
                if let Some(record) = &translation.recovering {
                    let recovery = recovery_status("translation", record);
                    sessions.push(SessionStatus {
                        session_id: partition.session_id.clone(),
                        source: partition.source.clone(),
                        route: None,
                        queued: untranslated,
                        spans_emitted: 0,
                        permalink: None,
                        last_error: Some(format!("{}; {}", recovery.diagnostic, recovery.guidance)),
                        recovery: Some(recovery),
                        held_root: false,
                    });
                }
            }
        }
        for (key, (source, error)) in self.route_errors.lock().unwrap().iter() {
            if p.session_id
                .as_ref()
                .is_some_and(|want| want != &key.session_id)
            {
                continue;
            }
            sessions.push(SessionStatus {
                session_id: key.session_id.clone(),
                source: source.clone(),
                route: serde_json::from_str(&key.route).ok(),
                queued: 0,
                spans_emitted: 0,
                permalink: None,
                last_error: Some(error.clone()),
                recovery: None,
                held_root: false,
            });
        }
        for state in self.pending_sessions.lock().unwrap().values() {
            let Some(first) = state.events.first() else {
                continue;
            };
            if p.session_id
                .as_ref()
                .is_some_and(|want| want != &first.env.session_id)
            {
                continue;
            }
            let route = state.linked_route.as_ref().or(first.env.route.as_ref());
            let message = if state.decided() {
                "correlation resolved; delivery registering"
            } else {
                "awaiting parent process confirmation; standalone fallback within 5 seconds"
            };
            let existing = sessions.iter_mut().find(|session| {
                session.session_id == first.env.session_id && session.source == first.env.source
            });
            if let Some(session) = existing {
                if session.last_error.is_none() {
                    session.last_error = Some(message.to_string());
                }
            } else {
                sessions.push(SessionStatus {
                    session_id: first.env.session_id.clone(),
                    source: first.env.source.clone(),
                    route: route.cloned(),
                    queued: state.events.len() as u64,
                    spans_emitted: 0,
                    permalink: None,
                    last_error: Some(message.to_string()),
                    recovery: None,
                    held_root: false,
                });
            }
        }
        StatusResult {
            daemon_version: self.version.clone(),
            uptime_ms: self.started.elapsed().as_millis() as u64,
            sessions,
        }
    }
}
