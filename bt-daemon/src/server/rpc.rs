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
            // Explicit flushes wait for the daemon-owned ingress queue. Hook
            // capture never waits on this barrier.
            daemon.settle_ingress().await;
            if let Err(error) =
                resolve_pending_sessions(daemon, |env| env.session_id == p.session_id).await
            {
                return Response::err(id, RpcError::new(error_code::INTERNAL, error));
            }
            let delivery_keys: Vec<_> = daemon
                .sessions
                .lock()
                .unwrap()
                .keys()
                .filter(|key| key.session_id == p.session_id)
                .cloned()
                .collect();
            let accepted_sessions = delivery_keys.len() as u64;
            let mut flushed = true;
            let mut pending = 0u64;
            for key in delivery_keys {
                if let Err(error) = daemon.refresh_session_before_flush(&key).await {
                    return Response::err(
                        id,
                        RpcError::new(
                            error_code::INTERNAL,
                            format!("session auth refresh failed: {error}"),
                        ),
                    );
                }
                let session = { daemon.sessions.lock().unwrap().get(&key).cloned() };
                if let Some(session) = session {
                    let (route_flushed, route_pending) =
                        session.flush(Duration::from_millis(p.timeout_ms)).await;
                    flushed &= route_flushed;
                    pending = pending.saturating_add(route_pending);
                }
            }
            Response::ok(
                id,
                serde_json::to_value(FlushResult {
                    flushed,
                    pending,
                    accepted_sessions,
                })
                .unwrap(),
            )
        }
        method::MANAGED_RUN_FLUSH => {
            let params = parse!(ManagedRunFlushParams);
            daemon.settle_ingress().await;
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
            daemon.settle_ingress().await;
            Response::ok(id, serde_json::to_value(daemon.status(p)).unwrap())
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

impl Daemon {
    pub(super) fn status(&self, p: StatusParams) -> StatusResult {
        let map = self.sessions.lock().unwrap();
        let mut sessions: Vec<_> = map
            .iter()
            .filter(|(key, _)| {
                p.session_id
                    .as_ref()
                    .is_none_or(|want| want == &key.session_id)
            })
            .map(|(key, s)| SessionStatus {
                session_id: key.session_id.clone(),
                source: s.source.clone(),
                route: serde_json::from_str(&key.route).ok(),
                queued: s.counters.queued.load(Ordering::Relaxed),
                spans_emitted: s.counters.spans_emitted.load(Ordering::Relaxed),
                permalink: s.permalink.lock().unwrap().clone(),
                last_error: s.last_error.lock().unwrap().clone(),
            })
            .collect();
        for (key, (source, error)) in self.auth_errors.lock().unwrap().iter() {
            if p.session_id
                .as_ref()
                .is_some_and(|want| want != &key.session_id)
            {
                continue;
            }
            if map.contains_key(key) {
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
            let existing = sessions.iter_mut().find(|session| {
                session.session_id == first.env.session_id
                    && session.source == first.env.source
                    && match (session.route.as_ref(), route) {
                        (Some(left), Some(right)) => left.same_route(right),
                        (None, None) => true,
                        _ => false,
                    }
            });
            let message = if state.decided() {
                "correlation resolved; journaled events awaiting delivery"
            } else {
                "awaiting parent process confirmation; standalone fallback within 5 seconds"
            };
            if let Some(session) = existing {
                session.queued = session.queued.saturating_add(state.events.len() as u64);
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
