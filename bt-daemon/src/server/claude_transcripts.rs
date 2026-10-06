//! Claude Code transcript polling. The daemon remembers each Claude session's
//! transcript and emits synthetic `TranscriptUpdate` events for it, so records
//! written after the session's last hook are still translated.

use super::*;

#[derive(Clone)]
pub(super) struct ClaudeTranscriptObservation {
    /// Synthetic-event provenance only: never retain native prompts or responses.
    pub(super) template: Envelope,
    pub(super) through: Option<u64>,
}

impl Daemon {
    pub(super) fn remember_claude_transcripts(&self, env: &Envelope) {
        if env.source != "claude-code" || env.payload.get("_bt_import_through_offset").is_some() {
            return;
        }
        if env.event == "SessionEnd" {
            self.claude_transcripts
                .lock()
                .unwrap()
                .retain(|key, _| key.source != env.source || key.session_id != env.session_id);
            return;
        }
        let Some(route) = env.route.as_ref() else {
            return;
        };
        let Some(path) = env.payload.get("transcript_path").and_then(Value::as_str) else {
            return;
        };
        let Ok(key) = DeliveryKey::new(&env.source, &env.session_id, route) else {
            return;
        };
        let through = env
            .payload
            .get("_bt_claude_transcript_mirrors")
            .and_then(|mirrors| mirrors.get(path))
            .or_else(|| {
                env.payload
                    .get("_bt_transcript_mirror")
                    .filter(|mirror| mirror.get("path").and_then(Value::as_str) == Some(path))
            })
            .and_then(|mirror| mirror.get("through"))
            .and_then(Value::as_u64);
        let mut payload = serde_json::json!({"transcript_path": path});
        if let Some(cwd) = env.payload.get("cwd").and_then(Value::as_str) {
            payload["cwd"] = Value::String(cwd.to_string());
        }
        let template = Envelope {
            source: env.source.clone(),
            source_version: env.source_version.clone(),
            plugin_version: env.plugin_version.clone(),
            session_id: env.session_id.clone(),
            event: "TranscriptUpdate".to_string(),
            ts_ms: env.ts_ms,
            managed_run_id: env.managed_run_id.clone(),
            capture: None,
            payload,
            route: env.route.clone(),
            config: None,
        };
        let mut observations = self.claude_transcripts.lock().unwrap();
        let previous = observations
            .get(&key)
            .filter(|observation| {
                observation
                    .template
                    .payload
                    .get("transcript_path")
                    .and_then(Value::as_str)
                    == Some(path)
            })
            .and_then(|observation| observation.through);
        observations.insert(
            key,
            ClaudeTranscriptObservation {
                template,
                through: through.or(previous),
            },
        );
    }

    /// Poll native paths only on the capture side. Each observed boundary is
    /// journaled before translation, including observations made during shutdown.
    pub(super) async fn observe_claude_transcripts(
        &self,
        only: Option<&DeliveryKey>,
        quiesced: bool,
    ) {
        // Shutdown already owns the exclusive gate. Ordinary polling must not
        // append after that final drain begins.
        let _gate = if quiesced {
            None
        } else {
            Some(self.capture_gate.read().await)
        };
        if !quiesced && self.quiescing.load(Ordering::SeqCst) {
            return;
        }
        let keys: Vec<_> = self
            .claude_transcripts
            .lock()
            .unwrap()
            .keys()
            .filter(|key| {
                only.is_none_or(|only| {
                    key.source == only.source && key.session_id == only.session_id
                })
            })
            .cloned()
            .collect();
        for key in keys {
            // Routes share mirror files, so serialize by source session, not
            // destination. Never hold this lock across an ingress barrier.
            let lock = self.session_lock(&key.source, &key.session_id);
            let _guard = lock.lock().await;
            let observation = self.claude_transcripts.lock().unwrap().get(&key).cloned();
            let Some(observation) = observation else {
                continue;
            };
            let path = observation.template.payload["transcript_path"]
                .as_str()
                .unwrap();
            let Ok(metadata) = tokio::fs::metadata(path).await else {
                continue;
            };
            if observation.through == Some(metadata.len()) {
                continue;
            }
            let mut env = observation.template;
            env.ts_ms = now_ms();
            hydrate_transcript_reference(&self.data_dir, &mut env).await;
            let through = env
                .payload
                .get("_bt_claude_transcript_mirrors")
                .and_then(|mirrors| mirrors.get(env.payload["transcript_path"].as_str().unwrap()))
                .and_then(|mirror| mirror.get("through"))
                .and_then(Value::as_u64);
            if through.is_none() || through == observation.through {
                continue;
            }
            if let Err(error) = self.journal_and_enqueue(env).await {
                tracing::warn!(session_id = %key.session_id, %error, "Claude transcript observation failed");
            }
        }
    }
}
