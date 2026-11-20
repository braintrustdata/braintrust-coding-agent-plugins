//! Claude Code transcript polling. The daemon remembers each Claude session's
//! transcript and emits synthetic `TranscriptUpdate` events for it, so records
//! written after the session's last hook are still translated.

use super::*;
use crate::transcript_mirror::TranscriptPayload;

#[derive(Clone)]
pub(super) struct ClaudeTranscriptObservation {
    /// Synthetic-event provenance only: never retain native prompts or responses.
    pub(super) template: Envelope,
    pub(super) through: Option<u64>,
    pub(super) path: String,
    pub(super) children: HashMap<String, ClaudeChildObservation>,
}

#[derive(Clone)]
pub(super) struct ClaudeChildObservation {
    pub(super) template: Envelope,
    pub(super) through: Option<u64>,
    pub(super) path: String,
}

impl Daemon {
    pub(super) fn remember_claude_transcripts(&self, env: &Envelope) {
        if env.source != "claude-code" {
            return;
        }
        let Ok(fields) = TranscriptPayload::decode(&env.payload) else {
            return;
        };
        if fields.import_through_offset.is_some() {
            return;
        }
        let Some(route) = env.route.as_ref() else {
            return;
        };
        let Some(path) = fields.transcript_path.as_deref() else {
            return;
        };
        let Ok(key) = DeliveryKey::new(&env.source, &env.session_id, route) else {
            return;
        };
        let through = fields
            .claude_reference(path)
            .and_then(|reference| reference.through);
        #[derive(serde::Serialize)]
        struct PollPayload<'a> {
            transcript_path: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            cwd: Option<&'a str>,
        }
        let payload = serde_json::json!(PollPayload {
            transcript_path: path,
            cwd: fields.cwd.as_deref()
        });
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
        let mut observations = self.claude_transcripts.lock();
        let old = observations.remove(&key);
        let previous = old
            .as_ref()
            .filter(|observation| observation.path == path)
            .and_then(|observation| observation.through);
        let mut children = old
            .map(|observation| observation.children)
            .unwrap_or_default();
        if let Some(saved) = fields.claude_children.as_ref() {
            for (agent_id, observation) in saved {
                let child_path = &observation.path;
                let mut child_template = template.clone();
                child_template.payload["agent_id"] = serde_json::json!(agent_id);
                child_template.payload["agent_transcript_path"] = serde_json::json!(child_path);
                children.insert(
                    agent_id.clone(),
                    ClaudeChildObservation {
                        template: child_template,
                        through: observation.through,
                        path: child_path.clone(),
                    },
                );
            }
        }
        if matches!(env.event.as_str(), "SubagentStop" | "TranscriptUpdate") {
            if let (Some(agent_id), Some(child_path)) = (
                fields.agent_id.as_deref(),
                fields.agent_transcript_path.as_deref(),
            ) {
                let child_through = fields
                    .claude_mirrors
                    .as_ref()
                    .and_then(|mirrors| mirrors.get(child_path))
                    .and_then(|mirror| mirror.through);
                let mut child_template = template.clone();
                child_template.payload["agent_id"] = serde_json::json!(agent_id);
                child_template.payload["agent_transcript_path"] = serde_json::json!(child_path);
                let previous = children.get(agent_id).and_then(|child| child.through);
                children.insert(
                    agent_id.to_string(),
                    ClaudeChildObservation {
                        template: child_template,
                        through: child_through.or(previous),
                        path: child_path.to_owned(),
                    },
                );
            }
        }
        observations.insert(
            key,
            ClaudeTranscriptObservation {
                template,
                path: path.to_owned(),
                through: through.or(previous),
                children,
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
            let observation = self.claude_transcripts.lock().get(&key).cloned();
            let Some(observation) = observation else {
                continue;
            };
            let mut targets = vec![(observation.template, observation.through, observation.path)];
            targets.extend(
                observation
                    .children
                    .into_values()
                    .map(|child| (child.template, child.through, child.path)),
            );
            for (mut env, previous, path) in targets {
                let Ok(metadata) = tokio::fs::metadata(&path).await else {
                    continue;
                };
                if previous == Some(metadata.len()) {
                    continue;
                }
                env.ts_ms = now_ms();
                hydrate_transcript_reference(&self.data_dir, &mut env).await;
                let through = TranscriptPayload::decode(&env.payload)
                    .ok()
                    .and_then(|fields| fields.claude_mirrors?.get(&path)?.through);
                if through.is_none() || through == previous {
                    continue;
                }
                if let Err(error) = self.journal_and_enqueue(env).await {
                    tracing::warn!(session_id = %key.session_id, %error, "Claude transcript observation failed");
                }
            }
        }
    }
}
