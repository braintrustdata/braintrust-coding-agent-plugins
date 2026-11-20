use super::{ClaudeChildReference, GrokReferences, Observation, Reference, Snapshot};

/// Decode the known capture protocol once at the envelope boundary. Native
/// agent data remains in the raw journal and is decoded by its own translator.
#[derive(Default, serde::Deserialize)]
pub(crate) struct TranscriptPayload {
    #[serde(default, alias = "transcriptPath")]
    pub transcript_path: Option<String>,
    pub agent_transcript_path: Option<String>,
    pub agent_id: Option<String>,
    #[serde(rename = "_bt_claude_children")]
    pub claude_children: Option<std::collections::BTreeMap<String, ClaudeChildReference>>,
    pub cwd: Option<String>,
    #[serde(rename = "_bt_transcript_mirror")]
    pub mirror: Option<Reference>,
    #[serde(rename = "_bt_claude_transcript_mirrors")]
    pub claude_mirrors: Option<std::collections::BTreeMap<String, Reference>>,
    #[serde(rename = "_bt_grok_transcript_mirrors")]
    pub grok_mirrors: Option<GrokReferences>,
    #[serde(rename = "_bt_transcript_observation")]
    pub observation: Option<Observation>,
    #[serde(rename = "_bt_transcript_snapshot")]
    pub snapshot: Option<Snapshot>,
    #[serde(rename = "_bt_transcript_replay")]
    pub replay: Option<bool>,
    #[serde(rename = "_bt_import_through_offset")]
    pub import_through_offset: Option<u64>,
    #[serde(rename = "_bt_import_through_ms")]
    pub import_through_ms: Option<i64>,
}

impl TranscriptPayload {
    pub fn decode(payload: &serde_json::Value) -> serde_json::Result<Self> {
        serde::Deserialize::deserialize(payload)
    }

    /// Modern Claude observations include a map even when capture found no
    /// readable files. Its presence must prevent a later native-file reread.
    pub fn has_claude_capture(&self) -> bool {
        self.claude_mirrors.is_some()
            || self.mirror.is_some()
            || self.snapshot.is_some()
            || self.replay.is_some()
    }

    /// The latest observation for the poller, including the legacy single
    /// reference format. Readers still distinguish a modern map's absence
    /// from a missing entry so captured absence never falls back to live data.
    pub fn claude_reference(&self, path: &str) -> Option<&Reference> {
        self.claude_mirrors
            .as_ref()
            .and_then(|mirrors| mirrors.get(path))
            .or_else(|| self.matching_mirror(path))
    }

    pub fn matching_mirror(&self, path: &str) -> Option<&Reference> {
        self.mirror
            .as_ref()
            .filter(|mirror| mirror.matches_path(path))
    }

    pub fn snapshot_for(&self, path: &str) -> Option<&str> {
        self.snapshot
            .as_ref()
            .filter(|snapshot| snapshot.path == path)
            .map(|snapshot| snapshot.contents.as_str())
    }

    pub fn native_paths(&self) -> impl Iterator<Item = &str> {
        [
            self.transcript_path.as_deref(),
            self.agent_transcript_path.as_deref(),
        ]
        .into_iter()
        .flatten()
    }

    /// Codex stop hooks must read the child transcript; other hooks prefer
    /// the main path. A captured mirror takes precedence in either case.
    pub fn codex_read_path(&self, subagent_stop: bool) -> Option<&str> {
        self.mirror
            .as_ref()
            .and_then(|reference| reference.mirror.as_deref())
            .or_else(|| {
                if subagent_stop {
                    self.agent_transcript_path.as_deref()
                } else {
                    self.transcript_path
                        .as_deref()
                        .or(self.agent_transcript_path.as_deref())
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn codex_selects_captured_or_native_path_for_the_event_scope() {
        for (raw, main, child) in [
            (
                json!({"transcript_path":"main","agent_transcript_path":"child"}),
                Some("main"),
                Some("child"),
            ),
            (json!({"transcript_path":"main"}), Some("main"), None),
            (
                json!({"agent_transcript_path":"child"}),
                Some("child"),
                Some("child"),
            ),
            (
                json!({"transcript_path":"main","_bt_transcript_mirror":{"mirror":"captured","through":12}}),
                Some("captured"),
                Some("captured"),
            ),
        ] {
            let payload = TranscriptPayload::decode(&raw).unwrap();
            assert_eq!(payload.codex_read_path(false), main);
            assert_eq!(payload.codex_read_path(true), child);
        }
    }

    #[test]
    fn captured_absence_and_legacy_references_keep_their_meaning() {
        let payload = TranscriptPayload::decode(&json!({
            "_bt_claude_transcript_mirrors":{},
            "_bt_transcript_mirror":{"path":"main","mirror":"legacy","through":42},
            "_bt_transcript_snapshot":{"path":"main","contents":"snapshot"}
        }))
        .unwrap();
        assert!(payload.has_claude_capture());
        assert!(payload
            .claude_mirrors
            .as_ref()
            .unwrap()
            .get("main")
            .is_none());
        // Pollers can reuse a legacy checkpoint; readers still treat the
        // explicit empty modern capture as absence instead of falling back.
        assert_eq!(
            payload.claude_reference("main").unwrap().captured_prefix(),
            Some(("legacy", 42))
        );
        assert!(payload.claude_reference("child").is_none());
        assert_eq!(payload.snapshot_for("main"), Some("snapshot"));
        assert_eq!(payload.snapshot_for("child"), None);
        let absent = TranscriptPayload::decode(&json!({"_bt_transcript_mirror":{}})).unwrap();
        assert!(absent.has_claude_capture());
        assert!(absent.mirror.unwrap().captured_prefix().is_none());
        assert!(!TranscriptPayload::default().has_claude_capture());
    }
}
