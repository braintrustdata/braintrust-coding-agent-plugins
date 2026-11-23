//! Native Pi compaction and branch-summary payloads and their span inputs.
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
pub(super) struct SessionCompact {
    #[serde(default)]
    compaction_entry: Option<CompactionEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
struct CompactionEntry {
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    tokens_before: Option<u64>,
    #[serde(default)]
    timestamp: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
struct Preparation {
    #[serde(default)]
    tokens_before: Option<u64>,
    #[serde(default)]
    first_kept_entry_id: Option<String>,
    #[serde(default)]
    is_split_turn: Option<bool>,
    #[serde(default)]
    messages_to_summarize: Option<Vec<Value>>,
    #[serde(default)]
    turn_prefix_messages: Option<Vec<Value>>,
    #[serde(default)]
    target_id: Option<String>,
    #[serde(default)]
    old_leaf_id: Option<String>,
    #[serde(default)]
    common_ancestor_id: Option<String>,
    #[serde(default)]
    user_wants_summary: Option<bool>,
    #[serde(default)]
    custom_instructions: Option<String>,
    #[serde(default)]
    replace_instructions: Option<bool>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    entries_to_summarize: Option<Vec<Value>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
pub(super) struct BeforeCompact {
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    will_retry: Option<bool>,
    #[serde(default)]
    custom_instructions: Option<String>,
    #[serde(default)]
    preparation: Option<Preparation>,
    #[serde(default)]
    branch_entries: Option<Vec<Value>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
pub(super) struct BeforeTree {
    #[serde(default)]
    preparation: Option<Preparation>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Serialize)]
pub(super) struct SessionTree {
    #[serde(default)]
    pub(super) summary_entry: Option<Value>,
}

impl SessionCompact {
    pub(super) fn summary_message(&self) -> Option<Value> {
        let entry = self.compaction_entry.as_ref()?;
        let summary = entry.summary.clone()?;
        Some(json!({
            "role": "compactionSummary",
            "summary": summary,
            "tokensBefore": entry.tokens_before,
            "timestamp": entry.timestamp,
        }))
    }
}
impl BeforeCompact {
    pub(super) fn input(&self) -> Value {
        let preparation = self.preparation.as_ref();
        json!({
            "reason": self.reason.clone(),
            "willRetry": self.will_retry.clone(),
            "customInstructions": self.custom_instructions.clone(),
            "tokensBefore": preparation.and_then(|value| value.tokens_before),
            "firstKeptEntryId": preparation.and_then(|value| value.first_kept_entry_id.clone()),
            "isSplitTurn": preparation.and_then(|value| value.is_split_turn),
            "messagesToSummarizeCount": preparation
                .and_then(|value| value.messages_to_summarize.as_ref())
                .map(Vec::len),
            "turnPrefixMessagesCount": preparation
                .and_then(|value| value.turn_prefix_messages.as_ref())
                .map(Vec::len),
            "branchEntryCount": self.branch_entries.as_ref().map(Vec::len),
        })
    }
}
impl BeforeTree {
    pub(super) fn wants_summary(&self) -> bool {
        self.preparation
            .as_ref()
            .and_then(|preparation| preparation.user_wants_summary)
            == Some(true)
    }

    pub(super) fn input(&self) -> Value {
        let preparation = self.preparation.as_ref();
        json!({
            "targetId": preparation.and_then(|value| value.target_id.clone()),
            "oldLeafId": preparation.and_then(|value| value.old_leaf_id.clone()),
            "commonAncestorId": preparation.and_then(|value| value.common_ancestor_id.clone()),
            "userWantsSummary": preparation.and_then(|value| value.user_wants_summary),
            "customInstructions": preparation.and_then(|value| value.custom_instructions.clone()),
            "replaceInstructions": preparation.and_then(|value| value.replace_instructions),
            "label": preparation.and_then(|value| value.label.clone()),
            "entriesToSummarizeCount": preparation
                .and_then(|value| value.entries_to_summarize.as_ref())
                .map(Vec::len),
        })
    }
}
