use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TurnLimit {
    #[serde(skip_serializing_if = "Option::is_none")]
    max_turns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    turn_count: Option<u64>,
}

#[derive(Deserialize)]
struct Attachment {
    attachment: NativeAttachment,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum NativeAttachment {
    MaxTurnsReached(TurnLimit),
}

impl TurnLimit {
    pub(super) fn at_end(records: &[Value]) -> Option<Self> {
        records
            .iter()
            .rev()
            .take_while(|record| !super::is_real_user_record(record))
            .find_map(|record| {
                let Attachment {
                    attachment: NativeAttachment::MaxTurnsReached(limit),
                } = Attachment::deserialize(record).ok()?;
                Some(limit)
            })
    }

    pub(super) fn add_metadata(&self, metadata: &mut serde_json::Map<String, Value>) {
        metadata.insert("termination_reason".into(), "max_turns".into());
        if let Some(max_turns) = self.max_turns {
            metadata.insert("max_turns".into(), max_turns.into());
        }
        if let Some(turn_count) = self.turn_count {
            metadata.insert("turn_count".into(), turn_count.into());
        }
    }
}
