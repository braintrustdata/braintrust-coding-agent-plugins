use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TurnLimit {
    max_turns: u64,
    turn_count: u64,
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
        metadata.insert("max_turns".into(), self.max_turns.into());
        metadata.insert("turn_count".into(), self.turn_count.into());
    }
}
