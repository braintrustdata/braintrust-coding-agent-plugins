use super::{ExportMetadata, RunEvent, RunModelRecord, TaskEvent};
use serde::Deserialize;

pub(crate) enum NativePayload {
    Metadata { record: Option<ExportMetadata> },
    RunModel { record: Option<RunModelRecord> },
    Run { run_id: String, event: RunEvent },
    Task { event: Option<TaskEvent> },
    SessionEnd,
    Ignored,
}

impl<'de> Deserialize<'de> for NativePayload {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde_json::value::RawValue;
        #[derive(Default, Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Kind {
            Metadata,
            RunModel,
            Run,
            Task,
            SessionEnd,
            #[default]
            #[serde(other)]
            Ignored,
        }
        #[derive(Deserialize)]
        struct Payload {
            #[serde(default)]
            kind: Option<Kind>,
            run_id: Option<String>,
            event: Option<Box<RawValue>>,
            record: Option<Box<RawValue>>,
        }
        fn decode<T: serde::de::DeserializeOwned, E: serde::de::Error>(
            value: Option<Box<RawValue>>,
        ) -> Result<Option<T>, E> {
            value
                .map(|raw| serde_json::from_str(raw.get()).map_err(E::custom))
                .transpose()
        }
        let raw = Payload::deserialize(d)?;
        Ok(match raw.kind.unwrap_or_default() {
            Kind::Ignored => Self::Ignored,
            Kind::SessionEnd => Self::SessionEnd,
            Kind::Metadata => Self::Metadata {
                record: decode(raw.record)?,
            },
            Kind::RunModel => Self::RunModel {
                record: decode(raw.record)?,
            },
            Kind::Task => Self::Task {
                event: decode(raw.event)?,
            },
            Kind::Run => match decode(raw.event)? {
                None | Some(RunEvent::Ignored) => Self::Ignored,
                Some(event) => Self::Run {
                    run_id: raw
                        .run_id
                        .ok_or_else(|| serde::de::Error::missing_field("run_id"))?,
                    event,
                },
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn non_event_payloads_and_future_records_are_ignored_but_known_records_are_checked() {
        for payload in [
            json!({"record":{"future":true}}),
            json!({"kind":null,"record":{"future":true}}),
            json!({"kind":"future_payload","event":{"future":true}}),
            json!({"kind":"run","event":{"kind":"future_event"}}),
            json!({"kind":"run"}),
        ] {
            assert!(matches!(
                serde_json::from_str::<NativePayload>(&payload.to_string()).unwrap(),
                NativePayload::Ignored
            ));
        }
        assert!(serde_json::from_str::<NativePayload>(r#"{"kind":"run","run_id":"r","event":{"kind":"model_completed","usage":{"input_tokens":"wrong"}}}"#).is_err());
        assert!(serde_json::from_str::<NativePayload>(
            r#"{"kind":"run","event":{"kind":"started"}}"#
        )
        .is_err());
    }
}
