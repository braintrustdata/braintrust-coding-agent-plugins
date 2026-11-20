use super::*;
use crate::json::Field;
use serde_json::json;

#[test]
fn known_assistant_fields_and_image_variants_round_trip() {
    for raw in [
        json!({"type":"assistant_message","text":"done","provider_item_id":null,"phase":"final_answer","future":true}),
        json!({"type":"assistant_message","text":"done","phase":"future-phase"}),
        json!({"type":"image","source":{"url":"https://example.com/image","future":null},"future":true}),
        json!({"type":"image","source":{"type":"base64","media_type":"image/png","data":"YWJj","future":true}}),
        json!({"type":"image_url","image_url":{"url":"https://example.com/image","detail":"high","future":true}}),
        json!({"type":"image","source":{"type":"future","opaque":[null,42]}}),
        json!({"type":"future","opaque":[null,42]}),
    ] {
        let parsed: ContentBlock = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), raw);
    }
    let parsed: ContentBlock = serde_json::from_value(
        json!({"type":"assistant_message","text":"done","phase":"final_answer"}),
    )
    .unwrap();
    assert!(matches!(
        parsed,
        ContentBlock::Known(KnownContentBlock::AssistantMessage {
            phase: Field::Value(MessagePhase::FinalAnswer),
            ..
        })
    ));
}

#[test]
fn malformed_known_fields_cannot_hide_as_unknown_content() {
    for raw in [
        json!({"type":"image","source":42}),
        json!({"type":"assistant_message","text":"done","provider_item_id":42}),
        json!({"type":"assistant_message","text":"done","phase":42}),
        json!({"type":"image","source":{"url":42}}),
        json!({"type":"image","source":{"type":"base64","media_type":"image/png","data":42}}),
        json!({"type":"image_url","image_url":{"url":"example","detail":42}}),
    ] {
        assert!(serde_json::from_value::<ContentBlock>(raw).is_err());
    }
}
