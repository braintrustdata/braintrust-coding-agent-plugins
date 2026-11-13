//! Storage behavior shared by the concrete collections.
use super::inspect::{Fault, Inspector, Table};
use super::span_ops::Translated;
use super::*;
use crate::journal::{journal_path, source_journal_path};
use crate::translate::{SpanOp, SpanRow};
use crate::wire::Envelope;
use serde_json::json;

fn event(source: &str, session: &str, name: &str, payload: serde_json::Value) -> Envelope {
    serde_json::from_value(json!({
        "source": source, "session_id": session, "event": name, "ts_ms": 1,
        "payload": payload,
    }))
    .unwrap()
}

fn line(value: &serde_json::Value) -> Vec<u8> {
    let mut line = serde_json::to_vec(value).unwrap();
    line.push(b'\n');
    line
}

async fn write_legacy(path: &std::path::Path, bytes: &[u8]) {
    tokio::fs::create_dir_all(path.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(path, bytes).await.unwrap();
}

#[tokio::test]
async fn capture_stores_only_the_pi_fields_translation_reads() {
    let temp = tempfile::tempdir().unwrap();
    let events = EventCollection::open(temp.path(), "pi", "session")
        .await
        .unwrap();
    let marker = "growing-partial-marker".repeat(64);
    events
        .append(
            (&event(
                "pi",
                "session",
                "message_update",
                json!({"event": {
                    "type": "message_update",
                    "assistantMessageEvent": {"type": "text_delta", "partial": marker},
                    "message": {"role": "assistant", "content": marker},
                }}),
            ))
                .into(),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    events
        .append(
            (&event(
                "pi",
                "session",
                "before_provider_request",
                json!({"event": {
                    "type": "before_provider_request",
                    "payload": {
                        "system": "system-prompt-marker",
                        "messages": [{"role": "user", "content": "message-marker"}],
                        "tools": [{"name": "read", "input_schema": {"type": "object"}}],
                        "max_tokens": 1024,
                    },
                }}),
            ))
                .into(),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let stored = Inspector::session(temp.path(), "pi", "session")
        .unwrap()
        .events();
    let text = serde_json::to_string(&stored).unwrap();
    for marker in [
        "growing-partial-marker",
        "system-prompt-marker",
        "message-marker",
    ] {
        assert!(!text.contains(marker), "{marker} reached storage");
    }
    let mut reader = events.read(0, events.tail().await.unwrap());
    let update = reader.next_event().await.unwrap().unwrap();
    assert_eq!(
        update.payload.pointer("/event/assistantMessageEvent/type"),
        Some(&json!("text_delta"))
    );
    let request = reader.next_event().await.unwrap().unwrap();
    assert_eq!(
        request.payload.pointer("/event/payload"),
        Some(&json!({
            "max_tokens": 1024,
            "tools": [{"type": "function", "function": {
                "name": "read", "parameters": {"type": "object"},
            }}],
        }))
    );
}

#[tokio::test]
async fn legacy_import_keeps_byte_positions_expands_pi_history_and_deletes_the_file() {
    let temp = tempfile::tempdir().unwrap();
    let path = source_journal_path(temp.path(), "pi", "session");
    let first = json!({"source":"pi","session_id":"session","event":"context","ts_ms":1,
        "payload":{"event":{"type":"context","messages":[{"role":"user","content":"a"}]}}});
    let second = json!({"source":"pi","session_id":"session","event":"context","ts_ms":2,
        "payload":{"event":{"type":"context","_bt_messages_delta":{
            "common_prefix":1,"suffix":[{"role":"user","content":"b"}]}}}});
    let mut bytes = line(&first);
    let boundary = bytes.len() as u64;
    bytes.extend(line(&second));
    // An unacknowledged partial append at the tail is not imported.
    bytes.extend(b"{\"partial\":");
    write_legacy(&path, &bytes).await;
    let events = EventCollection::open(temp.path(), "pi", "session")
        .await
        .unwrap();
    assert!(!path.exists());
    let through = events.tail().await.unwrap();
    assert_eq!(through, bytes.len() as u64 - 11);
    let mut reader = events.read(0, through);
    let entry = reader.next().await.unwrap().unwrap();
    assert_eq!(entry.id, boundary);
    let entry = reader.next().await.unwrap().unwrap();
    let EventRecord::Event(context) = entry.record else {
        panic!("expected an event")
    };
    assert_eq!(
        context.payload.pointer("/event/messages"),
        Some(&json!([{"role":"user","content":"a"},{"role":"user","content":"b"}]))
    );
    assert!(reader.next().await.unwrap().is_none());
    let appended = events
        .append((&event("pi", "session", "context", json!({}))).into(), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(appended.before, through);
}

#[tokio::test]
async fn an_unreadable_legacy_journal_is_retained_until_it_can_be_imported() {
    let temp = tempfile::tempdir().unwrap();
    let path = source_journal_path(temp.path(), "debug", "session");
    let valid =
        line(&json!({"source":"debug","session_id":"session","event":"e","ts_ms":1,"payload":{}}));
    write_legacy(&path, &[valid.as_slice(), b"{broken}\n"].concat()).await;
    assert!(EventCollection::open(temp.path(), "debug", "session")
        .await
        .is_err());
    assert!(path.exists(), "a failed import keeps its source");
    write_legacy(&path, &valid).await;
    let events = EventCollection::open(temp.path(), "debug", "session")
        .await
        .unwrap();
    assert!(!path.exists());
    assert_eq!(events.tail().await.unwrap(), valid.len() as u64);
}

#[tokio::test]
async fn a_session_only_journal_splits_by_source_and_keeps_its_namespace_owner() {
    let temp = tempfile::tempdir().unwrap();
    let path = journal_path(temp.path(), "shared");
    let mut bytes = Vec::new();
    for source in ["codex", "claude"] {
        bytes.extend(line(
            &json!({"source":source,"session_id":"shared","event":"e",
            "ts_ms":1,"payload":{},"route":{}}),
        ));
    }
    write_legacy(&path, &bytes).await;
    let codex = EventCollection::open(temp.path(), "codex", "shared")
        .await
        .unwrap();
    assert!(!path.exists());
    let claude = EventCollection::open(temp.path(), "claude-code", "shared")
        .await
        .unwrap();
    for (events, source, owner) in [(&codex, "codex", true), (&claude, "claude", false)] {
        assert_eq!(events.legacy_namespace().await.unwrap(), owner);
        let mut reader = events.read(0, events.tail().await.unwrap());
        assert_eq!(reader.next_event().await.unwrap().unwrap().source, source);
        assert!(reader.next_event().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn storage_is_removed_only_after_its_last_handle_closes() {
    let temp = tempfile::tempdir().unwrap();
    let events = EventCollection::open(temp.path(), "debug", "session")
        .await
        .unwrap();
    events
        .append((&event("debug", "session", "e", json!({}))).into(), None)
        .await
        .unwrap()
        .unwrap();
    let path = super::inspect::session_path(temp.path(), "debug", "session");
    let ops = SpanOps::for_events(&events).await.unwrap();
    drop(events);
    assert!(!remove_session(&path).await.unwrap());
    assert!(path.exists());
    drop(ops);
    assert!(remove_session(&path).await.unwrap());
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.clone().into_os_string();
        file.push(suffix);
        assert!(!std::path::Path::new(&file).exists(), "{suffix}");
    }
}

#[tokio::test]
async fn input_is_removed_only_in_the_transaction_that_stores_its_output() {
    let temp = tempfile::tempdir().unwrap();
    let events = EventCollection::open(temp.path(), "debug", "session")
        .await
        .unwrap();
    let input = events
        .append((&event("debug", "session", "e", json!({}))).into(), None)
        .await
        .unwrap()
        .unwrap()
        .through;
    let ops = SpanOps::for_events(&events).await.unwrap();
    let translated = Translated {
        input,
        observation: true,
        ops: vec![SpanOp::Insert(SpanRow {
            span_id: "span".into(),
            ..Default::default()
        })],
        roots: Default::default(),
        state: Some(b"{}".to_vec()),
    };
    let db = Inspector::session(temp.path(), "debug", "session").unwrap();
    db.inject(Fault::RemoveEvents).unwrap();
    assert!(ops.commit_translation(translated.clone()).await.is_err());
    // Neither stage changed: the event remains and no output is visible.
    assert_eq!(ops.translated_through().await.unwrap(), 0);
    assert_eq!(db.count(Table::Events), 1);
    assert_eq!(db.count(Table::Ops), 0);
    assert_eq!(ops.tail().await.unwrap(), 0);
    assert!(ops.checkpoint().await.unwrap().is_none());
    db.clear(Fault::RemoveEvents).unwrap();
    let committed = ops.commit_translation(translated.clone()).await.unwrap();
    assert_eq!(committed, 1);
    assert_eq!(db.count(Table::Ops), 1);
    assert_eq!(ops.translated_through().await.unwrap(), input);
    assert_eq!(db.count(Table::Events), 0);
    assert_eq!(ops.checkpoint().await.unwrap().unwrap().0, input);
}

#[tokio::test]
async fn reopening_storage_does_not_count_as_activity() {
    let temp = tempfile::tempdir().unwrap();
    let events = EventCollection::open(temp.path(), "debug", "session")
        .await
        .unwrap();
    events
        .append((&event("debug", "session", "e", json!({}))).into(), None)
        .await
        .unwrap()
        .unwrap();
    drop(events);
    let day = std::time::Duration::from_secs(24 * 60 * 60);
    Inspector::session(temp.path(), "debug", "session")
        .unwrap()
        .age(day)
        .unwrap();
    // Restart recovery reopens every session database.
    let events = EventCollection::open(temp.path(), "debug", "session")
        .await
        .unwrap();
    assert!(events.idle_for().await.unwrap() >= day);
    events
        .append(
            (&event("debug", "session", "later", json!({}))).into(),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(events.idle_for().await.unwrap() < day);
}

#[tokio::test]
async fn a_live_session_expires_only_what_it_no_longer_uses() {
    let retention = std::time::Duration::from_secs(7 * 24 * 60 * 60);
    let temp = tempfile::tempdir().unwrap();
    let events = EventCollection::open(temp.path(), "debug", "session")
        .await
        .unwrap();
    let mut captured = event("debug", "session", "e", json!({}));
    captured.route = Some(Default::default());
    let input = events
        .append((&captured).into(), None)
        .await
        .unwrap()
        .unwrap()
        .through;
    assert!(events
        .append((&captured).into(), None)
        .await
        .unwrap()
        .is_none());
    let ops = SpanOps::for_events(&events).await.unwrap();
    let route = |name: &str| crate::wire::SessionRoute {
        tags: [name.into()].into_iter().collect(),
        ..Default::default()
    };
    let delivered = ops.subscribe(&route("delivered")).await.unwrap().position;
    // A route registered for output whose delivery never starts.
    ops.register(&route("unsubscribed")).await.unwrap();
    let span = |id: &str, end_ms: Option<i64>| {
        SpanOp::Insert(SpanRow {
            span_id: id.into(),
            root_span_id: id.into(),
            span_type: crate::translate::SpanType::Tool,
            end_ms,
            origin: Some(
                crate::translate::OriginSnapshot {
                    plugin_version: None,
                    source_version: None,
                    bt_version: "test".into(),
                }
                .into(),
            ),
            ..Default::default()
        })
    };
    let tail = ops
        .commit_translation(Translated {
            input,
            observation: true,
            ops: vec![span("ended", Some(2)), span("abandoned", None)],
            roots: Default::default(),
            state: Some(b"{}".to_vec()),
        })
        .await
        .unwrap();
    let key = serde_json::to_string(&route("delivered")).unwrap();
    ops.remove_through(&key, delivered, tail).await.unwrap();
    let db = Inspector::session(temp.path(), "debug", "session").unwrap();
    assert_eq!(
        db.count(Table::Ops),
        2,
        "the unsubscribed head holds output"
    );

    db.age(retention + std::time::Duration::from_secs(60))
        .unwrap();
    let unsubscribed = serde_json::to_string(&route("unsubscribed")).unwrap();
    ops.expire(retention, vec![unsubscribed]).await.unwrap();
    assert_eq!(db.count(Table::Heads), 2, "a live route is never expired");

    events.expire(retention).await.unwrap();
    ops.expire(retention, Vec::new()).await.unwrap();
    assert_eq!(db.count(Table::Heads), 1);
    assert_eq!(db.count(Table::Ops), 0);
    assert_eq!(db.count(Table::Origins), 1, "an open span keeps its origin");
    assert!(ops.observations().await.unwrap().is_empty());
    assert!(
        events
            .append((&captured).into(), None)
            .await
            .unwrap()
            .is_some(),
        "an expired capture identity is recorded again"
    );
}
