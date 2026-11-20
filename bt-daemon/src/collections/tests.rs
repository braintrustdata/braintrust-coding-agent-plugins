//! Storage behavior shared by the concrete collections.
use super::inspect::{Fault, Inspector, Table};
use super::span_ops::Translated;
use super::store::One;
use super::*;
use crate::journal::{journal_path, source_journal_path};
use crate::translate::{SpanOp, SpanRow};
use crate::wire::Envelope;
use futures::TryStreamExt;
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
    let events = SessionStorage::open(temp.path(), "pi", "session")
        .await
        .unwrap();
    let marker = "growing-partial-marker".repeat(64);
    events
        .append(
            &event(
                "pi",
                "session",
                "message_update",
                json!({"event": {
                    "type": "message_update",
                    "assistantMessageEvent": {"type": "text_delta", "partial": marker},
                    "message": {"role": "assistant", "content": marker},
                }}),
            ),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    events
        .append(
            &event(
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
            ),
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
    let tail = events.status().await.unwrap().event_tail;
    let mut reader = std::pin::pin!(events.read(0, tail).events());
    let update = reader.try_next().await.unwrap().unwrap();
    assert_eq!(
        update.payload.pointer("/event/assistantMessageEvent/type"),
        Some(&json!("text_delta"))
    );
    let request = reader.try_next().await.unwrap().unwrap();
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
    let events = SessionStorage::open(temp.path(), "pi", "session")
        .await
        .unwrap();
    assert!(!path.exists());
    let through = events.status().await.unwrap().event_tail;
    assert_eq!(through, bytes.len() as u64 - 11);
    let mut reader = events.read(0, through);
    let entry = reader.try_next().await.unwrap().unwrap();
    assert_eq!(entry.id, boundary);
    let entry = reader.try_next().await.unwrap().unwrap();
    let EventRecord::Event(context) = entry.record else {
        panic!("expected an event")
    };
    assert_eq!(
        context.payload.pointer("/event/messages"),
        Some(&json!([{"role":"user","content":"a"},{"role":"user","content":"b"}]))
    );
    assert!(reader.try_next().await.unwrap().is_none());
    let appended = events
        .append(&event("pi", "session", "context", json!({})), None)
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
    assert!(SessionStorage::open(temp.path(), "debug", "session")
        .await
        .is_err());
    assert!(path.exists(), "a failed import keeps its source");
    write_legacy(&path, &valid).await;
    let events = SessionStorage::open(temp.path(), "debug", "session")
        .await
        .unwrap();
    assert!(!path.exists());
    assert_eq!(
        events.status().await.unwrap().event_tail,
        valid.len() as u64
    );
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
    let codex = SessionStorage::open(temp.path(), "codex", "shared")
        .await
        .unwrap();
    assert!(!path.exists());
    let claude = SessionStorage::open(temp.path(), "claude-code", "shared")
        .await
        .unwrap();
    for (events, source, owner) in [(&codex, "codex", true), (&claude, "claude", false)] {
        assert_eq!(events.status().await.unwrap().legacy_namespace, owner);
        let tail = events.status().await.unwrap().event_tail;
        let sources: Vec<_> = events
            .read(0, tail)
            .events()
            .map_ok(|event| event.source)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(sources, [source]);
    }
}

#[tokio::test]
async fn storage_is_removed_only_after_its_last_handle_closes() {
    let temp = tempfile::tempdir().unwrap();
    let events = SessionStorage::open(temp.path(), "debug", "session")
        .await
        .unwrap();
    events
        .append(&event("debug", "session", "e", json!({})), None)
        .await
        .unwrap()
        .unwrap();
    let path = super::inspect::session_path(temp.path(), "debug", "session");
    let ops = events.clone();
    drop(events);
    assert!(!SessionStorage::remove(&path).await.unwrap());
    assert!(path.exists());
    drop(ops);
    assert!(SessionStorage::remove(&path).await.unwrap());
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.clone().into_os_string();
        file.push(suffix);
        assert!(!std::path::Path::new(&file).exists(), "{suffix}");
    }
}

#[tokio::test]
async fn input_is_removed_only_in_the_transaction_that_stores_its_output() {
    let temp = tempfile::tempdir().unwrap();
    let events = SessionStorage::open(temp.path(), "debug", "session")
        .await
        .unwrap();
    let input = events
        .append(&event("debug", "session", "e", json!({})), None)
        .await
        .unwrap()
        .unwrap()
        .through;
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
    assert!(events.commit_translation(translated.clone()).await.is_err());
    // Neither stage changed: the event remains and no output is visible.
    let status = events.status().await.unwrap();
    assert_eq!(status.translated_through, 0);
    assert_eq!(db.count(Table::Events), 1);
    assert_eq!(db.count(Table::Ops), 0);
    assert_eq!(status.op_tail, 0);
    assert!(events.continuation().await.unwrap().is_none());
    db.clear(Fault::RemoveEvents).unwrap();
    let committed = events.commit_translation(translated.clone()).await.unwrap();
    assert_eq!(committed, 1);
    assert_eq!(db.count(Table::Ops), 1);
    assert_eq!(events.status().await.unwrap().translated_through, input);
    assert_eq!(db.count(Table::Events), 0);
    assert!(events.continuation().await.unwrap().is_some());
}

#[tokio::test]
async fn reopening_storage_does_not_count_as_activity() {
    let temp = tempfile::tempdir().unwrap();
    let events = SessionStorage::open(temp.path(), "debug", "session")
        .await
        .unwrap();
    events
        .append(&event("debug", "session", "e", json!({})), None)
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
    let events = SessionStorage::open(temp.path(), "debug", "session")
        .await
        .unwrap();
    assert!(events.status().await.unwrap().idle_for >= day);
    events
        .append(&event("debug", "session", "later", json!({})), None)
        .await
        .unwrap()
        .unwrap();
    assert!(events.status().await.unwrap().idle_for < day);
}

#[tokio::test]
async fn a_live_session_expires_only_what_it_no_longer_uses() {
    let retention = std::time::Duration::from_secs(7 * 24 * 60 * 60);
    let temp = tempfile::tempdir().unwrap();
    let events = SessionStorage::open(temp.path(), "debug", "session")
        .await
        .unwrap();
    let mut captured = event("debug", "session", "e", json!({}));
    captured.route = Some(Default::default());
    let input = events
        .append(&captured, None)
        .await
        .unwrap()
        .unwrap()
        .through;
    assert!(events.append(&captured, None).await.unwrap().is_none());
    let route = |name: &str| crate::wire::SessionRoute {
        tags: [name.into()].into_iter().collect(),
        ..Default::default()
    };
    let delivered = events
        .subscribe(&route("delivered"))
        .await
        .unwrap()
        .position;
    // A route registered for output whose delivery never starts.
    events.register(&route("unsubscribed")).await.unwrap();
    // A route whose destination keeps rejecting what it is sent.
    events.subscribe(&route("rejected")).await.unwrap();
    let rejected = serde_json::to_string(&route("rejected")).unwrap();
    events
        .set_failure(&rejected, Some("413 Request Too Long".into()))
        .await
        .unwrap();
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
    let tail = events
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
    events.remove_through(&key, delivered, tail).await.unwrap();
    let db = Inspector::session(temp.path(), "debug", "session").unwrap();
    assert_eq!(db.count(Table::Ops), 2, "undelivered heads hold output");
    assert!(events.stalled(retention).await.unwrap().is_empty());

    db.age(retention + std::time::Duration::from_secs(60))
        .unwrap();
    let mut stalled: Vec<_> = events
        .stalled(retention)
        .await
        .unwrap()
        .into_iter()
        .flat_map(|route| route.tags)
        .collect();
    stalled.sort();
    assert_eq!(stalled, ["rejected", "unsubscribed"]);
    let unsubscribed = serde_json::to_string(&route("unsubscribed")).unwrap();
    events.expire(retention, vec![unsubscribed]).await.unwrap();
    assert_eq!(db.count(Table::Heads), 2, "a live route is never expired");
    assert_eq!(db.count(Table::Ops), 2);

    events.expire(retention, Vec::new()).await.unwrap();
    assert_eq!(db.count(Table::Heads), 1);
    assert_eq!(db.count(Table::Ops), 0);
    assert_eq!(db.count(Table::Origins), 1, "an open span keeps its origin");
    assert!(events.observations().await.unwrap().is_empty());
    assert!(
        events.append(&captured, None).await.unwrap().is_some(),
        "an expired capture identity is recorded again"
    );
}

#[tokio::test]
async fn compaction_returns_deleted_space_to_the_file_system() {
    let temp = tempfile::tempdir().unwrap();
    let path = super::inspect::session_path(temp.path(), "debug", "session");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    // Earlier daemons created storage without incremental auto-vacuum.
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("PRAGMA auto_vacuum=NONE; CREATE TABLE filler(value BLOB)")
        .unwrap();
    let raw = || rusqlite::Connection::open(&path).unwrap();
    let pragma = |name: &str| raw().one::<i64>(&format!("PRAGMA {name}"), []).unwrap();
    let size = || std::fs::metadata(&path).unwrap().len();
    let churn = || {
        raw()
            .execute_batch(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<1000)
                 INSERT INTO filler SELECT randomblob(4096) FROM n;
                 DELETE FROM filler; PRAGMA wal_checkpoint(TRUNCATE);",
            )
            .unwrap()
    };
    let storage = SessionStorage::at(&path).await.unwrap();
    assert_eq!(pragma("auto_vacuum"), 0);

    churn();
    let bloated = size();
    assert!(pragma("freelist_count") > 0);
    storage.compact().await.unwrap();
    assert_eq!(
        pragma("auto_vacuum"),
        2,
        "rebuilt with incremental auto-vacuum"
    );
    assert_eq!(pragma("freelist_count"), 0);
    assert!(size() < bloated / 4, "{} of {bloated} bytes remain", size());

    churn();
    assert!(pragma("freelist_count") > 0);
    storage.compact().await.unwrap();
    assert_eq!(pragma("freelist_count"), 0);
    assert!(size() < bloated / 4, "{} of {bloated} bytes remain", size());
}

#[tokio::test]
async fn pending_history_deltas_survive_pruning_and_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("history.sqlite");
    let storage = SessionStorage::at(&path).await.unwrap();
    let message = "retained history ".repeat(1024);
    let records = [
        json!({"messages":[{"content":message}]}),
        json!({"messages":[{"content":message},{"content":"second"}]}),
        json!({"messages":[{"content":message},{"content":"second"},{"content":"third"}]}),
        json!({"messages":[{"content":message},{"content":"edited"}]}),
    ];
    let mut ids = Vec::new();
    for payload in &records {
        let span = storage
            .append(&event("debug", "session", "model", payload.clone()), None)
            .await
            .unwrap()
            .unwrap();
        ids.push(span.through);
    }
    let second = ids[1];
    let stored: Vec<u8> = storage
        .run(move |db| {
            Ok(db.one(
                "SELECT value FROM events WHERE id=? AND base IS NOT NULL",
                [second],
            )?)
        })
        .await
        .unwrap();
    assert!(stored.len() < 1024);
    let stored: serde_json::Value = serde_json::from_slice(&stored).unwrap();
    assert_eq!(
        stored["history"],
        json!({"input_delta":[{"content":"second"}]})
    );

    storage
        .commit_translation(Translated {
            input: ids[0],
            observation: true,
            ops: vec![],
            roots: Default::default(),
            state: None,
        })
        .await
        .unwrap();
    let first = ids[0];
    let fourth = ids[3];
    storage
        .run(move |db| {
            assert_eq!(
                db.one::<u64>("SELECT count(*) FROM events WHERE id<=?", [first])?,
                0
            );
            assert_eq!(
                db.one::<u64>(
                    "SELECT count(*) FROM events WHERE base IS NULL AND id IN (?,?)",
                    [second, fourth]
                )?,
                2
            );
            Ok(())
        })
        .await
        .unwrap();
    drop(storage);

    let storage = SessionStorage::at(&path).await.unwrap();
    let pending: Vec<_> = storage
        .read(first, fourth)
        .events()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        pending
            .into_iter()
            .map(|event| event.payload)
            .collect::<Vec<_>>(),
        records[1..]
    );
    let from_middle: Vec<_> = storage
        .read(second, fourth)
        .events()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        from_middle
            .into_iter()
            .map(|event| event.payload)
            .collect::<Vec<_>>(),
        records[2..]
    );
}

#[tokio::test]
async fn pending_history_has_no_periodic_snapshot_limit() {
    let temp = tempfile::tempdir().unwrap();
    let storage = SessionStorage::at(&temp.path().join("chain.sqlite"))
        .await
        .unwrap();
    let mut messages = vec![json!({"content":"history".repeat(2048)})];
    let mut first = 0;
    let mut last = 0;
    for i in 0..32 {
        messages.push(json!({"content":i}));
        last = storage
            .append(
                &event("debug", "session", "model", json!({"messages":messages})),
                None,
            )
            .await
            .unwrap()
            .unwrap()
            .through;
        if i == 0 {
            first = last;
        }
    }
    storage
        .run(|db| {
            assert_eq!(
                db.one::<u64>("SELECT count(*) FROM events WHERE base IS NULL", [])?,
                1
            );
            Ok(())
        })
        .await
        .unwrap();
    storage
        .commit_translation(Translated {
            input: first,
            observation: true,
            ops: vec![],
            roots: Default::default(),
            state: None,
        })
        .await
        .unwrap();
    let rows: Vec<_> = storage
        .read(first, last)
        .events()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(rows.last().unwrap().payload["messages"], json!(messages));

    // Exercise a chain long enough to overflow recursive reconstruction without
    // making capture repeatedly materialize every earlier input in this fixture.
    storage
        .write(move |tx| {
            let encoded: Vec<u8> = tx.one("SELECT value FROM events WHERE id=?", [last])?;
            let mut delta: serde_json::Value = serde_json::from_slice(&encoded)?;
            delta["history"] = json!({"input_delta":[]});
            let encoded = serde_json::to_vec(&delta)?;
            let mut base = last;
            for _ in 0..10_000 {
                let id = base + 100_000;
                tx.execute(
                    "INSERT INTO events(id,control,value,base) VALUES(?,0,?,?)",
                    rusqlite::params![id, encoded, base],
                )?;
                base = id;
            }
            tx.execute("UPDATE session SET event_tail=? WHERE id=1", [base])?;
            super::event_history::remove_through(tx, base - 1)?;
            let stored: Vec<u8> = tx.one(
                "SELECT value FROM events WHERE id=? AND base IS NULL",
                [base],
            )?;
            let recovered: crate::wire::RedactedEnvelope = serde_json::from_slice(&stored)?;
            assert_eq!(recovered.payload["messages"], json!(messages));
            assert_eq!(tx.one::<u64>("SELECT count(*) FROM events", [])?, 1);
            Ok(())
        })
        .await
        .unwrap();
}
