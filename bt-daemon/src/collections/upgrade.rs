//! Upgrades of session databases written by earlier daemons. Everything else
//! in this module works only with the current schema.
use super::span_ops::{save_metadata, StoredOp};
use super::store::{NOW_MS, SCHEMA};
use rusqlite::{Connection, OptionalExtension};
use serde_json::value::RawValue;
use std::collections::BTreeMap;

/// Schema 7: each attachment is stored once, however many ops reference it,
/// and deleted with the last op that does.
const ATTACHMENTS: &str =
    "CREATE TABLE attachment_blobs(digest TEXT PRIMARY KEY, data BLOB NOT NULL,
                refs INTEGER NOT NULL CHECK(refs>=0));
            CREATE TABLE operation_attachments(position INTEGER NOT NULL, digest TEXT NOT NULL,
                PRIMARY KEY(position,digest));
            CREATE TRIGGER attachment_acquire AFTER INSERT ON operation_attachments BEGIN
                UPDATE attachment_blobs SET refs=refs+1 WHERE digest=NEW.digest; END;
            CREATE TRIGGER attachment_release AFTER DELETE ON operation_attachments BEGIN
                UPDATE attachment_blobs SET refs=refs-1 WHERE digest=OLD.digest;
                DELETE FROM attachment_blobs WHERE digest=OLD.digest AND refs=0; END;
            CREATE TRIGGER operation_release AFTER DELETE ON ops BEGIN
                DELETE FROM operation_attachments WHERE position=OLD.position; END;";

/// Read the version under the same write transaction as every upgrade, so a
/// concurrent opener cannot run a stale migration plan.
pub(super) fn migrate(db: &Connection) -> anyhow::Result<()> {
    let version: u32 = db.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    anyhow::ensure!(
        version <= SCHEMA,
        "unsupported session storage schema {version}"
    );
    if version < 3 {
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS batches (
                input INTEGER PRIMARY KEY, committed INTEGER NOT NULL DEFAULT 0,
                observation INTEGER NOT NULL DEFAULT 1);
            CREATE TABLE IF NOT EXISTS ops (
                position INTEGER PRIMARY KEY AUTOINCREMENT,
                input INTEGER NOT NULL, value BLOB NOT NULL);
            CREATE INDEX IF NOT EXISTS ops_input ON ops(input);
            CREATE TABLE IF NOT EXISTS heads (
                route TEXT PRIMARY KEY, position INTEGER NOT NULL DEFAULT 0,
                failure TEXT);
            CREATE TABLE IF NOT EXISTS recovery (id INTEGER PRIMARY KEY CHECK(id=1),failure TEXT);",
        )?;
        if version == 1 {
            db.execute_batch(
                "ALTER TABLE batches ADD COLUMN observation INTEGER NOT NULL DEFAULT 1",
            )?;
        }
        db.execute_batch("ALTER TABLE heads ADD COLUMN binding TEXT")?;
    }
    if version < 4 {
        db.execute_batch(
            "CREATE TABLE root_candidates (span_id TEXT PRIMARY KEY, value BLOB NOT NULL)",
        )?;
    }
    if version < 5 {
        db.execute_batch(
            "ALTER TABLE heads ADD COLUMN settings TEXT;
             ALTER TABLE heads ADD COLUMN settings_through INTEGER NOT NULL DEFAULT 0",
        )?;
    }
    if version < 6 {
        db.execute_batch(
            "DELETE FROM ops WHERE input IN (SELECT input FROM batches WHERE committed=0);
            CREATE TABLE receipts (input INTEGER NOT NULL, ordinal INTEGER NOT NULL, PRIMARY KEY(input,ordinal));
            CREATE TABLE publication (id INTEGER PRIMARY KEY CHECK(id=1), tail INTEGER NOT NULL, floor INTEGER NOT NULL);
            INSERT INTO publication VALUES(1,(SELECT coalesce(max(position),0) FROM ops),0);
            CREATE TABLE origins (span_id TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE span_state (span_id TEXT PRIMARY KEY, kind TEXT NOT NULL, value BLOB NOT NULL, end_ms INTEGER);",
        )?;
    }
    if version < 7 {
        db.execute_batch(ATTACHMENTS)?;
    }
    if version < 8 {
        // One session row replaces per-input batches: an input is handed over
        // in one transaction, so only ops a route could already see survive.
        // A partly published input is translated again.
        db.execute_batch(&format!(
            "CREATE TABLE session(id INTEGER PRIMARY KEY CHECK(id=1),
                source TEXT, session_id TEXT, identity BLOB,
                legacy_namespace INTEGER NOT NULL DEFAULT 0,
                active_ms INTEGER NOT NULL, event_tail INTEGER NOT NULL DEFAULT 0,
                through INTEGER NOT NULL DEFAULT 0, op_tail INTEGER NOT NULL DEFAULT 0,
                continuation BLOB, roots BLOB, failure TEXT);
            INSERT INTO session(id,active_ms,through,op_tail,failure) VALUES(1,{NOW_MS},
                (SELECT coalesce(max(input),0) FROM batches WHERE committed=1),
                (SELECT tail FROM publication WHERE id=1),
                (SELECT failure FROM recovery WHERE id=1));
            DELETE FROM ops WHERE position>(SELECT op_tail FROM session);"
        ))?;
        migrate_roots(db)?;
        // Metadata kept for a live session expires once it goes unused.
        db.execute_batch(&format!(
            "ALTER TABLE origins ADD COLUMN ended_ms INTEGER;
            CREATE INDEX origins_ended ON origins(ended_ms);
            ALTER TABLE span_state ADD COLUMN touched_ms INTEGER NOT NULL DEFAULT 0;
            UPDATE span_state SET touched_ms={NOW_MS};
            ALTER TABLE heads ADD COLUMN progress_ms INTEGER NOT NULL DEFAULT 0;
            ALTER TABLE heads ADD COLUMN subscribed INTEGER NOT NULL DEFAULT 0;
            UPDATE heads SET progress_ms={NOW_MS}, subscribed=1;"
        ))?;
        if version < 6 {
            // One bounded pass extracts the metadata a schema-5 channel kept
            // only inside its stored ops.
            extract_metadata(db)?;
        }
        db.execute_batch(
            "DROP TABLE batches; DROP TABLE receipts; DROP TABLE root_candidates;
            DROP TABLE publication; DROP TABLE recovery;
            DROP INDEX ops_input; ALTER TABLE ops DROP COLUMN input;
            CREATE TABLE events (id INTEGER PRIMARY KEY, control INTEGER NOT NULL, value BLOB NOT NULL);
            CREATE TABLE captured (digest BLOB PRIMARY KEY, at_ms INTEGER NOT NULL) WITHOUT ROWID;
            CREATE INDEX captured_at ON captured(at_ms);",
        )?;
    }
    if version < SCHEMA {
        // Opening an up-to-date store writes nothing, so it keeps its age.
        db.execute_batch(&format!("PRAGMA user_version={SCHEMA}"))?;
    }
    Ok(())
}

/// A schema-5 channel kept span metadata only inside its stored ops. One
/// bounded pass extracts what recovery needs, then drops delivered ops.
fn extract_metadata(db: &Connection) -> anyhow::Result<()> {
    let mut after = 0;
    loop {
        let record: Option<(u64, Vec<u8>, bool)> = db
            .query_row(
                "SELECT position,value,observation FROM ops JOIN batches USING(input)
                 WHERE position>? ORDER BY position LIMIT 1",
                [after],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((position, value, observation)) = record else {
            break;
        };
        save_metadata(db, &StoredOp::decode(&value)?, observation)?;
        after = position;
    }
    super::span_ops::prune(db)
}

/// A pre-collection channel stored each root candidate in its own row.
/// Translation now replaces them all in one value.
fn migrate_roots(db: &Connection) -> anyhow::Result<()> {
    let mut roots = BTreeMap::<String, Vec<Box<RawValue>>>::new();
    let mut query = db.prepare("SELECT span_id,value FROM root_candidates")?;
    for row in query.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
    })? {
        let (id, value) = row?;
        let ops: Vec<Vec<u8>> = serde_json::from_slice(&value)?;
        let ops = ops
            .into_iter()
            .map(|op| Ok(RawValue::from_string(String::from_utf8(op)?)?))
            .collect::<anyhow::Result<Vec<_>>>()?;
        roots.insert(id, ops);
    }
    if !roots.is_empty() {
        db.execute(
            "UPDATE session SET roots=? WHERE id=1",
            [serde_json::to_vec(&roots)?],
        )?;
    }
    Ok(())
}

/// Upgrades from SpanOp channel schemas that predate collections.
#[cfg(test)]
mod tests {
    use super::super::inspect::{Inspector, Table};
    use super::super::{BindingChanged, SessionStorage, Translated};
    use super::*;
    use crate::pipeline::cursor::Cursor;
    use crate::translate::{OriginSnapshot, SpanOp, SpanRow};
    use crate::wire::SessionRoute;
    use rusqlite::params;
    use std::sync::Arc;

    fn op(name: &str) -> SpanOp {
        SpanOp::Insert(SpanRow {
            span_id: name.into(),
            name: name.into(),
            late_merge_key: Some("late".into()),
            turn_span_id: Some("turn".into()),
            is_turn: true,
            origin: Some(
                OriginSnapshot {
                    plugin_version: Some("capture".into()),
                    source_version: None,
                    bt_version: "test".into(),
                }
                .into(),
            ),
            ..SpanRow::default()
        })
    }

    fn route(name: &str) -> SessionRoute {
        SessionRoute {
            tags: [name.into()].into_iter().collect(),
            ..SessionRoute::default()
        }
    }

    /// The SpanOp channel schema written by the last pre-collection release.
    const SCHEMA_FIVE: &str = "CREATE TABLE batches(input INTEGER PRIMARY KEY,
            committed INTEGER NOT NULL DEFAULT 0, observation INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE ops(position INTEGER PRIMARY KEY AUTOINCREMENT,
            input INTEGER NOT NULL, value BLOB NOT NULL);
        CREATE INDEX ops_input ON ops(input);
        CREATE TABLE heads(route TEXT PRIMARY KEY, position INTEGER NOT NULL DEFAULT 0,
            failure TEXT, binding TEXT, settings TEXT, settings_through INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE recovery(id INTEGER PRIMARY KEY CHECK(id=1), failure TEXT);
        CREATE TABLE root_candidates(span_id TEXT PRIMARY KEY, value BLOB NOT NULL);
        PRAGMA user_version=5;";

    const SCHEMA_SIX: &str = "CREATE TABLE receipts(input INTEGER NOT NULL,
            ordinal INTEGER NOT NULL, PRIMARY KEY(input,ordinal));
        CREATE TABLE publication(id INTEGER PRIMARY KEY CHECK(id=1),
            tail INTEGER NOT NULL, floor INTEGER NOT NULL);
        CREATE TABLE origins(span_id TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE span_state(span_id TEXT PRIMARY KEY, kind TEXT NOT NULL,
            value BLOB NOT NULL, end_ms INTEGER);
        PRAGMA user_version=6;";

    #[tokio::test]
    async fn schema_seven_attachments_survive_the_upgrade() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("upgrade.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(SCHEMA_FIVE).unwrap();
        db.execute_batch(SCHEMA_SIX).unwrap();
        db.execute_batch(ATTACHMENTS).unwrap();
        db.execute(
            "INSERT INTO heads(route,position) VALUES(?,0)",
            [serde_json::to_string(&route("a")).unwrap()],
        )
        .unwrap();
        db.execute_batch(
            "INSERT INTO batches(input,committed) VALUES(10,1);
             INSERT INTO publication VALUES(1,1,0);
             PRAGMA user_version=7;",
        )
        .unwrap();
        let attachment = crate::attachments::Attachment::new(
            "document.pdf".into(),
            "application/pdf".into(),
            b"pdf bytes".to_vec(),
        );
        let mut stored = op("document");
        stored.row_mut().attachments = vec![crate::attachments::Attachment {
            data: Vec::new(),
            ..attachment.clone()
        }];
        db.execute(
            "INSERT INTO ops(input,value) VALUES(10,?)",
            [StoredOp::encode(&stored).unwrap()],
        )
        .unwrap();
        db.execute(
            "INSERT INTO attachment_blobs(digest,data,refs) VALUES(?,?,0)",
            params![attachment.digest, attachment.data],
        )
        .unwrap();
        db.execute(
            "INSERT INTO operation_attachments(position,digest) VALUES(1,?)",
            [&attachment.digest],
        )
        .unwrap();
        drop(db);
        let upgraded = SessionStorage::at(&path).await.unwrap();
        let ops = upgraded.stored().await;
        assert_eq!(ops[0].row().attachments[0].data, b"pdf bytes");
        assert_eq!(Inspector::open(&path).unwrap().attachment_refs(), [1]);
    }

    #[tokio::test]
    async fn schema_five_upgrade_reclaims_acknowledged_payloads_without_losing_provenance() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("upgrade.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(SCHEMA_FIVE).unwrap();
        db.execute(
            "INSERT INTO heads(route,position) VALUES(?,1)",
            [serde_json::to_string(&route("old")).unwrap()],
        )
        .unwrap();
        db.execute_batch("INSERT INTO batches(input,committed) VALUES(10,1)")
            .unwrap();
        db.execute(
            "INSERT INTO ops(input,value) VALUES(10,?)",
            [StoredOp::encode(&op("old")).unwrap()],
        )
        .unwrap();
        drop(db);
        let upgraded = SessionStorage::at(&path).await.unwrap();
        assert_eq!(upgraded.status().await.unwrap().op_tail, 1);
        let origins = upgraded.origins_for(vec!["old".into()]).await.unwrap();
        assert_eq!(origins["old"].bt_version, "test");
        assert_eq!(Inspector::open(&path).unwrap().count(Table::Ops), 0);
        assert!(upgraded.complete().await.unwrap());
    }

    #[tokio::test]
    async fn schema_six_upgrade_keeps_only_published_ops_and_translation_state() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("upgrade.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(SCHEMA_FIVE).unwrap();
        db.execute_batch(SCHEMA_SIX).unwrap();
        db.execute(
            "INSERT INTO heads(route,position) VALUES(?,0)",
            [serde_json::to_string(&route("a")).unwrap()],
        )
        .unwrap();
        // Input 10 committed; input 20 published one op, then the daemon
        // stopped before storing or publishing the rest.
        db.execute_batch(
            "INSERT INTO batches(input,committed) VALUES(10,1),(20,0);
             INSERT INTO receipts VALUES(20,0);
             INSERT INTO publication VALUES(1,2,0);
             INSERT INTO recovery VALUES(1,'held');",
        )
        .unwrap();
        for (input, name) in [(10, "committed"), (20, "published"), (20, "unpublished")] {
            db.execute(
                "INSERT INTO ops(input,value) VALUES(?,?)",
                params![input, StoredOp::encode(&op(name)).unwrap()],
            )
            .unwrap();
        }
        let root = serde_json::to_vec(&[StoredOp::encode(&op("root")).unwrap()]).unwrap();
        db.execute("INSERT INTO root_candidates VALUES('root',?)", [root])
            .unwrap();
        drop(db);
        let upgraded = SessionStorage::at(&path).await.unwrap();
        let status = upgraded.status().await.unwrap();
        assert_eq!(status.translated_through, 10);
        assert_eq!(status.op_tail, 2);
        let names: Vec<_> = upgraded
            .stored()
            .await
            .into_iter()
            .map(|op| op.row().span_id.clone())
            .collect();
        assert_eq!(names, ["committed", "published"]);
        assert_eq!(status.source_failure.as_deref(), Some("held"));
        let roots = upgraded.root_candidates().await.unwrap();
        assert_eq!(
            roots["root"][0].row().late_merge_key.as_deref(),
            Some("late")
        );
        assert!(upgraded.continuation().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn concurrent_opens_upgrade_one_schema_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("concurrent-upgrade.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch("CREATE TABLE batches(input INTEGER PRIMARY KEY,committed INTEGER NOT NULL DEFAULT 0); PRAGMA user_version=1;").unwrap();
        drop(db);
        let ready = Arc::new(tokio::sync::Barrier::new(8));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let path = path.clone();
            let ready = ready.clone();
            tasks.push(tokio::spawn(async move {
                ready.wait().await;
                SessionStorage::at(&path).await
            }));
        }
        let mut opened = Vec::new();
        for task in tasks {
            opened.push(task.await.unwrap().unwrap());
        }
        for session in opened {
            assert_eq!(session.status().await.unwrap().op_tail, 0);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn translation_begin_waits_for_a_concurrent_writer_before_reading() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("concurrent.sqlite");
        let session = SessionStorage::at(&path).await.unwrap();
        let mut peer = Connection::open(&path).unwrap();
        let transaction = peer
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute("UPDATE session SET through=10", [])
            .unwrap();
        let begin = tokio::spawn(async move {
            session.translated(10, vec![op("late")]).await.unwrap();
            session
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let failed_early = begin.is_finished();
        transaction.commit().unwrap();
        begin.await.unwrap();
        assert_eq!(
            Inspector::open(&path).unwrap().count(Table::Ops),
            0,
            "a committed input must remain committed after waiting for its writer"
        );
        assert!(
            !failed_early,
            "a read-before-write transaction must wait rather than fail to upgrade its snapshot"
        );
    }

    #[tokio::test]
    async fn preparatory_schema_upgrades_without_changing_committed_positions() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch("CREATE TABLE batches(input INTEGER PRIMARY KEY,committed INTEGER NOT NULL DEFAULT 0); INSERT INTO batches VALUES(10,1); PRAGMA user_version=1;").unwrap();
        drop(db);
        let session = SessionStorage::at(&path).await.unwrap();
        assert_eq!(session.status().await.unwrap().translated_through, 10);
        session
            .commit_translation(Translated {
                input: 20,
                ops: vec![op("retirement")],
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(
            session.observations().await.unwrap().is_empty(),
            "retirement output is not a native correlation observation"
        );
        let mut route = Cursor::open(&session, &route("a")).await.unwrap();
        assert_eq!(
            route.next_batch(8).await.unwrap().unwrap().value().len(),
            1,
            "all published output still reaches durable delivery subscribers"
        );
    }

    #[tokio::test]
    async fn schema_two_heads_upgrade_and_keep_a_nonsecret_backend_binding() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch("CREATE TABLE heads(route TEXT PRIMARY KEY,position INTEGER NOT NULL DEFAULT 0,failure TEXT); PRAGMA user_version=2;").unwrap();
        let route = route("canonical");
        let key = serde_json::to_string(&route).unwrap();
        db.execute("INSERT INTO heads(route,position) VALUES(?,7)", [&key])
            .unwrap();
        drop(db);
        let session = SessionStorage::at(&path).await.unwrap();
        let mut config = route.with_auth(crate::wire::BackendAuth {
            token: "secret-a".into(),
            api_url: Some("https://one.test".into()),
            app_url: None,
            org_id: Some("org".into()),
            org_name: None,
        });
        session.bind(&key, &config).await.unwrap();
        config.auth.token = "secret-b".into();
        session.bind(&key, &config).await.unwrap();
        config.auth.api_url = Some("https://other.test".into());
        assert!(session
            .bind(&key, &config)
            .await
            .unwrap_err()
            .downcast_ref::<BindingChanged>()
            .is_some());
        let restored = Cursor::open(&session, &route).await.unwrap();
        assert_eq!(restored.completed_position(), 7);
        let binding: String = Connection::open(&path)
            .unwrap()
            .query_row("SELECT binding FROM heads", [], |r| r.get(0))
            .unwrap();
        assert!(!binding.contains("secret-"));
    }
}
