//! One source session's database: its event collection, its SpanOp
//! collection, and the single row that records how far each has progressed.
//! Everything that spans both collections, such as handing an input's
//! translation over or deciding the session owes nothing, lives here.
use super::span_ops::{
    decode_roots, detach_attachments, encode_roots, prune, save_metadata, stalled,
    store_attachments, Translated,
};
use super::store::{select, One, SessionStorage, NOW_MS};
use crate::pipeline::live::{Record, SharedOp};
use crate::translate::SpanOp;
use rusqlite::params;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

/// How far a session has progressed, read in one query.
#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct Status {
    /// The source that owns this database, once claimed.
    pub source: Option<String>,
    pub session_id: Option<String>,
    /// The ID of the last event record ever inserted, including removed records.
    pub event_tail: u64,
    /// The last input whose translation has been handed over.
    pub translated_through: u64,
    /// The position of the last committed op.
    pub op_tail: u64,
    /// Whether this source kept the pre-qualification span-id namespace.
    pub legacy_namespace: bool,
    /// A translation failure recorded for recovery.
    pub source_failure: Option<String>,
    /// Time since capture, translation or delivery last made progress.
    #[serde(deserialize_with = "millis")]
    pub idle_for: Duration,
}

impl Status {
    /// The source and native session this database belongs to.
    pub fn owner(&self) -> Option<(String, String)> {
        self.source.clone().zip(self.session_id.clone())
    }
}

fn millis<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    <u64 as serde::Deserialize>::deserialize(deserializer).map(Duration::from_millis)
}

/// The database file for one source session.
pub(crate) fn session_path(data_dir: &Path, source: &str, session_id: &str) -> PathBuf {
    data_dir.join("pipeline").join(format!(
        "{}.sqlite",
        crate::ids::session_storage_id(source, session_id)
    ))
}

impl SessionStorage {
    /// Every session database in the data directory, after importing legacy journals.
    pub async fn all(data_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
        super::legacy::import_all(data_dir).await;
        let mut sessions = Vec::new();
        match tokio::fs::read_dir(data_dir.join("pipeline")).await {
            Ok(mut entries) => {
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    let session = path
                        .file_stem()
                        .and_then(|stem| stem.to_str())
                        .is_some_and(|stem| uuid::Uuid::parse_str(stem).is_ok());
                    if session && path.extension().is_some_and(|ext| ext == "sqlite") {
                        sessions.push(path);
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        sessions.sort();
        Ok(sessions)
    }

    /// Open the session, first importing any legacy NDJSON journal for it. A
    /// legacy file is deleted only after its records are committed here.
    pub async fn open(data_dir: &Path, source: &str, session_id: &str) -> anyhow::Result<Self> {
        let source = crate::translate::canonical_source_name(source).to_owned();
        let session = Self::claim(data_dir, &source, session_id).await?;
        super::legacy::import_session(data_dir, &source, session_id, &session).await?;
        Ok(session)
    }

    /// Open the session database without legacy import, recording its owner.
    pub(super) async fn claim(
        data_dir: &Path,
        source: &str,
        session_id: &str,
    ) -> anyhow::Result<Self> {
        let session = Self::at(&session_path(data_dir, source, session_id)).await?;
        let (source, session_id) = (source.to_owned(), session_id.to_owned());
        session
            .write(move |tx| {
                tx.execute(
                    "UPDATE session SET source=?, session_id=? WHERE id=1 AND source IS NULL",
                    params![source, session_id],
                )?;
                Ok(())
            })
            .await?;
        Ok(session)
    }

    pub async fn status(&self) -> anyhow::Result<Status> {
        self.run(|db| {
            select(
                db,
                &format!(
                    "SELECT source, session_id, event_tail, through AS translated_through,
                     op_tail, legacy_namespace, failure AS source_failure,
                     max(0,{NOW_MS}-active_ms) AS idle_for FROM session WHERE id=1"
                ),
                [],
            )?
            .pop()
            .ok_or_else(|| anyhow::anyhow!("session row is missing"))
        })
        .await
    }

    /// The translator continuation saved with the last handed-over input.
    pub async fn continuation(&self) -> anyhow::Result<Option<Vec<u8>>> {
        self.run(|db| Ok(db.one("SELECT continuation FROM session WHERE id=1", [])?))
            .await
    }

    /// Root candidates still waiting for work.
    pub async fn root_candidates(&self) -> anyhow::Result<BTreeMap<String, Vec<SpanOp>>> {
        self.run(|db| {
            let bytes: Option<Vec<u8>> = db.one("SELECT roots FROM session WHERE id=1", [])?;
            bytes.map_or_else(|| Ok(BTreeMap::new()), |bytes| decode_roots(&bytes))
        })
        .await
    }

    pub async fn set_source_failure(&self, failure: Option<String>) -> anyhow::Result<()> {
        self.write(move |tx| {
            tx.execute("UPDATE session SET failure=? WHERE id=1", [failure])?;
            Ok(())
        })
        .await
    }

    /// Hand one input event's complete translation to delivery in a single
    /// transaction: store its ops, metadata, root candidates and translator
    /// continuation, and remove the consumed events through `input`. Routes
    /// then receive the original op instances. An input that is already
    /// handed over is left unchanged. Returns the published tail.
    ///
    /// Inline media become attachments: routes receive them with their bytes,
    /// and storage keeps one copy of each until no op references it.
    pub async fn commit_translation(&self, mut translated: Translated) -> anyhow::Result<u64> {
        let encoded = translated
            .ops
            .iter_mut()
            .map(|op| {
                let (value, blobs) = detach_attachments(op)?;
                Ok((value, blobs, super::metadata_only(op)))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let roots = encode_roots(&translated.roots)?;
        let (input, observation, state) =
            (translated.input, translated.observation, translated.state);
        let (tail, positions) = self
            .write(move |tx| {
                let (through, tail): (u64, u64) =
                    tx.query_row("SELECT through,op_tail FROM session WHERE id=1", [], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?;
                if input <= through {
                    return Ok((tail, Vec::new()));
                }
                let mut positions = Vec::with_capacity(encoded.len());
                for (value, blobs, metadata) in encoded {
                    save_metadata(tx, &metadata, observation)?;
                    let bytes = blobs.iter().fold(value.len(), |bytes, blob| {
                        bytes.saturating_add(blob.data.len())
                    });
                    tx.execute("INSERT INTO ops(value) VALUES (?)", [value])?;
                    let position = tx.last_insert_rowid() as u64;
                    store_attachments(tx, position, &blobs)?;
                    positions.push((position, bytes));
                }
                let new_tail = positions.last().map_or(tail, |(position, _)| *position);
                if !positions.is_empty() {
                    // Heads that had caught up start waiting for this output now.
                    tx.execute(
                        &format!("UPDATE heads SET progress_ms={NOW_MS} WHERE position>=?"),
                        [tail],
                    )?;
                }
                tx.execute(
                    "UPDATE session SET through=?, op_tail=?, roots=?,
                     continuation=coalesce(?,continuation) WHERE id=1",
                    params![input, new_tail, roots, state],
                )?;
                tx.execute("DELETE FROM events WHERE id<=?", [input])?;
                prune(tx)?;
                super::store::touch(tx)?;
                Ok((new_tail, positions))
            })
            .await?;
        let records = positions
            .into_iter()
            .zip(translated.ops)
            .map(|((position, bytes), op)| Record {
                position,
                bytes,
                op: SharedOp::new(op),
            });
        self.channel().publish(tail, records).await;
        Ok(tail)
    }

    /// Every obligation is met: all captured input is translated and every
    /// route has delivered every op. Root candidates still waiting for work
    /// and input retained only to rebuild translator state are not owed.
    pub async fn complete(&self) -> anyhow::Result<bool> {
        self.run(|db| {
            Ok(db.one(
                "SELECT NOT EXISTS(SELECT 1 FROM heads WHERE failure IS NOT NULL OR position<s.op_tail)
                 AND s.failure IS NULL AND s.through>=s.event_tail
                 FROM session s WHERE id=1",
                [],
            )?)
        })
        .await
    }

    /// Expire what a live session no longer uses once `retention` passes:
    /// capture identities, metadata of spans that ended or were never
    /// completed, and the heads of [`stalled`](Self::stalled) routes. Ops no
    /// remaining head needs are then deleted. Every route named in `live`,
    /// which has an active delivery, is kept.
    pub async fn expire(&self, retention: Duration, live: Vec<String>) -> anyhow::Result<()> {
        let retention = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
        self.write(move |tx| {
            let cutoff: i64 = tx.one(&format!("SELECT {NOW_MS}-?"), [retention])?;
            tx.execute("DELETE FROM captured WHERE at_ms<?", [cutoff])?;
            tx.execute("DELETE FROM origins WHERE ended_ms<?", [cutoff])?;
            tx.execute(
                "DELETE FROM span_state WHERE touched_ms<? AND (kind='tool' OR end_ms IS NOT NULL)",
                [cutoff],
            )?;
            let mut expired = false;
            for route in stalled(tx, cutoff)?
                .into_iter()
                .filter(|route| !live.contains(route))
            {
                let failure: Option<String> =
                    tx.one("SELECT failure FROM heads WHERE route=?", [&route])?;
                tracing::warn!(%route, ?failure, "undelivered output expired for a stalled route");
                tx.execute("DELETE FROM heads WHERE route=?", [route])?;
                expired = true;
            }
            if expired && tx.one::<i64>("SELECT count(*) FROM heads", [])? == 0 {
                // Everything stored was owed only to expired routes.
                tx.execute("DELETE FROM ops", [])?;
            }
            prune(tx)
        })
        .await
    }
}
