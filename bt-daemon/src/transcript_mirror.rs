//! Daemon-owned append-only mirrors of agent transcript files.
//!
//! Claude transcript files are external mutable state, so a journaled
//! lifecycle event must stay replayable even after the agent rewrites or
//! deletes the path it came from. Embedding the whole transcript in every
//! lifecycle event bought that durability at quadratic cost: a session
//! re-journaled its entire (growing) transcript on every turn, so an 18 MB
//! transcript produced a 1.6 GB journal that replay then had to hold in
//! memory all at once.
//!
//! Append-only mirroring stores each transcript byte exactly once. Cursor
//! rewrites use separate immutable generation snapshots so older observations
//! remain replayable. The journal carries
//! only a reference — the mirror path plus the high-water offset that existed
//! when the event was accepted — so replay reads the same bytes the live run
//! saw, straight off disk, without the daemon ever holding a transcript in
//! memory.

use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

/// Namespace for mirror file names (distinct from the span-id namespace).
const NAMESPACE: Uuid = Uuid::from_u128(0x3d51_9a02_7c64_4b8f_9e17_a2c5_0d63_88f1);

pub fn mirror_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("transcripts")
}

/// A stable per-(session, transcript path) mirror file name. Keyed by both so
/// one session's main and subagent transcripts never collide, and so two
/// sessions reading the same path keep independent mirrors.
pub fn mirror_path(data_dir: &Path, session_id: &str, source: &str) -> PathBuf {
    let name = format!("{session_id}\u{1f}{source}");
    let digest = Uuid::new_v5(&NAMESPACE, name.as_bytes())
        .simple()
        .to_string();
    mirror_dir(data_dir).join(format!("{digest}.jsonl"))
}

/// Append everything written to `source` since the last capture, returning the
/// mirror path and the mirror's resulting length. That length is the exact
/// high-water offset the caller should journal: replay bounded by it sees the
/// transcript as of this moment and no further.
///
/// If `source` is shorter than the mirror (the agent rewrote or truncated it),
/// the mirror is rebuilt from scratch so it never interleaves two generations.
pub async fn capture(
    data_dir: &Path,
    session_id: &str,
    source: &str,
) -> anyhow::Result<(PathBuf, u64)> {
    let path = mirror_path(data_dir, session_id, source);
    crate::paths::ensure_private_dir(&mirror_dir(data_dir))?;

    let mirrored = tokio::fs::metadata(&path)
        .await
        .map(|meta| meta.len())
        .unwrap_or(0);
    let mut input = tokio::fs::File::open(source).await?;
    let source_len = input.metadata().await?.len();

    // A shorter source means the file was replaced; start the mirror over.
    let restart = source_len < mirrored;
    let from = if restart { 0 } else { mirrored };
    if from >= source_len {
        return Ok((path, mirrored));
    }

    input.seek(std::io::SeekFrom::Start(from)).await?;
    let mut mirror = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(restart)
        .append(!restart)
        .open(&path)
        .await?;
    // Streamed, never buffered whole: the delta is copied through a small
    // fixed buffer, so mirroring an arbitrarily large transcript costs
    // arbitrarily little memory. The copy is unbounded in bytes on purpose —
    // the mirror is the durable record and must stay complete.
    let copied = tokio::io::copy(&mut input, &mut mirror).await?;
    mirror.flush().await?;
    Ok((path, from + copied))
}

/// Capture a hook-bounded observation while preserving every previously
/// journaled generation. Cursor can rewrite its JSONL transcript, including
/// replacements with the same length, so length alone cannot identify an
/// append. Compare the existing prefix in fixed-size buffers before extending
/// it; replacements get a separate file and never invalidate older references.
pub async fn capture_generation(
    data_dir: &Path,
    session_id: &str,
    source: &str,
    observed_bytes: Option<u64>,
) -> anyhow::Result<(PathBuf, u64)> {
    crate::paths::ensure_private_dir(&mirror_dir(data_dir))?;
    let base = mirror_path(data_dir, session_id, source);
    let current = base.with_extension("current");
    // The pointer is private daemon state, never a path supplied by a hook.
    let mut path = tokio::fs::read_to_string(&current)
        .await
        .ok()
        .map(|name| mirror_dir(data_dir).join(name.trim()))
        .filter(|path| path.parent() == Some(mirror_dir(data_dir).as_path()))
        .unwrap_or_else(|| base.clone());
    let mut input = tokio::fs::File::open(source).await?;
    let source_len = input.metadata().await?.len();
    let through = observed_bytes.unwrap_or(source_len).min(source_len);
    let mirrored = tokio::fs::metadata(&path)
        .await
        .map(|meta| meta.len())
        .unwrap_or(0);
    let mut changed = source_len < mirrored;
    if !changed && mirrored > 0 {
        let mut prior = tokio::fs::File::open(&path).await?;
        let mut source_buffer = [0; 16 * 1024];
        let mut mirror_buffer = [0; 16 * 1024];
        let mut remaining = mirrored.min(through);
        while remaining > 0 {
            let len = remaining.min(source_buffer.len() as u64) as usize;
            input.read_exact(&mut source_buffer[..len]).await?;
            prior.read_exact(&mut mirror_buffer[..len]).await?;
            if source_buffer[..len] != mirror_buffer[..len] {
                changed = true;
                break;
            }
            remaining -= len as u64;
        }
    }
    let from = if changed {
        path = base.with_file_name(format!(
            "{}-{}.jsonl",
            base.file_stem().unwrap().to_string_lossy(),
            Uuid::new_v4().simple()
        ));
        0
    } else {
        mirrored.min(through)
    };
    input.seek(std::io::SeekFrom::Start(from)).await?;
    let mut mirror = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;
    // `take` freezes the hook's observation: bytes appended by a later native
    // event while this copy runs belong to that later journal envelope.
    let copied = tokio::io::copy(&mut input.take(through - from), &mut mirror).await?;
    mirror.flush().await?;
    mirror.sync_data().await?;
    tokio::fs::write(
        &current,
        path.file_name().unwrap().to_string_lossy().as_bytes(),
    )
    .await?;
    Ok((path, from + copied))
}

/// Age only permits collection of unreferenced auxiliary input. Remaining events,
/// committed translator continuation, and current observation templates protect mirrors.
pub async fn gc_old_mirrors(data_dir: &Path, max_age: std::time::Duration) {
    let data_dir = data_dir.to_owned();
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        use std::collections::HashSet;
        fn references(value: &serde_json::Value, names: &mut HashSet<String>) {
            match value {
                serde_json::Value::String(value) => {
                    if let Some(name) = Path::new(value).file_name().and_then(|n| n.to_str()) {
                        names.insert(name.to_owned());
                    }
                }
                serde_json::Value::Array(values) => {
                    for value in values {
                        references(value, names);
                    }
                }
                serde_json::Value::Object(values) => {
                    for (key, value) in values {
                        references(&serde_json::Value::String(key.clone()), names);
                        references(value, names);
                    }
                }
                _ => {}
            }
        }
        let mut protected = HashSet::new();
        // Scan remaining JSONL input, including framed and released unframed records. Fail closed
        // on unreadable control/input state so maintenance cannot make recovery lossy.
        if crate::journal::journal_dir(&data_dir).exists() {
            for entry in std::fs::read_dir(crate::journal::journal_dir(&data_dir))? {
                let path = entry?.path();
                if path.extension().and_then(|e| e.to_str()) != Some("ndjson") {
                    continue;
                }
                use std::io::BufRead;
                for line in std::io::BufReader::new(std::fs::File::open(&path)?).lines() {
                    let line = line?;
                    if !line.trim().is_empty() {
                        references(&serde_json::from_str(&line)?, &mut protected);
                    }
                }
                for route in crate::journal::captured_routes(&path)? {
                    references(&serde_json::to_value(route.envelope)?, &mut protected);
                }
            }
        }
        let derived = data_dir.join("derived");
        if derived.exists() {
            for entry in std::fs::read_dir(derived)? {
                let path = entry?.path().join("spans.sqlite");
                if !path.exists() {
                    continue;
                }
                let db = rusqlite::Connection::open_with_flags(
                    path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )?;
                db.busy_timeout(std::time::Duration::from_secs(5))?;
                let mut stmt = db.prepare("SELECT state FROM continuation")?;
                for state in stmt.query_map([], |r| r.get::<_, String>(0))? {
                    references(&serde_json::from_str(&state?)?, &mut protected);
                }
            }
        }
        let dir = mirror_dir(&data_dir);
        if !dir.exists() {
            return Ok(());
        }
        let entries = std::fs::read_dir(&dir)?.collect::<Result<Vec<_>, _>>()?;
        let now = std::time::SystemTime::now();
        for entry in entries {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("jsonl")
                || protected.contains(path.file_name().unwrap().to_str().unwrap_or_default())
            {
                continue;
            }
            let old = entry
                .metadata()?
                .modified()
                .ok()
                .and_then(|time| now.duration_since(time).ok())
                .is_some_and(|age| age > max_age);
            if old {
                std::fs::remove_file(path)?;
            }
        }
        Ok(())
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(%error,"transcript mirror collection skipped"),
        Err(error) => tracing::warn!(%error,"transcript mirror collection task failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn collection_protects_untranslated_input_and_continuation_references() {
        let temp = tempfile::tempdir().unwrap();
        let native = temp.path().join("native.jsonl");
        std::fs::write(&native, "original\n").unwrap();
        let (mirror, _) = capture(temp.path(), "session", native.to_str().unwrap())
            .await
            .unwrap();
        let env: crate::wire::Envelope = serde_json::from_value(serde_json::json!({
            "source":"debug","session_id":"session","event":"unknown","ts_ms":1,
            "payload":{"_bt_transcript_mirror":{"mirror":mirror}}
        }))
        .unwrap();
        let journal = crate::journal::source_journal_path(temp.path(), "debug", "session");
        let mut writer = crate::journal::JournalWriter::open_path(&journal)
            .await
            .unwrap();
        let through = writer.append(&env).await.unwrap();
        gc_old_mirrors(temp.path(), std::time::Duration::ZERO).await;
        assert!(
            mirror.exists(),
            "untranslated events protect their observations"
        );
        let db_path = temp.path().join("derived/test/spans.sqlite");
        std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();
        let db = rusqlite::Connection::open(db_path).unwrap();
        db.execute_batch("CREATE TABLE continuation(state TEXT)")
            .unwrap();
        db.execute(
            "INSERT INTO continuation VALUES(?1)",
            [serde_json::json!({"path":mirror}).to_string()],
        )
        .unwrap();
        writer.collect_through(through).await.unwrap();
        gc_old_mirrors(temp.path(), std::time::Duration::ZERO).await;
        assert!(
            mirror.exists(),
            "continuation protects observations after input collection"
        );
        db.execute("DELETE FROM continuation", []).unwrap();
        gc_old_mirrors(temp.path(), std::time::Duration::ZERO).await;
        assert!(
            !mirror.exists(),
            "unreferenced observations are not retained forever"
        );
    }

    #[tokio::test]
    async fn capture_is_incremental_and_reports_the_high_water_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("t.jsonl");
        tokio::fs::write(&source, b"one\n").await.unwrap();

        let (mirror, first) = capture(tmp.path(), "s1", source.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(first, 4);

        tokio::fs::write(&source, b"one\ntwo\n").await.unwrap();
        let (_, second) = capture(tmp.path(), "s1", source.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(second, 8);
        assert_eq!(tokio::fs::read(&mirror).await.unwrap(), b"one\ntwo\n");
    }

    #[tokio::test]
    async fn a_truncated_source_restarts_the_mirror() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("t.jsonl");
        tokio::fs::write(&source, b"aaaa\nbbbb\n").await.unwrap();
        capture(tmp.path(), "s1", source.to_str().unwrap())
            .await
            .unwrap();

        tokio::fs::write(&source, b"cc\n").await.unwrap();
        let (mirror, len) = capture(tmp.path(), "s1", source.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(len, 3);
        assert_eq!(tokio::fs::read(&mirror).await.unwrap(), b"cc\n");
    }

    #[tokio::test]
    async fn separate_sessions_and_paths_get_separate_mirrors() {
        let tmp = tempfile::tempdir().unwrap();
        assert_ne!(
            mirror_path(tmp.path(), "s1", "/a.jsonl"),
            mirror_path(tmp.path(), "s2", "/a.jsonl")
        );
        assert_ne!(
            mirror_path(tmp.path(), "s1", "/a.jsonl"),
            mirror_path(tmp.path(), "s1", "/b.jsonl")
        );
    }

    #[tokio::test]
    async fn generation_capture_preserves_replaced_and_truncated_observations() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("t.jsonl");
        tokio::fs::write(&source, b"first\n").await.unwrap();
        let (first, first_len) =
            capture_generation(tmp.path(), "s1", source.to_str().unwrap(), None)
                .await
                .unwrap();
        // A same-length replacement must be detectable, not treated as an append.
        tokio::fs::write(&source, b"other\n").await.unwrap();
        let (second, second_len) =
            capture_generation(tmp.path(), "s1", source.to_str().unwrap(), None)
                .await
                .unwrap();
        tokio::fs::write(&source, b"ok\n").await.unwrap();
        let (third, third_len) =
            capture_generation(tmp.path(), "s1", source.to_str().unwrap(), None)
                .await
                .unwrap();
        assert_ne!(first, second);
        assert_ne!(second, third);
        assert_eq!((first_len, second_len, third_len), (6, 6, 3));
        tokio::fs::remove_file(&source).await.unwrap();
        assert_eq!(tokio::fs::read(first).await.unwrap(), b"first\n");
        assert_eq!(tokio::fs::read(second).await.unwrap(), b"other\n");
        assert_eq!(tokio::fs::read(third).await.unwrap(), b"ok\n");
    }

    #[tokio::test]
    async fn generation_capture_respects_hook_bounds_and_partial_lines() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("t.jsonl");
        tokio::fs::write(&source, b"one\npartial\nfuture\n")
            .await
            .unwrap();
        let (mirror, first) =
            capture_generation(tmp.path(), "s1", source.to_str().unwrap(), Some(7))
                .await
                .unwrap();
        assert_eq!(first, 7);
        assert_eq!(tokio::fs::read(&mirror).await.unwrap(), b"one\npar");
        let (second_mirror, second) =
            capture_generation(tmp.path(), "s1", source.to_str().unwrap(), Some(12))
                .await
                .unwrap();
        assert_eq!(second_mirror, mirror);
        assert_eq!(second, 12);
        assert_eq!(tokio::fs::read(&mirror).await.unwrap(), b"one\npartial\n");
        // A queued older hook is still bounded even after a newer observation.
        let (older_mirror, older) =
            capture_generation(tmp.path(), "s1", source.to_str().unwrap(), Some(4))
                .await
                .unwrap();
        assert_eq!(older_mirror, mirror);
        assert_eq!(older, 4);
        assert_eq!(tokio::fs::metadata(&mirror).await.unwrap().len(), 12);
    }

    #[tokio::test]
    async fn generation_capture_resumes_the_current_generation_and_creates_empty_files() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("t.jsonl");
        tokio::fs::write(&source, b"older\n").await.unwrap();
        capture_generation(tmp.path(), "s1", source.to_str().unwrap(), None)
            .await
            .unwrap();
        tokio::fs::write(&source, b"new\n").await.unwrap();
        let (mirror, _) = capture_generation(tmp.path(), "s1", source.to_str().unwrap(), None)
            .await
            .unwrap();
        tokio::fs::write(&source, b"new\nnext\n").await.unwrap();
        let (resumed, len) = capture_generation(tmp.path(), "s1", source.to_str().unwrap(), None)
            .await
            .unwrap();
        assert_eq!(resumed, mirror);
        assert_eq!(len, 9);
        assert_eq!(tokio::fs::read(resumed).await.unwrap(), b"new\nnext\n");
        tokio::fs::write(&source, b"").await.unwrap();
        let (empty, len) = capture_generation(tmp.path(), "s1", source.to_str().unwrap(), None)
            .await
            .unwrap();
        assert_eq!(len, 0);
        assert!(empty.is_file());
    }
}
