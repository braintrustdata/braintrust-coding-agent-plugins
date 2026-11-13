//! Discovery and removal of per-session databases.
use super::{events::EventCollection, span_ops::SpanOps, store};
use std::path::{Path, PathBuf};

/// One session database found on disk. Inspecting it opens the shared store.
pub(crate) struct SessionStorage {
    pub path: PathBuf,
}

impl SessionStorage {
    pub async fn events(&self) -> anyhow::Result<EventCollection> {
        EventCollection::open_path(&self.path).await
    }

    pub async fn span_ops(&self) -> anyhow::Result<SpanOps> {
        SpanOps::open(&self.path).await
    }

    /// Release the space of rows that have been deleted.
    pub async fn compact(&self) -> anyhow::Result<()> {
        store::Store::open(&self.path).await?.compact().await
    }

    /// Time since the database file last changed, for storage with no owner.
    pub async fn modified_ago(&self) -> Option<std::time::Duration> {
        let modified = tokio::fs::metadata(&self.path)
            .await
            .ok()?
            .modified()
            .ok()?;
        std::time::SystemTime::now().duration_since(modified).ok()
    }
}

/// Every session database in the data directory, after importing legacy journals.
pub(crate) async fn sessions(data_dir: &Path) -> anyhow::Result<Vec<SessionStorage>> {
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
                    sessions.push(SessionStorage { path });
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    sessions.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(sessions)
}

/// Delete a session database if nothing in this process has it open.
pub(crate) async fn remove_session(path: &Path) -> anyhow::Result<bool> {
    store::remove(path).await
}
