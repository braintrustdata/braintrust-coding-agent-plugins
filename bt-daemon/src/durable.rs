//! Atomic, private control-state commits used by the recovery pipeline.
use serde::Serialize;
use std::path::Path;
use tokio::io::AsyncWriteExt;

pub(crate) async fn create_private(path: &Path) -> anyhow::Result<tokio::fs::File> {
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    Ok(options.open(path).await?)
}

pub(crate) fn sync_parent(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    std::fs::File::open(path.parent().expect("state directory"))?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

pub(crate) async fn write_json(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    let temp = path
        .parent()
        .expect("state directory")
        .join(format!("{}.tmp", uuid::Uuid::new_v4()));
    let result = async {
        let mut file = create_private(&temp).await?;
        file.write_all(&serde_json::to_vec(value)?).await?;
        file.flush().await?;
        file.sync_data().await?;
        crate::paths::restrict_file_to_owner(&temp)?;
        tokio::fs::rename(&temp, path).await?;
        sync_parent(path)?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(temp).await;
    }
    result
}
