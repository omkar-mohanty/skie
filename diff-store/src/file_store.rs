use async_trait::async_trait;
use skie_common::FileID;

use crate::{DataStore, DataStoreError, Fetch, Persist, Result};
const UPSERT_QUERY: &str = r#"
    INSERT INTO files (file_id, name, path, hash)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT(file_id) DO UPDATE SET
        name = excluded.name,
        path = excluded.path,
        hash = excluded.hash
"#;

#[derive(sqlx::FromRow)]
pub struct FileTableEntry {
    pub file_id: String,
    pub name: String,
    pub path: String,
    pub hash: Vec<u8>,
}

#[async_trait]
impl Persist<FileTableEntry> for DataStore {
    /// Implementation Detail: Uses an UPSERT (ON CONFLICT) strategy.
    /// This ensures that if a file is moved or renamed, we update the existing
    /// metadata rather than creating duplicate entries for the same file_id.
    async fn store_all(&self, items: Vec<FileTableEntry>) -> Result<()> {
        // Start a transaction. If any insert fails, the whole thing rolls back.
        let mut transaction = self.pool.begin().await?;

        // Loop through our DOD arrays.
        for entry in items {
            sqlx::query(UPSERT_QUERY)
                .bind(entry.file_id)
                .bind(&entry.name)
                .bind(entry.path)
                .bind(entry.hash)
                .execute(&mut *transaction)
                .await?;
        }

        // Commit everything to disk
        transaction.commit().await?;
        Ok(())
    }

    async fn store(&self, item: FileTableEntry) -> Result<()> {
        // Start a transaction. If any insert fails, the whole thing rolls back.
        sqlx::query(UPSERT_QUERY)
            .bind(item.file_id)
            .bind(&item.name)
            .bind(item.path)
            .bind(item.hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Fetch<FileID, FileTableEntry> for DataStore {
    async fn fetch_by(&self, key: &FileID) -> Result<FileTableEntry> {
        // Reuse fetch_many logic for a single key
        let mut results = self.fetch_many(&[*key]).await?;

        // Take the first item out of the vector or return NotFound
        results.pop().ok_or(DataStoreError::NotFound)
    }

    async fn fetch_many(&self, keys: &[FileID]) -> Result<Vec<FileTableEntry>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // 1. Create the string: "SELECT * FROM files WHERE file_id IN ($1, $2, $3)"
        let placeholders = (1..=keys.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<_>>()
            .join(",");

        let sql = format!("SELECT * FROM files WHERE file_id IN ({})", placeholders);

        // 2. Bind and execute
        let mut query = sqlx::query_as::<_, FileTableEntry>(&sql);
        for id in keys {
            let id = (*id).to_string();
            query = query.bind(id);
        }

        let entries = query.fetch_all(&self.pool).await?;

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup;
    use skie_common::FileID;
    #[tokio::test]
    async fn test_file_metadata_lifecycle() {
        let store = setup().await;
        let id = FileID::new();

        let entry = FileTableEntry {
            file_id: id.to_string(),
            name: "init.txt".into(),
            path: "/a/init.txt".into(),
            hash: vec![0xCC],
        };

        // Test: Persist
        store.store(entry).await.expect("Store failed");

        // Test: Fetch
        let fetched: FileTableEntry = store.fetch_by(&id).await.expect("Fetch failed");
        assert_eq!(fetched.name, "init.txt");

        // Test: Update (Upsert)
        let updated = FileTableEntry {
            name: "moved.txt".into(),
            path: "/b/moved.txt".into(),
            file_id: id.to_string(),
            hash: vec![0xCC],
        };
        store.store(updated).await.expect("Update failed");

        let fetched_updated: FileTableEntry = store.fetch_by(&id).await.unwrap();
        assert_eq!(fetched_updated.name, "moved.txt");
    }
}
