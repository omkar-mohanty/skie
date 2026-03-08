// SPDX-License-Identifier: GPL-3.0-or-later

//! Module for storing and querying file paths in the data store.
//!
//! This module provides structures and implementations to fetch file entries by their
//! paths. It is particularly useful when handling file renames, allowing the data store
//! to resolve path changes and map them to the corresponding file IDs.
use crate::{DataStore, DataStoreError, Fetch, Persist, Result};
use async_trait::async_trait;
use camino::Utf8PathBuf;

#[derive(sqlx::FromRow, Debug)]
pub struct PathEntry {
    pub path: String,
    pub file_id: String,
}

#[async_trait]
impl Persist<PathEntry> for DataStore {
    async fn store(&self, item: PathEntry) -> Result<()> {
        // Update the path for the given file ID
        let rows = sqlx::query("UPDATE files SET path = $1 WHERE file_id = $2")
            .bind(item.path)
            .bind(item.file_id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if rows == 0 {
            return Err(DataStoreError::NotFound);
        }
        Ok(())
    }

    async fn store_all(&self, items: Vec<PathEntry>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;
        for item in items {
            let rows = sqlx::query("UPDATE files SET path = $1 WHERE file_id = $2")
                .bind(item.path)
                .bind(item.file_id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            if rows == 0 {
                return Err(DataStoreError::NotFound);
            }
        }
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl Fetch<Utf8PathBuf, PathEntry> for DataStore {
    async fn fetch_by(&self, key: &Utf8PathBuf) -> Result<PathEntry> {
        // Delegate to fetch_many for a single key
        let mut results = self.fetch_many(&[key.clone()]).await?;
        // Return the single entry or NotFound error
        results.pop().ok_or(DataStoreError::NotFound)
    }

    async fn fetch_many(&self, key: &[Utf8PathBuf]) -> Result<Vec<PathEntry>> {
        // Return early on empty input
        if key.is_empty() {
            return Ok(Vec::new());
        }
        // Build dynamic placeholders ($1, $2, ...)
        let placeholders = (1..=key.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<_>>()
            .join(",");
        // Select matching paths and their file IDs
        let sql = format!(
            "SELECT path, file_id FROM files WHERE path IN ({})",
            placeholders
        );
        let mut query = sqlx::query_as::<_, PathEntry>(&sql);
        // Bind each path parameter
        for p in key {
            query = query.bind(p.to_string());
        }
        // Execute and return all matching entries
        let entries = query.fetch_all(&self.pool).await?;
        Ok(entries)
    }
}

// Unit tests for Persist<PathEntry> and Fetch<Utf8PathBuf, PathEntry>
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileTableEntry, setup};
    use camino::Utf8PathBuf;
    use common::FileID;

    #[tokio::test]
    async fn test_store_and_fetch_path_entry() -> Result<()> {
        // Setup store and a file entry
        let store = setup().await;
        let file_id = FileID::new();
        let fid = file_id.to_string();
        // Insert initial file record
        store
            .store(FileTableEntry {
                file_id: fid.clone(),
                name: "test.txt".into(),
                path: "/old/path.txt".into(),
                hash: vec![0xAA],
            })
            .await?;

        // Update only the path via PathEntry
        let new_path = "/new/path.txt".to_string();
        store
            .store(PathEntry {
                path: new_path.clone(),
                file_id: fid.clone(),
            })
            .await?;

        // Fetch single by path
        let entry = store.fetch_by(&Utf8PathBuf::from(new_path.clone())).await?;
        assert_eq!(entry.file_id, fid);
        assert_eq!(entry.path, new_path);

        Ok(())
    }

    #[tokio::test]
    async fn test_store_all_and_fetch_many() -> Result<()> {
        let store = setup().await;
        let file1 = FileID::new();
        let file2 = FileID::new();
        let f1 = file1.to_string();
        let f2 = file2.to_string();
        // Insert initial file records
        store
            .store(FileTableEntry {
                file_id: f1.clone(),
                name: "a".into(),
                path: "/p1".into(),
                hash: vec![1],
            })
            .await?;
        store
            .store(FileTableEntry {
                file_id: f2.clone(),
                name: "b".into(),
                path: "/p2".into(),
                hash: vec![2],
            })
            .await?;

        // Prepare batch path updates
        let entries = vec![
            PathEntry {
                path: "/p1_new".into(),
                file_id: f1.clone(),
            },
            PathEntry {
                path: "/p2_new".into(),
                file_id: f2.clone(),
            },
        ];

        // Fetch many by their new paths
        let paths = entries
            .iter()
            .map(|e| Utf8PathBuf::from(e.path.clone()))
            .collect::<Vec<_>>();
        store.store_all(entries).await?;

        let results = store.fetch_many(&paths).await?;
        assert_eq!(results.len(), 2);
        // Ensure returned file_ids match and paths match
        let mut got: Vec<(&String, &String)> =
            results.iter().map(|e| (&e.file_id, &e.path)).collect();
        got.sort();
        let p1_key = "/p1_new".to_string();
        let p2_key = "/p2_new".to_string();
        let mut want = vec![(&f1, &p1_key), (&f2, &p2_key)];
        want.sort();
        assert_eq!(got, want);

        // Empty operations
        store.store_all(Vec::<FileTableEntry>::new()).await?;
        let empty: Vec<FileTableEntry> = store.fetch_many(&[]).await?;
        assert!(empty.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_not_found() -> Result<()> {
        let store = setup().await;
        // Fetch non-existent path should error
        let err = store
            .fetch_by(&Utf8PathBuf::from("/no/such"))
            .await
            .unwrap_err();
        assert!(matches!(err, DataStoreError::NotFound));
        Ok(())
    }
}
