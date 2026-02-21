use crate::{DataStore, DataStoreError, Fetch, Persist, Result};
use async_trait::async_trait;
use skie_common::FileID;
use sqlx::prelude::FromRow;

#[derive(FromRow)]
pub struct FileSectionEntry {
    pub file_id: String,
    pub chunk_hash: Vec<u8>,
    pub length: i64,
    pub offset: i64,
}

#[async_trait]
impl Persist<FileSectionEntry> for DataStore {
    async fn store(&self, entry: FileSectionEntry) -> Result<()> {
        // The conflict target is the PRIMARY KEY: (file_id, length)
        sqlx::query(
            r#"
            INSERT INTO file_sections (file_id, chunk_hash, length, offset)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT(file_id, offset) DO UPDATE SET
                chunk_hash = excluded.chunk_hash,
                length = excluded.length
            "#,
        )
        .bind(entry.file_id)
        .bind(entry.chunk_hash)
        .bind(entry.length)
        .bind(entry.offset)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn store_all(&self, entries: Vec<FileSectionEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for entry in entries {
            sqlx::query(
                r#"
                INSERT INTO file_sections (file_id, chunk_hash, length, offset) 
                VALUES ($1, $2, $3, $4)
                ON CONFLICT(file_id, offset) DO UPDATE SET 
                    chunk_hash = excluded.chunk_hash,
                    length = excluded.length
                "#,
            )
            .bind(entry.file_id)
            .bind(entry.chunk_hash)
            .bind(entry.length)
            .bind(entry.offset)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl Fetch<FileID, Vec<FileSectionEntry>> for DataStore {
    /// Returns ALL sections for a single file, sorted by index for reconstruction.
    async fn fetch_by(&self, file_id: &FileID) -> Result<Vec<FileSectionEntry>> {
        let entries = sqlx::query_as::<_, FileSectionEntry>(
            r#"
            SELECT file_id, chunk_hash, length, offset 
            FROM file_sections 
            WHERE file_id = $1 
            ORDER BY offset ASC
            "#,
        )
        .bind(file_id.to_string())
        .fetch_all(&self.pool)
        .await?;

        if entries.is_empty() {
            return Err(DataStoreError::NotFound);
        }

        Ok(entries)
    }

    /// Returns a Vec of Vecs. Each inner Vec represents one complete file's sections.
    async fn fetch_many(&self, file_ids: &[FileID]) -> Result<Vec<Vec<FileSectionEntry>>> {
        if file_ids.is_empty() {
            return Ok(vec![]);
        }

        let placeholders = (1..=file_ids.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<_>>()
            .join(",");

        // We MUST sort by file_id first so we can group them easily in one pass
        let sql = format!(
            "SELECT * FROM file_sections WHERE file_id IN ({}) ORDER BY file_id, offset ASC",
            placeholders
        );

        let mut query = sqlx::query_as::<_, FileSectionEntry>(&sql);
        for id in file_ids {
            query = query.bind(id.to_string());
        }

        let flat_entries = query.fetch_all(&self.pool).await?;

        // Grouping Logic: Converting the flat list into Vec<Vec<...>>
        let mut grouped: Vec<Vec<FileSectionEntry>> = Vec::new();
        let mut current_file_vec: Vec<FileSectionEntry> = Vec::new();
        let mut last_id: Option<String> = None;

        for entry in flat_entries {
            if let Some(ref id) = last_id
                && id != &entry.file_id
            {
                // New file detected: push the old one and start a new buffer
                grouped.push(std::mem::take(&mut current_file_vec));
            }

            last_id = Some(entry.file_id.clone());
            current_file_vec.push(entry);
        }

        // Don't forget to push the final group
        if !current_file_vec.is_empty() {
            grouped.push(current_file_vec);
        }

        Ok(grouped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChunkTableEntry, FileSectionEntry, FileTableEntry, setup};
    use skie_common::FileID;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_store() -> Result<()> {
        let store = setup().await;
        let file_id = FileID::new();
        let file_id_str = file_id.to_string();
        let hash = vec![0x16];

        // 1. PREPARE PARENTS (Fixes the Foreign Key Error)
        // Insert the file
        store
            .store(FileTableEntry {
                file_id: file_id_str.clone(),
                name: "test.bin".into(),
                path: "/tmp/test.bin".into(),
                hash: vec![0x99],
            })
            .await?;

        // Insert the chunk
        store
            .store(ChunkTableEntry {
                hash: hash.clone(),
                size: 100,
            })
            .await?;

        // 2. NOW TEST SECTIONS
        let file_sections = vec![
            FileSectionEntry {
                file_id: file_id_str.clone(),
                length: 0,
                chunk_hash: hash.clone(),
                offset: 0,
            },
            FileSectionEntry {
                file_id: file_id_str.clone(),
                length: 1,
                chunk_hash: hash.clone(),
                offset: 100,
            },
        ];

        store.store_all(file_sections).await?;

        // Using fetch_many as you did in your test
        let fetched_sections: Vec<FileSectionEntry> = store.fetch_by(&file_id).await?;

        assert_eq!(fetched_sections.len(), 2);
        assert_eq!(fetched_sections[0].length, 0);
        assert_eq!(fetched_sections[1].length, 1);
        Ok(())
    }

    // Helper to satisfy Foreign Key constraints
    async fn seed_db(store: &DataStore, tempfile: &NamedTempFile, fid: &str, hashes: &[Vec<u8>]) {
        store
            .store(FileTableEntry {
                file_id: fid.to_string(),
                name: tempfile.path().to_string_lossy().to_ascii_lowercase(),
                path: tempfile.path().to_string_lossy().to_ascii_lowercase(),
                hash: vec![0x99],
            })
            .await
            .unwrap();

        for h in hashes {
            store
                .store(ChunkTableEntry {
                    hash: h.clone(),
                    size: 1024,
                })
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_section_update_upsert() -> Result<()> {
        let named_temp_file = NamedTempFile::new().unwrap();
        let store = setup().await;
        let fid = FileID::new();
        let hash_v1 = vec![0x11, 0x11];
        let hash_v2 = vec![0x22, 0x22];

        seed_db(
            &store,
            &named_temp_file,
            &fid.to_string(),
            &[hash_v1.clone(), hash_v2.clone()],
        )
        .await;

        // 1. Initial State: Index 0 points to Hash V1
        let initial = FileSectionEntry {
            file_id: fid.to_string(),
            length: 0,
            chunk_hash: hash_v1.clone(),
            offset: 0,
        };
        store.store(initial).await?;

        // 2. Update: Same FileID and Index, but new Hash (V2)
        let update = FileSectionEntry {
            file_id: fid.to_string(),
            length: 0,
            chunk_hash: hash_v2.clone(),
            offset: 0,
        };
        store.store(update).await?;

        // 3. Verify: We should still only have 1 row, and it should have Hash V2
        let results: Vec<FileSectionEntry> = store.fetch_by(&fid).await?;
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].chunk_hash, hash_v2,
            "The chunk hash was not updated (UPSERT failed)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_many_isolation_and_grouping() -> Result<()> {
        let named_temp_file_a = NamedTempFile::new().unwrap();
        let named_temp_file_b = NamedTempFile::new().unwrap();
        let store = setup().await;
        let fid_a = FileID::new();
        let fid_b = FileID::new();
        let hash = vec![0xCC];

        seed_db(
            &store,
            &named_temp_file_a,
            &fid_a.to_string(),
            &[hash.clone()],
        )
        .await;
        seed_db(
            &store,
            &named_temp_file_b,
            &fid_b.to_string(),
            &[hash.clone()],
        )
        .await;

        // Store 2 chunks for File A and 1 chunk for File B
        let sections = vec![
            FileSectionEntry {
                file_id: fid_a.to_string(),
                length: 0,
                chunk_hash: hash.clone(),
                offset: 0,
            },
            FileSectionEntry {
                file_id: fid_a.to_string(),
                length: 1,
                chunk_hash: hash.clone(),
                offset: 100,
            },
            FileSectionEntry {
                file_id: fid_b.to_string(),
                length: 0,
                chunk_hash: hash.clone(),
                offset: 0,
            },
        ];
        store.store_all(sections).await?;

        // Fetch both
        let results: Vec<Vec<_>> = store.fetch_many(&[fid_a.clone(), fid_b.clone()]).await?;

        // Verify structure: Vec<Vec<FileSectionEntry>>
        assert_eq!(
            results.len(),
            2,
            "Should have 2 separate inner vectors (one per file)"
        );

        // Find the inner vec for File A (since SQL order might vary, we check the ID)
        let file_a_results = results
            .iter()
            .find(|v| v[0].file_id == fid_a.to_string())
            .unwrap();
        let file_b_results = results
            .iter()
            .find(|v| v[0].file_id == fid_b.to_string())
            .unwrap();

        assert_eq!(
            file_a_results.len(),
            2,
            "File A should have exactly 2 chunks"
        );
        assert_eq!(
            file_b_results.len(),
            1,
            "File B should have exactly 1 chunk"
        );

        Ok(())
    }
}
