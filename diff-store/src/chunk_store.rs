use crate::{DataStore, Fetch, Persist, Result};
use async_trait::async_trait;
use skie_common::ChunkID;

const INSERT_QUERY: &str = "INSERT OR IGNORE INTO chunks (hash, size) VALUES ($1, $2)";

#[derive(sqlx::FromRow)]
pub struct ChunkTableEntry {
    pub hash: Vec<u8>,
    pub size: i64,
}

impl PartialEq for ChunkTableEntry {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

#[async_trait]
impl Persist<ChunkTableEntry> for DataStore {
    async fn store_all(&self, items: Vec<ChunkTableEntry>) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        for item in items {
            // "OR IGNORE" is the secret sauce for deduplication
            sqlx::query(INSERT_QUERY)
                .bind(item.hash)
                .bind(item.size)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn store(&self, item: ChunkTableEntry) -> Result<()> {
        sqlx::query(INSERT_QUERY)
            .bind(item.hash)
            .bind(item.size)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Fetch<ChunkID, ChunkTableEntry> for DataStore {
    async fn fetch_by(&self, key: &ChunkID) -> Result<ChunkTableEntry> {
        let mut results = self.fetch_many(&[*key]).await?;
        results.pop().ok_or(crate::DataStoreError::NotFound)
    }

    async fn fetch_many(&self, keys: &[ChunkID]) -> Result<Vec<ChunkTableEntry>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // 1. Build the dynamic placeholders ($1, $2, ...)
        let placeholders = (1..=keys.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<_>>()
            .join(",");

        let sql = format!(
            "SELECT hash, size FROM chunks WHERE hash IN ({})",
            placeholders
        );

        // 2. Bind the hashes (as Vec<u8> since ChunkID likely wraps or converts to that)
        let mut query = sqlx::query_as::<_, ChunkTableEntry>(&sql);
        for id in keys {
            // Assuming ChunkID can be converted to bytes for the BLOB column
            query = query.bind((*id).as_bytes().to_vec());
        }

        let entries = query.fetch_all(&self.pool).await?;
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::setup;

    #[tokio::test]
    async fn test_chunk_deduplication_logic() {
        let store = setup().await;
        let raw_hash = vec![0xDE, 0xAD, 0xBE, 0xEF];

        let chunk_1 = ChunkTableEntry {
            hash: raw_hash.clone(),
            size: 4096,
        };

        let chunk_2 = ChunkTableEntry {
            hash: raw_hash.clone(),
            size: 4096,
        };
        // Store the same chunk multiple times
        store.store(chunk_1).await.unwrap();
        store.store(chunk_2).await.unwrap();

        // Verify: Only 1 row should exist in the 'chunks' table
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks")
            .fetch_one(&store.pool)
            .await
            .unwrap();

        assert_eq!(count, 1, "3NF violation: Duplicate chunk hash found!");
    }

    #[tokio::test]
    async fn test_batch_fetch_empty_set() {
        let store = setup().await;
        // Test that fetch_many doesn't crash or error on empty input
        let ids = Vec::<ChunkID>::new();
        let results = store.fetch_many(&ids).await.expect("Empty fetch failed");
        assert!(results.is_empty());
    }
}
