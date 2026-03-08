// SPDX-License-Identifier: GPL-3.0-or-later

//! Module for storing and querying file paths in the data store.
//!
//! This module provides structures and implementations to fetch file entries by their
//! paths. It is particularly useful when handling file renames, allowing the data store
//! to resolve path changes and map them to the corresponding file IDs.
use crate::{DataStore, DataStoreError, Fetch, Result};
use async_trait::async_trait;
use camino::Utf8PathBuf;

#[derive(sqlx::FromRow)]
pub struct PathEntry {
    pub path: String,
    pub file_id: String,
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
