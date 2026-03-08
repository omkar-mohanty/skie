// SPDX-License-Identifier: GPL-3.0-or-later

//! Plugin extension point for the Reactor/service layer.
//! Plugins are collected at compile time via the `inventory` crate.

use async_trait::async_trait;
use camino::Utf8PathBuf;
use store::{ChunkTableEntry, FileTableEntry, FileSectionEntry};

/// Hooks for custom logic on Reactor events.
#[async_trait]
pub trait Plugin: Sync + Send + 'static {
    /// Called after a file metadata upsert (create or rename).
    async fn on_file_upsert(&self, _entry: &FileTableEntry) {}

    /// Called after a file removal by path.
    async fn on_file_remove(&self, _path: &Utf8PathBuf) {}

    /// Called after deduplicated chunks have been stored.
    async fn on_chunks_stored(&self, _chunks: &[ChunkTableEntry]) {}

    /// Called after file sections (mappings) have been stored.
    async fn on_sections_stored(&self, _sections: &[FileSectionEntry]) {}
}

/// Collect all registered plugins.
inventory::collect!(Box<dyn Plugin>);