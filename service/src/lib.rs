// SPDX-License-Identifier: GPL-3.0-or-later

use anyhow::Result;
use camino::{Utf8DirEntry, Utf8PathBuf};
use common::FileID;
use notify_debouncer_full::{
    DebouncedEvent,
    notify::event::{Event, EventKind},
};
use std::{io::Cursor, sync::Arc};
use std::{iter::Peekable, time::Instant};
use store::{ChunkConfig, ChunkedSource, DataStore, FileTableEntry, Persist, chunk_source};

pub struct OsEvent {
    pub kind: EventKind,
    pub paths: Vec<Utf8PathBuf>,
    pub time: Instant,
}

impl From<DebouncedEvent> for OsEvent {
    fn from(value: DebouncedEvent) -> Self {
        let DebouncedEvent { event, time, .. } = value;
        let Event { kind, paths, .. } = event;

        let paths = paths
            .into_iter()
            .map(|path| Utf8PathBuf::from_path_buf(path).unwrap())
            .collect();

        Self { kind, paths, time }
    }
}

pub struct Reactor {
    store: Arc<DataStore>,
    chunk_config: ChunkConfig,
}

impl Reactor {
    /// Create a new Reactor backed by the given DataStore and chunk configuration.
    pub fn new(store: Arc<DataStore>, chunk_config: ChunkConfig) -> Self {
        Reactor {
            store,
            chunk_config,
        }
    }

    /// Process a batch of OS file events.
    pub async fn process_events(&self, events: &[OsEvent]) -> Result<()> {
        for ev in events {
            for path in &ev.paths {
                match ev.kind {
                    EventKind::Create(_) | EventKind::Modify(_) => {
                        self.handle_upsert(path).await?;
                    }
                    EventKind::Remove(_) => {
                        self.handle_remove(path).await?;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Handle creation or modification of a file: read, chunk, and persist.
    async fn handle_upsert(&self, path: &Utf8PathBuf) -> Result<()> {
        // Check file metadata in blocking task
        let metadata = std::fs::metadata(path)?;

        if !metadata.is_file() {
            return Ok(());
        }

        let data = tokio::fs::read(path).await?;

        // Read file contents

        // Generate a new FileID
        let file_id = FileID::new();

        // Perform CDC chunking
        let reader = Cursor::new(data);
        let ChunkedSource {
            chunks,
            file_sections,
            file_hash,
        } = chunk_source(&file_id, reader, Some(self.chunk_config))?;

        // Persist deduplicated chunks
        self.store.store_all(chunks).await?;

        // Persist or update file metadata
        let entry = FileTableEntry {
            file_id: file_id.to_string(),
            name: path.file_name().map(|n| n.to_string()).unwrap_or_default(),
            path: path.to_string(),
            hash: file_hash,
        };
        self.store.store(entry).await?;

        // Persist file sections mapping
        self.store.store_all(file_sections).await?;
        Ok(())
    }

    /// Handle removal of a file: delete from store.
    async fn handle_remove(&self, path: &Utf8PathBuf) -> Result<()> {
        todo!()
    }
}
