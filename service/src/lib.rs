// SPDX-License-Identifier: GPL-3.0-or-later

use camino::Utf8PathBuf;
use std::{time::Instant, iter::Peekable };
use notify_debouncer_full::{notify::event::{EventKind, Event}, DebouncedEvent};
use anyhow::Result;
use camino::Utf8PathBuf;
use notify_debouncer_full::{
    DebouncedEvent,
    notify::{Event, EventKind},
};
use std::{iter::Peekable, sync::Arc, time::Instant, io::Cursor};
use common::FileID;
use store::{DataStore, Persist, chunk_source, ChunkConfig, FileTableEntry, ChunkTableEntry, FileSectionEntry};

pub struct OsEvent {
    pub kind: EventKind,
    pub paths: Vec<Utf8PathBuf>,
    pub time: Instant,
}

pub fn build_events_iter<I: Iterator<Item = DebouncedEvent>>(
    mut events: Peekable<I>,
) -> Vec<OsEvent> {
    let mut os_events = vec![];
    while let Some(debounced_event) = events.next() {
        use EventKind::*;
        let DebouncedEvent { event, time } = debounced_event;
        let Event { kind, paths, .. } = event;

        let paths: Vec<Utf8PathBuf> = paths
            .into_iter()
            .map(|path| Utf8PathBuf::from_path_buf(path).unwrap())
            .collect();

        if let Remove(_) = kind {
            // Check if the NEXT event is a Create for the SAME path
            let is_atomic_update = events.peek().map_or(false, |next_event| {
                matches!(next_event.event.kind, Create(_)) && next_event.event.paths == paths // Paths must match!
            });

            if is_atomic_update {
                // Consume the paired 'Create' event from the iterator
                events.next();

                os_events.push(OsEvent {
                    kind,
                    paths,
                    time,
                });
            } else {
                // It was just a normal delete
                os_events.push(OsEvent {
                    kind,
                    paths,
                    time,
                });
                os_events.push(OsEvent { kind, paths, time });
            } else {
                // It was just a normal delete
                os_events.push(OsEvent { kind, paths, time });
            }
        } else {
            // Not a remove, map directly
            let kind = kind.into();
            os_events.push(OsEvent { kind, paths, time });
        }
    }
    os_events
}

pub struct Reactor {
    store: Arc<DataStore>,
    chunk_config: ChunkConfig,
}

impl Reactor {
    /// Create a new Reactor backed by the given DataStore and chunk configuration.
    pub fn new(store: Arc<DataStore>, chunk_config: ChunkConfig) -> Self {
        Reactor { store, chunk_config }
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
        let metadata = tokio::task::spawn_blocking({
            let p = path.clone();
            move || std::fs::metadata(p.as_std_path())
        })
        .await??;
        if !metadata.is_file() {
            return Ok(());
        }

        // Read file contents
        let data = tokio::task::spawn_blocking({
            let p = path.clone();
            move || std::fs::read(p.as_std_path())
        })
        .await??;

        // Generate a new FileID
        let file_id = FileID::new();

        // Compute overall file hash
        let file_hash = blake3::hash(&data).as_bytes().to_vec();

        // Perform CDC chunking
        let reader = Cursor::new(data);
        let (chunks, sections) =
            chunk_source(&file_id, reader, Some(self.chunk_config))?;

        // Persist deduplicated chunks
        self.store.store_all(chunks).await?;

        // Persist or update file metadata
        let entry = FileTableEntry {
            file_id: file_id.to_string(),
            name: path
                .file_name()
                .map(|n| n.to_string())
                .unwrap_or_default(),
            path: path.to_string(),
            hash: file_hash,
        };
        self.store.store(entry).await?;

        // Persist file sections mapping
        self.store.store_all(sections).await?;
        Ok(())
    }

    /// Handle removal of a file: delete from store.
    async fn handle_remove(&self, path: &Utf8PathBuf) -> Result<()> {
        self.store.delete_file_by_path(&path.to_string()).await?;
        Ok(())
    }
}
