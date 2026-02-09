mod hash_engine;
pub use hash_engine::{HashEngine, HashEngineError};
use loro::{LoroDoc, LoroError, LoroMap};
use skie_common::{ChunkIndex, ChunkMetadata, FileID, FileMetadata};
use std::collections::{BTreeMap, HashMap};
use thiserror::Error;
use uuid::Uuid;

type Result<E> = std::result::Result<E, SyncError>;
pub(crate) type DeviceID = Uuid;

pub struct SyncFile {
    pub file_metadata: FileMetadata,
    pub doc: LoroDoc,
    pub changes: LoroMap,
}

impl SyncFile {
    pub fn new(id: DeviceID, file_metadata: FileMetadata) -> Self {
        let doc = LoroDoc::new();
        let changes = doc.get_map(id.to_string());
        Self {
            file_metadata,
            doc,
            changes,
        }
    }

    pub fn sync_new_chunks(&mut self, new_tree: BTreeMap<ChunkIndex, ChunkMetadata>) -> Result<()> {
        let old_tree = &self.file_metadata.manifest_tree;

        for index in old_tree.keys() {
            if !new_tree.contains_key(index) {
                self.changes.delete(&index.to_string())?;
            }
        }

        let new_chunks = new_tree
            .iter()
            .filter(|(index, chunk_data)| match old_tree.get(*index) {
                Some(old_chunk_metadata) => !(*chunk_data).eq(old_chunk_metadata),
                None => true,
            })
            .map(|(index, chunk_data)| {
                let bytes = postcard::to_allocvec(chunk_data).map_err(SyncError::Binary);
                (index, bytes)
            });

        for (index, bytes) in new_chunks {
            let bytes = bytes?;
            self.changes.insert(&index.to_string(), bytes)?;
        }

        Ok(())
    }
}

pub struct SyncEngine {
    hash_engine: HashEngine,
    file_map: HashMap<FileID, FileMetadata>,
}

impl SyncEngine {
    pub fn new() {}
}

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("Consistency Error")]
    ConsistencyError(#[from] LoroError),
    #[error("Hash Engine")]
    HashEngineError(#[from] HashEngineError),
    #[error("Sync Engine")]
    Binary(#[from] postcard::Error),
}
