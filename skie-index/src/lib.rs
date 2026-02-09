mod hash_engine;
pub use hash_engine::{HashEngine, HashEngineError};
use loro::{LoroDoc, LoroMap};
use skie_common::{ChunkIndex, ChunkMetadata, FileID, FileMetadata};
use thiserror::Error;
use std::{
    collections::{BTreeMap, HashMap},
};
use uuid::Uuid;

pub(crate) type Result<E> = std::result::Result<E, HashEngineError>;
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

    pub fn sync_new_chunks(
        &mut self,
        new_tree: BTreeMap<ChunkIndex, ChunkMetadata>,
    ) -> Result<()>  {
        let old_tree = &self.file_metadata.manifest_tree;


        let mp =new_tree.iter().filter(|(index, chunk_data)| {
            match old_tree.get(*index) {
                Some(old_chunk_metadata) => (*chunk_data).eq(old_chunk_metadata),
                None => true
            }
        }).map(|(index, chunk_data)| {
            let bytes = postcard::to_allocvec(chunk_data).map_err(|err| SyncError::Binary(err));
            (index, bytes)
        });

        for (index, bytes) in mp {
            let bytes = bytes?;
        }

        todo!("Handle Chunks which have been added and removed");
    }
}

pub struct SyncEngine {
    hash_engine: HashEngine,
    file_map: HashMap<FileID, FileMetadata>,
}

impl SyncEngine {
    pub fn new() {
        let loro_doc = LoroDoc::new();
    }
}

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("Sync Engine")]
    Binary(#[from] postcard::Error)
}
