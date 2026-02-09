mod hash_engine;
pub use hash_engine::{HashEngine, HashEngineError};
use loro::{LoroDoc, LoroError, LoroMap};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use skie_common::{ChunkIndex, ChunkMetadata, FileID, FileMetadata, IndexEngineConfig};
use std::{
    collections::{BTreeMap, HashMap}, path::PathBuf
};
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
    _device_id: DeviceID,
    _engine_config: IndexEngineConfig,
    _hash_engine: HashEngine,
    file_map: HashMap<FileID, SyncFile>,
}

impl SyncEngine {
    pub fn sync_files(&mut self, file_ids: &[FileID]) -> Result<Option<(FileID, Vec<u8>)>> {
        file_ids.par_iter().for_each(|file_id| {
            let _file = &mut self.file_map.get(file_id);
        });
        todo!()
    }
}

#[derive(Default)]
pub struct SyncEngineBuilder {
    engine_config: Option<IndexEngineConfig>,
    sync_dir: Option<PathBuf>,
}

impl SyncEngineBuilder {
    pub fn new() -> Self {
        Self {
            engine_config: None,
            sync_dir: None,
        }
    }

    pub fn build(self) -> Result<SyncEngine> {
        if self.engine_config.is_none() {
            return Err(SyncError::BuildError("engine config not provided".to_string()))
        }

        if self.sync_dir.is_none() {
            return Err(SyncError::BuildError("No sync directory given".to_string()))
        }

        let sync_dir = self.sync_dir.unwrap();
        let engine_config = self.engine_config.unwrap();

        if !sync_dir.is_dir() {
            return Err(SyncError::BuildError("Given path is not a directory".to_string()));
        }

        let _hash_engine = HashEngine::new(engine_config.clone())?;
        let _device_id = DeviceID::new_v4();

        todo!("Implement the sync engine builder")

    }
}

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("Error while building Sync Engine")]
    BuildError(String),
    #[error("Consistency Error")]
    ConsistencyError(#[from] LoroError),
    #[error("Hash Engine")]
    HashEngineError(#[from] HashEngineError),
    #[error("Sync Engine")]
    Binary(#[from] postcard::Error),
}
