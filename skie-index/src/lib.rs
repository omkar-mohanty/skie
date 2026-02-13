mod hash_engine;
use blake3::Hash;
pub use hash_engine::{HashEngine, HashEngineError};
use loro::{LoroDoc, LoroError};
use skie_common::{ChunkID, ChunkIndex, ChunkMetadata, FileID, FileMetadata, IndexEngineConfig, FileTable, ChunkTable};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ops::Deref,
    path::PathBuf,
};
use thiserror::Error;
use uuid::Uuid;


pub struct ComputeResource {
    pub thread_pool: rayon::ThreadPool,
    pub hasher: blake3::Hasher,
}

pub struct World {
    pub files: FileTable,
    pub chunks: ChunkTable,
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
