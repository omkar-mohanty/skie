mod hash_engine;
use std::collections::{BTreeMap, HashSet};

use blake3::Hash;
pub use hash_engine::{HashEngine, HashEngineError};
use loro::{LoroDoc, LoroError};
use skie_common::{ChunkIndex, ChunkMetadata, FileID, FileMetadata, IndexEngineConfig};
use thiserror::Error;
use uuid::Uuid;

type HashIndex = usize;

struct FileEntry {
    hash_indices: HashSet<HashIndex>,
    file_hash_index: HashIndex,
}

struct World {
    chunk_hashes: Vec<Hash>,
    file_hashes: Vec<Hash>,
    files: BTreeMap<FileID, FileEntry>,
}

pub struct ComputeResource {
    pub thread_pool: rayon::ThreadPool,
    pub hasher: blake3::Hasher,
}

type Result<E> = std::result::Result<E, SyncError>;
pub(crate) type DeviceID = Uuid;

pub struct SyncFile {
    pub file_metadata: FileMetadata,
    pub doc: LoroDoc,
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
