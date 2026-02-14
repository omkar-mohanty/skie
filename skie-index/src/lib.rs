mod hash_engine;
pub use hash_engine::{HashEngineError, get_chunk_hashes};
use skie_common::{ChunkTable, FileTable};
use thiserror::Error;

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
    #[error("Hash Engine")]
    HashEngineError(#[from] HashEngineError),
}
