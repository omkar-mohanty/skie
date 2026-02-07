mod config;
use std::{collections::BTreeMap, path::PathBuf};

use blake3::Hash;
pub use config::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type ChunkIndex = usize;
pub type FileID = Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkMetadata {
    pub index: ChunkIndex,
    pub hash: Hash,
    pub offset: u64,
    pub length: usize,
}

impl PartialEq for ChunkMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileMetadata {
    pub file_id: Uuid,
    pub path: PathBuf,
    pub manifest_tree: BTreeMap<ChunkIndex, ChunkMetadata>,
}
