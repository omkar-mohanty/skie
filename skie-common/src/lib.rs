mod config;
use std::{
    collections::{BTreeMap, HashMap},
    ops::Deref,
    path::PathBuf,
};

use blake3::Hash;
pub use config::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type ChunkIndex = usize;
pub type FileTableIndex = usize;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct FileID(Uuid);

impl Deref for FileID {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FileID {
    pub fn new() -> Self {
        FileID(Uuid::new_v4())
    }
}

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
    pub file_hash: Hash,
    pub file_id: Uuid,
    pub path: PathBuf,
    pub manifest_tree: BTreeMap<ChunkIndex, ChunkMetadata>,
}

pub struct FileTable {
    pub file_ids: Vec<FileID>,
    pub names: Vec<String>,
    pub paths: Vec<PathBuf>,
    pub hashes: Vec<Hash>,
}

pub struct ChunkTable {
    pub hashes: Vec<Hash>,
    pub indices: Vec<u64>,
    pub file_ids: Vec<FileID>,
    pub sizes: Vec<u64>,
    pub offsets: Vec<u64>,
}

pub struct FileIndex(HashMap<FileID, FileTableIndex>);

impl Deref for FileIndex {
    type Target = HashMap<FileID, FileTableIndex>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct FileHashIndex(HashMap<Hash, FileID>);

impl Deref for FileHashIndex {
    type Target = HashMap<Hash, FileID>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
