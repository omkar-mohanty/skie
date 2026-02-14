mod config;
pub use config::*;
use std::{collections::HashMap, ops::Deref, path::PathBuf};

use blake3::Hash;
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
pub struct ChunkTableEntry {
    pub hash: Hash,
    pub index: ChunkIndex,
    pub file_id: String,
    pub size: u64,
    pub offset: u64,
}

impl PartialEq for ChunkTableEntry {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
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
