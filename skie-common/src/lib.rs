mod config;
pub use config::*;
use std::{collections::HashMap, fmt::Display, ops::Deref, path::PathBuf};

use blake3::Hash;
use uuid::Uuid;

pub type ChunkIndex = usize;
pub type FileTableIndex = usize;

#[derive(Clone, Copy, Debug, Hash, PartialEq)]
pub struct ChunkID(blake3::Hash);

impl Deref for ChunkID {
    type Target = blake3::Hash;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct FileID(Uuid);

impl Default for FileID {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for FileID {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for FileID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.0.to_string().as_str()))
    }
}

impl FileID {
    pub fn new() -> Self {
        FileID(Uuid::new_v4())
    }
}

pub struct FileTable {
    pub file_ids: Vec<FileID>,
    pub names: Vec<String>,
    pub paths: Vec<PathBuf>,
    pub hashes: Vec<Hash>,
}

pub struct ChunkTable {
    pub hashes: Vec<ChunkID>,
    pub sizes: Vec<u64>,
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
