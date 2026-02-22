mod config;
pub use config::*;
use std::{fmt::Display, ops::Deref};
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

impl AsRef<[u8; 32]> for ChunkID {
    fn as_ref(&self) -> &[u8; 32] {
        self.0.as_bytes()
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

impl AsRef<Uuid> for FileID {
    fn as_ref(&self) -> &Uuid {
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
