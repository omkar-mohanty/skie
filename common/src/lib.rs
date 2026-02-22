use std::{fmt::Display, ops::Deref};
use uuid::Uuid;

pub type ChunkIndex = usize;
pub type FileTableIndex = usize;

use directories::UserDirs;
use std::path::PathBuf;

const DIFF_SYNC_DIR_NAME: &str = "Diff";

pub fn get_default_sync_path() -> PathBuf {
    if let Some(user_dirs) = UserDirs::new() {
        // Try to get Documents first
        if let Some(docs) = user_dirs.document_dir() {
            return docs.join(DIFF_SYNC_DIR_NAME);
        }
        return user_dirs.home_dir().join(DIFF_SYNC_DIR_NAME);
    }

    // Absolute final fallback: current directory
    PathBuf::from(DIFF_SYNC_DIR_NAME)
}

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
