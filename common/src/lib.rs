use std::{fmt::Display, ops::Deref};
use uuid::Uuid;

pub type ChunkIndex = usize;
pub type FileTableIndex = usize;

use directories::UserDirs;
use std::path::PathBuf;

const DIFF_SYNC_DIR_NAME: &str = "Diff";

/// Resolve the platform-specific default synchronization directory.
///
/// This function attempts to locate the user's "Documents" folder to create
/// the `Diff` sync directory.
///
/// ### Resolution Strategy:
/// 1. **Documents Folder**: e.g., `C:\Users\Name\Documents\Diff` or `~/Documents/Diff`.
/// 2. **Home Folder**: If Documents is unavailable, falls back to `~/Diff`.
/// 3. **Current Directory**: Absolute fallback if the OS environment is restricted.
///
/// Returns a [`PathBuf`] pointing to the resolved location.
pub fn get_default_sync_path() -> PathBuf {
    if let Some(user_dirs) = UserDirs::new() {
        if let Some(docs) = user_dirs.document_dir() {
            return docs.join(DIFF_SYNC_DIR_NAME);
        }
        return user_dirs.home_dir().join(DIFF_SYNC_DIR_NAME);
    }

    PathBuf::from(DIFF_SYNC_DIR_NAME)
}

/// A type-safe wrapper around a BLAKE3 hash representing a unique data chunk.
///
/// `ChunkID` serves as the primary key for deduplicated storage. Because it
/// is derived from the content (Content-Defined Indexing), identical data
/// blocks will always produce the same `ChunkID`.
#[derive(Clone, Copy, Debug, Hash, PartialEq)]
pub struct ChunkID(blake3::Hash);

impl Deref for ChunkID {
    type Target = blake3::Hash;
    /// Provides transparent access to the underlying [`blake3::Hash`].
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8; 32]> for ChunkID {
    /// Allows treating the ID as a raw 32-byte array for cryptographic operations
    /// or database storage.
    fn as_ref(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

/// A unique identifier for a file tracked by Skie.
///
/// Unlike paths, which can change (renames/moves), the `FileID` remains
/// constant for the lifetime of the file's tracking. This allows the system
/// to follow a file even if its metadata on the filesystem changes.
///
/// Internally uses a **UUID v4** (Randomly generated).
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct FileID(Uuid);

impl Default for FileID {
    /// Creates a new, unique `FileID`.
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for FileID {
    type Target = Uuid;
    /// Provides transparent access to the underlying [`Uuid`].
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
    /// Formats the ID as a standard UUID string (e.g., `550e8400-e29b-41d4-a716-446655440000`).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.0.to_string().as_str()))
    }
}

impl FileID {
    /// Generates a new random UUID v4 and wraps it in a `FileID`.
    pub fn new() -> Self {
        FileID(Uuid::new_v4())
    }
}
