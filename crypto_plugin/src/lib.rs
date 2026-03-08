// SPDX-License-Identifier: GPL-3.0-or-later

//! A plugin that encrypts file chunks after they are stored.
//!
//! Demonstrates compile-time registration via `inventory` and uses AES-GCM encryption.

use async_trait::async_trait;
use camino::Utf8PathBuf;
use aes_gcm::{Aes256Gcm, Key};
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use base64::{engine::general_purpose, Engine as _};
use log::info;

use store::{ChunkTableEntry, FileTableEntry, FileSectionEntry};
use diff_d::Plugin;

/// CryptoPlugin holds the AES key.
pub struct CryptoPlugin {
    key: Key<Aes256Gcm>,
}

impl CryptoPlugin {
    /// Creates a new plugin instance with a random key.
    pub fn new() -> Self {
        let key = Aes256Gcm::generate_key(&mut OsRng);
        CryptoPlugin { key }
    }
}

#[async_trait]
impl Plugin for CryptoPlugin {
    /// Encrypt chunk data (metadata-only demo).
    async fn on_chunks_stored(&self, chunks: &[ChunkTableEntry]) {
        let cipher = Aes256Gcm::new(&self.key);
        for chunk in chunks {
            // NOTE: Actual chunk data is not passed here;
            // you'd retrieve it by hash from your store or disk,
            // then encrypt. Here we log the intent.
            info!("CryptoPlugin: would encrypt chunk {} ({} bytes)",
                  general_purpose::STANDARD.encode(&chunk.hash), chunk.size);
        }
    }
}

// Register our plugin at compile time
inventory::submit! {
    Box::new(CryptoPlugin::new()) as Box<dyn Plugin>
}