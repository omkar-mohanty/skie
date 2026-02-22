use directories::{ProjectDirs, UserDirs};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const QUALIFIER: &str = "com";
const ORGANIZATION: &str = "ultrafinite";
const APPLICATION: &str = "skie";

#[cfg(not(test))]
const SERVER_URL: &str = "https://api.skie.ultrafinite.com";
#[cfg(test)]
const SERVER_URL: &str = "http://[::1]:8080";
const SKIE_SYNC_DIR_NAME: &str = "Skie";
const SKIE_VAULE_DIR_NAME: &str = "Vault";

/// Debounce time in ms
pub const DEBOUNCE_TIME_IN_MS: u64 = 500;

/// The minimum size of a chunk (2 KB).
/// FastCDC will skip hashing for this many bytes to save CPU.
pub const CHUNK_MIN_SIZE: u32 = 2 * 1024;

/// The target average size of a chunk (8 KB).
/// This is the "Sweet Spot" for balancing metadata overhead and dedupe ratio.
pub const CHUNK_AVG_SIZE: u32 = 8 * 1024;

/// The maximum size of a chunk (32 KB).
/// Prevents chunks from becoming too large when no cut-point is found.
pub const CHUNK_MAX_SIZE: u32 = 32 * 1024;

/// The max number of threads to be used by the index engine.
/// This number should be lower in Handheld devices and laptops.
/// For Desktops this can be the number of CPU's
pub const INDEX_ENGINE_MAX_THREAD: usize = 8;

/// The default size for crossbeam sender
pub const CROSSBEAM_CHANNEL_SIZE: usize = 64;

/// The protocol version for the chunking logic.
/// If you ever change the constants above, you MUST increment this
/// to trigger a re-index of the client's local files.
pub const SKIE_CHUNK_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub server_url: String,
    pub max_upload_kbps: u32,
}

#[cfg(test)]
impl Default for NetworkConfig {
    fn default() -> Self {
        NetworkConfig {
            server_url: SERVER_URL.into(),
            max_upload_kbps: 0,
        }
    }
}

#[cfg(not(test))]
impl Default for NetworkConfig {
    fn default() -> Self {
        NetworkConfig {
            server_url: SERVER_URL.into(),
            max_upload_kbps: 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrivacyConfig {
    pub vault: bool,
    pub vault_folder: PathBuf,
}

impl Default for PrivacyConfig {
    fn default() -> Self {
        PrivacyConfig {
            vault: true,
            vault_folder: get_default_sync_path().join(SKIE_VAULE_DIR_NAME),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashConfig {
    pub min_chunk_size: u32,
    pub avg_chunk_size: u32,
    pub max_chunk_size: u32,
    pub num_threads: usize,
    pub channel_size: usize,
    pub engine_config: PathBuf,
}

impl Default for HashConfig {
    fn default() -> Self {
        let project_dirs = ProjectDirs::from(QUALIFIER, ORGANIZATION, APPLICATION).unwrap();
        let engine_config = project_dirs.data_dir().to_path_buf();
        HashConfig {
            min_chunk_size: CHUNK_MIN_SIZE,
            avg_chunk_size: CHUNK_AVG_SIZE,
            max_chunk_size: CHUNK_MAX_SIZE,
            num_threads: INDEX_ENGINE_MAX_THREAD,
            channel_size: CROSSBEAM_CHANNEL_SIZE,
            engine_config,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub sync_dir: PathBuf,
    pub engine_config: HashConfig,
    pub network_config: NetworkConfig,
    pub privacy_config: PrivacyConfig,
    pub debounce_ms: u64,
}

impl Default for AppConfig {
    fn default() -> Self {
        let sync_dir = get_default_sync_path();
        AppConfig {
            sync_dir,
            engine_config: HashConfig::default(),
            network_config: NetworkConfig::default(),
            privacy_config: PrivacyConfig::default(),
            debounce_ms: DEBOUNCE_TIME_IN_MS,
        }
    }
}

#[derive(Default, Debug)]
pub struct SkieContext {
    pub app_config: AppConfig,
}

impl SkieContext {
    pub fn new(app_config: AppConfig) -> SkieContext {
        SkieContext { app_config }
    }
}

pub fn get_default_sync_path() -> PathBuf {
    if let Some(user_dirs) = UserDirs::new() {
        // Try to get Documents first
        if let Some(docs) = user_dirs.document_dir() {
            return docs.join(SKIE_SYNC_DIR_NAME);
        }
        return user_dirs.home_dir().join(SKIE_SYNC_DIR_NAME);
    }

    // Absolute final fallback: current directory
    PathBuf::from(SKIE_SYNC_DIR_NAME)
}
