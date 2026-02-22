mod chunk_store;
mod file_section;
mod file_store;
pub use chunk_store::*;
pub use file_section::*;
pub use file_store::*;

use async_trait::async_trait;
use common::FileID;
use fastcdc::v2020::StreamCDC;
use sqlx::{AnyPool, migrate::MigrateError};
use std::io::Read;
use thiserror::Error;

/// A Result type specialized for DataStore operations.
pub(crate) type Result<T> = std::result::Result<T, DataStoreError>;

/// Configuration parameters for the Content-Defined Chunking (CDC) algorithm.
///
/// These values determine the granularity of the deduplication. Smaller chunks
/// provide better deduplication ratios but increase database metadata overhead.
#[derive(Clone, Copy, Debug)]
pub struct ChunkConfig {
    /// The minimum size of a chunk in bytes.
    pub min_chunk_size: u32,
    /// The desired average size of a chunk in bytes.
    /// Note: `fastcdc` usually requires this to be at least 1024.
    pub avg_chunk_size: u32,
    /// The maximum size a chunk can reach before being forced to cut.
    pub max_chunk_size: u32,
}

impl Default for ChunkConfig {
    /// Returns the recommended default settings for general-purpose file sync.
    /// (512B min, 1KB avg, 2KB max)
    fn default() -> Self {
        Self {
            min_chunk_size: 512,
            avg_chunk_size: 1024,
            max_chunk_size: 2048,
        }
    }
}

/// Streams data from a source and partitions it into variable-sized chunks using FastCDC.
///
/// This function is the core of the indexing engine. It transforms a raw byte stream
/// into structural metadata ready for persistence in the `DataStore`.
///
/// # Arguments
/// * `file_id` - The unique identifier of the file being processed.
/// * `source` - Any type implementing [`Read`], such as a `File` or `Cursor`.
/// * `chunk_config` - Optional CDC parameters. If `None`, defaults to 1KB average chunks.
///
/// # Returns
/// A result containing a tuple:
/// 1. `Vec<ChunkTableEntry>`: The unique physical data blocks identified.
/// 2. `Vec<FileSectionEntry>`: The logical map linking this file to its chunks.
///
/// # Errors
/// Returns an error if the source reader fails or if the CDC parameters
/// violate the underlying algorithm's constraints.
pub fn chunk_source<R: Read>(
    file_id: &FileID,
    source: R,
    chunk_config: Option<ChunkConfig>,
) -> Result<(Vec<ChunkTableEntry>, Vec<FileSectionEntry>)> {
    let chunk_config = chunk_config.unwrap_or_default();

    let ChunkConfig {
        min_chunk_size,
        avg_chunk_size,
        max_chunk_size,
    } = chunk_config;

    let chunker = StreamCDC::new(source, min_chunk_size, avg_chunk_size, max_chunk_size);
    let mut chunks = Vec::new();
    let mut sections = Vec::new();

    for chunk in chunker {
        let chunk = chunk?;
        let hash = blake3::hash(&chunk.data).as_bytes().to_vec();

        chunks.push(ChunkTableEntry {
            hash: hash.clone(),
            size: chunk.length as i64,
        });

        sections.push(FileSectionEntry {
            file_id: file_id.to_string(),
            chunk_hash: hash,
            length: chunk.length as i64,
            offset: chunk.offset as i64,
        });
    }
    Ok((chunks, sections))
}

/// `DataStore` is the central "Universal Hub" for database interactions.
///
/// ### Architectural Intent:
/// It acts as a wrapper around the `sqlx::Pool`, managing the lifecycle of
/// database connections. Instead of specialized store structs, it uses
/// generic traits to handle different data types (Files, Chunks, etc.).
///
/// ### Reasoning:
/// By using a single Pool and generic traits, we ensure that:
/// 1. Connection management is centralized.
/// 2. We avoid "Borrow Checker" hell by passing an immutable reference (`&self`).
pub struct DataStore {
    pool: AnyPool,
}

impl DataStore {
    /// Initializes a new DataStore and runs migrations.
    /// Ensures the 3NF schema is ready before any operations begin.
    pub async fn new(pool: AnyPool) -> Result<Self> {
        let migrator = sqlx::migrate!("db/migrations");
        migrator.run(&pool).await?;

        Ok(Self { pool })
    }
}

/// `Persist<Data>` handles the "Storage" part of the database.
///
/// ### Intent:
/// To provide an atomic, write-only interface for storing data.
///
/// ### Reasoning:
/// Writing data is destructive and requires strict coordination (Transactions).
/// By grouping multiple items in `store_all`, we significantly reduce disk I/O
/// overhead, which is critical for high-performance file syncing.
#[async_trait]
pub trait Persist<Data: Send + Sync> {
    /// DOD Batch Insert: Processes a collection of data in a single transaction.
    /// Best used for high-throughput operations like initial folder scans.
    async fn store_all(&self, items: Vec<Data>) -> Result<()>;

    /// Atomic Single Insert: Persists a single record to the database.
    /// Best used for incremental updates or low-frequency events.
    async fn store(&self, item: Data) -> Result<()>;
}

/// `Fetch<ID, Data>` handles the "Query" side of database.
///
/// ### Intent:
/// To provide a high-performance, read-only interface for data retrieval.
///
/// ### Reasoning:
/// Reading is non-destructive and highly parallelizable. Separating Fetch from Persist
/// allows the system to scale read operations across the pool without needing
/// mutable access to the store.
#[async_trait]
pub trait Fetch<ID: Send + Sync, Data: Send + Sync> {
    /// Retrieves a single record by its unique identifier.
    async fn fetch_by(&self, key: &ID) -> Result<Data>;

    /// Retrieves multiple records in a single database round-trip.
    ///
    /// ### Performance Note:
    /// Uses SQL `IN` clauses to minimize network latency and optimize index usage.
    async fn fetch_many(&self, keys: &[ID]) -> Result<Vec<Data>>;
}

#[derive(Error, Debug)]
pub enum DataStoreError {
    #[error("Error while chunking: {0}")]
    ChunkingError(#[from] fastcdc::v2020::Error),
    #[error("Database Error: {0}")]
    DbError(#[from] sqlx::Error),
    #[error("Migration Error: {0}")]
    MigrationError(#[from] MigrateError),
    #[error("Requested record was not found in the store")]
    NotFound,
}

#[cfg(test)]
async fn setup() -> DataStore {
    use sqlx::any::{AnyPoolOptions, install_default_drivers};
    // Use PoolOptions to ensure the connection stays alive
    install_default_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(1) // Force a single connection for stability in memory
        .idle_timeout(None) // Never let the connection drop due to inactivity
        .connect("sqlite::memory:")
        .await
        .expect("Could not create pool");
    // Using an in-memory database ensures tests are fast and side-effect free
    DataStore::new(pool)
        .await
        .expect("Failed to create test store")
}
