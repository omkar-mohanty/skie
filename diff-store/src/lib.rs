mod chunk_store;
mod file_store;

pub use chunk_store::*;
pub use file_store::*;

use async_trait::async_trait;
use sqlx::{Any, AnyPool, Pool, any::install_default_drivers, migrate::MigrateError};
use thiserror::Error;

/// A Result type specialized for DataStore operations.
pub type Result<T> = std::result::Result<T, DataStoreError>;

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
    pool: Pool<Any>,
}

impl DataStore {
    /// Initializes a new DataStore and runs migrations.
    /// Ensures the 3NF schema is ready before any operations begin.
    pub async fn new(url: &str) -> Result<Self> {
        install_default_drivers();
        let pool = AnyPool::connect(url).await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
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
    #[error("Database Error: {0}")]
    DbError(#[from] sqlx::Error),
    #[error("Migration Error: {0}")]
    MigrationError(#[from] MigrateError),
    #[error("Requested record was not found in the store")]
    NotFound,
}
#[cfg(test)]
async fn setup() -> DataStore {
    // Using an in-memory database ensures tests are fast and side-effect free
    DataStore::new("sqlite::memory:")
        .await
        .expect("Failed to create test store")
}
