use store::DataStore;

pub const KB: usize = 1024;

pub async fn setup() -> DataStore {
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
