use skie_common::FileTable;
use sqlx::{AnyConnection, Connection, Executor};
use thiserror::Error;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

// The "Lazy" Senior Query: One string to rule them all.
const UPSERT_QUERY: &str = r#"
    INSERT INTO files (file_id, name, path, hash)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT(file_id) DO UPDATE SET
        name = excluded.name,
        path = excluded.path,
        hash = excluded.hash
"#;

pub struct FileStore {
    connection: AnyConnection,
}

impl FileStore {
    pub async fn new(url: &str) -> Result<Self> {
        let mut connection = AnyConnection::connect(url).await?;

        // Senior Move: Run migrations automatically on startup
        // This ensures the table exists before any 'upsert' is called.
        sqlx::migrate!("./migrations").run(&mut connection).await?;

        Ok(Self { connection })
    }

    /// DOD Upsert: Processes the entire table in one atomic transaction.
    pub async fn upsert_table(&mut self, table: FileTable) -> Result<()> {
        // Start a transaction. If any insert fails, the whole thing rolls back.
        let mut transaction = self.connection.begin().await?;

        // Loop through our DOD arrays.
        for i in 0..table.file_ids.len() {
            sqlx::query(UPSERT_QUERY)
                .bind((*table.file_ids[i]).as_u128().to_string())
                .bind(&table.names[i])
                .bind(table.paths[i].to_string_lossy().to_string())
                .bind(table.hashes[i].as_slice())
                .execute(&mut *transaction) // Execute inside the transaction
                .await?;
        }

        // Commit everything to disk
        transaction.commit().await?;
        Ok(())
    }

    /// Helper to see if we are a "Fresh Install"
    pub async fn is_empty(&mut self) -> Result<bool> {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
            .fetch_one(&mut self.connection)
            .await?;
        Ok(count.0 == 0)
    }
}
