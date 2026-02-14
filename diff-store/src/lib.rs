use skie_common::{FileID, FileTable};
use sqlx::{AnyConnection, Connection, migrate::MigrateError};
use thiserror::Error;

type Result<T> = std::result::Result<T, FileStoreError>;

const UPSERT_QUERY: &str = r#"
    INSERT INTO files (file_iu, name, path, hash)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT(file_id) DO UPDATE SET
        name = excluded.name,
        path = excluded.path,
        hash = excluded.hash
"#;

#[derive(sqlx::FromRow)]
pub struct FileTableEntry {
    pub file_id: String,
    pub name: String,
    pub path: String,
    pub hash: Vec<u8>,
}

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
    pub async fn upsert_table(&mut self, table: &FileTable) -> Result<()> {
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

    pub async fn fetch_file_by_ids(&mut self, file_ids: &[FileID]) -> Result<Vec<FileTableEntry>> {
        if file_ids.is_empty() {
            return Ok(vec![]);
        }

        // 1. Create the string: "SELECT * FROM files WHERE file_id IN ($1, $2, $3)"
        let placeholders = (1..=file_ids.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<_>>()
            .join(",");

        let sql = format!("SELECT * FROM files WHERE file_id IN ({})", placeholders);

        // 2. Bind and execute
        let mut query = sqlx::query_as::<_, FileTableEntry>(&sql);
        for id in file_ids {
            let id = (*id).to_string();
            query = query.bind(id);
        }

        let entries = query.fetch_all(&mut self.connection).await?;

        Ok(entries)
    }

    /// Helper to see if we are a "Fresh Install"
    pub async fn is_empty(&mut self) -> Result<bool> {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
            .fetch_one(&mut self.connection)
            .await?;
        Ok(count.0 == 0)
    }
}

#[derive(Error, Debug)]
pub enum FileStoreError {
    #[error("Connection Error")]
    DbConnectionError(#[from] sqlx::Error),
    #[error("Migration Error")]
    MigrationError(#[from] MigrateError),
}
