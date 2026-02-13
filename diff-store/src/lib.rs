use rusqlite::Connection;
use skie_common::FileTable;
use thiserror::Error;

pub(crate) type Result<E> = std::result::Result<E, StoreError>;

pub struct FileStore {
    connection: Connection
}

impl FileStore {
    pub fn new() -> Result<Self> {
        let connection = Connection::open_in_memory()?;
        Ok(Self { connection })
    }

    pub fn flush(&self, files_table: FileTable) {

    }
}

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("Database Error")]
    DbError(#[from] rusqlite::Error)
}