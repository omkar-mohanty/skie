use skie_common::FileTable;
use sqlx::{AnyConnection, Connection, SqliteConnection};
use thiserror::Error;

type Result<E> = std::result::Result<E, Box<dyn std::error::Error>>;

pub struct FileStore {
    connection: AnyConnection,
}
impl FileStore {
    pub async fn new() -> Result<Self> {
        let i = 0;
       todo!() 
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
   #[error("Error while creating a database connection")]
   ConnectionError(#[from] sqlx::Error),
}
