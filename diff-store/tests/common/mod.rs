use diff_store::DataStore;
use diff_store::{ChunkTableEntry, FileSectionEntry};
use fastcdc::v2020::StreamCDC;
use skie_common::FileID;

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

/// Helper to chunk a buffer and return the entries
pub fn chunk_buffer(
    file_id: &FileID,
    buffer: &[u8],
) -> (Vec<ChunkTableEntry>, Vec<FileSectionEntry>) {
    let chunker = StreamCDC::new(buffer, 512, 1024, 2048);
    let mut chunks = Vec::new();
    let mut sections = Vec::new();

    for chunk in chunker {
        let chunk = chunk.unwrap();
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
    (chunks, sections)
}
