mod store_test_common;
use anyhow::Result;
use common::FileID;
use fastcdc::v2020::StreamCDC;
use rand::{RngCore, rng};
use std::io::Write;
use store::{ChunkTableEntry, FileSectionEntry, FileTableEntry, Persist};
pub use store_test_common::*;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_chunking() -> Result<()> {
    let store = setup().await;
    let mut file = NamedTempFile::new()?;
    let mut buffer = vec![0x0; 4 * KB];
    let file_id = FileID::new();

    rng().fill_bytes(&mut buffer);

    let hash = blake3::hash(&buffer);
    file.write_all(&buffer)?;
    file.flush()?;

    store
        .store(FileTableEntry {
            file_id: file_id.to_string(),
            name: file.path().to_string_lossy().to_lowercase(),
            path: file.path().to_string_lossy().to_lowercase(),
            hash: hash.as_bytes().to_vec(),
        })
        .await?;

    // REOPEN the file or use a handle, as StreamCDC consumes 'file'
    let read_handle = std::fs::File::open(file.path())?;
    let chunk_iter = StreamCDC::new(read_handle, 512, 1024, 2048).map(|res| res.unwrap());

    // 1. Collect everything into a temporary vector to avoid ownership issues in the iterator
    let chunk_results: Vec<_> = chunk_iter.collect();

    // 2. Map to your DB entries
    let mut chunk_table_entries = Vec::new();
    let mut file_section_entries = Vec::new();

    for chunk in chunk_results {
        let chunk_hash = blake3::hash(&chunk.data).as_bytes().to_vec();

        chunk_table_entries.push(ChunkTableEntry {
            hash: chunk_hash.clone(),
            size: chunk.length as i64,
        });

        file_section_entries.push(FileSectionEntry {
            chunk_hash,
            file_id: file_id.to_string(),
            length: chunk.length as i64,
            offset: chunk.offset as i64,
        });
    }

    // 3. PERSIST - Don't forget the .await and the ? for error handling!
    // Important: Store chunks FIRST to satisfy Foreign Key constraints
    store.store_all(chunk_table_entries).await?;
    store.store_all(file_section_entries).await?;

    Ok(())
}
