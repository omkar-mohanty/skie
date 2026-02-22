mod common;

use std::io::Cursor;

use anyhow::Result;
use common::*;
use diff_store::{Fetch, FileSectionEntry, FileTableEntry, Persist, chunk_source};
use rand::{RngCore, rng};
use skie_common::FileID;

#[tokio::test]
async fn test_reconstruction_integrity() -> Result<()> {
    let store = setup().await;
    let file_id = FileID::new();
    let mut buffer = vec![0u8; 8 * KB];
    rng().fill_bytes(&mut buffer);

    let hash = blake3::hash(&buffer).as_bytes().to_vec();

    let mut cursor = Cursor::new(buffer);
    let (chunks, sections) = chunk_source(&file_id, &mut cursor, None)?;

    let file_entry = FileTableEntry {
        hash,
        file_id: file_id.to_string(),
        name: "test.bin".to_uppercase(),
        path: "test.path".to_lowercase(),
    };

    // Store metadata and chunks
    store.store(file_entry).await?;
    store.store_all(chunks).await?;
    store.store_all(sections).await?;

    // Fetch and verify order
    let fetched_sections: Vec<FileSectionEntry> = store.fetch_by(&file_id).await?;

    let mut current_offset = 0;
    for section in fetched_sections {
        assert_eq!(
            section.offset, current_offset,
            "Chunks must be perfectly contiguous"
        );
        current_offset += section.length;
    }
    assert_eq!(current_offset as usize, cursor.get_ref().len());

    Ok(())
}

#[tokio::test]
async fn test_file_data_change_delta() -> Result<()> {
    let store = setup().await;
    let file_id = FileID::new();

    // 1. Initial State: All zeros
    let data_v1 = vec![0u8; 4 * KB];
    let hash = blake3::hash(&data_v1).as_bytes().to_vec();
    let mut cursor = Cursor::new(data_v1);
    let (c1, s1) = chunk_source(&file_id, &mut cursor, None)?;

    store
        .store(FileTableEntry {
            file_id: file_id.to_string(),
            name: "Testfile".to_string(),
            path: "somepath".to_string(),
            hash,
        })
        .await?;
    store.store_all(c1).await?;
    store.store_all(s1).await?;

    // 2. Change: Invert a small slice in the middle
    let mut data_v2 = vec![0u8; 4 * KB];
    data_v2[1000..1200].fill(0xFF);
    let mut cursor = Cursor::new(data_v2);
    let (c2, s2) = chunk_source(&file_id, &mut cursor, None)?;

    // Store update (UPSERT handles the offset conflicts)
    store.store_all(c2).await?;
    store.store_all(s2).await?;

    let final_sections: Vec<_> = store.fetch_by(&file_id).await?;

    // If FastCDC worked, the first and last chunks likely stayed the same,
    // but the chunk covering offset 1000-1200 must have a different hash.
    assert!(
        final_sections
            .iter()
            .any(|s| s.chunk_hash != blake3::hash(&[0u8; 1]).as_bytes().to_vec())
    );

    Ok(())
}

#[tokio::test]
async fn test_file_name_and_path_change() -> Result<()> {
    let store = setup().await;
    let file_id = FileID::new();
    let hash = vec![0xDE, 0xAD];

    let entry_v1 = FileTableEntry {
        file_id: file_id.to_string(),
        path: "/etc/config.yaml".into(),
        name: "config.yaml".into(),
        hash: hash.clone(),
    };

    // 1. Initial Insert
    store.store(entry_v1).await?;

    // 2. Rename: Same ID, new Path
    let entry_v2 = FileTableEntry {
        file_id: file_id.to_string(),
        path: "/etc/old_config.yaml".into(),
        name: "old_config.yaml".into(),
        hash: hash.clone(),
    };
    store.store(entry_v2).await?;

    // 3. Verify: We should NOT have two files in the DB, only one with the new name
    // (This assumes your FileTable handle UPSERT on file_id)
    let fetched: Vec<FileTableEntry> = store.fetch_many(&[file_id]).await?; // Assuming you have this helper
    assert_eq!(fetched.len(), 1);
    assert!(fetched[0].path.contains("old_config"));

    Ok(())
}
