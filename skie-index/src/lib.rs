mod hash_engine;
use fastcdc::ronomon::Chunk;
pub use hash_engine::{HashEngine, HashEngineError};
use loro::{LoroDoc, LoroList};
use rayon::iter::{ParallelBridge, ParallelIterator};
use skie_common::{ChunkIndex, ChunkMetadata, FileID, FileMetadata};
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
};
use uuid::Uuid;

pub(crate) type Result<E> = std::result::Result<E, HashEngineError>;
pub(crate) type DeviceID = Uuid;

pub struct SyncFile {
    pub file_metadata: FileMetadata,
    pub doc: LoroDoc,
    pub changes: LoroList,
}

impl SyncFile {
    pub fn new(id: DeviceID, file_metadata: FileMetadata) -> Self {
        let doc = LoroDoc::new();
        let changes = doc.get_list(id.to_string());
        Self {
            file_metadata,
            doc,
            changes,
        }
    }

    pub fn sync_new_chunks(
        mut self,
        new_tree: BTreeMap<ChunkIndex, ChunkMetadata>,
    ) -> BTreeMap<ChunkIndex, ChunkMetadata> {
        let mut old_tree = self.file_metadata.manifest_tree;
        let removed_enries = old_tree
            .extract_if(.., |key, _| new_tree.contains_key(key))
            .collect::<BTreeMap<ChunkIndex, ChunkMetadata>>();
        self.file_metadata.manifest_tree = new_tree;
        removed_enries
    }
}

pub struct SyncEngine {
    hash_engine: HashEngine,
    file_map: HashMap<FileID, FileMetadata>,
}

impl SyncEngine {
    pub fn new() {
        let loro_doc = LoroDoc::new();
    }
}

pub fn calculate_diff(engine: &HashEngine, source: PathBuf, metadata: &FileMetadata) {
    let manifest_map = &metadata.manifest_tree;
    let iter = engine
        .process(source)
        .par_bridge()
        .filter(|hash_res| match hash_res {
            Ok(hash_data) => match manifest_map.get(&hash_data.index) {
                Some(chunk_metadata) => *chunk_metadata == *hash_data,
                None => true,
            },
            Err(_) => true,
        });
}
