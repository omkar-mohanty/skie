use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use skie_common::IndexEngineConfig;
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::HashData;
use crate::HashEngine;
use crate::Result;

pub struct DifferenceEngine {
    config: IndexEngineConfig,
    hash_engine: HashEngine,
}

impl DifferenceEngine {
    pub fn new(config: IndexEngineConfig) -> Result<Self> {
        let hash_engine = HashEngine::new(config.clone())?;
        Ok(Self {
            config,
            hash_engine,
        })
    }

    pub fn calculate_diff(
        &self,
        source: PathBuf,
        manifest_map: BTreeMap<usize, HashData>,
    ) -> Vec<Result<HashData>> {
        let chunk_stream = self.hash_engine.process(source);

        chunk_stream
            .par_bridge()
            .filter(|hash_res| match hash_res {
                Ok(res) => match manifest_map.get(&res.index) {
                    Some(hash_res) => res == hash_res,
                    None => true,
                },
                Err(_) => true,
            })
            .collect::<Vec<_>>()
    }
}
