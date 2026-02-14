use fastcdc::v2020::{ChunkData, StreamCDC};
use rayon::{
    ThreadPoolBuildError,
    iter::{ParallelBridge, ParallelIterator},
};
use skie_common::{ChunkIndex, ChunkMetadata, IndexEngineConfig};
use std::{collections::BTreeMap, fs::File, path::PathBuf};
use thiserror::Error;

use crate::ComputeResource;

type Result<E> = std::result::Result<E, HashEngineError>;

pub fn get_chunk_hashes(
    path: PathBuf,
    config: Option<IndexEngineConfig>,
    resources: &ComputeResource,
) -> Result<BTreeMap<ChunkIndex, ChunkMetadata>> {
    let config = config.unwrap_or_default();
    let thread_pool = &resources.thread_pool;
    let (tx, rx) = crossbeam_channel::bounded::<Result<(usize, ChunkData)>>(config.channel_size);
    let (output_tx, output_rx) = crossbeam_channel::bounded(config.channel_size);

    let min_chunk_size = config.min_chunk_size;
    let avg_chunk_size = config.avg_chunk_size;
    let max_chunk_size = config.max_chunk_size;

    thread_pool.spawn(move || {
        let source = match File::open(path) {
            Ok(file) => file,
            Err(msg) => {
                let err = Err(HashEngineError::IoError(msg));
                let _ = tx.send(err);
                return;
            }
        };

        let stream_cdc = StreamCDC::new(source, min_chunk_size, avg_chunk_size, max_chunk_size);

        for (index, chunk_res) in stream_cdc.enumerate() {
            let res = match chunk_res {
                Ok(chunk_data) => Ok((index, chunk_data)),
                Err(msg) => Err(HashEngineError::ChunkError(msg)),
            };

            let _ = tx.send(res);
        }
    });

    thread_pool.spawn(move || {
        rx.iter().par_bridge().for_each(|chunk_res| {
            let res = match chunk_res {
                Ok((index, chunkdata)) => {
                    let chunk = chunkdata.data;
                    let hash = blake3::hash(&chunk);
                    let offset = chunkdata.offset;
                    let length = chunkdata.length;
                    let hash_data = ChunkMetadata {
                        index,
                        hash,
                        offset,
                        length,
                    };
                    Ok((index, hash_data))
                }
                Err(msg) => Err(msg),
            };

            let _ = output_tx.send(res);
        });
    });

    // 1. Collect everything from the channel into a Result Vec
    // This drains the channel completely in one pass.
    let results: Vec<Result<(usize, ChunkMetadata)>> = output_rx.iter().collect();

    // 2. Check if any item in our Vec is an error
    if results.iter().any(|res| res.is_err()) {
        let errs = results.into_iter().filter(|res| res.is_err()).collect();
        return Err(HashEngineError::HashError(errs));
    }

    // 3. If all good, transform the Vec into the BTreeMap
    Ok(results
        .into_iter()
        .map(|res| res.unwrap())
        .collect::<BTreeMap<ChunkIndex, ChunkMetadata>>())
}

#[derive(Debug, Error)]
pub enum HashEngineError {
    #[error("Hash Engine: Io Error")]
    IoError(#[from] std::io::Error),
    #[error("Hash Engine: Send Data Error")]
    SendDataError(#[from] crossbeam_channel::SendError<ChunkData>),
    #[error("Hash Engine: Chunk error")]
    ChunkError(#[from] fastcdc::v2020::Error),
    #[error("Hash Engine: ThreadPool error")]
    ThreadPoolError(#[from] ThreadPoolBuildError),
    #[error("Hash Engine: Error while hashing")]
    HashError(Vec<Result<(usize, ChunkMetadata)>>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{RngCore, rng};
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn test_config() -> IndexEngineConfig {
        IndexEngineConfig {
            num_threads: 4,
            min_chunk_size: 1024 * 16,
            avg_chunk_size: 1024 * 32,
            max_chunk_size: 1024 * 64,
            channel_size: 64,
            ..Default::default()
        }
    }

    fn test_resource() -> Result<ComputeResource> {
        Ok(ComputeResource {
            thread_pool: rayon::ThreadPoolBuilder::new().build()?,
            hasher: blake3::Hasher::new(),
        })
    }

    #[test]
    fn test_empty_file() -> Result<()> {
        let config = test_config();
        let file = NamedTempFile::new()?;
        let resources = test_resource()?;
        let res = get_chunk_hashes(file.path().to_path_buf(), Some(config), &resources)?;
        assert!(res.is_empty());
        Ok(())
    }

    #[test]
    fn test_small_file_single_chunk() -> Result<()> {
        let config = test_config();
        let resource = test_resource()?;
        let data = vec![0u8; 1024];
        let mut file = NamedTempFile::new()?;
        let _ = file.write_all(&data);
        file.flush()?;
        let path = file.path().to_path_buf();
        let tree = get_chunk_hashes(path, Some(config), &resource)?;

        assert_eq!(tree.len(), 1);
        let (index, chunk_metadata) = tree.first_key_value().unwrap();
        assert_eq!(chunk_metadata.hash, blake3::hash(&data));
        assert_eq!(*index, 0);
        Ok(())
    }

    #[test]
    fn test_determinism() -> Result<()> {
        let config = test_config();
        let resource = test_resource()?;

        // 1. Create random data
        let mut data = vec![0u8; 1024 * 100];
        rng().fill_bytes(&mut data);

        // 2. Write to a temp file
        let mut file = NamedTempFile::new()?;
        file.write_all(&data)?;
        file.flush()?;

        let path = file.path().to_path_buf();

        // 3. Run the functional engine twice on the same file
        // We clone the path because get_chunk_hashes takes ownership of the PathBuf
        let res_1 = get_chunk_hashes(path.clone(), Some(config.clone()), &resource)?;
        let res_2 = get_chunk_hashes(path, Some(config), &resource)?;

        // 4. Compare the BTreeMaps
        // BTreeMap implements PartialEq, comparing both keys and values in order.
        assert_eq!(res_1.len(), res_2.len(), "Chunk counts differ between runs");
        assert_eq!(res_1, res_2, "Hashes or indices are not deterministic");
        Ok(())
    }
}
