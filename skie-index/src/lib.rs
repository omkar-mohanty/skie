use blake3::Hash;
use fastcdc::v2020::{ChunkData, StreamCDC};
use rayon::{
    ThreadPoolBuildError,
    iter::{ParallelBridge, ParallelIterator},
};
use skie_common::{ChunkIndex, HashConfig};
use std::{fs::File, path::Path};
use thiserror::Error;

type Result<E> = std::result::Result<E, HashEngineError>;

pub struct ComputeResource {
    pub thread_pool: rayon::ThreadPool,
    pub hasher: blake3::Hasher,
}

#[derive(Debug)]
pub struct ChunkResult {
    pub hash: Hash,
    pub index: ChunkIndex,
    pub size: u64,
    pub offset: u64,
}

impl PartialEq for ChunkResult {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

pub fn get_chunk_hashes(
    path: &Path,
    resources: &ComputeResource,
    config: &HashConfig,
) -> Result<Vec<ChunkResult>> {
    let path = path.to_path_buf();
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
                    let size = chunkdata.length as u64;
                    let hash_data = ChunkResult {
                        index,
                        hash,
                        offset,
                        size,
                    };
                    Ok(hash_data)
                }
                Err(msg) => Err(msg),
            };

            let _ = output_tx.send(res);
        });
    });

    // 1. Collect everything from the channel into a Result Vec
    // This drains the channel completely in one pass.
    let results: Vec<Result<ChunkResult>> = output_rx.iter().collect();

    // 2. Check if any item in our Vec is an error
    if results.iter().any(|res| res.is_err()) {
        let errs = results
            .into_iter()
            .filter(|res| res.is_err())
            .collect::<Vec<_>>();
        return Err(HashEngineError::HashError(errs));
    }

    // 3. If all good, transform the Vec into the BTreeMap
    Ok(results
        .into_iter()
        .map(|res| res.unwrap())
        .collect::<Vec<_>>())
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
    HashError(Vec<Result<ChunkResult>>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{RngCore, rng};
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn test_config() -> HashConfig {
        HashConfig {
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
        let res = get_chunk_hashes(file.path(), &resources, &config)?;
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
        let path = file.path();
        let hashes = get_chunk_hashes(path, &resource, &config)?;

        assert_eq!(hashes.len(), 1);
        let chunk_metadata = hashes.first().unwrap();
        assert_eq!(chunk_metadata.hash, blake3::hash(&data));
        Ok(())
    }

    #[test]
    fn test_determinism() -> Result<()> {
        let config = test_config();
        let resource = test_resource()?;

        let mut data = vec![0u8; 1024 * 100];
        rng().fill_bytes(&mut data);

        let mut file = NamedTempFile::new()?;
        file.write_all(&data)?;
        file.flush()?;

        let path = file.path();

        // 3. Get results
        let mut res_1 = get_chunk_hashes(path, &resource, &config)?;
        let mut res_2 = get_chunk_hashes(path, &resource, &config)?;

        // 4. SORTING IS MANDATORY
        // par_bridge() makes the Vec order non-deterministic.
        res_1.sort_by_key(|c| c.index);
        res_2.sort_by_key(|c| c.index);

        assert_eq!(res_1.len(), res_2.len(), "Chunk counts differ");
        assert_eq!(
            res_1, res_2,
            "Hashes or indices are not deterministic after sorting"
        );
        Ok(())
    }
}
