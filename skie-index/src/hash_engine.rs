use fastcdc::v2020::{ChunkData, StreamCDC};
use rayon::{
    ThreadPool, ThreadPoolBuildError, ThreadPoolBuilder,
    iter::{ParallelBridge, ParallelIterator},
};
use skie_common::{ChunkIndex, ChunkMetadata, IndexEngineConfig};
use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
};
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

    thread_pool.install(move || {
        let err = output_rx.iter().any(|res| res.is_err());
        if err {
            let errs = output_rx
                .iter()
                .filter(|res| res.is_err())
                .collect::<Vec<_>>();
            return Err(HashEngineError::HashError(errs));
        }

        Ok(output_rx
            .iter()
            .par_bridge()
            .map(|res| res.unwrap())
            .collect::<BTreeMap<ChunkIndex, ChunkMetadata>>())
    })
}

pub struct HashEngine {
    config: IndexEngineConfig,
    thread_pool: ThreadPool,
}

impl HashEngine {
    pub fn new(config: IndexEngineConfig) -> Result<Self> {
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(config.num_threads)
            .thread_name(|i| format!("skie-index-{}", i))
            .build()?;
        Ok(Self {
            config,
            thread_pool,
        })
    }

    pub fn process(&self, source: PathBuf) -> impl Iterator<Item = Result<ChunkMetadata>> {
        let (tx, rx) =
            crossbeam_channel::bounded::<Result<(usize, ChunkData)>>(self.config.channel_size);
        let (output_tx, output_rx) = crossbeam_channel::bounded(self.config.channel_size);

        let min_chunk_size = self.config.min_chunk_size;
        let avg_chunk_size = self.config.avg_chunk_size;
        let max_chunk_size = self.config.max_chunk_size;

        self.thread_pool.spawn(move || {
            let source = match File::open(source) {
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

        self.thread_pool.spawn(move || {
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
                        Ok(hash_data)
                    }
                    Err(msg) => Err(msg),
                };

                let _ = output_tx.send(res);
            });
        });

        output_rx.into_iter()
    }
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

    #[test]
    fn test_empty_file() -> Result<()> {
        let config = test_config();
        let file = NamedTempFile::new()?;
        let resources = ComputeResource {
            thread_pool: rayon::ThreadPoolBuilder::new().build()?,
            hasher: blake3::Hasher::new(),
        };
        let res = get_chunk_hashes(file.path().to_path_buf(), Some(config), &resources)?;
        assert!(res.is_empty());
        Ok(())
    }

    #[test]
    fn test_small_file_single_chunk() -> Result<()> {
        let engine = HashEngine::new(test_config())?;
        let data = vec![0u8; 1024];
        let mut file = NamedTempFile::new()?;
        let _ = file.write_all(&data);
        let source = file.path().to_path_buf();
        // Iterators let us just take the first item
        let mut it = engine.process(source);
        let first = it.next().expect("Should have one chunk")?;

        assert_eq!(first.hash, blake3::hash(&data));
        assert_eq!(first.index, 0);
        Ok(())
    }

    #[test]
    fn test_determinism() -> Result<()> {
        let engine = HashEngine::new(test_config())?;
        let mut data = vec![0u8; 1024 * 100];
        rng().fill_bytes(&mut data);
        let mut file = NamedTempFile::new()?;
        let _ = file.write_all(&data)?;

        let source_first = file.path().to_path_buf();
        let source_second = file.path().to_path_buf();

        // Determinism is tricky with par_bridge because order is random.
        // We collect and sort by index to verify the content is identical.

        let mut res_1: Vec<_> = engine.process(source_first).map(|r| r.unwrap()).collect();
        let mut res_2: Vec<_> = engine.process(source_second).map(|r| r.unwrap()).collect();

        res_1.sort_by_key(|d| d.index);
        res_2.sort_by_key(|d| d.index);

        assert_eq!(res_1.len(), res_2.len());
        for (a, b) in res_1.iter().zip(res_2.iter()) {
            assert_eq!(a.hash, b.hash);
            assert_eq!(a.index, b.index);
        }
        Ok(())
    }
}
