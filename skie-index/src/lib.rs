use blake3::Hash;
use fastcdc::v2020::{ChunkData, StreamCDC};
use rayon::{
    ThreadPool, ThreadPoolBuildError, ThreadPoolBuilder,
    iter::{ParallelBridge, ParallelIterator},
};
use skie_common::IndexEngineConfig;
use std::io::{BufReader, Read};
use thiserror::Error;

type Result<E> = std::result::Result<E, HashEngineError>;

#[derive(Debug)]
pub struct HashData {
    pub index: usize,
    pub chunk: Vec<u8>,
    pub hash: Hash,
}

impl PartialEq for HashData {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
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

    pub fn process<R: Read + Send>(
        &self,
        source: BufReader<R>,
    ) -> impl Iterator<Item = Result<HashData>> {
        let (tx, rx) = crossbeam_channel::bounded(self.config.channel_size);
        let (output_tx, output_rx) = crossbeam_channel::bounded(self.config.channel_size);
        self.thread_pool.scope(move |s| {
            s.spawn(move |_| {
                let chunker = StreamCDC::new(
                    source,
                    self.config.min_chunk_size,
                    self.config.avg_chunk_size,
                    self.config.max_chunk_size,
                );

                for (i, chunk) in chunker.enumerate() {
                    let msg = chunk.map_err(HashEngineError::ChunkError);
                    if tx.send((i, msg)).is_err() {
                        break;
                    }
                }
            });

            // 1. Hashing necessarily may not happen in the order the chunks come in.
            // To counter this we hash the chunks which and send them to the receiver and the sent chunks may be out of order.
            self.thread_pool.spawn(move || {
                rx.into_iter().par_bridge().for_each(|(index, chunk_res)| {
                    let hash_data = match chunk_res {
                        Ok(chunk) => {
                            let chunk = chunk.data;
                            let hash = blake3::hash(&chunk);
                            Ok(HashData { index, chunk, hash })
                        }
                        Err(msg) => Err(msg),
                    };

                    let _ = output_tx.send(hash_data);
                });
            });
        });
        output_rx.into_iter()
    }
}

#[derive(Debug, Error)]
pub enum HashEngineError {
    #[error("Index Engine: Send Data Error")]
    SendDataError(#[from] crossbeam_channel::SendError<ChunkData>),
    #[error("Index Engine: Chunk error")]
    ChunkError(#[from] fastcdc::v2020::Error),
    #[error("Index Engine: ThreadPool error")]
    ThreadPoolError(#[from] ThreadPoolBuildError),
}

#[cfg(test)]
mod tests {
use super::*;
    use rand::{RngCore, thread_rng};
    use std::io::{Cursor, Write};
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
        let engine = HashEngine::new(test_config())?;
        let reader = BufReader::new(Cursor::new(vec![]));

        // Testing the iterator: collect into a Vec
        let results: Vec<_> = engine.process(reader).collect();
        assert!(results.is_empty(), "Empty file should produce zero chunks");
        Ok(())
    }

    #[test]
    fn test_small_file_single_chunk() -> Result<()> {
        let engine = HashEngine::new(test_config())?;
        let data = vec![0u8; 1024]; 
        let reader = BufReader::new(Cursor::new(data.clone()));

        // Iterators let us just take the first item
        let mut it = engine.process(reader);
        let first = it.next().expect("Should have one chunk")?;
        
        assert_eq!(first.hash, blake3::hash(&data));
        assert_eq!(first.index, 0);
        Ok(())
    }

    #[test]
    fn test_determinism() -> Result<()> {
        let engine = HashEngine::new(test_config())?;
        let mut data = vec![0u8; 1024 * 100];
        thread_rng().fill_bytes(&mut data);

        // Determinism is tricky with par_bridge because order is random.
        // We collect and sort by index to verify the content is identical.
        let mut res_1: Vec<_> = engine.process(BufReader::new(Cursor::new(&data)))
            .map(|r| r.unwrap()).collect();
        let mut res_2: Vec<_> = engine.process(BufReader::new(Cursor::new(&data)))
            .map(|r| r.unwrap()).collect();

        res_1.sort_by_key(|d| d.index);
        res_2.sort_by_key(|d| d.index);

        assert_eq!(res_1.len(), res_2.len());
        for (a, b) in res_1.iter().zip(res_2.iter()) {
            assert_eq!(a.hash, b.hash);
            assert_eq!(a.index, b.index);
        }
        Ok(())
    }

    #[test]
    fn test_large_file_concurrency() -> Result<()> {
        let mut config = test_config();
        config.num_threads = 8;
        let engine = HashEngine::new(config)?;

        let mut tmp_file = NamedTempFile::new().unwrap();
        let mut content = vec![0u8; 1024 * 1024]; 
        for _ in 0..50 { // 50MB test
            thread_rng().fill_bytes(&mut content);
            tmp_file.write_all(&content).unwrap();
        }

        let file = std::fs::File::open(tmp_file.path()).unwrap();
        let reader = BufReader::with_capacity(1024 * 1024, file);

        let start = std::time::Instant::now();
        
        // Use the iterator to count chunks without holding all 50MB in RAM
        let _ = engine.process(reader)
            .collect::<Vec<_>>();
            
        let duration = start.elapsed();
        println!("Processed 50MB in {:?}", duration);
        Ok(())
    }
}
