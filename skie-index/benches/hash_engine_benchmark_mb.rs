use criterion::{Criterion, criterion_group, criterion_main};
use rand::{RngCore, rng};
use skie_common::IndexEngineConfig;
use skie_index::HashEngine;
use std::{hint::black_box, io::Write};
use tempfile::NamedTempFile;

const KB: usize = 1024;
const MB: usize = KB * KB;

fn hash_engine_benchmark_mb(c: &mut Criterion) {
    let config = IndexEngineConfig::default();
    let engine = HashEngine::new(config).unwrap();
    let named_temp_file = NamedTempFile::new().unwrap();
    let mut file = named_temp_file.as_file();
    let mut buffer = vec![0u8; MB];
    for _ in 0..50 {
        // 50MB test
        rng().fill_bytes(&mut buffer);
        file.write_all(&buffer).unwrap();
    }
    let source = named_temp_file.path().to_path_buf();
    c.bench_function("HashEngine 50MB", |b| {
        b.iter(|| {
            engine
                .process(black_box(source.clone()))
                .collect::<Vec<_>>()
        })
    });
}

criterion_group!(benches, hash_engine_benchmark_mb);
criterion_main!(benches);
