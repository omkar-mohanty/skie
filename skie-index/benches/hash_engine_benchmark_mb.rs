use criterion::{Criterion, criterion_group, criterion_main};
use rand::{RngCore, rng};
use skie_common::HashConfig;
use skie_index::{ComputeResource, get_chunk_hashes};
use std::hint::black_box;
use std::io::Write;
use tempfile::NamedTempFile;

const MB: usize = 1024 * 1024;

fn hash_engine_benchmark_mb(c: &mut Criterion) {
    let config = HashConfig::default();

    // Setup resources once outside the benchmark loop
    let resources = ComputeResource {
        thread_pool: rayon::ThreadPoolBuilder::new()
            .num_threads(config.num_threads)
            .build()
            .unwrap(),
        hasher: blake3::Hasher::new(),
    };

    let named_temp_file = NamedTempFile::new().unwrap();
    let mut file = named_temp_file.as_file();
    let mut buffer = vec![0u8; MB];

    // Create a 50MB file
    for _ in 0..50 {
        rng().fill_bytes(&mut buffer);
        file.write_all(&buffer).unwrap();
    }
    file.sync_all().unwrap(); // Ensure it's actually on disk/cache

    let source = named_temp_file.path();

    c.bench_function("get_chunk_hashes 50MB", |b| {
        b.iter(|| {
            // We clone the path because the function takes ownership
            let res =
                get_chunk_hashes(black_box(source), black_box(&resources), black_box(&config));

            // Black_box the result to prevent the compiler from
            // optimizing away the entire computation.
            black_box(res).unwrap();
        })
    });
}

criterion_group!(benches, hash_engine_benchmark_mb);
criterion_main!(benches);
