use skie_common::IndexEngineConfig;
use skie_index::HashEngine;

#[test]
fn test_init() {
    let config = IndexEngineConfig::default();
    let engine = HashEngine::new(config).unwrap();
}