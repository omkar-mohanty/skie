# diff-d

diff-d is a Rust-based content-addressable storage and synchronization system. It provides:
  - File chunking and hashing (BLAKE3) for deduplication and integrity.
  - A SQLite-backed data store for chunks, file metadata, and path history.
  - Watcher service (`diff-d`) that monitors a sync directory for changes.
  - Command-line tool (`ctl`) for querying and managing stored data.

## Overview

This workspace aims to implement a content-addressable system in Rust.  Files are split into chunks, each chunk is hashed and stored once, and files are represented by sequences of chunk IDs.  The data store also tracks file paths (including renames) and maps them to file IDs.

> **Note:** This project is under active development and may not yet fully realize all content-addressable system features.

## Crates
- `common`  : Shared types and utilities (`FileID`, `ChunkID`, default sync directory, etc.)
- `store`   : Persistence layer with `DataStore`, chunk/file tables, and path lookup (for renames).
- `diff-d`  : Service daemon that monitors filesystem events and syncs file content.
- `ctl`     : Simple CLI for interacting with the data store.

## Getting Started

### Prerequisites
- Rust toolchain (edition 2024)
- SQLite (via `sqlx` Any driver)

### Build
```bash
cargo build --workspace
```

### Run the service
```bash
cargo run -p diff-d -- --config path/to/config.toml
```

### Use the CLI
```bash
cargo run -p ctl -- <command> [args]
```

## License

This project is licensed under the GNU General Public License v3.0 or later (`GPL-3.0-or-later`).
See [LICENSE](LICENSE) for details.