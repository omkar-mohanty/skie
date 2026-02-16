-- 1. Metadata: Identifies the file
CREATE TABLE files (
    file_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    path TEXT UNIQUE NOT NULL,
    hash BLOB NOT NULL -- Helps verify if the whole file changed
);

-- 2. Deduplication: Identifies the unique data
-- Even if 100 files use this chunk, there is only ONE row here.
CREATE TABLE chunks (
    hash BLOB PRIMARY KEY,
    size INTEGER NOT NULL
);

-- 3. The Link: Reconstructs the file
-- This is where the file_id lives!
CREATE TABLE file_sections (
    file_id TEXT,
    chunk_hash BLOB,
    chunk_index INTEGER, -- 0, 1, 2...
    offset INTEGER,      -- Where it starts
    PRIMARY KEY (file_id, chunk_index),
    FOREIGN KEY(file_id) REFERENCES files(file_id),
    FOREIGN KEY(chunk_hash) REFERENCES chunks(hash)
);
