CREATE TABLE IF NOT EXISTS files (
    file_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    hash BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
    hash BLOB PRIMARY KEY,
    file_id TEXT,
    size INTEGER,
    FOREIGN KEY(file_id) REFERENCES files(file_id)
);
