CREATE TABLE codex_session_index_entry (
    id INTEGER PRIMARY KEY,
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    source_file_id INTEGER NOT NULL REFERENCES source_file(id) ON DELETE CASCADE,
    source_line_no INTEGER NOT NULL CHECK (source_line_no >= 1),
    session_id TEXT,
    first_seen_at_utc TEXT,
    last_seen_at_utc TEXT,
    raw_cwd_path TEXT,
    rollout_relative_path TEXT,
    raw_json TEXT NOT NULL,
    imported_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_file_id, source_line_no)
);

CREATE INDEX idx_codex_session_index_entry_session_id
    ON codex_session_index_entry(session_id);
