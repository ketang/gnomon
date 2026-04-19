CREATE TABLE codex_rollout_session (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    source_file_id INTEGER NOT NULL REFERENCES source_file(id) ON DELETE CASCADE,
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    session_id TEXT,
    raw_cwd_path TEXT,
    cli_version TEXT,
    originator TEXT,
    model_provider TEXT,
    model_name TEXT,
    started_at_utc TEXT,
    completed_at_utc TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    imported_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_file_id)
);

CREATE INDEX idx_codex_rollout_session_session_id
    ON codex_rollout_session(session_id);

CREATE TABLE codex_rollout_event (
    id INTEGER PRIMARY KEY,
    codex_rollout_session_id INTEGER NOT NULL REFERENCES codex_rollout_session(id) ON DELETE CASCADE,
    source_file_id INTEGER NOT NULL REFERENCES source_file(id) ON DELETE CASCADE,
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    source_line_no INTEGER NOT NULL CHECK (source_line_no >= 1),
    event_kind TEXT NOT NULL,
    recorded_at_utc TEXT,
    raw_json TEXT NOT NULL,
    UNIQUE (source_file_id, source_line_no)
);

CREATE INDEX idx_codex_rollout_event_session_line
    ON codex_rollout_event(codex_rollout_session_id, source_line_no);
