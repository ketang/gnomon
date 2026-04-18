CREATE TABLE pending_chunk_rebuild (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    chunk_day_local TEXT NOT NULL,
    created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (project_id, chunk_day_local)
);

CREATE INDEX idx_pending_chunk_rebuild_project_day
    ON pending_chunk_rebuild(project_id, chunk_day_local);
