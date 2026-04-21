CREATE TABLE skill_invocation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    history_event_id INTEGER NOT NULL REFERENCES history_event(id) ON DELETE CASCADE,
    source_file_id INTEGER NOT NULL REFERENCES source_file(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    recorded_at_utc TEXT,
    raw_project TEXT,
    skill_name TEXT NOT NULL,
    invocation_kind TEXT NOT NULL CHECK (invocation_kind IN ('explicit_history')),
    UNIQUE (history_event_id),
    UNIQUE (source_file_id, session_id, recorded_at_utc, skill_name, invocation_kind)
);

CREATE INDEX idx_skill_invocation_skill_session
    ON skill_invocation(skill_name, session_id, recorded_at_utc);

CREATE INDEX idx_skill_invocation_session
    ON skill_invocation(session_id, recorded_at_utc);
