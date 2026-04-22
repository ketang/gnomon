ALTER TABLE source_file
    ADD COLUMN source_kind TEXT NOT NULL DEFAULT 'transcript'
        CHECK (source_kind IN ('transcript', 'claude_history'));

CREATE TABLE history_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    source_file_id INTEGER NOT NULL REFERENCES source_file(id) ON DELETE CASCADE,
    source_line_no INTEGER NOT NULL CHECK (source_line_no > 0),
    session_id TEXT,
    recorded_at_utc TEXT,
    raw_project TEXT,
    display_text TEXT,
    pasted_contents_json TEXT,
    input_kind TEXT NOT NULL
        CHECK (input_kind IN ('plain_prompt', 'slash_command', 'other')),
    slash_command_name TEXT,
    raw_json TEXT NOT NULL,
    UNIQUE (source_file_id, source_line_no)
);

CREATE INDEX idx_history_event_session_timestamp
    ON history_event(session_id, recorded_at_utc);

CREATE INDEX idx_history_event_timestamp
    ON history_event(recorded_at_utc);
