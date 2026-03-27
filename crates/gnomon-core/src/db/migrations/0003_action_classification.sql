DROP TABLE IF EXISTS action_message;
DROP INDEX IF EXISTS idx_action_chunk_category_label;
DROP TABLE IF EXISTS action;

CREATE TABLE action (
    id INTEGER PRIMARY KEY,
    turn_id INTEGER NOT NULL REFERENCES turn(id) ON DELETE CASCADE,
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    category TEXT,
    normalized_action TEXT,
    command_family TEXT,
    base_command TEXT,
    classification_state TEXT NOT NULL
        CHECK (classification_state IN ('classified', 'mixed', 'unclassified')),
    classifier TEXT NOT NULL DEFAULT 'deterministic_v1',
    started_at_utc TEXT,
    ended_at_utc TEXT,
    input_tokens INTEGER CHECK (input_tokens >= 0),
    cache_creation_input_tokens INTEGER CHECK (cache_creation_input_tokens >= 0),
    cache_read_input_tokens INTEGER CHECK (cache_read_input_tokens >= 0),
    output_tokens INTEGER CHECK (output_tokens >= 0),
    message_count INTEGER NOT NULL DEFAULT 0 CHECK (message_count >= 0),
    UNIQUE (turn_id, sequence_no)
);

CREATE INDEX idx_action_chunk_classification
    ON action(import_chunk_id, category, normalized_action, command_family, base_command);

CREATE TABLE action_message (
    action_id INTEGER NOT NULL REFERENCES action(id) ON DELETE CASCADE,
    message_id INTEGER NOT NULL REFERENCES message(id) ON DELETE CASCADE,
    ordinal_in_action INTEGER NOT NULL CHECK (ordinal_in_action >= 0),
    PRIMARY KEY (action_id, message_id),
    UNIQUE (action_id, ordinal_in_action),
    UNIQUE (message_id)
);
