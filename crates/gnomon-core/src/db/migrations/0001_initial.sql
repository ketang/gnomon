CREATE TABLE project (
    id INTEGER PRIMARY KEY,
    identity_kind TEXT NOT NULL CHECK (identity_kind IN ('git', 'path')),
    canonical_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    root_path TEXT NOT NULL,
    git_root_path TEXT,
    git_origin TEXT,
    created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (canonical_key)
);

CREATE INDEX idx_project_root_path ON project(root_path);

CREATE TABLE source_file (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    relative_path TEXT NOT NULL,
    discovered_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_at_utc TEXT,
    size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    content_fingerprint TEXT,
    UNIQUE (project_id, relative_path)
);

CREATE TABLE import_chunk (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    chunk_day_local TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'complete', 'failed')),
    publish_seq INTEGER,
    started_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at_utc TEXT,
    imported_record_count INTEGER NOT NULL DEFAULT 0 CHECK (imported_record_count >= 0),
    imported_message_count INTEGER NOT NULL DEFAULT 0 CHECK (imported_message_count >= 0),
    imported_action_count INTEGER NOT NULL DEFAULT 0 CHECK (imported_action_count >= 0),
    UNIQUE (project_id, chunk_day_local),
    UNIQUE (publish_seq)
);

CREATE INDEX idx_import_chunk_project_state_day
    ON import_chunk(project_id, state, chunk_day_local DESC);

CREATE INDEX idx_import_chunk_state_publish_seq
    ON import_chunk(state, publish_seq);

CREATE TABLE conversation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    source_file_id INTEGER NOT NULL REFERENCES source_file(id) ON DELETE CASCADE,
    external_id TEXT,
    title TEXT,
    started_at_utc TEXT,
    ended_at_utc TEXT,
    imported_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_file_id),
    UNIQUE (project_id, external_id)
);

CREATE INDEX idx_conversation_project_started_at
    ON conversation(project_id, started_at_utc);

CREATE TABLE stream (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL REFERENCES conversation(id) ON DELETE CASCADE,
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    external_id TEXT,
    stream_kind TEXT NOT NULL,
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    opened_at_utc TEXT,
    closed_at_utc TEXT,
    UNIQUE (conversation_id, sequence_no),
    UNIQUE (conversation_id, external_id)
);

CREATE INDEX idx_stream_chunk_conversation_sequence
    ON stream(import_chunk_id, conversation_id, sequence_no);

CREATE TABLE record (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    source_file_id INTEGER NOT NULL REFERENCES source_file(id) ON DELETE CASCADE,
    conversation_id INTEGER NOT NULL REFERENCES conversation(id) ON DELETE CASCADE,
    stream_id INTEGER REFERENCES stream(id) ON DELETE SET NULL,
    source_line_no INTEGER NOT NULL CHECK (source_line_no > 0),
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    record_kind TEXT NOT NULL,
    recorded_at_utc TEXT,
    UNIQUE (source_file_id, source_line_no),
    UNIQUE (conversation_id, sequence_no)
);

CREATE INDEX idx_record_chunk_conversation_sequence
    ON record(import_chunk_id, conversation_id, sequence_no);

CREATE INDEX idx_record_stream_sequence
    ON record(stream_id, sequence_no);

CREATE TABLE message (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id INTEGER NOT NULL REFERENCES stream(id) ON DELETE CASCADE,
    conversation_id INTEGER NOT NULL REFERENCES conversation(id) ON DELETE CASCADE,
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    external_id TEXT,
    role TEXT NOT NULL,
    message_kind TEXT NOT NULL,
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    created_at_utc TEXT,
    completed_at_utc TEXT,
    input_tokens INTEGER CHECK (input_tokens >= 0),
    cache_creation_input_tokens INTEGER CHECK (cache_creation_input_tokens >= 0),
    cache_read_input_tokens INTEGER CHECK (cache_read_input_tokens >= 0),
    output_tokens INTEGER CHECK (output_tokens >= 0),
    model_name TEXT,
    stop_reason TEXT,
    usage_source TEXT,
    UNIQUE (conversation_id, sequence_no),
    UNIQUE (conversation_id, external_id)
);

CREATE INDEX idx_message_chunk_conversation_sequence
    ON message(import_chunk_id, conversation_id, sequence_no);

CREATE INDEX idx_message_conversation_role_sequence
    ON message(conversation_id, role, sequence_no);

CREATE TABLE message_part (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL REFERENCES message(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    part_kind TEXT NOT NULL,
    mime_type TEXT,
    text_value TEXT,
    tool_name TEXT,
    tool_call_id TEXT,
    metadata_json TEXT,
    is_error INTEGER NOT NULL DEFAULT 0 CHECK (is_error IN (0, 1)),
    UNIQUE (message_id, ordinal)
);

CREATE TABLE turn (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id INTEGER NOT NULL REFERENCES stream(id) ON DELETE CASCADE,
    conversation_id INTEGER NOT NULL REFERENCES conversation(id) ON DELETE CASCADE,
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    root_message_id INTEGER NOT NULL REFERENCES message(id) ON DELETE CASCADE,
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    started_at_utc TEXT,
    ended_at_utc TEXT,
    input_tokens INTEGER CHECK (input_tokens >= 0),
    cache_creation_input_tokens INTEGER CHECK (cache_creation_input_tokens >= 0),
    cache_read_input_tokens INTEGER CHECK (cache_read_input_tokens >= 0),
    output_tokens INTEGER CHECK (output_tokens >= 0),
    UNIQUE (conversation_id, sequence_no)
);

CREATE INDEX idx_turn_chunk_conversation_sequence
    ON turn(import_chunk_id, conversation_id, sequence_no);

CREATE TABLE turn_message (
    turn_id INTEGER NOT NULL REFERENCES turn(id) ON DELETE CASCADE,
    message_id INTEGER NOT NULL REFERENCES message(id) ON DELETE CASCADE,
    ordinal_in_turn INTEGER NOT NULL CHECK (ordinal_in_turn >= 0),
    PRIMARY KEY (turn_id, message_id),
    UNIQUE (turn_id, ordinal_in_turn),
    UNIQUE (message_id)
);

CREATE TABLE action (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    turn_id INTEGER NOT NULL REFERENCES turn(id) ON DELETE CASCADE,
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    sequence_no INTEGER NOT NULL CHECK (sequence_no >= 0),
    category TEXT NOT NULL,
    label TEXT NOT NULL,
    classifier TEXT NOT NULL DEFAULT 'deterministic_v1',
    started_at_utc TEXT,
    ended_at_utc TEXT,
    input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    cache_creation_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (cache_creation_input_tokens >= 0),
    cache_read_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (cache_read_input_tokens >= 0),
    output_tokens INTEGER NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    message_count INTEGER NOT NULL DEFAULT 0 CHECK (message_count >= 0),
    UNIQUE (turn_id, sequence_no)
);

CREATE INDEX idx_action_chunk_category_label
    ON action(import_chunk_id, category, label);

CREATE TABLE action_message (
    action_id INTEGER NOT NULL REFERENCES action(id) ON DELETE CASCADE,
    message_id INTEGER NOT NULL REFERENCES message(id) ON DELETE CASCADE,
    ordinal_in_action INTEGER NOT NULL CHECK (ordinal_in_action >= 0),
    PRIMARY KEY (action_id, message_id),
    UNIQUE (action_id, ordinal_in_action),
    UNIQUE (message_id)
);

CREATE TABLE path_node (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    parent_id INTEGER REFERENCES path_node(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    full_path TEXT NOT NULL,
    node_kind TEXT NOT NULL CHECK (node_kind IN ('root', 'dir', 'file')),
    depth INTEGER NOT NULL CHECK (depth >= 0),
    UNIQUE (project_id, full_path),
    UNIQUE (project_id, parent_id, name)
);

CREATE INDEX idx_path_node_project_parent
    ON path_node(project_id, parent_id, name);

CREATE TABLE message_path_ref (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL REFERENCES message(id) ON DELETE CASCADE,
    message_part_id INTEGER REFERENCES message_part(id) ON DELETE SET NULL,
    path_node_id INTEGER NOT NULL REFERENCES path_node(id) ON DELETE CASCADE,
    ref_kind TEXT NOT NULL CHECK (ref_kind IN ('read', 'write', 'edit', 'multiedit')),
    ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
    UNIQUE (message_id, path_node_id, ref_kind, ordinal)
);

CREATE INDEX idx_message_path_ref_path_kind
    ON message_path_ref(path_node_id, ref_kind, message_id);

CREATE TABLE import_warning (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    source_file_id INTEGER REFERENCES source_file(id) ON DELETE SET NULL,
    conversation_id INTEGER REFERENCES conversation(id) ON DELETE SET NULL,
    code TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'warning' CHECK (severity IN ('info', 'warning', 'error')),
    message TEXT NOT NULL,
    created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_import_warning_chunk_severity
    ON import_warning(import_chunk_id, severity);
