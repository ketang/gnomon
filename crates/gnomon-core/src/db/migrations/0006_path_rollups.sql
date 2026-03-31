CREATE TABLE chunk_path_rollup (
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    display_category TEXT NOT NULL,
    classification_state TEXT NOT NULL
        CHECK (classification_state IN ('classified', 'mixed', 'unclassified')),
    normalized_action TEXT,
    command_family TEXT,
    base_command TEXT,
    parent_path TEXT,
    child_path TEXT NOT NULL,
    child_label TEXT NOT NULL,
    child_kind TEXT NOT NULL CHECK (child_kind IN ('directory', 'file')),
    leaf_file_path TEXT NOT NULL,
    input_tokens REAL NOT NULL DEFAULT 0.0,
    cache_creation_input_tokens REAL NOT NULL DEFAULT 0.0,
    cache_read_input_tokens REAL NOT NULL DEFAULT 0.0,
    output_tokens REAL NOT NULL DEFAULT 0.0,
    PRIMARY KEY (
        import_chunk_id,
        display_category,
        classification_state,
        normalized_action,
        command_family,
        base_command,
        parent_path,
        child_path,
        leaf_file_path
    )
);

CREATE INDEX idx_chunk_path_rollup_parent
    ON chunk_path_rollup(
        display_category,
        classification_state,
        normalized_action,
        command_family,
        base_command,
        parent_path,
        child_path
    );

CREATE INDEX idx_chunk_path_rollup_leaf
    ON chunk_path_rollup(import_chunk_id, leaf_file_path);
