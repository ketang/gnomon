ALTER TABLE import_chunk
    ADD COLUMN imported_conversation_count INTEGER NOT NULL DEFAULT 0
        CHECK (imported_conversation_count >= 0);

ALTER TABLE import_chunk
    ADD COLUMN imported_turn_count INTEGER NOT NULL DEFAULT 0
        CHECK (imported_turn_count >= 0);

CREATE TABLE chunk_action_rollup (
    import_chunk_id INTEGER NOT NULL REFERENCES import_chunk(id) ON DELETE CASCADE,
    display_category TEXT NOT NULL,
    classification_state TEXT NOT NULL
        CHECK (classification_state IN ('classified', 'mixed', 'unclassified')),
    normalized_action TEXT,
    command_family TEXT,
    base_command TEXT,
    input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    cache_creation_input_tokens INTEGER NOT NULL DEFAULT 0
        CHECK (cache_creation_input_tokens >= 0),
    cache_read_input_tokens INTEGER NOT NULL DEFAULT 0
        CHECK (cache_read_input_tokens >= 0),
    output_tokens INTEGER NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    action_count INTEGER NOT NULL DEFAULT 0 CHECK (action_count >= 0),
    PRIMARY KEY (
        import_chunk_id,
        display_category,
        classification_state,
        normalized_action,
        command_family,
        base_command
    )
);

CREATE INDEX idx_chunk_action_rollup_category_action
    ON chunk_action_rollup(
        display_category,
        classification_state,
        normalized_action,
        command_family,
        base_command
    );

UPDATE import_chunk
SET
    imported_conversation_count = (
        SELECT COUNT(DISTINCT stream.conversation_id)
        FROM stream
        WHERE stream.import_chunk_id = import_chunk.id
    ),
    imported_turn_count = (
        SELECT COUNT(*)
        FROM turn
        WHERE turn.import_chunk_id = import_chunk.id
    );

INSERT INTO chunk_action_rollup (
    import_chunk_id,
    display_category,
    classification_state,
    normalized_action,
    command_family,
    base_command,
    input_tokens,
    cache_creation_input_tokens,
    cache_read_input_tokens,
    output_tokens,
    action_count
)
SELECT
    action.import_chunk_id,
    CASE
        WHEN action.category IS NOT NULL THEN action.category
        WHEN action.classification_state = 'mixed' THEN 'mixed'
        WHEN action.classification_state = 'unclassified' THEN 'unclassified'
        ELSE 'classified'
    END AS display_category,
    action.classification_state,
    action.normalized_action,
    action.command_family,
    action.base_command,
    COALESCE(SUM(COALESCE(action.input_tokens, 0)), 0),
    COALESCE(SUM(COALESCE(action.cache_creation_input_tokens, 0)), 0),
    COALESCE(SUM(COALESCE(action.cache_read_input_tokens, 0)), 0),
    COALESCE(SUM(COALESCE(action.output_tokens, 0)), 0),
    COUNT(*)
FROM action
GROUP BY
    action.import_chunk_id,
    display_category,
    action.classification_state,
    action.normalized_action,
    action.command_family,
    action.base_command;
