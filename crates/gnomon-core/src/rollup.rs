use anyhow::{Context, Result};
use rusqlite::{Connection, params};

const DELETE_CHUNK_ACTION_ROLLUPS_SQL: &str = "
    DELETE FROM chunk_action_rollup
    WHERE import_chunk_id = ?1
";

const INSERT_CHUNK_ACTION_ROLLUPS_SQL: &str = "
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
            WHEN action.classification_state = 'mixed' THEN '[mixed]'
            WHEN action.classification_state = 'unclassified' THEN '[unclassified]'
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
    WHERE action.import_chunk_id = ?1
    GROUP BY
        action.import_chunk_id,
        display_category,
        action.classification_state,
        action.normalized_action,
        action.command_family,
        action.base_command
";

pub(crate) fn clear_chunk_action_rollups(conn: &Connection, import_chunk_id: i64) -> Result<()> {
    conn.execute(DELETE_CHUNK_ACTION_ROLLUPS_SQL, [import_chunk_id])
        .with_context(|| {
            format!("unable to clear chunk action rollups for import chunk {import_chunk_id}")
        })?;
    Ok(())
}

pub(crate) fn rebuild_chunk_action_rollups(conn: &Connection, import_chunk_id: i64) -> Result<()> {
    clear_chunk_action_rollups(conn, import_chunk_id)?;
    conn.execute(INSERT_CHUNK_ACTION_ROLLUPS_SQL, params![import_chunk_id])
        .with_context(|| {
            format!("unable to rebuild chunk action rollups for import chunk {import_chunk_id}")
        })?;
    Ok(())
}
