use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::{Connection, params};

use crate::perf::{PerfLogger, PerfScope};

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

const DELETE_CHUNK_PATH_ROLLUPS_SQL: &str = "
    DELETE FROM chunk_path_rollup
    WHERE import_chunk_id = ?1
";

const LOAD_CHUNK_PATH_FACTS_SQL: &str = "
    SELECT
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
        project.root_path,
        path_node.full_path,
        COALESCE(message.input_tokens, 0),
        COALESCE(message.cache_creation_input_tokens, 0),
        COALESCE(message.cache_read_input_tokens, 0),
        COALESCE(message.output_tokens, 0),
        (
            SELECT COUNT(*)
            FROM message_path_ref ref_count
            WHERE ref_count.message_id = message.id
        ) AS ref_count
    FROM action
    JOIN action_message ON action_message.action_id = action.id
    JOIN message ON message.id = action_message.message_id
    JOIN conversation ON conversation.id = message.conversation_id
    JOIN project ON project.id = conversation.project_id
    JOIN message_path_ref ON message_path_ref.message_id = message.id
    JOIN path_node ON path_node.id = message_path_ref.path_node_id
    WHERE action.import_chunk_id = ?1
      AND path_node.node_kind = 'file'
";
// Invariant: this query runs on a shard transaction during import finalization.
// Every JOIN target must be a shard-resident table. `import_chunk` and
// `source_file` live only in the main DB under p4-c1 sharding — do not add
// JOINs to them here.

const INSERT_CHUNK_PATH_ROLLUP_SQL: &str = "
    INSERT INTO chunk_path_rollup (
        import_chunk_id,
        display_category,
        classification_state,
        normalized_action,
        command_family,
        base_command,
        parent_path,
        child_path,
        child_label,
        child_kind,
        leaf_file_path,
        input_tokens,
        cache_creation_input_tokens,
        cache_read_input_tokens,
        output_tokens
    )
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
";

#[derive(Debug)]
struct LoadedChunkPathFact {
    display_category: String,
    classification_state: String,
    normalized_action: Option<String>,
    command_family: Option<String>,
    base_command: Option<String>,
    project_root: String,
    file_path: String,
    input_tokens: i64,
    cache_creation_input_tokens: i64,
    cache_read_input_tokens: i64,
    output_tokens: i64,
    ref_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ChunkPathRollupKey {
    display_category: String,
    classification_state: String,
    normalized_action: Option<String>,
    command_family: Option<String>,
    base_command: Option<String>,
    parent_path: Option<String>,
    child_path: String,
    child_label: String,
    child_kind: String,
    leaf_file_path: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct ChunkPathRollupMetrics {
    input_tokens: f64,
    cache_creation_input_tokens: f64,
    cache_read_input_tokens: f64,
    output_tokens: f64,
}

pub(crate) fn clear_chunk_action_rollups(conn: &Connection, import_chunk_id: i64) -> Result<()> {
    conn.execute(DELETE_CHUNK_ACTION_ROLLUPS_SQL, [import_chunk_id])
        .with_context(|| {
            format!("unable to clear chunk action rollups for import chunk {import_chunk_id}")
        })?;
    Ok(())
}

pub(crate) fn rebuild_chunk_action_rollups(
    conn: &Connection,
    import_chunk_id: i64,
    perf_logger: Option<PerfLogger>,
) -> Result<()> {
    let mut scope = PerfScope::new(perf_logger, "import.rebuild_action_rollups");
    scope.field("import_chunk_id", import_chunk_id);
    let result = (|| -> Result<()> {
        clear_chunk_action_rollups(conn, import_chunk_id)?;
        conn.execute(INSERT_CHUNK_ACTION_ROLLUPS_SQL, params![import_chunk_id])
            .with_context(|| {
                format!("unable to rebuild chunk action rollups for import chunk {import_chunk_id}")
            })?;
        Ok(())
    })();
    match &result {
        Ok(()) => scope.finish_ok(),
        Err(err) => scope.finish_error(err),
    }
    result
}

pub(crate) fn clear_chunk_path_rollups(conn: &Connection, import_chunk_id: i64) -> Result<()> {
    conn.execute(DELETE_CHUNK_PATH_ROLLUPS_SQL, [import_chunk_id])
        .with_context(|| {
            format!("unable to clear chunk path rollups for import chunk {import_chunk_id}")
        })?;
    Ok(())
}

pub(crate) fn rebuild_chunk_path_rollups(
    conn: &Connection,
    import_chunk_id: i64,
    perf_logger: Option<PerfLogger>,
) -> Result<()> {
    let mut scope = PerfScope::new(perf_logger, "import.rebuild_path_rollups");
    scope.field("import_chunk_id", import_chunk_id);
    let result = rebuild_chunk_path_rollups_inner(conn, import_chunk_id);
    match &result {
        Ok(()) => scope.finish_ok(),
        Err(err) => scope.finish_error(err),
    }
    result
}

fn rebuild_chunk_path_rollups_inner(conn: &Connection, import_chunk_id: i64) -> Result<()> {
    clear_chunk_path_rollups(conn, import_chunk_id)?;

    let mut stmt = conn.prepare(LOAD_CHUNK_PATH_FACTS_SQL).with_context(|| {
        format!("unable to prepare chunk path fact query for {import_chunk_id}")
    })?;
    let rows = stmt.query_map([import_chunk_id], |row| {
        Ok(LoadedChunkPathFact {
            display_category: row.get(0)?,
            classification_state: row.get(1)?,
            normalized_action: row.get(2)?,
            command_family: row.get(3)?,
            base_command: row.get(4)?,
            project_root: row.get(5)?,
            file_path: row.get(6)?,
            input_tokens: row.get(7)?,
            cache_creation_input_tokens: row.get(8)?,
            cache_read_input_tokens: row.get(9)?,
            output_tokens: row.get(10)?,
            ref_count: row.get(11)?,
        })
    })?;

    let mut aggregated = BTreeMap::<ChunkPathRollupKey, ChunkPathRollupMetrics>::new();
    for row in rows {
        let row = row.with_context(|| {
            format!("unable to read chunk path fact row for import chunk {import_chunk_id}")
        })?;
        let Some(relative_path) = relative_file_path(&row.project_root, &row.file_path) else {
            continue;
        };
        let components = relative_path
            .components()
            .map(|component| component.as_os_str().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        if components.is_empty() || row.ref_count <= 0 {
            continue;
        }

        let divisor = row.ref_count as f64;
        let distributed = ChunkPathRollupMetrics {
            input_tokens: row.input_tokens as f64 / divisor,
            cache_creation_input_tokens: row.cache_creation_input_tokens as f64 / divisor,
            cache_read_input_tokens: row.cache_read_input_tokens as f64 / divisor,
            output_tokens: row.output_tokens as f64 / divisor,
        };

        let mut parent_path = None::<PathBuf>;
        let mut child_path = PathBuf::from(&row.project_root);
        for (index, component) in components.iter().enumerate() {
            child_path.push(component);
            let child_kind = if index + 1 == components.len() {
                "file"
            } else {
                "directory"
            };
            let key = ChunkPathRollupKey {
                display_category: row.display_category.clone(),
                classification_state: row.classification_state.clone(),
                normalized_action: row.normalized_action.clone(),
                command_family: row.command_family.clone(),
                base_command: row.base_command.clone(),
                parent_path: parent_path
                    .as_ref()
                    .map(|path| path.to_string_lossy().to_string()),
                child_path: child_path.to_string_lossy().to_string(),
                child_label: component.clone(),
                child_kind: child_kind.to_string(),
                leaf_file_path: row.file_path.clone(),
            };
            let entry = aggregated.entry(key).or_default();
            entry.input_tokens += distributed.input_tokens;
            entry.cache_creation_input_tokens += distributed.cache_creation_input_tokens;
            entry.cache_read_input_tokens += distributed.cache_read_input_tokens;
            entry.output_tokens += distributed.output_tokens;
            parent_path = Some(child_path.clone());
        }
    }

    let mut insert = conn
        .prepare(INSERT_CHUNK_PATH_ROLLUP_SQL)
        .with_context(|| {
            format!("unable to prepare chunk path rollup insert statement for {import_chunk_id}")
        })?;
    for (key, metrics) in aggregated {
        insert
            .execute(params![
                import_chunk_id,
                key.display_category,
                key.classification_state,
                key.normalized_action,
                key.command_family,
                key.base_command,
                key.parent_path,
                key.child_path,
                key.child_label,
                key.child_kind,
                key.leaf_file_path,
                metrics.input_tokens,
                metrics.cache_creation_input_tokens,
                metrics.cache_read_input_tokens,
                metrics.output_tokens,
            ])
            .with_context(|| {
                format!("unable to insert chunk path rollup row for import chunk {import_chunk_id}")
            })?;
    }

    Ok(())
}

fn relative_file_path<'a>(project_root: &str, file_path: &'a str) -> Option<&'a Path> {
    Path::new(file_path).strip_prefix(project_root).ok()
}
