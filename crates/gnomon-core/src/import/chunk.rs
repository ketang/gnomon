use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use jiff::{Timestamp, ToSpan, tz::TimeZone};
use rusqlite::{Connection, Transaction, params};
use tracing::error;

use crate::classify::{BuildActionsParams, build_actions};
use crate::db::Database;
use crate::query::SnapshotBounds;

use super::{
    NormalizeJsonlFileParams, STARTUP_IMPORT_WINDOW_HOURS, STARTUP_OPEN_DEADLINE_SECS,
    normalize_jsonl_file,
};

const IMPORTER_THREAD_NAME: &str = "gnomon-importer";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupOpenReason {
    Last24hReady,
    TimedOut,
}

pub struct StartupImport {
    pub snapshot: SnapshotBounds,
    pub open_reason: StartupOpenReason,
    worker: Option<JoinHandle<Result<()>>>,
}

impl StartupImport {
    #[cfg(test)]
    fn wait_for_completion(mut self) -> Result<()> {
        join_worker(self.worker.take())
    }
}

impl Drop for StartupImport {
    fn drop(&mut self) {
        let _ = self.worker.take();
    }
}

#[derive(Debug, Clone, Default)]
struct ImportWorkerOptions {
    per_chunk_delay: Duration,
}

#[derive(Debug)]
enum StartupWorkerEvent {
    StartupReady,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportPlan {
    startup_chunks: Vec<ChunkCandidate>,
    deferred_chunks: Vec<ChunkCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedImportPlan {
    startup_chunks: Vec<PreparedChunk>,
    deferred_chunks: Vec<PreparedChunk>,
}

impl PreparedImportPlan {
    fn is_empty(&self) -> bool {
        self.startup_chunks.is_empty() && self.deferred_chunks.is_empty()
    }

    fn all_chunks(&self) -> impl Iterator<Item = &PreparedChunk> {
        self.startup_chunks
            .iter()
            .chain(self.deferred_chunks.iter())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ChunkDescriptor {
    project_id: i64,
    project_key: String,
    chunk_day_local: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChunkCandidate {
    project_id: i64,
    project_key: String,
    chunk_day_local: String,
    source_files: Vec<ChunkSourceFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedChunk {
    import_chunk_id: i64,
    project_id: i64,
    project_key: String,
    chunk_day_local: String,
    source_files: Vec<ChunkSourceFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChunkSourceFile {
    source_file_id: i64,
    relative_path: String,
}

#[derive(Debug)]
struct SourceFileRow {
    source_file_id: i64,
    project_id: i64,
    project_key: String,
    relative_path: String,
    modified_at_utc: Option<String>,
    discovered_at_utc: String,
    size_bytes: i64,
    imported_size_bytes: Option<i64>,
    imported_modified_at_utc: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportExecutionReport {
    pub startup_chunk_count: usize,
    pub deferred_chunk_count: usize,
}

pub fn start_startup_import(
    conn: &Connection,
    db_path: &Path,
    source_root: &Path,
) -> Result<StartupImport> {
    start_startup_import_with_options(
        conn,
        db_path,
        source_root,
        Duration::from_secs(STARTUP_OPEN_DEADLINE_SECS),
        ImportWorkerOptions::default(),
    )
}

pub fn import_all(
    conn: &Connection,
    db_path: &Path,
    source_root: &Path,
) -> Result<ImportExecutionReport> {
    let now = Timestamp::now();
    let time_zone = TimeZone::system();
    let plan = build_import_plan(conn, now, &time_zone)?;
    let prepared = prepare_import_plan(conn, &plan)?;
    let report = ImportExecutionReport {
        startup_chunk_count: prepared.startup_chunks.len(),
        deferred_chunk_count: prepared.deferred_chunks.len(),
    };

    let mut database =
        Database::open(db_path).with_context(|| format!("unable to open {}", db_path.display()))?;

    for chunk in &prepared.startup_chunks {
        import_chunk(
            &mut database,
            source_root,
            chunk,
            &ImportWorkerOptions::default(),
        )
        .with_context(|| {
            format!(
                "unable to import startup chunk {}:{}",
                chunk.project_key, chunk.chunk_day_local
            )
        })?;
    }

    for chunk in &prepared.deferred_chunks {
        import_chunk(
            &mut database,
            source_root,
            chunk,
            &ImportWorkerOptions::default(),
        )
        .with_context(|| {
            format!(
                "unable to import deferred chunk {}:{}",
                chunk.project_key, chunk.chunk_day_local
            )
        })?;
    }

    Ok(report)
}

fn start_startup_import_with_options(
    conn: &Connection,
    db_path: &Path,
    source_root: &Path,
    wait_timeout: Duration,
    worker_options: ImportWorkerOptions,
) -> Result<StartupImport> {
    let now = Timestamp::now();
    let time_zone = TimeZone::system();
    let plan = build_import_plan(conn, now, &time_zone)?;
    let prepared = prepare_import_plan(conn, &plan)?;

    if prepared.is_empty() {
        return Ok(StartupImport {
            snapshot: SnapshotBounds::load(conn)?,
            open_reason: StartupOpenReason::Last24hReady,
            worker: None,
        });
    }

    let (sender, receiver) = mpsc::channel();
    let db_path = db_path.to_path_buf();
    let source_root = source_root.to_path_buf();
    let prepared_for_worker = prepared.clone();
    let sender_for_worker = sender;

    let worker = match thread::Builder::new()
        .name(IMPORTER_THREAD_NAME.to_string())
        .spawn(move || {
            let result = run_import_worker(
                &db_path,
                &source_root,
                &prepared_for_worker,
                sender_for_worker,
                &worker_options,
            );
            if let Err(err) = &result {
                error!("background importer failed: {err:#}");
            }
            result
        }) {
        Ok(worker) => worker,
        Err(err) => {
            mark_chunks_failed(conn, &prepared)?;
            return Err(anyhow!(err)).context("unable to spawn background importer worker");
        }
    };

    let open_reason = match receiver.recv_timeout(wait_timeout) {
        Ok(StartupWorkerEvent::StartupReady) => StartupOpenReason::Last24hReady,
        Ok(StartupWorkerEvent::Failed(message)) => {
            let worker_result = join_worker(Some(worker));
            return Err(worker_result.err().unwrap_or_else(|| anyhow!(message)))
                .context("background importer failed before the startup gate opened");
        }
        Err(RecvTimeoutError::Timeout) => StartupOpenReason::TimedOut,
        Err(RecvTimeoutError::Disconnected) => {
            let worker_result = join_worker(Some(worker));
            return Err(worker_result.err().unwrap_or_else(|| {
                anyhow!("background importer exited before signaling startup readiness")
            }));
        }
    };

    Ok(StartupImport {
        snapshot: SnapshotBounds::load(conn)?,
        open_reason,
        worker: Some(worker),
    })
}

fn build_import_plan(
    conn: &Connection,
    now: Timestamp,
    time_zone: &TimeZone,
) -> Result<ImportPlan> {
    let source_files = load_source_files(conn)?;
    let startup_days = startup_days(now, time_zone)?;

    let mut current_files_by_chunk = BTreeMap::<ChunkDescriptor, Vec<ChunkSourceFile>>::new();
    let mut selected_chunks = BTreeSet::<ChunkDescriptor>::new();

    for row in source_files {
        let current_timestamp = current_chunk_timestamp(&row);
        let current_day = local_day_for_utc_timestamp(current_timestamp, time_zone)?;
        let descriptor = ChunkDescriptor {
            project_id: row.project_id,
            project_key: row.project_key.clone(),
            chunk_day_local: current_day.clone(),
        };
        current_files_by_chunk
            .entry(descriptor.clone())
            .or_default()
            .push(ChunkSourceFile {
                source_file_id: row.source_file_id,
                relative_path: row.relative_path.clone(),
            });

        if source_file_needs_import(&row) {
            selected_chunks.insert(descriptor.clone());

            if let Some(imported_modified_at_utc) = row.imported_modified_at_utc.as_deref() {
                let imported_day =
                    local_day_for_utc_timestamp(imported_modified_at_utc, time_zone)?;
                if imported_day != current_day {
                    selected_chunks.insert(ChunkDescriptor {
                        project_id: row.project_id,
                        project_key: row.project_key.clone(),
                        chunk_day_local: imported_day,
                    });
                }
            }
        }
    }

    let mut startup_candidates = Vec::new();
    let mut deferred_candidates = Vec::new();

    for descriptor in selected_chunks {
        let mut source_files = current_files_by_chunk
            .remove(&descriptor)
            .unwrap_or_default();
        source_files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));

        let chunk = ChunkCandidate {
            project_id: descriptor.project_id,
            project_key: descriptor.project_key,
            chunk_day_local: descriptor.chunk_day_local,
            source_files,
        };

        if startup_days.contains(&chunk.chunk_day_local) {
            startup_candidates.push(chunk);
        } else {
            deferred_candidates.push(chunk);
        }
    }

    Ok(ImportPlan {
        startup_chunks: round_robin_chunks(startup_candidates),
        deferred_chunks: round_robin_chunks(deferred_candidates),
    })
}

fn load_source_files(conn: &Connection) -> Result<Vec<SourceFileRow>> {
    let mut stmt = conn
        .prepare(
            "
            SELECT
                source_file.id,
                source_file.project_id,
                project.canonical_key,
                source_file.relative_path,
                source_file.modified_at_utc,
                source_file.discovered_at_utc,
                source_file.size_bytes,
                source_file.imported_size_bytes,
                source_file.imported_modified_at_utc
            FROM source_file
            JOIN project ON project.id = source_file.project_id
            ORDER BY project.canonical_key, source_file.relative_path
            ",
        )
        .context("unable to prepare source file import planning query")?;

    let rows = stmt
        .query_map([], |row| {
            Ok(SourceFileRow {
                source_file_id: row.get(0)?,
                project_id: row.get(1)?,
                project_key: row.get(2)?,
                relative_path: row.get(3)?,
                modified_at_utc: row.get(4)?,
                discovered_at_utc: row.get(5)?,
                size_bytes: row.get(6)?,
                imported_size_bytes: row.get(7)?,
                imported_modified_at_utc: row.get(8)?,
            })
        })
        .context("unable to enumerate source files for import planning")?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("unable to decode source file import planning rows")
}

fn current_chunk_timestamp(row: &SourceFileRow) -> &str {
    row.modified_at_utc
        .as_deref()
        .unwrap_or(row.discovered_at_utc.as_str())
}

fn source_file_needs_import(row: &SourceFileRow) -> bool {
    row.imported_size_bytes != Some(row.size_bytes)
        || row.imported_modified_at_utc != row.modified_at_utc
}

fn startup_days(now: Timestamp, time_zone: &TimeZone) -> Result<BTreeSet<String>> {
    let threshold = now
        .checked_sub(STARTUP_IMPORT_WINDOW_HOURS.hours())
        .context("unable to compute the startup import threshold")?;

    let mut days = BTreeSet::new();
    days.insert(now.to_zoned(time_zone.clone()).date().to_string());
    days.insert(threshold.to_zoned(time_zone.clone()).date().to_string());
    Ok(days)
}

fn local_day_for_utc_timestamp(timestamp: &str, time_zone: &TimeZone) -> Result<String> {
    let timestamp = parse_utc_timestamp(timestamp)?;
    Ok(timestamp.to_zoned(time_zone.clone()).date().to_string())
}

fn parse_utc_timestamp(timestamp: &str) -> Result<Timestamp> {
    if let Ok(parsed) = timestamp.parse::<Timestamp>() {
        return Ok(parsed);
    }

    let sqlite_utc = format!("{}Z", timestamp.replace(' ', "T"));
    sqlite_utc
        .parse::<Timestamp>()
        .with_context(|| format!("unable to parse timestamp {timestamp}"))
}

fn round_robin_chunks(chunks: Vec<ChunkCandidate>) -> Vec<ChunkCandidate> {
    let mut grouped = BTreeMap::<String, Vec<ChunkCandidate>>::new();
    for chunk in chunks {
        grouped
            .entry(chunk.project_key.clone())
            .or_default()
            .push(chunk);
    }

    let mut queues = BTreeMap::<String, VecDeque<ChunkCandidate>>::new();
    for (project_key, mut project_chunks) in grouped {
        project_chunks.sort_by(|left, right| right.chunk_day_local.cmp(&left.chunk_day_local));
        queues.insert(project_key, VecDeque::from(project_chunks));
    }

    let mut ordered = Vec::new();
    loop {
        let mut progressed = false;
        for queue in queues.values_mut() {
            if let Some(chunk) = queue.pop_front() {
                ordered.push(chunk);
                progressed = true;
            }
        }

        if !progressed {
            break;
        }
    }

    ordered
}

fn prepare_import_plan(conn: &Connection, plan: &ImportPlan) -> Result<PreparedImportPlan> {
    Ok(PreparedImportPlan {
        startup_chunks: plan
            .startup_chunks
            .iter()
            .map(|chunk| prepare_chunk(conn, chunk))
            .collect::<Result<Vec<_>>>()?,
        deferred_chunks: plan
            .deferred_chunks
            .iter()
            .map(|chunk| prepare_chunk(conn, chunk))
            .collect::<Result<Vec<_>>>()?,
    })
}

fn prepare_chunk(conn: &Connection, chunk: &ChunkCandidate) -> Result<PreparedChunk> {
    let import_chunk_id = conn
        .query_row(
            "
            INSERT INTO import_chunk (
                project_id,
                chunk_day_local,
                state,
                publish_seq,
                completed_at_utc,
                imported_record_count,
                imported_message_count,
                imported_action_count
            )
            VALUES (?1, ?2, 'pending', NULL, NULL, 0, 0, 0)
            ON CONFLICT(project_id, chunk_day_local) DO UPDATE SET
                state = 'pending',
                publish_seq = NULL,
                completed_at_utc = NULL,
                imported_record_count = 0,
                imported_message_count = 0,
                imported_action_count = 0
            RETURNING id
            ",
            params![chunk.project_id, chunk.chunk_day_local],
            |row| row.get(0),
        )
        .with_context(|| {
            format!(
                "unable to prepare import chunk {}:{}",
                chunk.project_key, chunk.chunk_day_local
            )
        })?;

    Ok(PreparedChunk {
        import_chunk_id,
        project_id: chunk.project_id,
        project_key: chunk.project_key.clone(),
        chunk_day_local: chunk.chunk_day_local.clone(),
        source_files: chunk.source_files.clone(),
    })
}

fn mark_chunks_failed(conn: &Connection, plan: &PreparedImportPlan) -> Result<()> {
    for chunk in plan.all_chunks() {
        conn.execute(
            "
            UPDATE import_chunk
            SET state = 'failed', publish_seq = NULL, completed_at_utc = CURRENT_TIMESTAMP
            WHERE id = ?1
            ",
            [chunk.import_chunk_id],
        )
        .with_context(|| {
            format!(
                "unable to mark import chunk {}:{} as failed",
                chunk.project_key, chunk.chunk_day_local
            )
        })?;
    }

    Ok(())
}

fn run_import_worker(
    db_path: &Path,
    source_root: &Path,
    plan: &PreparedImportPlan,
    sender: mpsc::Sender<StartupWorkerEvent>,
    options: &ImportWorkerOptions,
) -> Result<()> {
    let mut database =
        Database::open(db_path).with_context(|| format!("unable to open {}", db_path.display()))?;

    if plan.startup_chunks.is_empty() {
        let _ = sender.send(StartupWorkerEvent::StartupReady);
    }

    for chunk in &plan.startup_chunks {
        if let Err(err) = import_chunk(&mut database, source_root, chunk, options) {
            let _ = sender.send(StartupWorkerEvent::Failed(format!("{err:#}")));
            return Err(err).with_context(|| {
                format!(
                    "unable to import startup chunk {}:{}",
                    chunk.project_key, chunk.chunk_day_local
                )
            });
        }
    }

    if !plan.startup_chunks.is_empty() {
        let _ = sender.send(StartupWorkerEvent::StartupReady);
    }

    for chunk in &plan.deferred_chunks {
        import_chunk(&mut database, source_root, chunk, options).with_context(|| {
            format!(
                "unable to import deferred chunk {}:{}",
                chunk.project_key, chunk.chunk_day_local
            )
        })?;
    }

    Ok(())
}

fn import_chunk(
    database: &mut Database,
    source_root: &Path,
    chunk: &PreparedChunk,
    options: &ImportWorkerOptions,
) -> Result<()> {
    begin_chunk_import(database.connection_mut(), chunk)?;

    if options.per_chunk_delay > Duration::ZERO {
        thread::sleep(options.per_chunk_delay);
    }

    let import_result = (|| {
        for source_file in &chunk.source_files {
            let path = source_root.join(&source_file.relative_path);
            let conversation = normalize_jsonl_file(
                database.connection_mut(),
                &NormalizeJsonlFileParams {
                    project_id: chunk.project_id,
                    source_file_id: source_file.source_file_id,
                    import_chunk_id: chunk.import_chunk_id,
                    path,
                },
            )?;

            let _ = build_actions(
                database.connection_mut(),
                &BuildActionsParams {
                    conversation_id: conversation.conversation_id,
                },
            )?;
        }

        finalize_chunk_import(database.connection_mut(), chunk)?;
        Ok(())
    })();

    if let Err(err) = import_result {
        if let Err(mark_failed_err) =
            mark_chunk_failed(database.connection_mut(), chunk.import_chunk_id)
        {
            error!(
                "unable to mark failed chunk {}:{} after import error: {mark_failed_err:#}",
                chunk.project_key, chunk.chunk_day_local
            );
        }
        return Err(err);
    }

    Ok(())
}

fn begin_chunk_import(conn: &mut Connection, chunk: &PreparedChunk) -> Result<()> {
    let tx = conn
        .transaction()
        .context("unable to start an import chunk transaction")?;
    purge_chunk_data(&tx, chunk.import_chunk_id)?;
    tx.execute(
        "
        UPDATE import_chunk
        SET
            state = 'running',
            publish_seq = NULL,
            started_at_utc = CURRENT_TIMESTAMP,
            completed_at_utc = NULL,
            imported_record_count = 0,
            imported_message_count = 0,
            imported_action_count = 0
        WHERE id = ?1
        ",
        [chunk.import_chunk_id],
    )
    .with_context(|| {
        format!(
            "unable to mark import chunk {}:{} as running",
            chunk.project_key, chunk.chunk_day_local
        )
    })?;
    tx.commit()
        .context("unable to commit import chunk startup transaction")?;
    Ok(())
}

fn purge_chunk_data(tx: &Transaction<'_>, import_chunk_id: i64) -> Result<()> {
    tx.execute(
        "DELETE FROM import_warning WHERE import_chunk_id = ?1",
        [import_chunk_id],
    )
    .context("unable to clear prior chunk warnings")?;

    tx.execute(
        "
        DELETE FROM conversation
        WHERE id IN (
            SELECT DISTINCT conversation_id
            FROM stream
            WHERE import_chunk_id = ?1
            UNION
            SELECT DISTINCT conversation_id
            FROM record
            WHERE import_chunk_id = ?1
            UNION
            SELECT DISTINCT conversation_id
            FROM message
            WHERE import_chunk_id = ?1
            UNION
            SELECT DISTINCT conversation_id
            FROM turn
            WHERE import_chunk_id = ?1
        )
        ",
        [import_chunk_id],
    )
    .context("unable to clear prior conversation state for import chunk")?;

    Ok(())
}

fn finalize_chunk_import(conn: &mut Connection, chunk: &PreparedChunk) -> Result<()> {
    let tx = conn
        .transaction()
        .context("unable to start an import chunk publish transaction")?;

    for source_file in &chunk.source_files {
        tx.execute(
            "
            UPDATE source_file
            SET
                imported_size_bytes = size_bytes,
                imported_modified_at_utc = modified_at_utc
            WHERE id = ?1
            ",
            [source_file.source_file_id],
        )
        .with_context(|| {
            format!(
                "unable to mark source file {} as imported",
                source_file.relative_path
            )
        })?;
    }

    let next_publish_seq: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(publish_seq), 0) + 1 FROM import_chunk",
            [],
            |row| row.get(0),
        )
        .context("unable to allocate the next import publish sequence")?;

    tx.execute(
        "
        UPDATE import_chunk
        SET
            state = 'complete',
            publish_seq = ?2,
            completed_at_utc = CURRENT_TIMESTAMP
        WHERE id = ?1
        ",
        params![chunk.import_chunk_id, next_publish_seq],
    )
    .with_context(|| {
        format!(
            "unable to publish import chunk {}:{}",
            chunk.project_key, chunk.chunk_day_local
        )
    })?;

    tx.commit()
        .context("unable to commit the import chunk publish transaction")?;
    Ok(())
}

fn mark_chunk_failed(conn: &mut Connection, import_chunk_id: i64) -> Result<()> {
    conn.execute(
        "
        UPDATE import_chunk
        SET
            state = 'failed',
            publish_seq = NULL,
            completed_at_utc = CURRENT_TIMESTAMP
        WHERE id = ?1
        ",
        [import_chunk_id],
    )
    .context("unable to mark the import chunk as failed")?;
    Ok(())
}

fn join_worker(worker: Option<JoinHandle<Result<()>>>) -> Result<()> {
    let Some(worker) = worker else {
        return Ok(());
    };

    match worker.join() {
        Ok(result) => result,
        Err(_) => Err(anyhow!("background importer worker panicked")),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::time::Duration;

    use anyhow::{Context, Result};
    use jiff::{Timestamp, ToSpan, tz::TimeZone};
    use rusqlite::{Connection, params};
    use tempfile::tempdir;

    use super::{
        ImportWorkerOptions, StartupOpenReason, build_import_plan,
        start_startup_import_with_options,
    };
    use crate::db::Database;

    const WAIT_TIMEOUT_MS: u64 = 5;
    const WORKER_DELAY_MS: u64 = 50;

    #[test]
    fn import_plan_uses_local_days_and_round_robins_by_project() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let tz = TimeZone::get("America/Chicago")?;
        let now: Timestamp = "2026-03-26T18:00:00Z".parse()?;

        let project_a = insert_project(db.connection_mut(), "git:/projects/a", "project-a")?;
        let project_b = insert_project(db.connection_mut(), "git:/projects/b", "project-b")?;

        insert_source_file(
            db.connection_mut(),
            project_a,
            "a/today.jsonl",
            "2026-03-26T15:00:00Z",
        )?;
        insert_source_file(
            db.connection_mut(),
            project_a,
            "a/yesterday.jsonl",
            "2026-03-26T03:30:00Z",
        )?;
        insert_source_file(
            db.connection_mut(),
            project_a,
            "a/older.jsonl",
            "2026-03-24T16:00:00Z",
        )?;
        insert_source_file(
            db.connection_mut(),
            project_b,
            "b/today.jsonl",
            "2026-03-26T14:00:00Z",
        )?;
        insert_source_file(
            db.connection_mut(),
            project_b,
            "b/yesterday.jsonl",
            "2026-03-25T20:00:00Z",
        )?;
        insert_source_file(
            db.connection_mut(),
            project_b,
            "b/older.jsonl",
            "2026-03-24T15:00:00Z",
        )?;

        let plan = build_import_plan(db.connection(), now, &tz)?;

        let startup_order = plan
            .startup_chunks
            .iter()
            .map(|chunk| format!("{}:{}", chunk.project_key, chunk.chunk_day_local))
            .collect::<Vec<_>>();
        assert_eq!(
            startup_order,
            vec![
                "git:/projects/a:2026-03-26",
                "git:/projects/b:2026-03-26",
                "git:/projects/a:2026-03-25",
                "git:/projects/b:2026-03-25",
            ]
        );

        let deferred_order = plan
            .deferred_chunks
            .iter()
            .map(|chunk| format!("{}:{}", chunk.project_key, chunk.chunk_day_local))
            .collect::<Vec<_>>();
        assert_eq!(
            deferred_order,
            vec!["git:/projects/a:2026-03-24", "git:/projects/b:2026-03-24",]
        );

        let yesterday_chunk = &plan.startup_chunks[2];
        assert_eq!(yesterday_chunk.chunk_day_local, "2026-03-25");
        assert_eq!(yesterday_chunk.source_files.len(), 1);
        assert_eq!(
            yesterday_chunk.source_files[0].relative_path,
            "a/yesterday.jsonl"
        );

        Ok(())
    }

    #[test]
    fn startup_import_opens_when_last_24h_slice_is_ready() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let source_root = temp.path().join("source");
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root)?;

        let relative_path = "recent/session.jsonl";
        let source_path = source_root.join(relative_path);
        let size_bytes = write_session_fixture(&source_path, "session-ready")?;

        let recent = Timestamp::now()
            .checked_sub(1_i64.hours())
            .context("unable to construct recent test timestamp")?
            .to_string();

        let mut db = Database::open(&db_path)?;
        let project_id = insert_project_with_root(
            db.connection_mut(),
            "path:/startup-ready",
            "startup-ready",
            &project_root,
        )?;
        let _ = insert_seeded_source_file(
            db.connection_mut(),
            project_id,
            relative_path,
            &recent,
            size_bytes,
        )?;

        let startup = start_startup_import_with_options(
            db.connection(),
            &db_path,
            &source_root,
            Duration::from_secs(2),
            ImportWorkerOptions::default(),
        )?;

        assert_eq!(startup.open_reason, StartupOpenReason::Last24hReady);
        assert_eq!(startup.snapshot.max_publish_seq, 1);
        startup.wait_for_completion()?;

        let state: (String, Option<i64>) = db.connection().query_row(
            "SELECT state, publish_seq FROM import_chunk",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(state.0, "complete");
        assert_eq!(state.1, Some(1));

        Ok(())
    }

    #[test]
    fn startup_timeout_still_allows_background_import_to_finish() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let source_root = temp.path().join("source");
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root)?;

        let recent_relative_path = "recent/session.jsonl";
        let older_relative_path = "older/session.jsonl";
        let recent_size = write_session_fixture(
            &source_root.join(recent_relative_path),
            "session-timeout-recent",
        )?;
        let older_size = write_session_fixture(
            &source_root.join(older_relative_path),
            "session-timeout-older",
        )?;

        let recent = Timestamp::now()
            .checked_sub(1_i64.hours())
            .context("unable to construct recent startup test timestamp")?
            .to_string();
        let older = Timestamp::now()
            .checked_sub(72_i64.hours())
            .context("unable to construct older startup test timestamp")?
            .to_string();

        let mut db = Database::open(&db_path)?;
        let project_id = insert_project_with_root(
            db.connection_mut(),
            "path:/startup-timeout",
            "startup-timeout",
            &project_root,
        )?;
        let _ = insert_seeded_source_file(
            db.connection_mut(),
            project_id,
            recent_relative_path,
            &recent,
            recent_size,
        )?;
        let _ = insert_seeded_source_file(
            db.connection_mut(),
            project_id,
            older_relative_path,
            &older,
            older_size,
        )?;

        let startup = start_startup_import_with_options(
            db.connection(),
            &db_path,
            &source_root,
            Duration::from_millis(WAIT_TIMEOUT_MS),
            ImportWorkerOptions {
                per_chunk_delay: Duration::from_millis(WORKER_DELAY_MS),
            },
        )?;

        assert_eq!(startup.open_reason, StartupOpenReason::TimedOut);
        assert_eq!(startup.snapshot.max_publish_seq, 0);
        startup.wait_for_completion()?;

        let complete_count: i64 = db.connection().query_row(
            "SELECT COUNT(*) FROM import_chunk WHERE state = 'complete'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(complete_count, 2);

        Ok(())
    }

    fn insert_project(
        conn: &mut Connection,
        canonical_key: &str,
        display_name: &str,
    ) -> Result<i64> {
        insert_project_with_root(
            conn,
            canonical_key,
            display_name,
            Path::new("/tmp/project-root"),
        )
    }

    fn insert_project_with_root(
        conn: &mut Connection,
        canonical_key: &str,
        display_name: &str,
        root_path: &Path,
    ) -> Result<i64> {
        conn.query_row(
            "
            INSERT INTO project (identity_kind, canonical_key, display_name, root_path)
            VALUES ('path', ?1, ?2, ?3)
            RETURNING id
            ",
            params![canonical_key, display_name, root_path.display().to_string()],
            |row| row.get::<_, i64>(0),
        )
        .context("unable to insert a seeded project")
    }

    fn insert_source_file(
        conn: &mut Connection,
        project_id: i64,
        relative_path: &str,
        modified_at_utc: &str,
    ) -> Result<i64> {
        insert_seeded_source_file(conn, project_id, relative_path, modified_at_utc, 128)
    }

    fn insert_seeded_source_file(
        conn: &mut Connection,
        project_id: i64,
        relative_path: &str,
        modified_at_utc: &str,
        size_bytes: i64,
    ) -> Result<i64> {
        conn.query_row(
            "
            INSERT INTO source_file (
                project_id,
                relative_path,
                modified_at_utc,
                size_bytes
            )
            VALUES (?1, ?2, ?3, ?4)
            RETURNING id
            ",
            params![project_id, relative_path, modified_at_utc, size_bytes],
            |row| row.get::<_, i64>(0),
        )
        .context("unable to insert a seeded source file")
    }

    fn write_session_fixture(path: &Path, session_id: &str) -> Result<i64> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let content = format!(
            concat!(
                "{{\"type\":\"user\",\"uuid\":\"{session_id}-user\",\"timestamp\":\"2026-03-26T10:00:00Z\",\"sessionId\":\"{session_id}\",\"message\":{{\"role\":\"user\",\"content\":\"Inspect the project\"}}}}\n",
                "{{\"type\":\"assistant\",\"uuid\":\"{session_id}-assistant\",\"timestamp\":\"2026-03-26T10:00:01Z\",\"sessionId\":\"{session_id}\",\"message\":{{\"id\":\"msg-{session_id}\",\"role\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"Working on it\"}}],\"usage\":{{\"input_tokens\":3,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":0,\"output_tokens\":1}},\"model\":\"claude-haiku\",\"stop_reason\":\"end_turn\"}}}}\n"
            ),
            session_id = session_id,
        );

        fs::write(path, &content).with_context(|| format!("unable to write {}", path.display()))?;
        i64::try_from(content.len()).context("fixture size exceeded i64")
    }
}
