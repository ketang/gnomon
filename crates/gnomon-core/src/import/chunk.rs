use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::classify::{BuildActionsParams, build_actions_in_tx_with_messages};
use crate::db::Database;
use crate::perf::{PerfLogger, PerfScope};
use crate::query::SnapshotBounds;
use crate::rollup::{rebuild_chunk_action_rollups, rebuild_chunk_path_rollups};
use anyhow::{Context, Result, anyhow};
use jiff::{Timestamp, ToSpan, tz::TimeZone};
use rusqlite::{Connection, params};

use super::{
    ConfiguredSources, IMPORT_SCHEMA_VERSION, NormalizeImportWarning, NormalizeJsonlFileOutcome,
    NormalizeJsonlFileParams, ParseResult, STARTUP_IMPORT_WINDOW_HOURS, STARTUP_OPEN_DEADLINE_SECS,
    SourceDescriptor, SourceFileKind, normalize::parse_jsonl_file,
    normalize::write_parsed_file_in_tx,
};
use rayon::prelude::*;

const IMPORTER_THREAD_NAME: &str = "gnomon-importer";
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupOpenReason {
    Last24hReady,
    TimedOut,
    FullImportReady,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupImportMode {
    RecentFirst,
    Full,
}

pub struct StartupImport {
    pub snapshot: SnapshotBounds,
    pub open_reason: StartupOpenReason,
    pub startup_status_message: Option<String>,
    pub deferred_status_message: Option<String>,
    pub startup_progress_update: Option<StartupProgressUpdate>,
    status_updates: Option<Receiver<StartupWorkerEvent>>,
    worker: Option<JoinHandle<Result<()>>>,
}

impl StartupImport {
    pub fn take_status_updates(&mut self) -> Option<Receiver<StartupWorkerEvent>> {
        self.status_updates.take()
    }

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
    perf_logger: Option<PerfLogger>,
}

#[derive(Debug)]
pub enum StartupWorkerEvent {
    Progress {
        update: StartupProgressUpdate,
    },
    StartupSettled {
        startup_status_message: Option<String>,
    },
    DeferredFailures {
        deferred_status_message: Option<String>,
    },
    Finished,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupProgressUpdate {
    pub label: &'static str,
    pub current: usize,
    pub total: usize,
    pub detail: String,
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
    import_source_files: Vec<ChunkSourceFile>,
    remove_only_source_files: Vec<ChunkSourceFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedChunk {
    import_chunk_id: i64,
    project_id: i64,
    project_key: String,
    chunk_day_local: String,
    import_source_files: Vec<ChunkSourceFile>,
    remove_only_source_files: Vec<ChunkSourceFile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportPhase {
    Startup,
    Deferred,
}

impl ImportPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Deferred => "deferred",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChunkSourceFile {
    source_file_id: i64,
    relative_path: String,
    source_provider: super::SourceProvider,
    source_kind: SourceFileKind,
}

#[derive(Debug)]
struct SourceFileRow {
    source_file_id: i64,
    project_id: i64,
    project_key: String,
    relative_path: String,
    source_provider: String,
    source_kind: String,
    modified_at_utc: Option<String>,
    discovered_at_utc: String,
    size_bytes: i64,
    imported_size_bytes: Option<i64>,
    imported_modified_at_utc: Option<String>,
    imported_schema_version: Option<i64>,
}

#[derive(Debug)]
struct PendingChunkRebuildRow {
    project_id: i64,
    project_key: String,
    chunk_day_local: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ChunkChangeSet {
    import_source_files: Vec<ChunkSourceFile>,
    remove_only_source_files: Vec<ChunkSourceFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportExecutionReport {
    pub startup_chunk_count: usize,
    pub deferred_chunk_count: usize,
    pub deferred_failure_count: usize,
    pub deferred_failure_summary: Option<String>,
}

pub fn start_startup_import(
    conn: &Connection,
    db_path: &Path,
    source_root: &Path,
) -> Result<StartupImport> {
    let sources = ConfiguredSources::legacy_claude(source_root);
    start_startup_import_with_sources_and_mode_and_progress(
        conn,
        db_path,
        &sources,
        StartupImportMode::RecentFirst,
        |_| {},
    )
}

pub fn start_startup_import_with_progress<F>(
    conn: &Connection,
    db_path: &Path,
    source_root: &Path,
    on_progress: F,
) -> Result<StartupImport>
where
    F: FnMut(&StartupProgressUpdate),
{
    let sources = ConfiguredSources::legacy_claude(source_root);
    start_startup_import_with_sources_and_mode_and_progress(
        conn,
        db_path,
        &sources,
        StartupImportMode::RecentFirst,
        on_progress,
    )
}

pub fn start_startup_import_with_mode_and_progress<F>(
    conn: &Connection,
    db_path: &Path,
    source_root: &Path,
    import_mode: StartupImportMode,
    on_progress: F,
) -> Result<StartupImport>
where
    F: FnMut(&StartupProgressUpdate),
{
    let sources = ConfiguredSources::legacy_claude(source_root);
    start_startup_import_with_sources_and_mode_and_progress(
        conn,
        db_path,
        &sources,
        import_mode,
        on_progress,
    )
}

pub fn start_startup_import_with_sources_and_mode_and_progress<F>(
    conn: &Connection,
    db_path: &Path,
    sources: &ConfiguredSources,
    import_mode: StartupImportMode,
    on_progress: F,
) -> Result<StartupImport>
where
    F: FnMut(&StartupProgressUpdate),
{
    let state_dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    let perf_logger = PerfLogger::from_env(state_dir).ok().flatten();
    start_startup_import_with_sources_and_perf_logger(
        conn,
        db_path,
        sources,
        import_mode,
        perf_logger,
        on_progress,
    )
}

/// Like [`start_startup_import_with_mode_and_progress`] but accepts an
/// explicit [`PerfLogger`] instead of reading `GNOMON_PERF_LOG` from the
/// environment. Used by the `import_bench` example.
pub fn start_startup_import_with_perf_logger<F>(
    conn: &Connection,
    db_path: &Path,
    source_root: &Path,
    import_mode: StartupImportMode,
    perf_logger: Option<PerfLogger>,
    on_progress: F,
) -> Result<StartupImport>
where
    F: FnMut(&StartupProgressUpdate),
{
    let sources = ConfiguredSources::legacy_claude(source_root);
    start_startup_import_with_sources_and_perf_logger(
        conn,
        db_path,
        &sources,
        import_mode,
        perf_logger,
        on_progress,
    )
}

pub fn start_startup_import_with_sources_and_perf_logger<F>(
    conn: &Connection,
    db_path: &Path,
    sources: &ConfiguredSources,
    import_mode: StartupImportMode,
    perf_logger: Option<PerfLogger>,
    mut on_progress: F,
) -> Result<StartupImport>
where
    F: FnMut(&StartupProgressUpdate),
{
    let options = ImportWorkerOptions {
        perf_logger,
        ..ImportWorkerOptions::default()
    };
    start_startup_import_with_options_and_sources(
        conn,
        db_path,
        sources,
        Duration::from_secs(STARTUP_OPEN_DEADLINE_SECS),
        import_mode,
        options,
        Some(&mut on_progress),
    )
}

pub fn import_all(
    conn: &Connection,
    db_path: &Path,
    source_root: &Path,
) -> Result<ImportExecutionReport> {
    let state_dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    let perf_logger = PerfLogger::from_env(state_dir).ok().flatten();
    let sources = ConfiguredSources::legacy_claude(source_root);
    import_all_with_sources_and_perf_logger(conn, db_path, &sources, perf_logger)
}

/// Like [`import_all`] but accepts an explicit [`PerfLogger`] instead of
/// reading `GNOMON_PERF_LOG` from the environment. Used by the
/// `import_bench` example so it can route per-phase spans to a caller-chosen
/// path without mutating process env vars.
pub fn import_all_with_perf_logger(
    conn: &Connection,
    db_path: &Path,
    source_root: &Path,
    perf_logger: Option<PerfLogger>,
) -> Result<ImportExecutionReport> {
    let sources = ConfiguredSources::legacy_claude(source_root);
    import_all_with_sources_and_perf_logger(conn, db_path, &sources, perf_logger)
}

pub fn import_all_with_sources_and_perf_logger(
    conn: &Connection,
    db_path: &Path,
    sources: &ConfiguredSources,
    perf_logger: Option<PerfLogger>,
) -> Result<ImportExecutionReport> {
    let mut import_scope = PerfScope::new(perf_logger.clone(), "import.total");
    let now = Timestamp::now();
    let time_zone = TimeZone::system();

    let mut plan_scope = PerfScope::new(perf_logger.clone(), "import.build_plan");
    let plan = match build_import_plan(conn, now, &time_zone) {
        Ok(plan) => {
            plan_scope.field("startup_chunks", plan.startup_chunks.len());
            plan_scope.field("deferred_chunks", plan.deferred_chunks.len());
            plan_scope.finish_ok();
            plan
        }
        Err(err) => {
            plan_scope.finish_error(&err);
            return Err(err);
        }
    };

    let mut prepare_scope = PerfScope::new(perf_logger.clone(), "import.prepare_plan");
    let prepared = match prepare_import_plan(conn, &plan) {
        Ok(prepared) => {
            prepare_scope.field("startup_chunks", prepared.startup_chunks.len());
            prepare_scope.field("deferred_chunks", prepared.deferred_chunks.len());
            prepare_scope.finish_ok();
            prepared
        }
        Err(err) => {
            prepare_scope.finish_error(&err);
            return Err(err);
        }
    };

    let open_scope = PerfScope::new(perf_logger.clone(), "import.open_database");
    let mut database = match Database::open(db_path) {
        Ok(database) => {
            open_scope.finish_ok();
            database
        }
        Err(err) => {
            open_scope.finish_error(&err);
            return Err(err).with_context(|| format!("unable to open {}", db_path.display()));
        }
    };

    let options = ImportWorkerOptions {
        perf_logger,
        ..ImportWorkerOptions::default()
    };

    for chunk in &prepared.startup_chunks {
        import_chunk(
            &mut database,
            sources,
            chunk,
            ImportPhase::Startup,
            &options,
        )
        .with_context(|| {
            format!(
                "unable to import startup chunk {}:{}",
                chunk.project_key, chunk.chunk_day_local
            )
        })?;
    }

    let mut deferred_failures = Vec::new();
    for chunk in &prepared.deferred_chunks {
        if let Err(err) = import_chunk(
            &mut database,
            sources,
            chunk,
            ImportPhase::Deferred,
            &options,
        )
        .with_context(|| {
            format!(
                "unable to import deferred chunk {}:{}",
                chunk.project_key, chunk.chunk_day_local
            )
        }) {
            deferred_failures.push(compact_status_text(format!("{err:#}")));
        }
    }

    let report = ImportExecutionReport {
        startup_chunk_count: prepared.startup_chunks.len(),
        deferred_chunk_count: prepared.deferred_chunks.len(),
        deferred_failure_count: deferred_failures.len(),
        deferred_failure_summary: summarize_deferred_failures(&deferred_failures),
    };

    import_scope.field("startup_chunk_count", report.startup_chunk_count);
    import_scope.field("deferred_chunk_count", report.deferred_chunk_count);
    import_scope.field("deferred_failure_count", report.deferred_failure_count);
    import_scope.finish_ok();

    Ok(report)
}

#[cfg(test)]
fn start_startup_import_with_options(
    conn: &Connection,
    db_path: &Path,
    source_root: &Path,
    wait_timeout: Duration,
    import_mode: StartupImportMode,
    worker_options: ImportWorkerOptions,
    on_progress: Option<&mut dyn FnMut(&StartupProgressUpdate)>,
) -> Result<StartupImport> {
    let sources = ConfiguredSources::legacy_claude(source_root);
    start_startup_import_with_options_and_sources(
        conn,
        db_path,
        &sources,
        wait_timeout,
        import_mode,
        worker_options,
        on_progress,
    )
}

fn start_startup_import_with_options_and_sources(
    conn: &Connection,
    db_path: &Path,
    sources: &ConfiguredSources,
    wait_timeout: Duration,
    import_mode: StartupImportMode,
    worker_options: ImportWorkerOptions,
    mut on_progress: Option<&mut dyn FnMut(&StartupProgressUpdate)>,
) -> Result<StartupImport> {
    let now = Timestamp::now();
    let time_zone = TimeZone::system();
    let perf_logger = worker_options.perf_logger.clone();

    let mut plan_scope = PerfScope::new(perf_logger.clone(), "import.build_plan");
    let plan = match build_import_plan(conn, now, &time_zone) {
        Ok(plan) => {
            plan_scope.field("startup_chunks", plan.startup_chunks.len());
            plan_scope.field("deferred_chunks", plan.deferred_chunks.len());
            plan_scope.finish_ok();
            plan
        }
        Err(err) => {
            plan_scope.finish_error(&err);
            return Err(err);
        }
    };

    let mut prepare_scope = PerfScope::new(perf_logger.clone(), "import.prepare_plan");
    let prepared = match prepare_import_plan(conn, &plan) {
        Ok(prepared) => {
            prepare_scope.field("startup_chunks", prepared.startup_chunks.len());
            prepare_scope.field("deferred_chunks", prepared.deferred_chunks.len());
            prepare_scope.finish_ok();
            prepared
        }
        Err(err) => {
            prepare_scope.finish_error(&err);
            return Err(err);
        }
    };

    let snapshot_scope = PerfScope::new(perf_logger.clone(), "import.load_snapshot");
    let current_snapshot = match SnapshotBounds::load(conn) {
        Ok(snapshot) => {
            snapshot_scope.finish_ok();
            snapshot
        }
        Err(err) => {
            snapshot_scope.finish_error(&err);
            return Err(err);
        }
    };

    if prepared.is_empty() {
        return Ok(StartupImport {
            snapshot: current_snapshot,
            open_reason: match import_mode {
                StartupImportMode::RecentFirst => StartupOpenReason::Last24hReady,
                StartupImportMode::Full => StartupOpenReason::FullImportReady,
            },
            startup_status_message: None,
            deferred_status_message: None,
            startup_progress_update: None,
            status_updates: None,
            worker: None,
        });
    }

    let (sender, receiver) = mpsc::channel();
    let db_path = db_path.to_path_buf();
    let sources = sources.clone();
    let prepared_for_worker = prepared.clone();
    let sender_for_worker = sender;
    let worker = match thread::Builder::new()
        .name(IMPORTER_THREAD_NAME.to_string())
        .spawn(move || {
            run_import_worker(
                &db_path,
                &sources,
                &prepared_for_worker,
                sender_for_worker,
                &worker_options,
            )
        }) {
        Ok(worker) => worker,
        Err(err) => {
            let error_message =
                compact_status_text(format!("unable to spawn background importer worker: {err}"));
            mark_chunks_failed(conn, &prepared, Some(error_message.as_str()))?;
            return Err(anyhow!(err)).context("unable to spawn background importer worker");
        }
    };

    let mut startup_progress_update =
        plan.startup_chunks
            .first()
            .map(|chunk| StartupProgressUpdate {
                label: "rebuilding database",
                current: 1,
                total: plan.startup_chunks.len(),
                detail: format!("{}:{}", chunk.project_key, chunk.chunk_day_local),
            });
    match import_mode {
        StartupImportMode::RecentFirst => {
            let (open_reason, startup_status_message) = loop {
                match receiver.recv_timeout(wait_timeout) {
                    Ok(StartupWorkerEvent::Progress { update }) => {
                        startup_progress_update = Some(update.clone());
                        if let Some(callback) = on_progress.as_mut() {
                            callback(&update);
                        }
                    }
                    Ok(StartupWorkerEvent::StartupSettled {
                        startup_status_message,
                    }) => break (StartupOpenReason::Last24hReady, startup_status_message),
                    Ok(StartupWorkerEvent::DeferredFailures { .. }) => continue,
                    Ok(StartupWorkerEvent::Finished) => continue,
                    Err(RecvTimeoutError::Timeout) => break (StartupOpenReason::TimedOut, None),
                    Err(RecvTimeoutError::Disconnected) => {
                        let worker_result = join_worker(Some(worker));
                        return Err(worker_result.err().unwrap_or_else(|| {
                            anyhow!("background importer exited before signaling startup readiness")
                        }));
                    }
                }
            };

            let snapshot_scope =
                PerfScope::new(perf_logger.clone(), "import.load_snapshot_after_wait");
            let snapshot = match SnapshotBounds::load(conn) {
                Ok(snapshot) => {
                    snapshot_scope.finish_ok();
                    snapshot
                }
                Err(err) => {
                    snapshot_scope.finish_error(&err);
                    return Err(err);
                }
            };

            Ok(StartupImport {
                snapshot,
                open_reason,
                startup_status_message,
                deferred_status_message: None,
                startup_progress_update,
                status_updates: Some(receiver),
                worker: Some(worker),
            })
        }
        StartupImportMode::Full => {
            let mut startup_status_message = None;
            let mut deferred_status_message = None;

            loop {
                match receiver.recv() {
                    Ok(StartupWorkerEvent::Progress { update }) => {
                        if let Some(callback) = on_progress.as_mut() {
                            callback(&update);
                        }
                    }
                    Ok(StartupWorkerEvent::StartupSettled {
                        startup_status_message: status_message,
                    }) => {
                        startup_status_message = status_message;
                    }
                    Ok(StartupWorkerEvent::DeferredFailures {
                        deferred_status_message: status_message,
                    }) => {
                        deferred_status_message = status_message;
                    }
                    Ok(StartupWorkerEvent::Finished) => break,
                    Err(_) => {
                        let worker_result = join_worker(Some(worker));
                        return Err(worker_result.err().unwrap_or_else(|| {
                            anyhow!("background importer exited before signaling full completion")
                        }));
                    }
                }
            }

            join_worker(Some(worker))?;

            let snapshot_scope = PerfScope::new(perf_logger, "import.load_snapshot_after_wait");
            let snapshot = match SnapshotBounds::load(conn) {
                Ok(snapshot) => {
                    snapshot_scope.finish_ok();
                    snapshot
                }
                Err(err) => {
                    snapshot_scope.finish_error(&err);
                    return Err(err);
                }
            };

            Ok(StartupImport {
                snapshot,
                open_reason: StartupOpenReason::FullImportReady,
                startup_status_message,
                deferred_status_message,
                startup_progress_update: None,
                status_updates: None,
                worker: None,
            })
        }
    }
}

fn build_import_plan(
    conn: &Connection,
    now: Timestamp,
    time_zone: &TimeZone,
) -> Result<ImportPlan> {
    let source_files = load_source_files(conn)?;
    let pending_chunk_rebuilds = load_pending_chunk_rebuilds(conn)?;
    let startup_days = startup_days(now, time_zone)?;

    let mut chunk_changes = BTreeMap::<ChunkDescriptor, ChunkChangeSet>::new();

    for row in source_files {
        let current_timestamp = current_chunk_timestamp(&row);
        let current_day = local_day_for_utc_timestamp(current_timestamp, time_zone)?;
        let descriptor = ChunkDescriptor {
            project_id: row.project_id,
            project_key: row.project_key.clone(),
            chunk_day_local: current_day.clone(),
        };
        let source_file = ChunkSourceFile {
            source_file_id: row.source_file_id,
            relative_path: row.relative_path.clone(),
            source_provider: super::SourceProvider::from_db_value(&row.source_provider)
                .ok_or_else(|| anyhow!("unknown source file provider {}", row.source_provider))?,
            source_kind: SourceFileKind::from_db_value(&row.source_kind)
                .ok_or_else(|| anyhow!("unknown source file kind {}", row.source_kind))?,
        };
        let imported_day = imported_chunk_day_local(&row, time_zone)?;

        if source_file_needs_import(&row) {
            chunk_changes
                .entry(descriptor.clone())
                .or_default()
                .import_source_files
                .push(source_file.clone());

            if let Some(imported_day) = imported_day.as_deref()
                && imported_day != current_day
            {
                chunk_changes
                    .entry(ChunkDescriptor {
                        project_id: row.project_id,
                        project_key: row.project_key.clone(),
                        chunk_day_local: imported_day.to_string(),
                    })
                    .or_default()
                    .remove_only_source_files
                    .push(source_file);
            }
        }
    }

    for pending in pending_chunk_rebuilds {
        chunk_changes
            .entry(ChunkDescriptor {
                project_id: pending.project_id,
                project_key: pending.project_key,
                chunk_day_local: pending.chunk_day_local,
            })
            .or_default();
    }

    let mut startup_candidates = Vec::new();
    let mut deferred_candidates = Vec::new();

    for (descriptor, mut changes) in chunk_changes {
        changes.import_source_files.sort_by(|left, right| {
            (
                left.source_provider.as_str(),
                left.source_kind.as_str(),
                left.relative_path.as_str(),
            )
                .cmp(&(
                    right.source_provider.as_str(),
                    right.source_kind.as_str(),
                    right.relative_path.as_str(),
                ))
        });
        changes.remove_only_source_files.sort_by(|left, right| {
            (
                left.source_provider.as_str(),
                left.source_kind.as_str(),
                left.relative_path.as_str(),
            )
                .cmp(&(
                    right.source_provider.as_str(),
                    right.source_kind.as_str(),
                    right.relative_path.as_str(),
                ))
        });

        let chunk = ChunkCandidate {
            project_id: descriptor.project_id,
            project_key: descriptor.project_key,
            chunk_day_local: descriptor.chunk_day_local,
            import_source_files: changes.import_source_files,
            remove_only_source_files: changes.remove_only_source_files,
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

fn imported_chunk_day_local(row: &SourceFileRow, time_zone: &TimeZone) -> Result<Option<String>> {
    row.imported_modified_at_utc
        .as_deref()
        .map(|timestamp| local_day_for_utc_timestamp(timestamp, time_zone))
        .transpose()
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
                source_file.source_provider,
                source_file.source_kind,
                source_file.modified_at_utc,
                source_file.discovered_at_utc,
                source_file.size_bytes,
                source_file.imported_size_bytes,
                source_file.imported_modified_at_utc,
                source_file.imported_schema_version
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
                source_provider: row.get(4)?,
                source_kind: row.get(5)?,
                modified_at_utc: row.get(6)?,
                discovered_at_utc: row.get(7)?,
                size_bytes: row.get(8)?,
                imported_size_bytes: row.get(9)?,
                imported_modified_at_utc: row.get(10)?,
                imported_schema_version: row.get(11)?,
            })
        })
        .context("unable to enumerate source files for import planning")?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("unable to decode source file import planning rows")
}

fn load_pending_chunk_rebuilds(conn: &Connection) -> Result<Vec<PendingChunkRebuildRow>> {
    let mut stmt = conn
        .prepare(
            "
            SELECT
                pending_chunk_rebuild.project_id,
                project.canonical_key,
                pending_chunk_rebuild.chunk_day_local
            FROM pending_chunk_rebuild
            JOIN project ON project.id = pending_chunk_rebuild.project_id
            ORDER BY project.canonical_key, pending_chunk_rebuild.chunk_day_local
            ",
        )
        .context("unable to prepare pending chunk rebuild query")?;

    let rows = stmt
        .query_map([], |row| {
            Ok(PendingChunkRebuildRow {
                project_id: row.get(0)?,
                project_key: row.get(1)?,
                chunk_day_local: row.get(2)?,
            })
        })
        .context("unable to enumerate pending chunk rebuild rows")?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("unable to decode pending chunk rebuild rows")
}

fn current_chunk_timestamp(row: &SourceFileRow) -> &str {
    row.modified_at_utc
        .as_deref()
        .unwrap_or(row.discovered_at_utc.as_str())
}

fn source_file_needs_import(row: &SourceFileRow) -> bool {
    row.imported_size_bytes != Some(row.size_bytes)
        || row.imported_modified_at_utc != row.modified_at_utc
        || row.imported_schema_version != Some(IMPORT_SCHEMA_VERSION)
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
            .map(|chunk| prepare_chunk(conn, chunk, ImportPhase::Startup))
            .collect::<Result<Vec<_>>>()?,
        deferred_chunks: plan
            .deferred_chunks
            .iter()
            .map(|chunk| prepare_chunk(conn, chunk, ImportPhase::Deferred))
            .collect::<Result<Vec<_>>>()?,
    })
}

fn prepare_chunk(
    conn: &Connection,
    chunk: &ChunkCandidate,
    phase: ImportPhase,
) -> Result<PreparedChunk> {
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
                imported_action_count,
                imported_conversation_count,
                imported_turn_count,
                last_attempt_phase,
                last_error_message
            )
            VALUES (?1, ?2, 'pending', NULL, NULL, 0, 0, 0, 0, 0, ?3, NULL)
            ON CONFLICT(project_id, chunk_day_local) DO UPDATE SET
                state = 'pending',
                publish_seq = NULL,
                completed_at_utc = NULL,
                imported_record_count = 0,
                imported_message_count = 0,
                imported_action_count = 0,
                imported_conversation_count = 0,
                imported_turn_count = 0,
                last_attempt_phase = excluded.last_attempt_phase,
                last_error_message = NULL
            RETURNING id
            ",
            params![chunk.project_id, chunk.chunk_day_local, phase.as_str()],
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
        import_source_files: chunk.import_source_files.clone(),
        remove_only_source_files: chunk.remove_only_source_files.clone(),
    })
}

fn mark_chunks_failed(
    conn: &Connection,
    plan: &PreparedImportPlan,
    error_message: Option<&str>,
) -> Result<()> {
    for chunk in plan.all_chunks() {
        conn.execute(
            "
            UPDATE import_chunk
            SET
                state = 'failed',
                publish_seq = NULL,
                completed_at_utc = CURRENT_TIMESTAMP,
                last_error_message = ?2
            WHERE id = ?1
            ",
            params![chunk.import_chunk_id, error_message],
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
    sources: &ConfiguredSources,
    plan: &PreparedImportPlan,
    sender: mpsc::Sender<StartupWorkerEvent>,
    options: &ImportWorkerOptions,
) -> Result<()> {
    let mut worker_scope = PerfScope::new(options.perf_logger.clone(), "import.worker_total");
    let open_scope = PerfScope::new(options.perf_logger.clone(), "import.open_database");
    let mut database = match Database::open(db_path) {
        Ok(database) => {
            open_scope.finish_ok();
            database
        }
        Err(err) => {
            open_scope.finish_error(&err);
            worker_scope.finish_error(&err);
            return Err(err).with_context(|| format!("unable to open {}", db_path.display()));
        }
    };
    let mut startup_failures = Vec::new();
    let mut deferred_failures = Vec::new();

    if plan.startup_chunks.is_empty() {
        let _ = sender.send(StartupWorkerEvent::StartupSettled {
            startup_status_message: None,
        });
    }

    for (index, chunk) in plan.startup_chunks.iter().enumerate() {
        send_progress(
            &sender,
            "rebuilding database",
            index + 1,
            plan.startup_chunks.len(),
            chunk,
        );
        if let Err(err) = import_chunk(&mut database, sources, chunk, ImportPhase::Startup, options)
        {
            startup_failures.push(compact_status_text(format!("{err:#}")));
        }
    }

    if !plan.startup_chunks.is_empty() {
        let _ = sender.send(StartupWorkerEvent::StartupSettled {
            startup_status_message: summarize_startup_failures(&startup_failures),
        });
    }

    for (index, chunk) in plan.deferred_chunks.iter().enumerate() {
        send_progress(
            &sender,
            "importing older history",
            index + 1,
            plan.deferred_chunks.len(),
            chunk,
        );
        if let Err(err) = import_chunk(
            &mut database,
            sources,
            chunk,
            ImportPhase::Deferred,
            options,
        ) {
            deferred_failures.push(compact_status_text(format!("{err:#}")));
        }
    }

    if !deferred_failures.is_empty() {
        let _ = sender.send(StartupWorkerEvent::DeferredFailures {
            deferred_status_message: summarize_deferred_failures(&deferred_failures),
        });
    }
    worker_scope.field("startup_failure_count", startup_failures.len());
    worker_scope.field("deferred_failure_count", deferred_failures.len());
    worker_scope.finish_ok();
    let _ = sender.send(StartupWorkerEvent::Finished);

    Ok(())
}
fn send_progress(
    sender: &mpsc::Sender<StartupWorkerEvent>,
    label: &'static str,
    current: usize,
    total: usize,
    chunk: &PreparedChunk,
) {
    let _ = sender.send(StartupWorkerEvent::Progress {
        update: StartupProgressUpdate {
            label,
            current,
            total,
            detail: format!("{}:{}", chunk.project_key, chunk.chunk_day_local),
        },
    });
}

fn summarize_startup_failures(failures: &[String]) -> Option<String> {
    summarize_failures("startup import failed", failures)
}

fn summarize_deferred_failures(failures: &[String]) -> Option<String> {
    summarize_failures("deferred import failed", failures)
}

fn summarize_failures(prefix: &str, failures: &[String]) -> Option<String> {
    match failures {
        [] => None,
        [failure] => Some(format!("{prefix}: {failure}")),
        [first, ..] => Some(format!(
            "{prefix} for {} chunks; first error: {first}",
            failures.len()
        )),
    }
}

fn compact_status_text(text: impl Into<String>) -> String {
    text.into().split_whitespace().collect::<Vec<_>>().join(" ")
}

fn import_chunk(
    database: &mut Database,
    sources: &ConfiguredSources,
    chunk: &PreparedChunk,
    phase: ImportPhase,
    options: &ImportWorkerOptions,
) -> Result<()> {
    begin_chunk_import(
        database.connection_mut(),
        chunk,
        options.perf_logger.clone(),
    )?;

    if options.per_chunk_delay > Duration::ZERO {
        thread::sleep(options.per_chunk_delay);
    }

    let mut scope = PerfScope::new(options.perf_logger.clone(), "import.chunk");
    scope.field("project_key", chunk.project_key.as_str());
    scope.field("chunk_day_local", chunk.chunk_day_local.as_str());
    scope.field(
        "source_file_count",
        chunk.import_source_files.len() + chunk.remove_only_source_files.len(),
    );
    scope.field("import_source_file_count", chunk.import_source_files.len());
    scope.field(
        "remove_only_source_file_count",
        chunk.remove_only_source_files.len(),
    );
    scope.field("phase", phase.as_str());

    let import_result = (|| {
        let mut tx = database
            .connection_mut()
            .transaction()
            .context("unable to start chunk-level import transaction")?;

        for source_file in &chunk.remove_only_source_files {
            purge_source_file_from_chunk(&tx, chunk.import_chunk_id, source_file.source_file_id)?;
        }

        // Phase 1: Parse all JSONL files in parallel (CPU-only, no DB access).
        // rayon's par_iter preserves input ordering in the collected Vec.
        let mut parse_scope = PerfScope::new(options.perf_logger.clone(), "import.parse_phase");
        let parsed_files: Vec<ParseResult> = chunk
            .import_source_files
            .par_iter()
            .map(|source_file| {
                let path = sources.resolve_path(
                    SourceDescriptor::new(source_file.source_provider, source_file.source_kind),
                    &source_file.relative_path,
                );
                let path = match path {
                    Ok(path) => path,
                    Err(err) => {
                        return ParseResult::Warning(NormalizeImportWarning {
                            code: "missing_source_path",
                            message: format!(
                                "unable to resolve source path for provider={} kind={} relative_path={}: {err:#}",
                                source_file.source_provider.as_str(),
                                source_file.source_kind.as_str(),
                                source_file.relative_path
                            ),
                        });
                    }
                };
                parse_jsonl_file(&path, source_file.source_kind)
            })
            .collect();
        let mut parsed_file_count = 0usize;
        let mut warning_file_count = 0usize;
        let mut sessionless_metadata_file_count = 0usize;
        let mut parsed_record_count = 0usize;
        for parse_result in &parsed_files {
            match parse_result {
                ParseResult::Parsed(parsed_file) => {
                    parsed_file_count += 1;
                    parsed_record_count += parsed_file.records.len();
                }
                ParseResult::Warning(_) => {
                    warning_file_count += 1;
                }
                ParseResult::SessionlessMetadata => {
                    sessionless_metadata_file_count += 1;
                }
            }
        }
        parse_scope.field("parsed_file_count", parsed_file_count);
        parse_scope.field("warning_file_count", warning_file_count);
        parse_scope.field(
            "sessionless_metadata_file_count",
            sessionless_metadata_file_count,
        );
        parse_scope.field("parsed_record_count", parsed_record_count);
        parse_scope.finish_ok();

        // Phase 2: Write pre-parsed data to DB serially (single writer).
        for (source_file, parse_result) in chunk.import_source_files.iter().zip(parsed_files) {
            match parse_result {
                ParseResult::Warning(warning) => {
                    insert_import_warning(
                        &tx,
                        chunk.import_chunk_id,
                        source_file.source_file_id,
                        &warning,
                    )?;
                }
                ParseResult::SessionlessMetadata => {
                    // Nothing to write — file had no session ID and contained
                    // only metadata records. Create + immediately release a
                    // savepoint so the per-file purge still runs.
                    let mut savepoint_open_scope =
                        PerfScope::new(options.perf_logger.clone(), "import.savepoint_open");
                    savepoint_open_scope.field("source_file_id", source_file.source_file_id);
                    let sp = match tx.savepoint() {
                        Ok(sp) => {
                            savepoint_open_scope.finish_ok();
                            sp
                        }
                        Err(err) => {
                            savepoint_open_scope.finish_error(&err);
                            return Err(err).context("unable to create per-file savepoint");
                        }
                    };
                    let params = NormalizeJsonlFileParams {
                        project_id: chunk.project_id,
                        source_file_id: source_file.source_file_id,
                        import_chunk_id: chunk.import_chunk_id,
                        path: sources.resolve_path(
                            SourceDescriptor::new(
                                source_file.source_provider,
                                source_file.source_kind,
                            ),
                            &source_file.relative_path,
                        )?,
                        perf_logger: options.perf_logger.clone(),
                    };
                    super::normalize::purge_existing_import(&sp, &params)?;
                    let mut savepoint_release_scope =
                        PerfScope::new(options.perf_logger.clone(), "import.savepoint_release");
                    savepoint_release_scope.field("source_file_id", source_file.source_file_id);
                    match sp.commit() {
                        Ok(()) => savepoint_release_scope.finish_ok(),
                        Err(err) => {
                            savepoint_release_scope.finish_error(&err);
                            return Err(err).context("unable to release per-file savepoint");
                        }
                    }
                }
                ParseResult::Parsed(parsed_file) => {
                    let mut savepoint_open_scope =
                        PerfScope::new(options.perf_logger.clone(), "import.savepoint_open");
                    savepoint_open_scope.field("source_file_id", source_file.source_file_id);
                    let sp = match tx.savepoint() {
                        Ok(sp) => {
                            savepoint_open_scope.finish_ok();
                            sp
                        }
                        Err(err) => {
                            savepoint_open_scope.finish_error(&err);
                            return Err(err).context("unable to create per-file savepoint");
                        }
                    };
                    let params = NormalizeJsonlFileParams {
                        project_id: chunk.project_id,
                        source_file_id: source_file.source_file_id,
                        import_chunk_id: chunk.import_chunk_id,
                        path: sources.resolve_path(
                            SourceDescriptor::new(
                                source_file.source_provider,
                                source_file.source_kind,
                            ),
                            &source_file.relative_path,
                        )?,
                        perf_logger: options.perf_logger.clone(),
                    };

                    let (outcome, normalized_messages) = write_parsed_file_in_tx(
                        &sp,
                        &params,
                        parsed_file,
                        SourceDescriptor::new(source_file.source_provider, source_file.source_kind),
                    )
                    .with_context(|| {
                        format!("unable to normalize source file {}", params.path.display())
                    })?;

                    match outcome {
                        NormalizeJsonlFileOutcome::Imported(result) => {
                            if let Some(conversation_id) = result.conversation_id {
                                let _ = build_actions_in_tx_with_messages(
                                    &sp,
                                    &BuildActionsParams {
                                        conversation_id,
                                        perf_logger: options.perf_logger.clone(),
                                    },
                                    normalized_messages,
                                )
                                .with_context(|| {
                                    format!(
                                        "unable to build actions for source file {}",
                                        params.path.display()
                                    )
                                })?;
                            }
                            let mut savepoint_release_scope = PerfScope::new(
                                options.perf_logger.clone(),
                                "import.savepoint_release",
                            );
                            savepoint_release_scope
                                .field("source_file_id", source_file.source_file_id);
                            match sp.commit() {
                                Ok(()) => savepoint_release_scope.finish_ok(),
                                Err(err) => {
                                    savepoint_release_scope.finish_error(&err);
                                    return Err(err)
                                        .context("unable to release per-file savepoint");
                                }
                            }
                        }
                        NormalizeJsonlFileOutcome::Skipped => {
                            let mut savepoint_release_scope = PerfScope::new(
                                options.perf_logger.clone(),
                                "import.savepoint_release",
                            );
                            savepoint_release_scope
                                .field("source_file_id", source_file.source_file_id);
                            match sp.commit() {
                                Ok(()) => savepoint_release_scope.finish_ok(),
                                Err(err) => {
                                    savepoint_release_scope.finish_error(&err);
                                    return Err(err)
                                        .context("unable to release per-file savepoint");
                                }
                            }
                        }
                        NormalizeJsonlFileOutcome::Warning(warning) => {
                            let mut savepoint_rollback_scope = PerfScope::new(
                                options.perf_logger.clone(),
                                "import.savepoint_rollback",
                            );
                            savepoint_rollback_scope
                                .field("source_file_id", source_file.source_file_id);
                            drop(sp);
                            savepoint_rollback_scope.finish_ok();
                            insert_import_warning(
                                &tx,
                                chunk.import_chunk_id,
                                source_file.source_file_id,
                                &warning,
                            )?;
                        }
                    }
                }
            }
        }

        let mut finalize_scope =
            PerfScope::new(options.perf_logger.clone(), "import.finalize_chunk");
        finalize_scope.field("import_chunk_id", chunk.import_chunk_id);
        let finalize_result = finalize_chunk_import_core(&tx, chunk, options);
        match &finalize_result {
            Ok(()) => finalize_scope.finish_ok(),
            Err(err) => finalize_scope.finish_error(err),
        }
        finalize_result?;

        let mut commit_scope = PerfScope::new(options.perf_logger.clone(), "import.chunk_commit");
        commit_scope.field("import_chunk_id", chunk.import_chunk_id);
        match tx.commit() {
            Ok(()) => commit_scope.finish_ok(),
            Err(err) => {
                commit_scope.finish_error(&err);
                return Err(err).context("unable to commit chunk-level import transaction");
            }
        }
        Ok(())
    })();

    if let Err(err) = import_result {
        let error_message = compact_status_text(format!("{err:#}"));
        let _ = mark_chunk_failed(
            database.connection_mut(),
            chunk.import_chunk_id,
            &error_message,
        );
        scope.finish_error(&err);
        return Err(err);
    }

    scope.finish_ok();
    Ok(())
}

fn begin_chunk_import(
    conn: &mut Connection,
    chunk: &PreparedChunk,
    perf_logger: Option<PerfLogger>,
) -> Result<()> {
    let mut begin_scope = PerfScope::new(perf_logger, "import.begin_chunk");
    begin_scope.field("import_chunk_id", chunk.import_chunk_id);
    let tx = conn
        .transaction()
        .context("unable to start an import chunk transaction")?;
    tx.execute(
        "
        UPDATE import_chunk
        SET
            state = 'running',
            publish_seq = NULL,
            started_at_utc = CURRENT_TIMESTAMP,
            completed_at_utc = NULL,
            last_error_message = NULL,
            imported_record_count = 0,
            imported_message_count = 0,
            imported_action_count = 0,
            imported_conversation_count = 0,
            imported_turn_count = 0
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
    match tx.commit() {
        Ok(()) => begin_scope.finish_ok(),
        Err(err) => {
            begin_scope.finish_error(&err);
            return Err(err).context("unable to commit import chunk startup transaction");
        }
    }
    Ok(())
}

fn insert_import_warning(
    conn: &Connection,
    import_chunk_id: i64,
    source_file_id: i64,
    warning: &NormalizeImportWarning,
) -> Result<()> {
    conn.execute(
        "
        INSERT INTO import_warning (import_chunk_id, source_file_id, code, severity, message)
        VALUES (?1, ?2, ?3, 'warning', ?4)
        ",
        params![
            import_chunk_id,
            source_file_id,
            warning.code,
            warning.message
        ],
    )
    .with_context(|| {
        format!("unable to record import warning for source file id {source_file_id}")
    })?;
    Ok(())
}

fn purge_source_file_from_chunk(
    conn: &Connection,
    import_chunk_id: i64,
    source_file_id: i64,
) -> Result<()> {
    conn.prepare_cached(
        "
        DELETE FROM conversation
        WHERE source_file_id = ?1
          AND id IN (
              SELECT DISTINCT conversation_id
              FROM stream
              WHERE import_chunk_id = ?2
              UNION
              SELECT DISTINCT conversation_id
              FROM message
              WHERE import_chunk_id = ?2
              UNION
              SELECT DISTINCT conversation_id
              FROM turn
              WHERE import_chunk_id = ?2
          )
        ",
    )
    .and_then(|mut stmt| stmt.execute(params![source_file_id, import_chunk_id]))
    .context("unable to purge chunk-scoped conversation state for source file")?;

    conn.prepare_cached(
        "
        DELETE FROM history_event
        WHERE source_file_id = ?1 AND import_chunk_id = ?2
        ",
    )
    .and_then(|mut stmt| stmt.execute(params![source_file_id, import_chunk_id]))
    .context("unable to purge chunk-scoped history events for source file")?;

    conn.prepare_cached(
        "
        DELETE FROM skill_invocation
        WHERE source_file_id = ?1 AND import_chunk_id = ?2
        ",
    )
    .and_then(|mut stmt| stmt.execute(params![source_file_id, import_chunk_id]))
    .context("unable to purge chunk-scoped skill invocations for source file")?;

    conn.prepare_cached(
        "
        DELETE FROM codex_rollout_session
        WHERE source_file_id = ?1 AND import_chunk_id = ?2
        ",
    )
    .and_then(|mut stmt| stmt.execute(params![source_file_id, import_chunk_id]))
    .context("unable to purge chunk-scoped codex rollout raw state for source file")?;

    conn.prepare_cached(
        "
        DELETE FROM codex_session_index_entry
        WHERE source_file_id = ?1 AND import_chunk_id = ?2
        ",
    )
    .and_then(|mut stmt| stmt.execute(params![source_file_id, import_chunk_id]))
    .context("unable to purge chunk-scoped codex session-index state for source file")?;

    conn.prepare_cached(
        "
        DELETE FROM import_warning
        WHERE import_chunk_id = ?1 AND source_file_id = ?2
        ",
    )
    .and_then(|mut stmt| stmt.execute(params![import_chunk_id, source_file_id]))
    .context("unable to purge chunk-scoped import warnings for source file")?;

    Ok(())
}

fn finalize_chunk_import_core(
    conn: &Connection,
    chunk: &PreparedChunk,
    options: &ImportWorkerOptions,
) -> Result<()> {
    for source_file in &chunk.import_source_files {
        conn.execute(
            "
            UPDATE source_file
            SET
                imported_size_bytes = size_bytes,
                imported_modified_at_utc = modified_at_utc,
                imported_schema_version = ?2
            WHERE id = ?1
            ",
            params![source_file.source_file_id, IMPORT_SCHEMA_VERSION],
        )
        .with_context(|| {
            format!(
                "unable to mark source file {} as imported",
                source_file.relative_path
            )
        })?;
    }

    recompute_chunk_counts(conn, chunk.import_chunk_id)?;
    rebuild_chunk_action_rollups(conn, chunk.import_chunk_id, options.perf_logger.clone())?;
    rebuild_chunk_path_rollups(conn, chunk.import_chunk_id, options.perf_logger.clone())?;
    clear_pending_chunk_rebuild(conn, chunk.project_id, &chunk.chunk_day_local)?;

    publish_import_chunk(conn, chunk)?;

    Ok(())
}

fn recompute_chunk_counts(conn: &Connection, import_chunk_id: i64) -> Result<()> {
    conn.execute(
        "
        UPDATE import_chunk
        SET
            imported_record_count = (
                SELECT
                    (SELECT COUNT(*) FROM history_event WHERE import_chunk_id = ?1)
                    + (SELECT COUNT(*) FROM codex_rollout_event WHERE import_chunk_id = ?1)
                    + (SELECT COUNT(*) FROM codex_session_index_entry WHERE import_chunk_id = ?1)
            ),
            imported_message_count = (
                SELECT COUNT(*)
                FROM message
                WHERE import_chunk_id = ?1
            ),
            imported_action_count = (
                SELECT COUNT(*)
                FROM action
                WHERE import_chunk_id = ?1
            ),
            imported_conversation_count = (
                SELECT COUNT(*)
                FROM conversation
                WHERE id IN (
                    SELECT DISTINCT conversation_id
                    FROM stream
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
            ),
            imported_turn_count = (
                SELECT COUNT(*)
                FROM turn
                WHERE import_chunk_id = ?1
            )
        WHERE id = ?1
        ",
        [import_chunk_id],
    )
    .context("unable to recompute chunk counts")?;
    Ok(())
}

fn clear_pending_chunk_rebuild(
    conn: &Connection,
    project_id: i64,
    chunk_day_local: &str,
) -> Result<()> {
    conn.execute(
        "
        DELETE FROM pending_chunk_rebuild
        WHERE project_id = ?1 AND chunk_day_local = ?2
        ",
        params![project_id, chunk_day_local],
    )
    .context("unable to clear pending chunk rebuild rows")?;
    Ok(())
}

fn publish_import_chunk(conn: &Connection, chunk: &PreparedChunk) -> Result<()> {
    let next_publish_seq: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(publish_seq), 0) + 1 FROM import_chunk",
            [],
            |row| row.get(0),
        )
        .context("unable to allocate the next import publish sequence")?;

    conn.execute(
        "
        UPDATE import_chunk
        SET
            state = 'complete',
            publish_seq = ?2,
            completed_at_utc = CURRENT_TIMESTAMP,
            last_error_message = NULL
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

    Ok(())
}

fn mark_chunk_failed(
    conn: &mut Connection,
    import_chunk_id: i64,
    error_message: &str,
) -> Result<()> {
    conn.execute(
        "
        UPDATE import_chunk
        SET
            state = 'failed',
            publish_seq = NULL,
            completed_at_utc = CURRENT_TIMESTAMP,
            last_error_message = ?2
        WHERE id = ?1
        ",
        params![import_chunk_id, error_message],
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
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::Path;
    use std::thread;
    use std::time::Duration;

    use anyhow::{Context, Result};
    use jiff::{Timestamp, ToSpan, tz::TimeZone};
    use rusqlite::{Connection, params};
    use tempfile::tempdir;

    use super::{
        ImportWorkerOptions, StartupImportMode, StartupOpenReason, StartupWorkerEvent,
        build_import_plan, import_all, import_all_with_sources_and_perf_logger,
        start_startup_import_with_options,
    };
    use crate::config::ProjectIdentityPolicy;
    use crate::db::Database;
    use crate::import::{scan_source_manifest, scan_sources_manifest_with_policy};
    use crate::sources::{ConfiguredSource, ConfiguredSources, SourceFileKind, SourceProvider};

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
        assert_eq!(yesterday_chunk.import_source_files.len(), 1);
        assert!(yesterday_chunk.remove_only_source_files.is_empty());
        assert_eq!(
            yesterday_chunk.import_source_files[0].relative_path,
            "a/yesterday.jsonl"
        );

        Ok(())
    }

    #[test]
    fn import_plan_marks_old_chunk_for_remove_only_when_file_moves_days() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let tz = TimeZone::get("America/Chicago")?;
        let now: Timestamp = "2026-03-26T18:00:00Z".parse()?;

        let project_id = insert_project(db.connection_mut(), "git:/projects/move", "move")?;
        let source_file_id = insert_source_file(
            db.connection_mut(),
            project_id,
            "move/session.jsonl",
            "2026-03-26T12:00:00Z",
        )?;
        db.connection_mut().execute(
            "
            UPDATE source_file
            SET
                imported_size_bytes = size_bytes,
                imported_modified_at_utc = '2026-03-25T04:00:00Z',
                imported_schema_version = ?2
            WHERE id = ?1
            ",
            params![source_file_id, crate::import::IMPORT_SCHEMA_VERSION],
        )?;

        let plan = build_import_plan(db.connection(), now, &tz)?;
        assert_eq!(plan.startup_chunks.len(), 1);
        assert_eq!(plan.deferred_chunks.len(), 1);

        let current_chunk = plan
            .startup_chunks
            .iter()
            .find(|chunk| chunk.chunk_day_local == "2026-03-26")
            .expect("current-day chunk should exist");
        assert_eq!(current_chunk.import_source_files.len(), 1);
        assert!(current_chunk.remove_only_source_files.is_empty());

        let old_chunk = plan
            .deferred_chunks
            .iter()
            .find(|chunk| chunk.chunk_day_local == "2026-03-24")
            .expect("old imported-day chunk should exist");
        assert!(old_chunk.import_source_files.is_empty());
        assert_eq!(old_chunk.remove_only_source_files.len(), 1);
        assert_eq!(
            old_chunk.remove_only_source_files[0].relative_path,
            "move/session.jsonl"
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
            StartupImportMode::RecentFirst,
            ImportWorkerOptions::default(),
            None,
        )?;

        assert_eq!(startup.open_reason, StartupOpenReason::Last24hReady);
        assert_eq!(startup.snapshot.max_publish_seq, 1);
        assert_eq!(startup.startup_status_message, None);
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
            StartupImportMode::RecentFirst,
            ImportWorkerOptions {
                per_chunk_delay: Duration::from_millis(WORKER_DELAY_MS),
                perf_logger: None,
            },
            None,
        )?;

        assert_eq!(startup.open_reason, StartupOpenReason::TimedOut);
        assert_eq!(startup.snapshot.max_publish_seq, 0);
        assert_eq!(startup.startup_status_message, None);
        assert_eq!(
            startup.startup_progress_update.as_ref().map(|update| (
                update.label,
                update.current,
                update.total
            )),
            Some(("rebuilding database", 1, 1))
        );
        startup.wait_for_completion()?;

        let complete_count: i64 = db.connection().query_row(
            "SELECT COUNT(*) FROM import_chunk WHERE state = 'complete'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(complete_count, 2);

        Ok(())
    }

    #[test]
    fn startup_full_import_waits_for_deferred_chunks_before_opening() -> Result<()> {
        const WORKER_DELAY_MS: u64 = 75;

        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let source_root = temp.path().join("source");
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root)?;

        let recent_relative_path = "recent/session.jsonl";
        let older_relative_path = "older/session.jsonl";
        let recent_size = write_session_fixture(
            &source_root.join(recent_relative_path),
            "session-full-import-recent",
        )?;
        let older_size = write_session_fixture(
            &source_root.join(older_relative_path),
            "session-full-import-older",
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
            "path:/startup-full-import",
            "startup-full-import",
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
            Duration::from_millis(1),
            StartupImportMode::Full,
            ImportWorkerOptions {
                per_chunk_delay: Duration::from_millis(WORKER_DELAY_MS),
                perf_logger: None,
            },
            None,
        )?;

        assert_eq!(startup.open_reason, StartupOpenReason::FullImportReady);
        assert_eq!(startup.snapshot.max_publish_seq, 2);
        assert!(startup.startup_status_message.is_none());
        assert!(startup.deferred_status_message.is_none());
        assert!(startup.startup_progress_update.is_none());

        let complete_count: i64 = db.connection().query_row(
            "SELECT COUNT(*) FROM import_chunk WHERE state = 'complete'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(complete_count, 2);

        Ok(())
    }

    #[test]
    fn startup_import_records_warning_for_malformed_source_file_and_completes_chunk() -> Result<()>
    {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let source_root = temp.path().join("source");
        let bad_project_root = temp.path().join("bad-project");
        let good_project_root = temp.path().join("good-project");
        fs::create_dir_all(&bad_project_root)?;
        fs::create_dir_all(&good_project_root)?;

        let bad_relative_path = "bad/recent/session.jsonl";
        let good_relative_path = "good/recent/session.jsonl";
        let bad_source_path = source_root.join(bad_relative_path);
        let good_source_path = source_root.join(good_relative_path);
        let bad_size = write_malformed_session_fixture(&bad_source_path, "session-bad")?;
        let good_size = write_session_fixture(&good_source_path, "session-good")?;

        let recent = Timestamp::now()
            .checked_sub(1_i64.hours())
            .context("unable to construct recent startup test timestamp")?
            .to_string();

        let mut db = Database::open(&db_path)?;
        let bad_project_id = insert_project_with_root(
            db.connection_mut(),
            "path:/bad-project",
            "bad-project",
            &bad_project_root,
        )?;
        let good_project_id = insert_project_with_root(
            db.connection_mut(),
            "path:/good-project",
            "good-project",
            &good_project_root,
        )?;
        let _ = insert_seeded_source_file(
            db.connection_mut(),
            bad_project_id,
            bad_relative_path,
            &recent,
            bad_size,
        )?;
        let _ = insert_seeded_source_file(
            db.connection_mut(),
            good_project_id,
            good_relative_path,
            &recent,
            good_size,
        )?;

        let startup = start_startup_import_with_options(
            db.connection(),
            &db_path,
            &source_root,
            Duration::from_secs(2),
            StartupImportMode::RecentFirst,
            ImportWorkerOptions::default(),
            None,
        )?;

        assert_eq!(startup.open_reason, StartupOpenReason::Last24hReady);
        assert_eq!(startup.snapshot.max_publish_seq, 2);
        assert!(startup.startup_status_message.is_none());
        startup.wait_for_completion()?;

        let counts: (i64, i64) = db.connection().query_row(
            "
            SELECT
                COUNT(*) FILTER (WHERE state = 'complete'),
                COUNT(*) FILTER (WHERE state = 'failed')
            FROM import_chunk
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(counts, (2, 0));

        let warnings: Vec<String> = {
            let mut stmt = db.connection().prepare(
                "
                SELECT message
                FROM import_warning
                ORDER BY id
                ",
            )?;
            let rows = stmt.query_map([], |row| row.get(0))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains(&bad_source_path.display().to_string()));
        assert!(warnings[0].contains("line 2"));

        Ok(())
    }

    #[test]
    fn deferred_import_records_warnings_without_failure_status_updates() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let source_root = temp.path().join("source");
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root)?;

        let startup_relative_path = "startup/recent/session.jsonl";
        let deferred_one_relative_path = "deferred/older-one/session.jsonl";
        let deferred_two_relative_path = "deferred/older-two/session.jsonl";
        let startup_source_path = source_root.join(startup_relative_path);
        let deferred_one_source_path = source_root.join(deferred_one_relative_path);
        let deferred_two_source_path = source_root.join(deferred_two_relative_path);
        let startup_size = write_malformed_session_fixture(&startup_source_path, "startup-bad")?;
        let deferred_one_size =
            write_malformed_session_fixture(&deferred_one_source_path, "deferred-bad-one")?;
        let deferred_two_size =
            write_malformed_session_fixture(&deferred_two_source_path, "deferred-bad-two")?;

        let recent = Timestamp::now()
            .checked_sub(1_i64.hours())
            .context("unable to construct recent startup test timestamp")?
            .to_string();
        let older_one = Timestamp::now()
            .checked_sub(72_i64.hours())
            .context("unable to construct older deferred test timestamp")?
            .to_string();
        let older_two = Timestamp::now()
            .checked_sub(96_i64.hours())
            .context("unable to construct second older deferred test timestamp")?
            .to_string();

        let mut db = Database::open(&db_path)?;
        let project_id = insert_project_with_root(
            db.connection_mut(),
            "path:/deferred-status",
            "deferred-status",
            &project_root,
        )?;
        let _ = insert_seeded_source_file(
            db.connection_mut(),
            project_id,
            startup_relative_path,
            &recent,
            startup_size,
        )?;
        let _ = insert_seeded_source_file(
            db.connection_mut(),
            project_id,
            deferred_one_relative_path,
            &older_one,
            deferred_one_size,
        )?;
        let _ = insert_seeded_source_file(
            db.connection_mut(),
            project_id,
            deferred_two_relative_path,
            &older_two,
            deferred_two_size,
        )?;

        let mut startup = start_startup_import_with_options(
            db.connection(),
            &db_path,
            &source_root,
            Duration::from_secs(2),
            StartupImportMode::RecentFirst,
            ImportWorkerOptions::default(),
            None,
        )?;

        let status_updates = startup
            .take_status_updates()
            .context("missing deferred status update receiver")?;
        assert!(startup.startup_status_message.is_none());
        loop {
            match status_updates.recv_timeout(Duration::from_secs(2))? {
                StartupWorkerEvent::DeferredFailures { .. } => {
                    panic!("unexpected deferred failure update for warning-only imports")
                }
                StartupWorkerEvent::Finished => break,
                StartupWorkerEvent::Progress { .. } | StartupWorkerEvent::StartupSettled { .. } => {
                    continue;
                }
            }
        }

        startup.wait_for_completion()?;

        let counts: (i64, i64) = db.connection().query_row(
            "
            SELECT
                COUNT(*) FILTER (WHERE state = 'complete'),
                COUNT(*) FILTER (WHERE state = 'failed')
            FROM import_chunk
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(counts, (3, 0));

        let warnings: Vec<String> = {
            let mut stmt = db.connection().prepare(
                "
                SELECT message
                FROM import_warning
                ORDER BY id
                ",
            )?;
            let rows = stmt.query_map([], |row| row.get(0))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        assert_eq!(warnings.len(), 3);
        assert!(warnings[0].contains(&startup_source_path.display().to_string()));
        assert!(warnings[1].contains(&deferred_one_source_path.display().to_string()));
        assert!(warnings[2].contains(&deferred_two_source_path.display().to_string()));

        Ok(())
    }

    #[test]
    fn import_all_completes_when_deferred_file_is_malformed_and_records_warning() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let source_root = temp.path().join("source");
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root)?;

        let startup_relative_path = "startup/recent/session.jsonl";
        let deferred_relative_path = "deferred/older/session.jsonl";
        let startup_source_path = source_root.join(startup_relative_path);
        let deferred_source_path = source_root.join(deferred_relative_path);
        let startup_size = write_session_fixture(&startup_source_path, "startup-good")?;
        let deferred_size = write_malformed_session_fixture(&deferred_source_path, "deferred-bad")?;

        let recent = Timestamp::now()
            .checked_sub(1_i64.hours())
            .context("unable to construct recent startup test timestamp")?
            .to_string();
        let older = Timestamp::now()
            .checked_sub(72_i64.hours())
            .context("unable to construct older deferred test timestamp")?
            .to_string();

        let mut db = Database::open(&db_path)?;
        let project_id = insert_project_with_root(
            db.connection_mut(),
            "path:/import-all-deferred",
            "import-all-deferred",
            &project_root,
        )?;
        let _ = insert_seeded_source_file(
            db.connection_mut(),
            project_id,
            startup_relative_path,
            &recent,
            startup_size,
        )?;
        let _ = insert_seeded_source_file(
            db.connection_mut(),
            project_id,
            deferred_relative_path,
            &older,
            deferred_size,
        )?;

        let report = import_all(db.connection(), &db_path, &source_root)?;

        assert_eq!(report.startup_chunk_count, 1);
        assert_eq!(report.deferred_chunk_count, 1);
        assert_eq!(report.deferred_failure_count, 0);
        assert!(report.deferred_failure_summary.is_none());

        let counts: (i64, i64) = db.connection().query_row(
            "
            SELECT
                COUNT(*) FILTER (WHERE state = 'complete'),
                COUNT(*) FILTER (WHERE state = 'failed')
            FROM import_chunk
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(counts, (2, 0));

        let warning: String =
            db.connection()
                .query_row("SELECT message FROM import_warning LIMIT 1", [], |row| {
                    row.get(0)
                })?;
        assert!(warning.contains(&deferred_source_path.display().to_string()));
        assert!(warning.contains("line 2"));

        Ok(())
    }

    #[test]
    fn import_plan_reimports_files_when_import_schema_version_changes() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let tz = TimeZone::get("America/Chicago")?;
        let now: Timestamp = "2026-03-26T18:00:00Z".parse()?;

        let project_id = insert_project(db.connection_mut(), "git:/projects/schema", "schema")?;
        let source_file_id = insert_source_file(
            db.connection_mut(),
            project_id,
            "schema/session.jsonl",
            "2026-03-26T15:00:00Z",
        )?;

        db.connection_mut().execute(
            "
            UPDATE source_file
            SET
                imported_size_bytes = size_bytes,
                imported_modified_at_utc = modified_at_utc,
                imported_schema_version = 0
            WHERE id = ?1
            ",
            [source_file_id],
        )?;

        let plan = build_import_plan(db.connection(), now, &tz)?;
        assert_eq!(plan.startup_chunks.len(), 1);
        assert_eq!(plan.startup_chunks[0].project_id, project_id);
        assert_eq!(plan.startup_chunks[0].chunk_day_local, "2026-03-26");

        db.connection_mut().execute(
            "
            UPDATE source_file
            SET imported_schema_version = ?2
            WHERE id = ?1
            ",
            params![source_file_id, crate::import::IMPORT_SCHEMA_VERSION],
        )?;

        let settled_plan = build_import_plan(db.connection(), now, &tz)?;
        assert!(settled_plan.startup_chunks.is_empty());
        assert!(settled_plan.deferred_chunks.is_empty());

        Ok(())
    }

    #[test]
    fn import_all_preserves_unchanged_file_state_within_a_changed_day_chunk() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let source_root = temp.path().join("source");
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root)?;

        let first_relative_path = "same-day/first.jsonl";
        let second_relative_path = "same-day/second.jsonl";
        let first_path = source_root.join(first_relative_path);
        let second_path = source_root.join(second_relative_path);
        let _ = write_session_fixture_with_cwd(&first_path, "session-first", &project_root)?;
        let _ = write_session_fixture_with_cwd(&second_path, "session-second", &project_root)?;

        let mut db = Database::open(&db_path)?;
        scan_source_manifest(&mut db, &source_root)?;
        let initial_report = import_all(db.connection(), &db_path, &source_root)?;
        assert_eq!(initial_report.startup_chunk_count, 1);

        let original_ids = conversation_ids_by_relative_path(db.connection())?;
        let original_first = original_ids
            .get(first_relative_path)
            .copied()
            .expect("first file conversation should exist");
        let original_second = original_ids
            .get(second_relative_path)
            .copied()
            .expect("second file conversation should exist");

        thread::sleep(Duration::from_millis(1100));
        let _ =
            write_session_fixture_with_cwd(&first_path, "session-first-updated", &project_root)?;

        scan_source_manifest(&mut db, &source_root)?;
        let second_report = import_all(db.connection(), &db_path, &source_root)?;
        assert_eq!(second_report.startup_chunk_count, 1);

        let updated_ids = conversation_ids_by_relative_path(db.connection())?;
        assert_ne!(
            updated_ids.get(first_relative_path).copied(),
            Some(original_first),
            "changed file should be reimported"
        );
        assert_eq!(
            updated_ids.get(second_relative_path).copied(),
            Some(original_second),
            "unchanged file should keep its conversation row"
        );

        Ok(())
    }

    #[test]
    fn import_all_imports_codex_rollout_raw_sessions_without_blocking_claude_imports() -> Result<()>
    {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let claude_root = temp.path().join(".claude");
        let claude_projects = claude_root.join("projects");
        let claude_history = claude_root.join("history.jsonl");
        let project_root = temp.path().join("project");
        let project_cwd = project_root.join("workspace");
        fs::create_dir_all(&project_cwd)?;
        gix::init(&project_root)?;
        let _ = write_session_fixture_with_cwd(
            &claude_projects.join("project/session.jsonl"),
            "claude-session",
            &project_cwd,
        )?;
        fs::write(
            &claude_history,
            "{\"sessionId\":\"claude-history-1\",\"timestamp\":\"2026-04-18T12:00:00Z\",\"display\":\"hello\"}\n",
        )?;

        let codex_root = codex_fixture_root();
        let sources = ConfiguredSources::new(vec![
            ConfiguredSource::directory(
                SourceProvider::Claude,
                SourceFileKind::Transcript,
                claude_projects,
            ),
            ConfiguredSource::file(
                SourceProvider::Claude,
                SourceFileKind::History,
                claude_history,
            ),
            ConfiguredSource::directory(
                SourceProvider::Codex,
                SourceFileKind::Rollout,
                codex_root.join("sessions"),
            ),
            ConfiguredSource::file(
                SourceProvider::Codex,
                SourceFileKind::History,
                codex_root.join("history.jsonl"),
            ),
            ConfiguredSource::file(
                SourceProvider::Codex,
                SourceFileKind::SessionIndex,
                codex_root.join("session_index.jsonl"),
            ),
        ]);

        let mut db = Database::open(&db_path)?;
        let scan_report = scan_sources_manifest_with_policy(
            &mut db,
            &sources,
            &ProjectIdentityPolicy::default(),
            &[],
        )?;
        assert_eq!(scan_report.discovered_source_files, 5);

        let import_report =
            import_all_with_sources_and_perf_logger(db.connection(), &db_path, &sources, None)?;
        assert_eq!(import_report.deferred_failure_count, 0);
        assert!(import_report.deferred_failure_summary.is_none());

        let second_import_report =
            import_all_with_sources_and_perf_logger(db.connection(), &db_path, &sources, None)?;
        assert_eq!(second_import_report.deferred_failure_count, 0);
        assert!(second_import_report.deferred_failure_summary.is_none());

        let counts: (i64, i64, i64, i64, i64, i64) = db.connection().query_row(
            "
            SELECT
                (SELECT COUNT(*) FROM source_file WHERE imported_schema_version = ?1),
                (SELECT COUNT(*) FROM conversation),
                (SELECT COUNT(*) FROM history_event),
                (SELECT COUNT(*) FROM codex_rollout_session),
                (SELECT COUNT(*) FROM codex_rollout_event),
                (SELECT COUNT(*) FROM codex_session_index_entry)
            ",
            params![crate::import::IMPORT_SCHEMA_VERSION],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )?;
        assert_eq!(counts.0, 5);
        assert_eq!(counts.1, 2);
        assert_eq!(counts.2, 2);
        assert_eq!(counts.3, 1);
        assert_eq!(counts.4, 9);
        assert_eq!(counts.5, 1);

        let codex_shared_counts: (i64, i64, i64, i64) = db.connection().query_row(
            "
            SELECT
                (SELECT COUNT(*)
                 FROM conversation c
                 JOIN source_file sf ON sf.id = c.source_file_id
                 WHERE sf.source_provider = 'codex' AND sf.source_kind = 'rollout'),
                (SELECT COUNT(*)
                 FROM message m
                 JOIN conversation c ON c.id = m.conversation_id
                 JOIN source_file sf ON sf.id = c.source_file_id
                 WHERE sf.source_provider = 'codex' AND sf.source_kind = 'rollout'),
                (SELECT COUNT(*)
                 FROM turn t
                 JOIN conversation c ON c.id = t.conversation_id
                 JOIN source_file sf ON sf.id = c.source_file_id
                 WHERE sf.source_provider = 'codex' AND sf.source_kind = 'rollout'),
                (SELECT COUNT(*)
                 FROM action a
                 JOIN turn t ON t.id = a.turn_id
                 JOIN conversation c ON c.id = t.conversation_id
                 JOIN source_file sf ON sf.id = c.source_file_id
                 WHERE sf.source_provider = 'codex' AND sf.source_kind = 'rollout')
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(codex_shared_counts, (1, 5, 1, 3));

        let codex_tool_row: (String, Option<i64>, Option<i64>) = db.connection().query_row(
            "
            SELECT mp.tool_name, m.input_tokens, m.output_tokens
            FROM message_part mp
            JOIN message m ON m.id = mp.message_id
            JOIN conversation c ON c.id = m.conversation_id
            JOIN source_file sf ON sf.id = c.source_file_id
            WHERE sf.source_provider = 'codex'
                AND sf.source_kind = 'rollout'
                AND mp.part_kind = 'tool_use'
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(codex_tool_row.0, "Bash");
        assert_eq!(codex_tool_row.1, Some(123));
        assert_eq!(codex_tool_row.2, Some(45));

        let codex_history_row: (Option<String>, Option<String>, Option<String>) =
            db.connection().query_row(
                "
                SELECT session_id, raw_project, display_text
                FROM history_event he
                JOIN source_file sf ON sf.id = he.source_file_id
                WHERE sf.source_provider = 'codex' AND sf.source_kind = 'history'
                ",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
        assert_eq!(codex_history_row.0.as_deref(), Some("codex-session-1"));
        assert_eq!(
            codex_history_row.1.as_deref(),
            Some("/tmp/redacted/project-a")
        );
        assert_eq!(
            codex_history_row.2.as_deref(),
            Some("Investigate failing test")
        );

        let session_index_row: (Option<String>, Option<String>) = db.connection().query_row(
            "
            SELECT session_id, rollout_relative_path
            FROM codex_session_index_entry
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(session_index_row.0.as_deref(), Some("codex-session-1"));
        assert_eq!(
            session_index_row.1.as_deref(),
            Some("2026/04/18/rollout-2026-04-18T12-00-00Z.jsonl")
        );

        let rollout_row: (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
        ) = db.connection().query_row(
            "
                SELECT
                    crs.session_id,
                    crs.raw_cwd_path,
                    crs.cli_version,
                    crs.model_name,
                    p.root_path
                FROM codex_rollout_session crs
                JOIN project p ON p.id = crs.project_id
                ",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;
        assert_eq!(rollout_row.0.as_deref(), Some("codex-session-1"));
        assert_eq!(rollout_row.1.as_deref(), Some("/tmp/redacted/project-a"));
        assert_eq!(rollout_row.2.as_deref(), Some("0.0.0-test"));
        assert_eq!(rollout_row.3.as_deref(), Some("gpt-5.4-codex"));
        assert_eq!(rollout_row.4, "/tmp/redacted/project-a");

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
                source_provider,
                source_kind,
                modified_at_utc,
                size_bytes
            )
            VALUES (?1, ?2, 'claude', 'transcript', ?3, ?4)
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

    #[allow(dead_code)]
    fn write_action_fixture(path: &Path, session_id: &str, cwd: &Path) -> Result<i64> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let project_file = cwd.join("src").join("main.rs");
        let content = format!(
            concat!(
                "{{\"type\":\"user\",\"uuid\":\"{session_id}-user\",\"timestamp\":\"2026-03-26T10:00:00Z\",\"sessionId\":\"{session_id}\",\"cwd\":\"{cwd}\",\"message\":{{\"role\":\"user\",\"content\":\"Inspect the project\"}}}}\n",
                "{{\"type\":\"assistant\",\"uuid\":\"{session_id}-assistant\",\"timestamp\":\"2026-03-26T10:00:01Z\",\"sessionId\":\"{session_id}\",\"message\":{{\"id\":\"msg-{session_id}\",\"role\":\"assistant\",\"content\":[{{\"type\":\"tool_use\",\"id\":\"toolu-{session_id}\",\"name\":\"Read\",\"input\":{{\"file_path\":\"{project_file}\"}}}},{{\"type\":\"text\",\"text\":\"Reading the file\"}}],\"usage\":{{\"input_tokens\":3,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":0,\"output_tokens\":1}},\"model\":\"claude-haiku\",\"stop_reason\":\"tool_use\"}}}}\n",
                "{{\"type\":\"user\",\"uuid\":\"{session_id}-tool-result\",\"timestamp\":\"2026-03-26T10:00:02Z\",\"sessionId\":\"{session_id}\",\"message\":{{\"role\":\"user\",\"content\":[{{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-{session_id}\",\"content\":\"fn main() {{}}\",\"is_error\":false}}]}}}}\n"
            ),
            session_id = session_id,
            cwd = cwd.display(),
            project_file = project_file.display(),
        );

        fs::write(path, &content).with_context(|| format!("unable to write {}", path.display()))?;
        i64::try_from(content.len()).context("fixture size exceeded i64")
    }

    fn write_malformed_session_fixture(path: &Path, session_id: &str) -> Result<i64> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let content = format!(
            concat!(
                "{{\"type\":\"user\",\"uuid\":\"{session_id}-user\",\"timestamp\":\"2026-03-26T10:00:00Z\",\"sessionId\":\"{session_id}\",\"message\":{{\"role\":\"user\",\"content\":\"Inspect the project\"}}}}\n",
                "{{\"type\":\"assistant\",\"uuid\":\"{session_id}-assistant\",\"timestamp\":\"2026-03-26T10:00:01Z\",\"sessionId\":\"{session_id}\",\"message\":{{\"id\":\"msg-{session_id}\",\"role\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"broken\"}}]"
            ),
            session_id = session_id,
        );

        fs::write(path, &content).with_context(|| format!("unable to write {}", path.display()))?;
        i64::try_from(content.len()).context("fixture size exceeded i64")
    }

    fn write_session_fixture_with_cwd(path: &Path, session_id: &str, cwd: &Path) -> Result<i64> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let content = format!(
            concat!(
                "{{\"type\":\"user\",\"uuid\":\"{session_id}-user\",\"timestamp\":\"2026-03-26T10:00:00Z\",\"sessionId\":\"{session_id}\",\"cwd\":\"{cwd}\",\"message\":{{\"role\":\"user\",\"content\":\"Inspect the project\"}}}}\n",
                "{{\"type\":\"assistant\",\"uuid\":\"{session_id}-assistant\",\"timestamp\":\"2026-03-26T10:00:01Z\",\"sessionId\":\"{session_id}\",\"message\":{{\"id\":\"msg-{session_id}\",\"role\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"Working on it\"}}],\"usage\":{{\"input_tokens\":3,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":0,\"output_tokens\":1}},\"model\":\"claude-haiku\",\"stop_reason\":\"end_turn\"}}}}\n"
            ),
            session_id = session_id,
            cwd = cwd.display(),
        );

        fs::write(path, &content).with_context(|| format!("unable to write {}", path.display()))?;
        i64::try_from(content.len()).context("fixture size exceeded i64")
    }

    fn conversation_ids_by_relative_path(conn: &Connection) -> Result<BTreeMap<String, i64>> {
        let mut stmt = conn.prepare(
            "
            SELECT source_file.relative_path, conversation.id
            FROM conversation
            JOIN source_file ON source_file.id = conversation.source_file_id
            ORDER BY source_file.relative_path
            ",
        )?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        rows.collect::<rusqlite::Result<BTreeMap<_, _>>>()
            .context("unable to collect conversation ids by relative path")
    }

    fn codex_fixture_root() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("codex")
    }
}
