use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use gix::init;
use jiff::{Timestamp, ToSpan, tz::TimeZone};
use rusqlite::params;

use crate::db::Database;
use crate::import::{StartupOpenReason, scan_source_manifest, start_startup_import};
use crate::query::{
    BrowseFilters, BrowsePath, BrowseRequest, MetricLens, QueryEngine, RootView, SnapshotBounds,
};

const DAY_SPACING_HOURS: i64 = 24;
const SESSION_SPACING_MINUTES: i64 = 5;
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const POLL_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleValidationProfile {
    Quick,
    TenX,
}

impl ScaleValidationProfile {
    pub const fn spec(self) -> ScaleValidationSpec {
        match self {
            Self::Quick => ScaleValidationSpec {
                project_count: 2,
                day_count: 4,
                sessions_per_day: 3,
            },
            Self::TenX => ScaleValidationSpec {
                project_count: 6,
                day_count: 14,
                sessions_per_day: 10,
            },
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::TenX => "ten-x",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScaleValidationSpec {
    pub project_count: usize,
    pub day_count: usize,
    pub sessions_per_day: usize,
}

impl ScaleValidationSpec {
    pub const fn quick() -> Self {
        ScaleValidationProfile::Quick.spec()
    }

    pub const fn ten_x() -> Self {
        ScaleValidationProfile::TenX.spec()
    }

    pub fn expected_source_files(self) -> usize {
        self.project_count * self.day_count * self.sessions_per_day
    }
}

impl Default for ScaleValidationSpec {
    fn default() -> Self {
        Self::ten_x()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScaleValidationReport {
    pub spec: ScaleValidationSpec,
    pub artifacts_root: PathBuf,
    pub source_root: PathBuf,
    pub db_path: PathBuf,
    pub discovered_source_files: usize,
    pub discovered_projects: usize,
    pub total_chunks: usize,
    pub startup_chunks: usize,
    pub startup_open_reason: StartupOpenReason,
    pub startup_snapshot: SnapshotBounds,
    pub final_snapshot: SnapshotBounds,
    pub fixture_generation_ms: u128,
    pub scan_ms: u128,
    pub last_24h_ready_ms: u128,
    pub first_usable_ui_ms: u128,
    pub full_backfill_ms: u128,
    pub filter_options_ms: u128,
    pub project_root_browse_ms: u128,
    pub category_root_browse_ms: u128,
    pub project_drill_ms: u128,
    pub project_root_row_count: usize,
    pub category_root_row_count: usize,
    pub project_drill_row_count: usize,
}

#[derive(Debug)]
struct GeneratedCorpus {
    source_root: PathBuf,
    db_path: PathBuf,
    metadata: Vec<GeneratedSourceFile>,
    total_chunks: usize,
    startup_chunks: usize,
}

#[derive(Debug)]
struct GeneratedSourceFile {
    relative_path: String,
    modified_at_utc: String,
}

pub fn run_scale_validation(
    root: &Path,
    spec: ScaleValidationSpec,
) -> Result<ScaleValidationReport> {
    let generation_started_at = Instant::now();
    let corpus = generate_synthetic_corpus(root, spec)?;
    let fixture_generation_ms = generation_started_at.elapsed().as_millis();

    let pipeline_started_at = Instant::now();
    let mut database = Database::open(&corpus.db_path)?;
    let scan_report = scan_source_manifest(&mut database, &corpus.source_root)?;
    apply_manifest_timestamps(database.connection_mut(), &corpus.metadata)?;
    let scan_ms = pipeline_started_at.elapsed().as_millis();

    let discovered_projects = count_projects(database.connection())?;
    let startup_import =
        start_startup_import(database.connection(), &corpus.db_path, &corpus.source_root)?;
    let first_usable_ui_ms = pipeline_started_at.elapsed().as_millis();
    let startup_open_reason = startup_import.open_reason;
    let startup_snapshot = startup_import.snapshot.clone();
    drop(startup_import);

    let last_24h_ready_ms = if startup_open_reason == StartupOpenReason::Last24hReady {
        first_usable_ui_ms
    } else {
        wait_for_completed_chunks(
            &corpus.db_path,
            corpus.startup_chunks,
            pipeline_started_at,
            "last-24h chunk slice",
        )?
    };

    let full_backfill_ms = wait_for_completed_chunks(
        &corpus.db_path,
        corpus.total_chunks,
        pipeline_started_at,
        "full backfill",
    )?;

    let query_db = Database::open(&corpus.db_path)?;
    let engine = QueryEngine::new(query_db.connection());
    let final_snapshot = engine.latest_snapshot_bounds()?;

    let filter_options_started_at = Instant::now();
    let filter_options = engine.filter_options(&final_snapshot)?;
    let filter_options_ms = filter_options_started_at.elapsed().as_millis();

    let project_root_started_at = Instant::now();
    let project_root_rows = engine.browse(&BrowseRequest {
        snapshot: final_snapshot.clone(),
        root: RootView::ProjectHierarchy,
        lens: MetricLens::UncachedInput,
        filters: BrowseFilters::default(),
        path: BrowsePath::Root,
    })?;
    let project_root_browse_ms = project_root_started_at.elapsed().as_millis();

    let category_root_started_at = Instant::now();
    let category_root_rows = engine.browse(&BrowseRequest {
        snapshot: final_snapshot.clone(),
        root: RootView::CategoryHierarchy,
        lens: MetricLens::UncachedInput,
        filters: BrowseFilters::default(),
        path: BrowsePath::Root,
    })?;
    let category_root_browse_ms = category_root_started_at.elapsed().as_millis();

    let project_id = filter_options
        .projects
        .first()
        .map(|project| project.id)
        .ok_or_else(|| anyhow::anyhow!("scale validation produced no visible projects"))?;
    let project_drill_started_at = Instant::now();
    let project_drill_rows = engine.browse(&BrowseRequest {
        snapshot: final_snapshot.clone(),
        root: RootView::ProjectHierarchy,
        lens: MetricLens::UncachedInput,
        filters: BrowseFilters::default(),
        path: BrowsePath::Project { project_id },
    })?;
    let project_drill_ms = project_drill_started_at.elapsed().as_millis();

    Ok(ScaleValidationReport {
        spec,
        artifacts_root: root.to_path_buf(),
        source_root: corpus.source_root,
        db_path: corpus.db_path,
        discovered_source_files: scan_report.discovered_source_files,
        discovered_projects,
        total_chunks: corpus.total_chunks,
        startup_chunks: corpus.startup_chunks,
        startup_open_reason,
        startup_snapshot,
        final_snapshot,
        fixture_generation_ms,
        scan_ms,
        last_24h_ready_ms,
        first_usable_ui_ms,
        full_backfill_ms,
        filter_options_ms,
        project_root_browse_ms,
        category_root_browse_ms,
        project_drill_ms,
        project_root_row_count: project_root_rows.len(),
        category_root_row_count: category_root_rows.len(),
        project_drill_row_count: project_drill_rows.len(),
    })
}

fn generate_synthetic_corpus(root: &Path, spec: ScaleValidationSpec) -> Result<GeneratedCorpus> {
    let source_root = root.join("source");
    let projects_root = root.join("projects");
    let db_path = root.join("validation.sqlite3");
    fs::create_dir_all(&source_root)?;
    fs::create_dir_all(&projects_root)?;

    let time_zone = TimeZone::system();
    let baseline = stable_validation_baseline(&time_zone)?;
    let startup_days = startup_days(baseline, &time_zone)?;
    let mut metadata = Vec::with_capacity(spec.expected_source_files());
    let mut chunk_keys = BTreeSet::new();
    let mut startup_chunk_keys = BTreeSet::new();

    for project_index in 0..spec.project_count {
        let project_root = projects_root.join(format!("project-{project_index:02}"));
        init_project_repo(&project_root)?;
        let cwd = project_root.join("workspace");
        fs::create_dir_all(&cwd)?;

        for day_index in 0..spec.day_count {
            for session_index in 0..spec.sessions_per_day {
                let base_timestamp =
                    session_timestamp(baseline, day_index, session_index).with_context(|| {
                        format!(
                            "unable to create synthetic session timestamp for project {project_index}, day {day_index}, session {session_index}"
                        )
                    })?;
                let modified_at_utc = base_timestamp.to_string();
                let chunk_day_local = local_day_for_timestamp(base_timestamp, &time_zone);
                let relative_path = format!(
                    "project-{project_index:02}/day-{day_index:02}/session-{session_index:03}.jsonl"
                );
                let session_path = source_root.join(&relative_path);

                let contents = if session_index % 2 == 0 {
                    build_read_edit_test_session(
                        &cwd,
                        project_index,
                        day_index,
                        session_index,
                        base_timestamp,
                    )?
                } else {
                    build_documentation_session(
                        &cwd,
                        project_index,
                        day_index,
                        session_index,
                        base_timestamp,
                    )?
                };

                if let Some(parent) = session_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&session_path, contents)
                    .with_context(|| format!("unable to write {}", session_path.display()))?;

                metadata.push(GeneratedSourceFile {
                    relative_path,
                    modified_at_utc: modified_at_utc.clone(),
                });
                let project_key = format!("git:{}", project_root.display());
                let chunk_key = (project_key.clone(), chunk_day_local.clone());
                chunk_keys.insert(chunk_key.clone());
                if startup_days.contains(&chunk_day_local) {
                    startup_chunk_keys.insert(chunk_key);
                }
            }
        }
    }

    Ok(GeneratedCorpus {
        source_root,
        db_path,
        metadata,
        total_chunks: chunk_keys.len(),
        startup_chunks: startup_chunk_keys.len(),
    })
}

fn stable_validation_baseline(time_zone: &TimeZone) -> Result<Timestamp> {
    let today = Timestamp::now().to_zoned(time_zone.clone()).date();
    today
        .at(12, 0, 0, 0)
        .to_zoned(time_zone.clone())
        .map(|zoned| zoned.timestamp())
        .context("unable to construct stable local-noon validation baseline")
}

fn init_project_repo(project_root: &Path) -> Result<()> {
    if !project_root.exists() {
        init(project_root).with_context(|| {
            format!(
                "unable to initialize git repo at {}",
                project_root.display()
            )
        })?;
    }
    fs::write(
        project_root.join("README.md"),
        "synthetic validation repo\n",
    )
    .with_context(|| format!("unable to seed {}", project_root.display()))?;
    Ok(())
}

fn session_timestamp(
    baseline: Timestamp,
    day_index: usize,
    session_index: usize,
) -> Result<Timestamp> {
    let day_offset_hours = i64::try_from(day_index)
        .context("day index overflowed i64")?
        .checked_mul(DAY_SPACING_HOURS)
        .context("day offset hours overflowed i64")?;
    let session_offset_minutes = i64::try_from(session_index)
        .context("session index overflowed i64")?
        .checked_mul(SESSION_SPACING_MINUTES)
        .context("session offset minutes overflowed i64")?;

    baseline
        .checked_sub(day_offset_hours.hours())
        .and_then(|timestamp| timestamp.checked_sub(session_offset_minutes.minutes()))
        .context("unable to offset synthetic validation timestamp")
}

fn build_read_edit_test_session(
    cwd: &Path,
    project_index: usize,
    day_index: usize,
    session_index: usize,
    base_timestamp: Timestamp,
) -> Result<String> {
    let project_root = cwd
        .parent()
        .ok_or_else(|| anyhow::anyhow!("workspace cwd {} had no parent", cwd.display()))?;
    let parser_path = project_root.join("src").join("parser.rs");
    let prompt_timestamp = base_timestamp.to_string();
    let read_timestamp = base_timestamp.checked_add(1.seconds())?.to_string();
    let read_result_timestamp = base_timestamp.checked_add(2.seconds())?.to_string();
    let edit_timestamp = base_timestamp.checked_add(3.seconds())?.to_string();
    let edit_result_timestamp = base_timestamp.checked_add(4.seconds())?.to_string();
    let test_timestamp = base_timestamp.checked_add(5.seconds())?.to_string();
    let test_result_timestamp = base_timestamp.checked_add(6.seconds())?.to_string();
    let session_id =
        format!("project-{project_index:02}-day-{day_index:02}-session-{session_index:03}");

    Ok(format!(
        concat!(
            "{{\"type\":\"user\",\"uuid\":\"{session_id}-prompt\",\"timestamp\":\"{prompt_timestamp}\",\"sessionId\":\"{session_id}\",\"cwd\":\"{cwd}\",\"message\":{{\"role\":\"user\",\"content\":\"Investigate the parser failure.\"}}}}\n",
            "{{\"type\":\"assistant\",\"uuid\":\"{session_id}-read\",\"timestamp\":\"{read_timestamp}\",\"sessionId\":\"{session_id}\",\"message\":{{\"id\":\"msg-{session_id}-read\",\"role\":\"assistant\",\"content\":[{{\"type\":\"tool_use\",\"id\":\"toolu-{session_id}-read\",\"name\":\"Read\",\"input\":{{\"file_path\":\"{parser_path}\"}}}}],\"usage\":{{\"input_tokens\":3,\"cache_creation_input_tokens\":4,\"cache_read_input_tokens\":0,\"output_tokens\":1}},\"model\":\"claude-opus\",\"stop_reason\":\"tool_use\"}}}}\n",
            "{{\"type\":\"user\",\"uuid\":\"{session_id}-read-result\",\"timestamp\":\"{read_result_timestamp}\",\"sessionId\":\"{session_id}\",\"message\":{{\"role\":\"user\",\"content\":[{{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-{session_id}-read\",\"content\":\"fn parse() {{}}\",\"is_error\":false}}]}},\"toolUseResult\":{{\"stdout\":\"fn parse() {{}}\"}}}}\n",
            "{{\"type\":\"assistant\",\"uuid\":\"{session_id}-edit\",\"timestamp\":\"{edit_timestamp}\",\"sessionId\":\"{session_id}\",\"message\":{{\"id\":\"msg-{session_id}-edit\",\"role\":\"assistant\",\"content\":[{{\"type\":\"tool_use\",\"id\":\"toolu-{session_id}-edit\",\"name\":\"Edit\",\"input\":{{\"file_path\":\"{parser_path}\",\"old_string\":\"fn parse() {{}}\",\"new_string\":\"fn parse(input: &str) {{}}\"}}}}],\"usage\":{{\"input_tokens\":5,\"cache_creation_input_tokens\":6,\"cache_read_input_tokens\":0,\"output_tokens\":2}},\"model\":\"claude-opus\",\"stop_reason\":\"tool_use\"}}}}\n",
            "{{\"type\":\"user\",\"uuid\":\"{session_id}-edit-result\",\"timestamp\":\"{edit_result_timestamp}\",\"sessionId\":\"{session_id}\",\"message\":{{\"role\":\"user\",\"content\":[{{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-{session_id}-edit\",\"content\":\"updated\",\"is_error\":false}}]}},\"toolUseResult\":{{\"type\":\"edit\"}}}}\n",
            "{{\"type\":\"assistant\",\"uuid\":\"{session_id}-test\",\"timestamp\":\"{test_timestamp}\",\"sessionId\":\"{session_id}\",\"message\":{{\"id\":\"msg-{session_id}-test\",\"role\":\"assistant\",\"content\":[{{\"type\":\"tool_use\",\"id\":\"toolu-{session_id}-test\",\"name\":\"Bash\",\"input\":{{\"command\":\"cargo test\"}}}}],\"usage\":{{\"input_tokens\":7,\"cache_creation_input_tokens\":8,\"cache_read_input_tokens\":9,\"output_tokens\":3}},\"model\":\"claude-opus\",\"stop_reason\":\"tool_use\"}}}}\n",
            "{{\"type\":\"user\",\"uuid\":\"{session_id}-test-result\",\"timestamp\":\"{test_result_timestamp}\",\"sessionId\":\"{session_id}\",\"message\":{{\"role\":\"user\",\"content\":[{{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-{session_id}-test\",\"content\":\"ok\",\"is_error\":false}}]}},\"toolUseResult\":{{\"stdout\":\"ok\"}}}}\n"
        ),
        session_id = session_id,
        prompt_timestamp = prompt_timestamp,
        read_timestamp = read_timestamp,
        read_result_timestamp = read_result_timestamp,
        edit_timestamp = edit_timestamp,
        edit_result_timestamp = edit_result_timestamp,
        test_timestamp = test_timestamp,
        test_result_timestamp = test_result_timestamp,
        cwd = cwd.display(),
        parser_path = parser_path.display(),
    ))
}

fn build_documentation_session(
    cwd: &Path,
    project_index: usize,
    day_index: usize,
    session_index: usize,
    base_timestamp: Timestamp,
) -> Result<String> {
    let project_root = cwd
        .parent()
        .ok_or_else(|| anyhow::anyhow!("workspace cwd {} had no parent", cwd.display()))?;
    let doc_path = project_root.join("docs").join("README.md");
    let prompt_timestamp = base_timestamp.to_string();
    let mixed_timestamp = base_timestamp.checked_add(1.seconds())?.to_string();
    let mixed_result_timestamp = base_timestamp.checked_add(2.seconds())?.to_string();
    let write_timestamp = base_timestamp.checked_add(3.seconds())?.to_string();
    let write_result_timestamp = base_timestamp.checked_add(4.seconds())?.to_string();
    let session_id =
        format!("project-{project_index:02}-day-{day_index:02}-session-{session_index:03}");

    Ok(format!(
        concat!(
            "{{\"type\":\"user\",\"uuid\":\"{session_id}-prompt\",\"timestamp\":\"{prompt_timestamp}\",\"sessionId\":\"{session_id}\",\"cwd\":\"{cwd}\",\"message\":{{\"role\":\"user\",\"content\":\"Fix docs and rerun checks.\"}}}}\n",
            "{{\"type\":\"assistant\",\"uuid\":\"{session_id}-mixed\",\"timestamp\":\"{mixed_timestamp}\",\"sessionId\":\"{session_id}\",\"message\":{{\"id\":\"msg-{session_id}-mixed\",\"role\":\"assistant\",\"content\":[{{\"type\":\"tool_use\",\"id\":\"toolu-{session_id}-mixed\",\"name\":\"Bash\",\"input\":{{\"command\":\"git status && cargo test\"}}}}],\"usage\":{{\"input_tokens\":2,\"cache_creation_input_tokens\":3,\"cache_read_input_tokens\":0,\"output_tokens\":1}},\"model\":\"claude-haiku\",\"stop_reason\":\"tool_use\"}}}}\n",
            "{{\"type\":\"user\",\"uuid\":\"{session_id}-mixed-result\",\"timestamp\":\"{mixed_result_timestamp}\",\"sessionId\":\"{session_id}\",\"message\":{{\"role\":\"user\",\"content\":[{{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-{session_id}-mixed\",\"content\":\"mixed output\",\"is_error\":false}}]}},\"toolUseResult\":{{\"stdout\":\"mixed output\"}}}}\n",
            "{{\"type\":\"assistant\",\"uuid\":\"{session_id}-doc\",\"timestamp\":\"{write_timestamp}\",\"sessionId\":\"{session_id}\",\"message\":{{\"id\":\"msg-{session_id}-doc\",\"role\":\"assistant\",\"content\":[{{\"type\":\"tool_use\",\"id\":\"toolu-{session_id}-doc\",\"name\":\"Write\",\"input\":{{\"file_path\":\"{doc_path}\",\"content\":\"updated docs\"}}}}],\"usage\":{{\"input_tokens\":4,\"cache_creation_input_tokens\":5,\"cache_read_input_tokens\":0,\"output_tokens\":2}},\"model\":\"claude-haiku\",\"stop_reason\":\"tool_use\"}}}}\n",
            "{{\"type\":\"user\",\"uuid\":\"{session_id}-doc-result\",\"timestamp\":\"{write_result_timestamp}\",\"sessionId\":\"{session_id}\",\"message\":{{\"role\":\"user\",\"content\":[{{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-{session_id}-doc\",\"content\":\"written\",\"is_error\":false}}]}},\"toolUseResult\":{{\"stdout\":\"written\"}}}}\n"
        ),
        session_id = session_id,
        prompt_timestamp = prompt_timestamp,
        mixed_timestamp = mixed_timestamp,
        mixed_result_timestamp = mixed_result_timestamp,
        write_timestamp = write_timestamp,
        write_result_timestamp = write_result_timestamp,
        cwd = cwd.display(),
        doc_path = doc_path.display(),
    ))
}

fn apply_manifest_timestamps(
    conn: &mut rusqlite::Connection,
    metadata: &[GeneratedSourceFile],
) -> Result<()> {
    let tx = conn
        .transaction()
        .context("unable to start synthetic manifest timestamp update transaction")?;

    for file in metadata {
        tx.execute(
            "
            UPDATE source_file
            SET modified_at_utc = ?2
            WHERE relative_path = ?1
            ",
            params![file.relative_path, file.modified_at_utc],
        )
        .with_context(|| {
            format!(
                "unable to set synthetic modified_at_utc for {}",
                file.relative_path
            )
        })?;
    }

    tx.commit()
        .context("unable to commit synthetic manifest timestamps")?;
    Ok(())
}

fn count_projects(conn: &rusqlite::Connection) -> Result<usize> {
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM project", [], |row| row.get(0))
        .context("unable to count discovered projects")?;
    usize::try_from(count).context("project count overflowed usize")
}

fn wait_for_completed_chunks(
    db_path: &Path,
    expected_complete_chunks: usize,
    pipeline_started_at: Instant,
    label: &str,
) -> Result<u128> {
    let deadline = Instant::now() + POLL_TIMEOUT;
    let database = Database::open(db_path)?;

    loop {
        let completed = completed_chunk_count(database.connection())?;
        if completed >= expected_complete_chunks {
            return Ok(pipeline_started_at.elapsed().as_millis());
        }

        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for {label}; completed {completed} of {expected_complete_chunks} chunks"
            );
        }

        thread::sleep(POLL_INTERVAL);
    }
}

fn completed_chunk_count(conn: &rusqlite::Connection) -> Result<usize> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM import_chunk WHERE state = 'complete'",
            [],
            |row| row.get(0),
        )
        .context("unable to count completed import chunks")?;
    usize::try_from(count).context("completed chunk count overflowed usize")
}

fn startup_days(now: Timestamp, time_zone: &TimeZone) -> Result<BTreeSet<String>> {
    let threshold = now
        .checked_sub(24_i64.hours())
        .context("unable to compute startup validation threshold")?;

    let mut days = BTreeSet::new();
    days.insert(local_day_for_timestamp(now, time_zone));
    days.insert(local_day_for_timestamp(threshold, time_zone));
    Ok(days)
}

fn local_day_for_timestamp(timestamp: Timestamp, time_zone: &TimeZone) -> String {
    timestamp.to_zoned(time_zone.clone()).date().to_string()
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{ScaleValidationSpec, run_scale_validation};
    use crate::import::StartupOpenReason;

    #[test]
    fn small_scale_validation_completes_and_exercises_queries() -> anyhow::Result<()> {
        let temp = tempdir()?;
        let spec = ScaleValidationSpec {
            project_count: 2,
            day_count: 4,
            sessions_per_day: 3,
        };

        let report = run_scale_validation(temp.path(), spec)?;

        assert_eq!(report.discovered_source_files, spec.expected_source_files());
        assert_eq!(report.discovered_projects, spec.project_count);
        assert_eq!(report.total_chunks, spec.project_count * spec.day_count);
        assert_eq!(report.startup_chunks, spec.project_count * 2);
        assert_eq!(report.startup_open_reason, StartupOpenReason::Last24hReady);
        assert_eq!(
            report.startup_snapshot.max_publish_seq,
            report.startup_chunks as u64
        );
        assert_eq!(
            report.final_snapshot.max_publish_seq,
            report.total_chunks as u64
        );
        assert_eq!(
            report.final_snapshot.published_chunk_count,
            report.total_chunks
        );
        assert!(report.project_root_row_count > 0);
        assert!(report.category_root_row_count > 0);
        assert!(report.project_drill_row_count > 0);
        assert!(report.last_24h_ready_ms <= report.full_backfill_ms);
        assert!(report.first_usable_ui_ms <= report.full_backfill_ms);

        Ok(())
    }
}
