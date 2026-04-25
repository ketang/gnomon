use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use gnomon_core::db::Database;
use gnomon_core::import::{
    StartupOpenReason, StartupWorkerEvent, import_all, scan_source_manifest, start_startup_import,
};
use rusqlite::Connection;
use tempfile::TempDir;

const EVENT_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
// The mutation target is a file in a chunk whose `chunk_day_local` is unique
// (no other project has a chunk on that day) so `lookup_chunk_publish_seq`
// resolves unambiguously. chunk 72 (bento, 2026-03-09) has 5 files — the
// touched chunk stays non-empty after one file moves to a "recent" chunk.
const MUTATION_TARGET_RELATIVE_PATH: &str =
    "-home-ketan-project-bento/65d724c9-d73d-41c3-8c8d-722a3a0ec57d.jsonl";
const MUTATION_TARGET_CHUNK_DAY: &str = "2026-03-09";
// Different from the file's current mtime (2026-03-10T04:50:27Z), still
// resolving to chunk_day_local=2026-03-09 in any UTC-5..UTC-8 timezone.
const MUTATION_TARGET_SHIFTED_TIMESTAMP: &str = "2026-03-10T04:15:00Z";
const SUBSET_CHUNK_DAY_SEQUENCE: &[&str] = &[
    "2026-03-21",
    "2026-03-13",
    "2026-03-22",
    "2026-02-24",
    "2026-03-19",
    "2026-03-13",
    "2026-03-18",
    "2026-03-20",
    "2026-02-23",
    "2026-03-12",
    "2026-03-15",
    "2026-03-16",
    "2026-02-21",
    "2026-03-21",
    "2026-02-13",
    "2026-02-13",
    "2026-03-19",
    "2026-03-18",
    "2026-03-10",
    "2026-03-02",
    "2026-03-19",
    "2026-03-18",
    "2026-03-22",
    "2026-03-14",
    "2026-03-17",
    "2026-03-17",
    "2026-03-22",
    "2026-03-16",
    "2026-03-21",
    "2026-03-14",
    "2026-03-16",
    "2026-03-13",
    "2026-03-21",
    "2026-03-12",
    "2026-03-20",
    "2026-02-12",
    "2026-03-19",
    "2026-03-11",
    "2026-03-18",
    "2026-03-13",
    "2026-03-17",
    "2026-03-12",
    "2026-03-10",
    "2026-03-03",
    "2026-03-01",
    "2026-02-25",
    "2026-02-24",
    "2026-03-16",
    "2026-03-15",
    "2026-02-20",
    "2026-02-10",
    "2026-02-11",
    "2026-03-20",
    "2026-03-10",
    "2026-03-09",
    "2026-02-13",
    "2026-02-12",
    "2026-02-08",
    "2026-03-08",
    "2026-02-11",
    "2026-03-19",
    "2026-03-06",
    "2026-02-07",
    "2026-03-17",
    "2026-03-05",
    "2026-02-06",
    "2026-02-04",
    "2026-03-04",
    "2026-03-03",
    "2026-03-02",
    "2026-02-28",
    "2026-02-27",
    "2026-02-26",
    "2026-02-10",
    "2026-03-16",
    "2026-02-25",
    "2026-02-24",
    "2026-03-15",
    "2026-02-23",
    "2026-02-21",
    "2026-02-09",
    "2026-03-14",
    "2026-03-13",
    "2026-02-08",
    "2026-03-12",
    "2026-03-11",
    "2026-02-07",
    "2026-02-06",
    "2026-03-10",
    "2026-03-05",
    "2026-03-04",
    "2026-03-03",
    "2026-03-01",
    "2026-02-28",
    "2026-02-27",
    "2026-02-25",
    "2026-02-24",
];
const SUBSET_TOP_ACTION_SIGNATURE: &[(&str, i64)] = &[
    ("<null>", 4_758),
    ("file read", 3_265),
    ("prompt", 2_693),
    ("assistant reasoning", 2_005),
    ("content search", 981),
    ("file edit", 796),
    ("directory inspection", 503),
    ("file glob", 398),
    ("git inspection", 354),
    ("task coordination", 320),
    ("document edit", 197),
    ("filesystem find", 189),
];

const SUBSET_EXPECTATIONS: CorpusExpectations = CorpusExpectations {
    discovered_source_files: 1_126,
    inserted_projects: 11,
    inserted_source_files: 1_126,
    import_chunk_count: 97,
    project_count: 11,
    source_file_count: 1_126,
    transcript_source_file_count: 1_126,
    claude_history_source_file_count: 0,
    conversation_count: 1_119,
    stream_count: 1_119,
    message_count: 42_965,
    message_part_count: 60_791,
    turn_count: 2_699,
    action_count: 17_800,
    history_event_count: 0,
    import_warning_count: 7,
    imported_message_count_sum: 42_965,
    imported_action_count_sum: 17_800,
    imported_conversation_count_sum: 1_119,
    imported_turn_count_sum: 2_699,
    complete_chunk_count: 97,
    failed_chunk_count: 0,
    deferred_chunk_count: 97,
    startup_chunk_count: 0,
    chunk_action_rollup_count: 1_400,
    chunk_path_rollup_count: 20_767,
};

#[derive(Debug, Clone, Copy)]
struct CorpusExpectations {
    discovered_source_files: usize,
    inserted_projects: usize,
    inserted_source_files: usize,
    import_chunk_count: i64,
    project_count: i64,
    source_file_count: i64,
    transcript_source_file_count: i64,
    claude_history_source_file_count: i64,
    conversation_count: i64,
    stream_count: i64,
    message_count: i64,
    message_part_count: i64,
    turn_count: i64,
    action_count: i64,
    history_event_count: i64,
    import_warning_count: i64,
    imported_message_count_sum: i64,
    imported_action_count_sum: i64,
    imported_conversation_count_sum: i64,
    imported_turn_count_sum: i64,
    complete_chunk_count: i64,
    failed_chunk_count: i64,
    deferred_chunk_count: i64,
    startup_chunk_count: i64,
    chunk_action_rollup_count: i64,
    chunk_path_rollup_count: i64,
}

#[derive(Debug)]
struct PreparedCorpus {
    _temp_dir: TempDir,
    source_root: PathBuf,
}

#[derive(Debug, PartialEq, Eq)]
struct DatabaseCounts {
    project_count: i64,
    source_file_count: i64,
    transcript_source_file_count: i64,
    claude_history_source_file_count: i64,
    import_chunk_count: i64,
    complete_chunk_count: i64,
    failed_chunk_count: i64,
    deferred_chunk_count: i64,
    startup_chunk_count: i64,
    conversation_count: i64,
    stream_count: i64,
    message_count: i64,
    message_part_count: i64,
    turn_count: i64,
    action_count: i64,
    history_event_count: i64,
    import_warning_count: i64,
    imported_message_count_sum: i64,
    imported_action_count_sum: i64,
    imported_conversation_count_sum: i64,
    imported_turn_count_sum: i64,
    imported_source_file_count: i64,
    source_files_missing_import_metadata_count: i64,
    chunk_action_rollup_count: i64,
    chunk_path_rollup_count: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct SubsetSemanticCounts {
    chunk_day_sequence: Vec<String>,
    classified_action_count: i64,
    mixed_action_count: i64,
    unclassified_action_count: i64,
    tool_use_part_count: i64,
    tool_result_part_count: i64,
    parts_with_tool_name_count: i64,
    parts_with_tool_call_id_count: i64,
    relay_assistant_message_count: i64,
    assistant_message_count: i64,
    actions_with_null_normalized_action_count: i64,
    source_files_with_scan_warnings_count: i64,
    git_project_count: i64,
    path_project_count: i64,
    top_action_signature: Vec<(String, i64)>,
}

#[test]
#[ignore = "requires local import corpus fixture tarballs"]
fn subset_corpus_import_all_matches_expected_database_shape() -> Result<()> {
    let prepared = extract_corpus_archive("subset.tar.zst")?;
    let db_temp = TempDir::new().context("unable to create subset db tempdir")?;
    let db_path = db_temp.path().join("usage.sqlite3");
    let mut database = Database::open(&db_path)?;

    let scan_report = scan_source_manifest(&mut database, &prepared.source_root)?;
    assert_scan_report(&scan_report, SUBSET_EXPECTATIONS);

    let import_report = import_all(database.connection(), &db_path, &prepared.source_root)?;
    assert_eq!(import_report.startup_chunk_count, 0);
    assert_eq!(
        import_report.deferred_chunk_count as i64,
        SUBSET_EXPECTATIONS.deferred_chunk_count
    );
    assert_eq!(import_report.deferred_failure_count, 0);
    assert!(import_report.deferred_failure_summary.is_none());

    let counts = load_database_counts(database.connection(), &db_path)?;
    assert_database_counts(&counts, SUBSET_EXPECTATIONS);
    assert_subset_baseline_signature(database.connection(), &db_path)?;
    assert_query_layer_surfaces_sharded_data(database.connection(), SUBSET_EXPECTATIONS)?;
    assert_imported_count_sums(&counts, SUBSET_EXPECTATIONS);

    Ok(())
}

// After a sharded import, exercise the production query layer end-to-end:
//   * `QueryEngine::filter_options` must return a non-empty model list (needs `message`
//     rows from shards via the TEMP VIEW)
//   * A top-level `browse_request` at `RootView::ProjectHierarchy` must return one row
//     per discovered project with per-project action counts summing to the total
//   * A drill-down into a project must return its category rows with consistent counts
// This is the safety net we lacked when C1's first KEPT result shipped with empty
// `"rows": []` from `gnomon report` — if views break, this test fails loudly.
fn assert_query_layer_surfaces_sharded_data(
    conn: &Connection,
    expectations: CorpusExpectations,
) -> Result<()> {
    use gnomon_core::query::{
        BrowseFilters, BrowsePath, BrowseRequest, MetricLens, QueryEngine, RootView,
    };

    let engine = QueryEngine::new(conn);
    let snapshot = engine.latest_snapshot_bounds()?;
    assert!(
        snapshot.max_publish_seq > 0,
        "latest_snapshot_bounds should report published chunks"
    );

    let options = engine.filter_options(&snapshot)?;
    assert!(
        !options.models.is_empty(),
        "filter_options().models should contain at least one model drawn from the \
         `message` table in shards; empty means the TEMP VIEW is not wired up"
    );

    let project_rows = engine.browse(&BrowseRequest {
        snapshot: snapshot.clone(),
        root: RootView::ProjectHierarchy,
        lens: MetricLens::UncachedInput,
        filters: BrowseFilters::default(),
        path: BrowsePath::Root,
    })?;
    assert_eq!(
        project_rows.len(),
        expectations.project_count as usize,
        "top-level project browse should return one row per project"
    );

    // Drill into the first project; its category rows must cover the project.
    let first_project = project_rows
        .first()
        .context("project rows are empty after sharded import")?;
    let project_id = first_project
        .project_id
        .context("top-level project row missing project_id")?;
    let category_rows = engine.browse(&BrowseRequest {
        snapshot,
        root: RootView::ProjectHierarchy,
        lens: MetricLens::UncachedInput,
        filters: BrowseFilters::default(),
        path: BrowsePath::Project { project_id },
    })?;
    assert!(
        !category_rows.is_empty(),
        "drill-down into project {project_id} should return at least one category row"
    );
    Ok(())
}

#[test]
#[ignore = "requires local import corpus fixture tarballs"]
fn subset_corpus_recent_first_startup_import_defers_every_chunk_and_reaches_same_final_state()
-> Result<()> {
    let prepared = extract_corpus_archive("subset.tar.zst")?;
    let db_temp = TempDir::new().context("unable to create startup db tempdir")?;
    let db_path = db_temp.path().join("usage.sqlite3");
    let mut database = Database::open(&db_path)?;

    let scan_report = scan_source_manifest(&mut database, &prepared.source_root)?;
    assert_scan_report(&scan_report, SUBSET_EXPECTATIONS);

    let mut startup = start_startup_import(database.connection(), &db_path, &prepared.source_root)?;
    assert_eq!(startup.open_reason, StartupOpenReason::Last24hReady);
    assert_eq!(startup.snapshot.max_publish_seq, 0);
    assert!(startup.startup_status_message.is_none());
    assert!(startup.startup_progress_update.is_none());

    let status_updates = startup
        .take_status_updates()
        .context("startup import should expose a status update receiver")?;

    loop {
        match status_updates.recv_timeout(EVENT_WAIT_TIMEOUT) {
            Ok(StartupWorkerEvent::Progress { .. }) => {}
            Ok(StartupWorkerEvent::StartupSettled {
                startup_status_message,
            }) => {
                assert!(startup_status_message.is_none());
            }
            Ok(StartupWorkerEvent::DeferredFailures {
                deferred_status_message,
            }) => {
                bail!(
                    "subset corpus should complete without deferred failures, got: {:?}",
                    deferred_status_message
                );
            }
            Ok(StartupWorkerEvent::Finished) => break,
            Err(RecvTimeoutError::Timeout) => {
                bail!("timed out waiting for startup importer to finish")
            }
            Err(RecvTimeoutError::Disconnected) => {
                bail!("startup importer disconnected before sending Finished")
            }
        }
    }

    drop(startup);

    let counts = load_database_counts(database.connection(), &db_path)?;
    assert_database_counts(&counts, SUBSET_EXPECTATIONS);
    assert_subset_baseline_signature(database.connection(), &db_path)?;

    Ok(())
}

#[test]
#[ignore = "requires local import corpus fixture tarballs"]
fn subset_corpus_reimport_is_a_no_op_when_files_are_unchanged() -> Result<()> {
    let prepared = extract_corpus_archive("subset.tar.zst")?;
    let db_temp = TempDir::new().context("unable to create reimport db tempdir")?;
    let db_path = db_temp.path().join("usage.sqlite3");
    let mut database = Database::open(&db_path)?;

    let first_scan_report = scan_source_manifest(&mut database, &prepared.source_root)?;
    assert_scan_report(&first_scan_report, SUBSET_EXPECTATIONS);

    let first_import_report = import_all(database.connection(), &db_path, &prepared.source_root)?;
    assert_eq!(first_import_report.startup_chunk_count, 0);
    assert_eq!(
        first_import_report.deferred_chunk_count as i64,
        SUBSET_EXPECTATIONS.deferred_chunk_count
    );
    assert_eq!(first_import_report.deferred_failure_count, 0);
    assert!(first_import_report.deferred_failure_summary.is_none());

    let first_counts = load_database_counts(database.connection(), &db_path)?;
    assert_database_counts(&first_counts, SUBSET_EXPECTATIONS);

    let second_scan_report = scan_source_manifest(&mut database, &prepared.source_root)?;
    assert_eq!(
        second_scan_report.discovered_source_files,
        SUBSET_EXPECTATIONS.discovered_source_files
    );
    assert_eq!(second_scan_report.excluded_source_files, 0);
    assert_eq!(second_scan_report.inserted_projects, 0);
    assert_eq!(second_scan_report.updated_projects, 0);
    assert_eq!(second_scan_report.inserted_source_files, 0);
    assert_eq!(second_scan_report.updated_source_files, 0);
    assert_eq!(second_scan_report.deleted_source_files, 0);

    let second_import_report = import_all(database.connection(), &db_path, &prepared.source_root)?;
    assert_eq!(second_import_report.startup_chunk_count, 0);
    assert_eq!(second_import_report.deferred_chunk_count, 0);
    assert_eq!(second_import_report.deferred_failure_count, 0);
    assert!(second_import_report.deferred_failure_summary.is_none());

    let second_counts = load_database_counts(database.connection(), &db_path)?;
    assert_eq!(first_counts, second_counts);
    assert_subset_baseline_signature(database.connection(), &db_path)?;

    Ok(())
}

#[test]
#[ignore = "requires local import corpus fixture tarballs"]
fn subset_corpus_reimports_only_the_touched_chunk_when_a_file_mtime_changes() -> Result<()> {
    let prepared = extract_corpus_archive("subset.tar.zst")?;
    let db_temp = TempDir::new().context("unable to create touched-file db tempdir")?;
    let db_path = db_temp.path().join("usage.sqlite3");
    let mut database = Database::open(&db_path)?;

    let first_scan_report = scan_source_manifest(&mut database, &prepared.source_root)?;
    assert_scan_report(&first_scan_report, SUBSET_EXPECTATIONS);
    let first_import_report = import_all(database.connection(), &db_path, &prepared.source_root)?;
    assert_eq!(
        first_import_report.deferred_chunk_count as i64,
        SUBSET_EXPECTATIONS.deferred_chunk_count
    );

    let original_counts = load_database_counts(database.connection(), &db_path)?;
    assert_database_counts(&original_counts, SUBSET_EXPECTATIONS);
    let publish_seq_before =
        lookup_chunk_publish_seq(database.connection(), MUTATION_TARGET_CHUNK_DAY)?;
    assert!(
        (1..=SUBSET_EXPECTATIONS.import_chunk_count).contains(&publish_seq_before),
        "publish_seq for {} must be in 1..={}, got {publish_seq_before}",
        MUTATION_TARGET_CHUNK_DAY,
        SUBSET_EXPECTATIONS.import_chunk_count,
    );

    let target_path = prepared.source_root.join(MUTATION_TARGET_RELATIVE_PATH);
    set_file_mtime_rfc3339(&target_path, MUTATION_TARGET_SHIFTED_TIMESTAMP)?;

    let second_scan_report = scan_source_manifest(&mut database, &prepared.source_root)?;
    assert_eq!(
        second_scan_report.discovered_source_files,
        SUBSET_EXPECTATIONS.discovered_source_files
    );
    assert_eq!(second_scan_report.inserted_source_files, 0);
    assert_eq!(second_scan_report.deleted_source_files, 0);

    let second_import_report = import_all(database.connection(), &db_path, &prepared.source_root)?;
    assert_eq!(second_import_report.startup_chunk_count, 0);
    assert_eq!(second_import_report.deferred_chunk_count, 1);
    assert_eq!(second_import_report.deferred_failure_count, 0);
    assert!(second_import_report.deferred_failure_summary.is_none());

    let final_counts = load_database_counts(database.connection(), &db_path)?;
    assert_eq!(final_counts, original_counts);
    let publish_seq_after =
        lookup_chunk_publish_seq(database.connection(), MUTATION_TARGET_CHUNK_DAY)?;
    assert!(
        publish_seq_after > publish_seq_before,
        "touched chunk should be republished with a higher publish_seq: \
         before={publish_seq_before}, after={publish_seq_after}"
    );
    assert_subset_semantic_totals(database.connection(), &db_path)?;

    Ok(())
}

#[test]
#[ignore = "requires local import corpus fixture tarballs"]
fn subset_corpus_keeps_prior_rows_when_a_reimported_file_turns_malformed_and_recovers_after_restore()
-> Result<()> {
    let prepared = extract_corpus_archive("subset.tar.zst")?;
    let db_temp = TempDir::new().context("unable to create malformed-file db tempdir")?;
    let db_path = db_temp.path().join("usage.sqlite3");
    let mut database = Database::open(&db_path)?;

    let first_scan_report = scan_source_manifest(&mut database, &prepared.source_root)?;
    assert_scan_report(&first_scan_report, SUBSET_EXPECTATIONS);
    let first_import_report = import_all(database.connection(), &db_path, &prepared.source_root)?;
    assert_eq!(
        first_import_report.deferred_chunk_count as i64,
        SUBSET_EXPECTATIONS.deferred_chunk_count
    );

    let baseline_counts = load_database_counts(database.connection(), &db_path)?;
    assert_database_counts(&baseline_counts, SUBSET_EXPECTATIONS);
    let target_path = prepared.source_root.join(MUTATION_TARGET_RELATIVE_PATH);
    let original_contents = fs::read(&target_path)
        .with_context(|| format!("unable to read {}", target_path.display()))?;

    fs::write(
        &target_path,
        b"{\"type\":\"user\",\"uuid\":\"broken\",\"message\":{\"role\":\"user\",\"content\":\"oops\"}}\nnot-json\n",
    )
    .with_context(|| format!("unable to corrupt {}", target_path.display()))?;
    set_file_mtime_rfc3339(&target_path, MUTATION_TARGET_SHIFTED_TIMESTAMP)?;

    let second_scan_report = scan_source_manifest(&mut database, &prepared.source_root)?;
    assert_eq!(
        second_scan_report.discovered_source_files,
        SUBSET_EXPECTATIONS.discovered_source_files
    );

    let second_import_report = import_all(database.connection(), &db_path, &prepared.source_root)?;
    assert_eq!(second_import_report.startup_chunk_count, 0);
    assert!(second_import_report.deferred_chunk_count > 0);
    assert_eq!(second_import_report.deferred_failure_count, 0);
    assert!(second_import_report.deferred_failure_summary.is_none());

    let warning_counts = load_database_counts(database.connection(), &db_path)?;
    assert!(warning_counts.conversation_count <= baseline_counts.conversation_count);
    assert!(warning_counts.message_count <= baseline_counts.message_count);
    assert!(warning_counts.message_part_count <= baseline_counts.message_part_count);
    assert!(warning_counts.turn_count <= baseline_counts.turn_count);
    assert!(warning_counts.action_count <= baseline_counts.action_count);
    assert_eq!(
        warning_counts.import_warning_count,
        baseline_counts.import_warning_count + 1
    );

    let recent_warning = most_recent_warning_message(database.connection())?;
    assert!(recent_warning.contains(&target_path.display().to_string()));
    assert!(recent_warning.contains("line 2"));

    fs::write(&target_path, &original_contents)
        .with_context(|| format!("unable to restore {}", target_path.display()))?;
    set_file_mtime_rfc3339(&target_path, MUTATION_TARGET_SHIFTED_TIMESTAMP)?;

    let third_scan_report = scan_source_manifest(&mut database, &prepared.source_root)?;
    assert_eq!(
        third_scan_report.discovered_source_files,
        SUBSET_EXPECTATIONS.discovered_source_files
    );

    let third_import_report = import_all(database.connection(), &db_path, &prepared.source_root)?;
    assert_eq!(third_import_report.startup_chunk_count, 0);
    assert!(third_import_report.deferred_chunk_count > 0);
    assert_eq!(third_import_report.deferred_failure_count, 0);
    assert!(third_import_report.deferred_failure_summary.is_none());

    let recovered_counts = load_database_counts(database.connection(), &db_path)?;
    assert_eq!(recovered_counts, baseline_counts);
    assert_subset_semantic_totals(database.connection(), &db_path)?;

    Ok(())
}

#[test]
#[ignore = "requires local import corpus fixture tarballs"]
fn subset_corpus_with_one_recent_file_imports_recent_chunk_before_opening() -> Result<()> {
    let prepared = extract_corpus_archive("subset.tar.zst")?;
    let target_path = prepared.source_root.join(MUTATION_TARGET_RELATIVE_PATH);
    set_file_mtime_to_recent(&target_path)?;

    let db_temp = TempDir::new().context("unable to create recent-startup db tempdir")?;
    let db_path = db_temp.path().join("usage.sqlite3");
    let mut database = Database::open(&db_path)?;

    let scan_report = scan_source_manifest(&mut database, &prepared.source_root)?;
    assert_scan_report(&scan_report, SUBSET_EXPECTATIONS);

    let mut startup = start_startup_import(database.connection(), &db_path, &prepared.source_root)?;
    assert_eq!(startup.open_reason, StartupOpenReason::Last24hReady);
    assert_eq!(startup.snapshot.max_publish_seq, 1);
    assert!(startup.startup_status_message.is_none());
    assert_eq!(
        startup.startup_progress_update.as_ref().map(|update| (
            update.label,
            update.current,
            update.total
        )),
        Some(("rebuilding database", 1, 1))
    );

    let status_updates = startup
        .take_status_updates()
        .context("startup import should expose a status update receiver")?;
    loop {
        match status_updates.recv_timeout(EVENT_WAIT_TIMEOUT) {
            Ok(StartupWorkerEvent::Progress { .. }) => {}
            Ok(StartupWorkerEvent::StartupSettled {
                startup_status_message,
            }) => {
                assert!(startup_status_message.is_none());
            }
            Ok(StartupWorkerEvent::DeferredFailures {
                deferred_status_message,
            }) => {
                bail!(
                    "recent-startup corpus should complete without deferred failures, got: {:?}",
                    deferred_status_message
                );
            }
            Ok(StartupWorkerEvent::Finished) => break,
            Err(RecvTimeoutError::Timeout) => {
                bail!("timed out waiting for recent-startup importer to finish")
            }
            Err(RecvTimeoutError::Disconnected) => {
                bail!("recent-startup importer disconnected before sending Finished")
            }
        }
    }

    drop(startup);

    let counts = load_database_counts(database.connection(), &db_path)?;
    assert_eq!(counts.project_count, SUBSET_EXPECTATIONS.project_count);
    assert_eq!(
        counts.source_file_count,
        SUBSET_EXPECTATIONS.source_file_count
    );
    assert_eq!(
        counts.conversation_count,
        SUBSET_EXPECTATIONS.conversation_count
    );
    assert_eq!(counts.message_count, SUBSET_EXPECTATIONS.message_count);
    assert_eq!(counts.action_count, SUBSET_EXPECTATIONS.action_count);
    // The mutated file moved out of its original chunk into a fresh "today"
    // chunk imported via startup. The original chunk still has its remaining
    // files so it stays as a deferred chunk → total = original + 1.
    assert_eq!(
        counts.complete_chunk_count,
        SUBSET_EXPECTATIONS.complete_chunk_count + 1
    );
    assert_eq!(counts.startup_chunk_count, 1);
    assert_eq!(
        counts.deferred_chunk_count,
        SUBSET_EXPECTATIONS.deferred_chunk_count
    );
    assert_eq!(counts.failed_chunk_count, 0);
    assert_eq!(
        counts.import_warning_count,
        SUBSET_EXPECTATIONS.import_warning_count
    );
    assert_eq!(counts.source_files_missing_import_metadata_count, 0);

    Ok(())
}

#[test]
#[ignore = "requires local import corpus fixture tarballs"]
fn full_corpus_import_all_matches_expected_database_shape() -> Result<()> {
    let prepared = extract_corpus_archive("full.tar.zst")?;
    let db_temp = TempDir::new().context("unable to create full db tempdir")?;
    let db_path = db_temp.path().join("usage.sqlite3");
    let mut database = Database::open(&db_path)?;

    let scan_report = scan_source_manifest(&mut database, &prepared.source_root)?;
    assert_eq!(scan_report.discovered_source_files, 4_343);
    assert_eq!(scan_report.excluded_source_files, 0);
    assert_eq!(scan_report.inserted_projects, 36);
    assert_eq!(scan_report.inserted_source_files, 4_343);
    assert_eq!(scan_report.updated_projects, 0);
    assert_eq!(scan_report.updated_source_files, 0);
    assert_eq!(scan_report.deleted_source_files, 0);

    let import_report = import_all(database.connection(), &db_path, &prepared.source_root)?;
    assert_eq!(import_report.startup_chunk_count, 0);
    assert!(import_report.deferred_chunk_count > 0);
    assert_eq!(import_report.deferred_failure_count, 0);
    assert!(import_report.deferred_failure_summary.is_none());

    let counts = load_database_counts(database.connection(), &db_path)?;
    assert!(counts.project_count > SUBSET_EXPECTATIONS.project_count);
    assert!(counts.source_file_count > SUBSET_EXPECTATIONS.source_file_count);
    assert_eq!(
        counts.transcript_source_file_count,
        counts.source_file_count
    );
    assert_eq!(counts.claude_history_source_file_count, 0);
    assert!(counts.import_chunk_count > SUBSET_EXPECTATIONS.import_chunk_count);
    assert_eq!(counts.complete_chunk_count, counts.import_chunk_count);
    assert_eq!(counts.failed_chunk_count, 0);
    assert_eq!(counts.deferred_chunk_count, counts.import_chunk_count);
    assert_eq!(counts.startup_chunk_count, 0);
    assert!(counts.conversation_count > SUBSET_EXPECTATIONS.conversation_count);
    assert_eq!(counts.stream_count, counts.conversation_count);
    assert!(counts.message_count > SUBSET_EXPECTATIONS.message_count);
    assert!(counts.message_part_count > SUBSET_EXPECTATIONS.message_part_count);
    assert!(counts.turn_count > SUBSET_EXPECTATIONS.turn_count);
    assert!(counts.action_count > SUBSET_EXPECTATIONS.action_count);
    assert_eq!(counts.history_event_count, 0);
    // The live corpus has a handful of files with naturally-malformed lines
    // (usually tail-truncated); the import should flag them as warnings, not
    // as failed chunks. Assert the warning count is bounded but non-zero.
    assert!(counts.import_warning_count >= SUBSET_EXPECTATIONS.import_warning_count);
    assert!(counts.import_warning_count < 100);
    assert_eq!(counts.imported_message_count_sum, counts.message_count);
    assert_eq!(counts.imported_action_count_sum, counts.action_count);
    assert_eq!(
        counts.imported_conversation_count_sum,
        counts.conversation_count
    );
    assert_eq!(counts.imported_turn_count_sum, counts.turn_count);
    assert_eq!(counts.imported_source_file_count, counts.source_file_count);
    assert_eq!(counts.source_files_missing_import_metadata_count, 0);
    // The full corpus has richer path activity than the subset — the
    // path-rollup writer must at least match the subset's coverage on this
    // superset; zero here would flag a regression in the sharded query path.
    assert!(counts.chunk_action_rollup_count > SUBSET_EXPECTATIONS.chunk_action_rollup_count);
    assert!(counts.chunk_path_rollup_count > SUBSET_EXPECTATIONS.chunk_path_rollup_count);

    Ok(())
}

fn extract_corpus_archive(archive_name: &str) -> Result<PreparedCorpus> {
    let archive_path = corpus_dir().join(archive_name);
    if !archive_path.is_file() {
        bail!(
            "missing corpus archive at {}; copy it into the current worktree first",
            archive_path.display()
        );
    }

    ensure_command_available("tar")?;
    ensure_command_available("zstd")?;

    let temp_dir = TempDir::new().context("unable to create extraction tempdir")?;
    let extraction_root = temp_dir.path().join("source");
    std::fs::create_dir_all(&extraction_root)
        .with_context(|| format!("unable to create {}", extraction_root.display()))?;

    let status = Command::new("tar")
        .arg("-C")
        .arg(&extraction_root)
        .arg("-I")
        .arg("zstd")
        .arg("-xf")
        .arg(&archive_path)
        .status()
        .with_context(|| format!("unable to extract {}", archive_path.display()))?;
    if !status.success() {
        bail!("tar extraction failed for {}", archive_path.display());
    }

    let source_root = if extraction_root.join("projects").is_dir() {
        extraction_root.join("projects")
    } else {
        extraction_root
    };

    Ok(PreparedCorpus {
        _temp_dir: temp_dir,
        source_root,
    })
}

fn corpus_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("GNOMON_IMPORT_CORPUS_DIR") {
        return PathBuf::from(dir);
    }

    workspace_root().join("tests/fixtures/import-corpus")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root should be two levels above crates/gnomon-core")
        .to_path_buf()
}

fn ensure_command_available(command_name: &str) -> Result<()> {
    let status = Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {command_name} >/dev/null"))
        .status()
        .with_context(|| format!("unable to look up required command `{command_name}`"))?;
    if !status.success() {
        bail!("required command `{command_name}` is not available in PATH");
    }

    Ok(())
}

// The connection passed to these helpers is opened via `Database::open`, which configures
// TEMP VIEWs that UNION the shard data tables under their main-DB names. Unqualified counts
// against `message`, `action`, etc. therefore return aggregated results across all shards.

fn most_recent_warning_message(conn: &Connection) -> Result<String> {
    // Order by `created_at_utc` (then id as a tiebreaker). Under p4-c1
    // sharding each shard seeds AUTOINCREMENT at `shard_idx * 1_000_000_000`,
    // so ORDER BY id DESC does not track wall-clock recency; use the explicit
    // timestamp instead.
    conn.query_row(
        "SELECT message FROM import_warning ORDER BY created_at_utc DESC, id DESC LIMIT 1",
        [],
        |r| r.get(0),
    )
    .context("unable to read most recent import warning")
}

fn load_database_counts(conn: &Connection, _db_path: &Path) -> Result<DatabaseCounts> {
    Ok(DatabaseCounts {
        project_count: query_count(conn, "SELECT COUNT(*) FROM project")?,
        source_file_count: query_count(conn, "SELECT COUNT(*) FROM source_file")?,
        transcript_source_file_count: query_count(
            conn,
            "SELECT COUNT(*) FROM source_file WHERE source_kind = 'transcript'",
        )?,
        claude_history_source_file_count: query_count(
            conn,
            "SELECT COUNT(*) FROM source_file WHERE source_kind = 'claude_history'",
        )?,
        import_chunk_count: query_count(conn, "SELECT COUNT(*) FROM import_chunk")?,
        complete_chunk_count: query_count(
            conn,
            "SELECT COUNT(*) FROM import_chunk WHERE state = 'complete'",
        )?,
        failed_chunk_count: query_count(
            conn,
            "SELECT COUNT(*) FROM import_chunk WHERE state = 'failed'",
        )?,
        deferred_chunk_count: query_count(
            conn,
            "SELECT COUNT(*) FROM import_chunk WHERE last_attempt_phase = 'deferred'",
        )?,
        startup_chunk_count: query_count(
            conn,
            "SELECT COUNT(*) FROM import_chunk WHERE last_attempt_phase = 'startup'",
        )?,
        // Data-table counts route through the TEMP VIEWs (configured by Database::open),
        // so these queries return aggregated totals across all shards.
        conversation_count: query_count(conn, "SELECT COUNT(*) FROM conversation")?,
        stream_count: query_count(conn, "SELECT COUNT(*) FROM stream")?,
        message_count: query_count(conn, "SELECT COUNT(*) FROM message")?,
        message_part_count: query_count(conn, "SELECT COUNT(*) FROM message_part")?,
        turn_count: query_count(conn, "SELECT COUNT(*) FROM turn")?,
        action_count: query_count(conn, "SELECT COUNT(*) FROM action")?,
        history_event_count: query_count(conn, "SELECT COUNT(*) FROM history_event")?,
        import_warning_count: query_count(conn, "SELECT COUNT(*) FROM import_warning")?,
        imported_message_count_sum: query_count(
            conn,
            "SELECT COALESCE(SUM(imported_message_count), 0) FROM import_chunk",
        )?,
        imported_action_count_sum: query_count(
            conn,
            "SELECT COALESCE(SUM(imported_action_count), 0) FROM import_chunk",
        )?,
        imported_conversation_count_sum: query_count(
            conn,
            "SELECT COALESCE(SUM(imported_conversation_count), 0) FROM import_chunk",
        )?,
        imported_turn_count_sum: query_count(
            conn,
            "SELECT COALESCE(SUM(imported_turn_count), 0) FROM import_chunk",
        )?,
        imported_source_file_count: query_count(
            conn,
            "SELECT COUNT(*) FROM source_file WHERE imported_schema_version IS NOT NULL",
        )?,
        source_files_missing_import_metadata_count: query_count(
            conn,
            "
            SELECT COUNT(*)
            FROM source_file
            WHERE imported_schema_version IS NULL
                OR imported_size_bytes IS NULL
                OR imported_modified_at_utc IS NULL
                OR imported_size_bytes != size_bytes
                OR imported_modified_at_utc != modified_at_utc
            ",
        )?,
        // Rollup counts route through the shard TEMP VIEWs (same as the data
        // tables above). Asserting them catches the class of bug where the
        // shard-side rebuild joins a main-only table and silently produces
        // zero rows — the symptom that motivated covering `chunk_path_rollup`
        // explicitly in these expectations.
        chunk_action_rollup_count: query_count(conn, "SELECT COUNT(*) FROM chunk_action_rollup")?,
        chunk_path_rollup_count: query_count(conn, "SELECT COUNT(*) FROM chunk_path_rollup")?,
    })
}

fn query_count(conn: &Connection, sql: &str) -> Result<i64> {
    conn.query_row(sql, [], |row| row.get(0))
        .with_context(|| format!("unable to run count query: {sql}"))
}

fn assert_scan_report(report: &gnomon_core::import::ScanReport, expectations: CorpusExpectations) {
    assert_eq!(
        report.discovered_source_files,
        expectations.discovered_source_files
    );
    assert_eq!(report.inserted_projects, expectations.inserted_projects);
    assert_eq!(
        report.inserted_source_files,
        expectations.inserted_source_files
    );
}

fn assert_database_counts(counts: &DatabaseCounts, expectations: CorpusExpectations) {
    // Raw row counts and chunk state first. These should always pass; if any fail the
    // failure is a real regression (schema mismatch, dropped data, etc.).
    assert_eq!(counts.project_count, expectations.project_count);
    assert_eq!(counts.source_file_count, expectations.source_file_count);
    assert_eq!(
        counts.transcript_source_file_count,
        expectations.transcript_source_file_count
    );
    assert_eq!(
        counts.claude_history_source_file_count,
        expectations.claude_history_source_file_count
    );
    assert_eq!(counts.import_chunk_count, expectations.import_chunk_count);
    assert_eq!(
        counts.complete_chunk_count,
        expectations.complete_chunk_count
    );
    assert_eq!(counts.failed_chunk_count, expectations.failed_chunk_count);
    assert_eq!(
        counts.deferred_chunk_count,
        expectations.deferred_chunk_count
    );
    assert_eq!(counts.startup_chunk_count, expectations.startup_chunk_count);
    assert_eq!(counts.conversation_count, expectations.conversation_count);
    assert_eq!(counts.stream_count, expectations.stream_count);
    assert_eq!(counts.message_count, expectations.message_count);
    assert_eq!(counts.message_part_count, expectations.message_part_count);
    assert_eq!(counts.turn_count, expectations.turn_count);
    assert_eq!(counts.action_count, expectations.action_count);
    assert_eq!(counts.history_event_count, expectations.history_event_count);
    assert_eq!(
        counts.import_warning_count,
        expectations.import_warning_count
    );
    assert_eq!(
        counts.imported_source_file_count,
        expectations.source_file_count
    );
    assert_eq!(counts.source_files_missing_import_metadata_count, 0);
    assert_eq!(
        counts.chunk_action_rollup_count,
        expectations.chunk_action_rollup_count
    );
    assert_eq!(
        counts.chunk_path_rollup_count,
        expectations.chunk_path_rollup_count
    );
}

fn assert_imported_count_sums(counts: &DatabaseCounts, expectations: CorpusExpectations) {
    assert_eq!(
        counts.imported_message_count_sum,
        expectations.imported_message_count_sum
    );
    assert_eq!(
        counts.imported_action_count_sum,
        expectations.imported_action_count_sum
    );
    assert_eq!(
        counts.imported_conversation_count_sum,
        expectations.imported_conversation_count_sum
    );
    assert_eq!(
        counts.imported_turn_count_sum,
        expectations.imported_turn_count_sum
    );
}

fn assert_subset_baseline_signature(conn: &Connection, db_path: &Path) -> Result<()> {
    let signature = load_subset_semantic_counts(conn, db_path)?;
    let expected_days = {
        let mut v = SUBSET_CHUNK_DAY_SEQUENCE
            .iter()
            .map(|day| (*day).to_string())
            .collect::<Vec<_>>();
        v.sort();
        v
    };
    let mut actual_days = signature.chunk_day_sequence.clone();
    actual_days.sort();
    assert_eq!(actual_days, expected_days);
    assert_subset_semantic_totals_from_signature(&signature);

    Ok(())
}

fn assert_subset_semantic_totals(conn: &Connection, db_path: &Path) -> Result<()> {
    let signature = load_subset_semantic_counts(conn, db_path)?;
    assert_subset_semantic_totals_from_signature(&signature);

    Ok(())
}

fn assert_subset_semantic_totals_from_signature(signature: &SubsetSemanticCounts) {
    assert_eq!(signature.classified_action_count, 13_042);
    assert_eq!(signature.mixed_action_count, 3_723);
    assert_eq!(signature.unclassified_action_count, 1_035);
    assert_eq!(signature.tool_use_part_count, 22_099);
    assert_eq!(signature.tool_result_part_count, 22_084);
    assert_eq!(signature.parts_with_tool_name_count, 22_099);
    assert_eq!(signature.parts_with_tool_call_id_count, 44_183);
    assert_eq!(signature.relay_assistant_message_count, 164);
    assert_eq!(signature.assistant_message_count, 18_002);
    assert_eq!(signature.actions_with_null_normalized_action_count, 4_758);
    assert_eq!(signature.source_files_with_scan_warnings_count, 14);
    assert_eq!(signature.git_project_count, 9);
    assert_eq!(signature.path_project_count, 2);
    assert_eq!(
        signature.top_action_signature,
        SUBSET_TOP_ACTION_SIGNATURE
            .iter()
            .map(|(name, count)| ((*name).to_string(), *count))
            .collect::<Vec<_>>()
    );
}

fn lookup_chunk_publish_seq(conn: &Connection, chunk_day_local: &str) -> Result<i64> {
    conn.query_row(
        "SELECT publish_seq FROM import_chunk WHERE chunk_day_local = ?1",
        [chunk_day_local],
        |row| row.get(0),
    )
    .with_context(|| format!("unable to load publish_seq for chunk {chunk_day_local}"))
}

// Data-table queries below route through the TEMP VIEWs configured by `Database::open`,
// so they return aggregated results across all 9 shards.
fn load_subset_semantic_counts(conn: &Connection, _db_path: &Path) -> Result<SubsetSemanticCounts> {
    // Ordered by (chunk_day_local, id). `publish_seq` is now pre-assigned
    // deterministically before parallel shard execution (see
    // `assign_phase_publish_seqs` in import/chunk.rs), so ordering is stable
    // across runs; this assertion only cares about the multiset of (day, chunk)
    // pairs that were imported.
    let chunk_day_sequence = {
        let mut stmt =
            conn.prepare("SELECT chunk_day_local FROM import_chunk ORDER BY chunk_day_local, id")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };

    let top_action_signature: Vec<(String, i64)> = {
        let mut stmt = conn.prepare(
            "SELECT COALESCE(normalized_action, '<null>'), COUNT(*)
             FROM action
             GROUP BY normalized_action
             ORDER BY COUNT(*) DESC, COALESCE(normalized_action, '<null>')
             LIMIT 12",
        )?;
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };

    Ok(SubsetSemanticCounts {
        chunk_day_sequence,
        classified_action_count: query_count(
            conn,
            "SELECT COUNT(*) FROM action WHERE classification_state = 'classified'",
        )?,
        mixed_action_count: query_count(
            conn,
            "SELECT COUNT(*) FROM action WHERE classification_state = 'mixed'",
        )?,
        unclassified_action_count: query_count(
            conn,
            "SELECT COUNT(*) FROM action WHERE classification_state = 'unclassified'",
        )?,
        tool_use_part_count: query_count(
            conn,
            "SELECT COUNT(*) FROM message_part WHERE part_kind = 'tool_use'",
        )?,
        tool_result_part_count: query_count(
            conn,
            "SELECT COUNT(*) FROM message_part WHERE part_kind = 'tool_result'",
        )?,
        parts_with_tool_name_count: query_count(
            conn,
            "SELECT COUNT(*) FROM message_part WHERE tool_name IS NOT NULL",
        )?,
        parts_with_tool_call_id_count: query_count(
            conn,
            "SELECT COUNT(*) FROM message_part WHERE tool_call_id IS NOT NULL",
        )?,
        relay_assistant_message_count: query_count(
            conn,
            "SELECT COUNT(*) FROM message WHERE message_kind = 'relay_assistant_message'",
        )?,
        assistant_message_count: query_count(
            conn,
            "SELECT COUNT(*) FROM message WHERE message_kind = 'assistant_message'",
        )?,
        actions_with_null_normalized_action_count: query_count(
            conn,
            "SELECT COUNT(*) FROM action WHERE normalized_action IS NULL",
        )?,
        source_files_with_scan_warnings_count: query_count(
            conn,
            "SELECT COUNT(*) FROM source_file WHERE scan_warnings_json != '[]'",
        )?,
        git_project_count: query_count(
            conn,
            "SELECT COUNT(*) FROM project WHERE identity_kind = 'git'",
        )?,
        path_project_count: query_count(
            conn,
            "SELECT COUNT(*) FROM project WHERE identity_kind = 'path'",
        )?,
        top_action_signature,
    })
}

fn set_file_mtime_rfc3339(path: &Path, timestamp: &str) -> Result<()> {
    let status = Command::new("touch")
        .arg("-d")
        .arg(timestamp)
        .arg(path)
        .status()
        .with_context(|| format!("unable to update mtime for {}", path.display()))?;
    if !status.success() {
        bail!(
            "touch failed while updating mtime for {} to {}",
            path.display(),
            timestamp
        );
    }

    Ok(())
}

fn set_file_mtime_to_recent(path: &Path) -> Result<()> {
    let recent_epoch_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_secs()
        .saturating_sub(60 * 60);
    let touch_arg = format!("@{recent_epoch_secs}");
    let status = Command::new("touch")
        .arg("-d")
        .arg(&touch_arg)
        .arg(path)
        .status()
        .with_context(|| format!("unable to update recent mtime for {}", path.display()))?;
    if !status.success() {
        bail!(
            "touch failed while updating mtime for {} to {}",
            path.display(),
            touch_arg
        );
    }

    Ok(())
}
