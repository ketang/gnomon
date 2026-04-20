use std::collections::HashMap;
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
const WARNING_LINE_NO: i64 = 73;
const WARNING_PATH_FRAGMENT: &str =
    "-home-ketan-project-shatter/4d823687-9cd9-46ee-89c1-1b8642207d6e.jsonl";
const MUTATION_TARGET_RELATIVE_PATH: &str = "-home-ketan-project-shatter/1d08353a-7590-4b93-b4f0-d2d9bcad2a09/subagents/agent-a67b052bc0b1f106f.jsonl";
const MUTATION_TARGET_CHUNK_DAY: &str = "2026-03-07";
const MUTATION_TARGET_SHIFTED_TIMESTAMP: &str = "2026-03-07T23:24:19Z";
const SUBSET_CHUNK_DAY_SEQUENCE: &[&str] = &[
    "2026-04-10",
    "2026-04-09",
    "2026-04-08",
    "2026-04-07",
    "2026-04-05",
    "2026-04-04",
    "2026-04-03",
    "2026-04-02",
    "2026-04-01",
    "2026-03-31",
    "2026-03-30",
    "2026-03-29",
    "2026-03-28",
    "2026-03-27",
    "2026-03-26",
    "2026-03-25",
    "2026-03-24",
    "2026-03-23",
    "2026-03-22",
    "2026-03-21",
    "2026-03-20",
    "2026-03-19",
    "2026-03-18",
    "2026-03-17",
    "2026-03-16",
    "2026-03-15",
    "2026-03-14",
    "2026-03-13",
    "2026-03-12",
    "2026-03-11",
    "2026-03-10",
    "2026-03-09",
    "2026-03-08",
    "2026-03-07",
    "2026-03-06",
];
const SUBSET_TOP_ACTION_SIGNATURE: &[(&str, i64)] = &[
    ("<null>", 14_495),
    ("file read", 10_677),
    ("content search", 5_232),
    ("prompt", 4_906),
    ("assistant reasoning", 3_545),
    ("file edit", 3_168),
    ("git inspection", 1_389),
    ("send message", 1_168),
    ("tool discovery", 850),
    ("file glob", 790),
    ("task coordination", 701),
    ("git mutation", 649),
];

const SUBSET_EXPECTATIONS: CorpusExpectations = CorpusExpectations {
    discovered_source_files: 1_649,
    inserted_projects: 1,
    inserted_source_files: 1_649,
    import_chunk_count: 35,
    project_count: 1,
    source_file_count: 1_649,
    transcript_source_file_count: 1_649,
    claude_history_source_file_count: 0,
    conversation_count: 1_648,
    stream_count: 1_648,
    message_count: 130_478,
    message_part_count: 179_412,
    turn_count: 4_915,
    action_count: 50_463,
    record_count: 0,
    history_event_count: 0,
    import_warning_count: 1,
    imported_record_count_sum: 212_788,
    imported_message_count_sum: 130_478,
    imported_action_count_sum: 50_463,
    imported_conversation_count_sum: 1_648,
    imported_turn_count_sum: 4_915,
    complete_chunk_count: 35,
    failed_chunk_count: 0,
    deferred_chunk_count: 35,
    startup_chunk_count: 0,
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
    record_count: i64,
    history_event_count: i64,
    import_warning_count: i64,
    imported_record_count_sum: i64,
    imported_message_count_sum: i64,
    imported_action_count_sum: i64,
    imported_conversation_count_sum: i64,
    imported_turn_count_sum: i64,
    complete_chunk_count: i64,
    failed_chunk_count: i64,
    deferred_chunk_count: i64,
    startup_chunk_count: i64,
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
    record_count: i64,
    message_count: i64,
    message_part_count: i64,
    turn_count: i64,
    action_count: i64,
    history_event_count: i64,
    import_warning_count: i64,
    imported_record_count_sum: i64,
    imported_message_count_sum: i64,
    imported_action_count_sum: i64,
    imported_conversation_count_sum: i64,
    imported_turn_count_sum: i64,
    imported_source_file_count: i64,
    source_files_missing_import_metadata_count: i64,
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
    assert_eq!(import_report.deferred_chunk_count, 35);
    assert_eq!(import_report.deferred_failure_count, 0);
    assert!(import_report.deferred_failure_summary.is_none());

    let counts = load_database_counts(database.connection(), &db_path)?;
    assert_database_counts(&counts, SUBSET_EXPECTATIONS);
    assert_warning_message_shard(&db_path, &prepared.source_root)?;
    assert_subset_baseline_signature(database.connection(), &db_path)?;

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
    assert_warning_message_shard(&db_path, &prepared.source_root)?;
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
    assert_eq!(first_import_report.deferred_chunk_count, 35);
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
    assert_warning_message_shard(&db_path, &prepared.source_root)?;
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
    assert_eq!(first_import_report.deferred_chunk_count, 35);

    let original_counts = load_database_counts(database.connection(), &db_path)?;
    assert_database_counts(&original_counts, SUBSET_EXPECTATIONS);
    assert_eq!(
        lookup_chunk_publish_seq(database.connection(), MUTATION_TARGET_CHUNK_DAY)?,
        34
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
    assert_eq!(
        lookup_chunk_publish_seq(database.connection(), MUTATION_TARGET_CHUNK_DAY)?,
        36
    );
    assert_warning_message_shard(&db_path, &prepared.source_root)?;
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
    assert_eq!(first_import_report.deferred_chunk_count, 35);

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

    // import_warning is in shard DBs with C1 sharding.
    let recent_warning =
        most_recent_shard_warning(&db_path)?.context("no import_warning found in any shard")?;
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
    assert_warning_message_shard(&db_path, &prepared.source_root)?;
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
    assert_eq!(counts.complete_chunk_count, 36);
    assert_eq!(counts.startup_chunk_count, 1);
    assert_eq!(counts.deferred_chunk_count, 35);
    assert_eq!(counts.failed_chunk_count, 0);
    assert_eq!(counts.import_warning_count, 1);
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
    assert_eq!(scan_report.discovered_source_files, 4_548);
    assert_eq!(scan_report.excluded_source_files, 0);
    assert_eq!(scan_report.inserted_projects, 31);
    assert_eq!(scan_report.inserted_source_files, 4_548);
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
    assert_eq!(counts.record_count, 0);
    assert!(counts.message_count > SUBSET_EXPECTATIONS.message_count);
    assert!(counts.message_part_count > SUBSET_EXPECTATIONS.message_part_count);
    assert!(counts.turn_count > SUBSET_EXPECTATIONS.turn_count);
    assert!(counts.action_count > SUBSET_EXPECTATIONS.action_count);
    assert_eq!(counts.history_event_count, 0);
    assert_eq!(counts.import_warning_count, 1);
    assert!(counts.imported_record_count_sum > SUBSET_EXPECTATIONS.imported_record_count_sum);
    assert_eq!(counts.imported_message_count_sum, counts.message_count);
    assert_eq!(counts.imported_action_count_sum, counts.action_count);
    assert_eq!(
        counts.imported_conversation_count_sum,
        counts.conversation_count
    );
    assert_eq!(counts.imported_turn_count_sum, counts.turn_count);
    assert_eq!(counts.imported_source_file_count, counts.source_file_count);
    assert_eq!(counts.source_files_missing_import_metadata_count, 0);
    assert_warning_message_shard(&db_path, &prepared.source_root)?;

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

// Aggregate a simple COUNT query across all per-project shard DBs (C1 sharding architecture).
fn shard_count(db_path: &Path, sql: &str) -> Result<i64> {
    let shards_dir = db_path
        .parent()
        .expect("db_path must have parent")
        .join("shards");
    let mut total = 0i64;
    if shards_dir.exists() {
        for entry in fs::read_dir(&shards_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("sqlite3") {
                continue;
            }
            let conn = Connection::open(&path)?;
            total += conn.query_row(sql, [], |r| r.get(0)).unwrap_or(0i64);
        }
    }
    Ok(total)
}

// Query `SELECT key, COUNT(*) FROM table GROUP BY key` across global DB and all shards.
fn shard_group_count(db_path: &Path, sql: &str) -> Result<Vec<(Option<String>, i64)>> {
    let mut totals: HashMap<Option<String>, i64> = HashMap::new();

    // Include global DB (start_startup_import path).
    let global_conn = Connection::open(db_path)?;
    if let Ok(mut stmt) = global_conn.prepare(sql) {
        for row in stmt.query_map([], |r| {
            Ok((r.get::<_, Option<String>>(0)?, r.get::<_, i64>(1)?))
        })? {
            let (k, v) = row?;
            *totals.entry(k).or_insert(0) += v;
        }
    }

    // Include shard DBs (import_all path).
    let shards_dir = db_path
        .parent()
        .expect("db_path must have parent")
        .join("shards");
    if shards_dir.exists() {
        for entry in fs::read_dir(&shards_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("sqlite3") {
                continue;
            }
            let conn = Connection::open(&path)?;
            let mut stmt = conn.prepare(sql)?;
            for row in stmt.query_map([], |r| {
                Ok((r.get::<_, Option<String>>(0)?, r.get::<_, i64>(1)?))
            })? {
                let (k, v) = row?;
                *totals.entry(k).or_insert(0) += v;
            }
        }
    }
    let mut result: Vec<(Option<String>, i64)> = totals.into_iter().collect();
    result.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    Ok(result)
}

// Find a recent import_warning message — check global DB and all shards.
fn most_recent_shard_warning(db_path: &Path) -> Result<Option<String>> {
    // Check global DB first (start_startup_import path writes here).
    let global_conn = Connection::open(db_path)?;
    if let Ok(msg) = global_conn.query_row::<String, _, _>(
        "SELECT message FROM import_warning ORDER BY id DESC LIMIT 1",
        [],
        |r| r.get(0),
    ) {
        return Ok(Some(msg));
    }

    // Check shard DBs (import_all path writes here).
    let shards_dir = db_path
        .parent()
        .expect("db_path must have parent")
        .join("shards");
    if !shards_dir.exists() {
        return Ok(None);
    }
    for entry in fs::read_dir(&shards_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("sqlite3") {
            continue;
        }
        let conn = Connection::open(&path)?;
        if let Ok(msg) = conn.query_row::<String, _, _>(
            "SELECT message FROM import_warning ORDER BY id DESC LIMIT 1",
            [],
            |r| r.get(0),
        ) {
            return Ok(Some(msg));
        }
    }
    Ok(None)
}

fn load_database_counts(conn: &Connection, db_path: &Path) -> Result<DatabaseCounts> {
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
        // Data tables may be in the global DB (start_startup_import path) or shard DBs
        // (import_all path). Sum both to handle either import path in tests.
        conversation_count: query_count(conn, "SELECT COUNT(*) FROM conversation")?
            + shard_count(db_path, "SELECT COUNT(*) FROM conversation")?,
        stream_count: query_count(conn, "SELECT COUNT(*) FROM stream")?
            + shard_count(db_path, "SELECT COUNT(*) FROM stream")?,
        record_count: query_count(conn, "SELECT COUNT(*) FROM record")?
            + shard_count(db_path, "SELECT COUNT(*) FROM record")?,
        message_count: query_count(conn, "SELECT COUNT(*) FROM message")?
            + shard_count(db_path, "SELECT COUNT(*) FROM message")?,
        message_part_count: query_count(conn, "SELECT COUNT(*) FROM message_part")?
            + shard_count(db_path, "SELECT COUNT(*) FROM message_part")?,
        turn_count: query_count(conn, "SELECT COUNT(*) FROM turn")?
            + shard_count(db_path, "SELECT COUNT(*) FROM turn")?,
        action_count: query_count(conn, "SELECT COUNT(*) FROM action")?
            + shard_count(db_path, "SELECT COUNT(*) FROM action")?,
        history_event_count: query_count(conn, "SELECT COUNT(*) FROM history_event")?
            + shard_count(db_path, "SELECT COUNT(*) FROM history_event")?,
        import_warning_count: query_count(conn, "SELECT COUNT(*) FROM import_warning")?
            + shard_count(db_path, "SELECT COUNT(*) FROM import_warning")?,
        imported_record_count_sum: query_count(
            conn,
            "SELECT COALESCE(SUM(imported_record_count), 0) FROM import_chunk",
        )?,
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
    assert_eq!(counts.record_count, expectations.record_count);
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
        counts.imported_record_count_sum,
        expectations.imported_record_count_sum
    );
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
    assert_eq!(
        counts.imported_source_file_count,
        expectations.source_file_count
    );
    assert_eq!(counts.source_files_missing_import_metadata_count, 0);
}

fn assert_subset_baseline_signature(conn: &Connection, db_path: &Path) -> Result<()> {
    let signature = load_subset_semantic_counts(conn, db_path)?;
    assert_eq!(
        signature.chunk_day_sequence,
        SUBSET_CHUNK_DAY_SEQUENCE
            .iter()
            .map(|day| (*day).to_string())
            .collect::<Vec<_>>()
    );
    assert_subset_semantic_totals_from_signature(&signature);

    Ok(())
}

fn assert_subset_semantic_totals(conn: &Connection, db_path: &Path) -> Result<()> {
    let signature = load_subset_semantic_counts(conn, db_path)?;
    assert_subset_semantic_totals_from_signature(&signature);

    Ok(())
}

fn assert_subset_semantic_totals_from_signature(signature: &SubsetSemanticCounts) {
    assert_eq!(signature.classified_action_count, 35_968);
    assert_eq!(signature.mixed_action_count, 11_715);
    assert_eq!(signature.unclassified_action_count, 2_780);
    assert_eq!(signature.tool_use_part_count, 68_965);
    assert_eq!(signature.tool_result_part_count, 68_882);
    assert_eq!(signature.parts_with_tool_name_count, 68_965);
    assert_eq!(signature.parts_with_tool_call_id_count, 137_847);
    assert_eq!(signature.relay_assistant_message_count, 7_480);
    assert_eq!(signature.assistant_message_count, 48_833);
    assert_eq!(signature.actions_with_null_normalized_action_count, 14_495);
    assert_eq!(signature.source_files_with_scan_warnings_count, 0);
    assert_eq!(signature.git_project_count, 1);
    assert_eq!(signature.path_project_count, 0);
    assert_eq!(
        signature.top_action_signature,
        SUBSET_TOP_ACTION_SIGNATURE
            .iter()
            .map(|(name, count)| ((*name).to_string(), *count))
            .collect::<Vec<_>>()
    );
}

fn assert_warning_message_shard(db_path: &Path, source_root: &Path) -> Result<()> {
    let warning_message = most_recent_shard_warning(db_path)?
        .context("unable to load most recent import warning from any shard")?;

    assert!(warning_message.contains(&format!("line {WARNING_LINE_NO}")));
    assert!(
        warning_message.contains(
            &source_root
                .join(WARNING_PATH_FRAGMENT)
                .display()
                .to_string()
        )
    );
    assert!(warning_message.contains("preview:"));

    Ok(())
}

fn lookup_chunk_publish_seq(conn: &Connection, chunk_day_local: &str) -> Result<i64> {
    conn.query_row(
        "SELECT publish_seq FROM import_chunk WHERE chunk_day_local = ?1",
        [chunk_day_local],
        |row| row.get(0),
    )
    .with_context(|| format!("unable to load publish_seq for chunk {chunk_day_local}"))
}

// With C1 sharding, action/message/message_part are in shard DBs; import_chunk/source_file/project are global.
fn load_subset_semantic_counts(conn: &Connection, db_path: &Path) -> Result<SubsetSemanticCounts> {
    let chunk_day_sequence = {
        let mut stmt = conn.prepare(
            "SELECT chunk_day_local FROM import_chunk ORDER BY publish_seq, chunk_day_local",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };

    let raw_top = shard_group_count(
        db_path,
        "SELECT COALESCE(normalized_action, '<null>'), COUNT(*) FROM action GROUP BY normalized_action",
    )?;
    let mut top_action_signature: Vec<(String, i64)> = raw_top
        .into_iter()
        .map(|(k, v)| (k.unwrap_or_else(|| "<null>".to_string()), v))
        .collect();
    top_action_signature.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    top_action_signature.truncate(12);

    Ok(SubsetSemanticCounts {
        chunk_day_sequence,
        // Sum global DB + shard DBs to handle both import_all and start_startup_import paths.
        classified_action_count: query_count(
            conn,
            "SELECT COUNT(*) FROM action WHERE classification_state = 'classified'",
        )? + shard_count(
            db_path,
            "SELECT COUNT(*) FROM action WHERE classification_state = 'classified'",
        )?,
        mixed_action_count: query_count(
            conn,
            "SELECT COUNT(*) FROM action WHERE classification_state = 'mixed'",
        )? + shard_count(
            db_path,
            "SELECT COUNT(*) FROM action WHERE classification_state = 'mixed'",
        )?,
        unclassified_action_count: query_count(
            conn,
            "SELECT COUNT(*) FROM action WHERE classification_state = 'unclassified'",
        )? + shard_count(
            db_path,
            "SELECT COUNT(*) FROM action WHERE classification_state = 'unclassified'",
        )?,
        tool_use_part_count: query_count(
            conn,
            "SELECT COUNT(*) FROM message_part WHERE part_kind = 'tool_use'",
        )? + shard_count(
            db_path,
            "SELECT COUNT(*) FROM message_part WHERE part_kind = 'tool_use'",
        )?,
        tool_result_part_count: query_count(
            conn,
            "SELECT COUNT(*) FROM message_part WHERE part_kind = 'tool_result'",
        )? + shard_count(
            db_path,
            "SELECT COUNT(*) FROM message_part WHERE part_kind = 'tool_result'",
        )?,
        parts_with_tool_name_count: query_count(
            conn,
            "SELECT COUNT(*) FROM message_part WHERE tool_name IS NOT NULL",
        )? + shard_count(
            db_path,
            "SELECT COUNT(*) FROM message_part WHERE tool_name IS NOT NULL",
        )?,
        parts_with_tool_call_id_count: query_count(
            conn,
            "SELECT COUNT(*) FROM message_part WHERE tool_call_id IS NOT NULL",
        )? + shard_count(
            db_path,
            "SELECT COUNT(*) FROM message_part WHERE tool_call_id IS NOT NULL",
        )?,
        relay_assistant_message_count: query_count(
            conn,
            "SELECT COUNT(*) FROM message WHERE message_kind = 'relay_assistant_message'",
        )? + shard_count(
            db_path,
            "SELECT COUNT(*) FROM message WHERE message_kind = 'relay_assistant_message'",
        )?,
        assistant_message_count: query_count(
            conn,
            "SELECT COUNT(*) FROM message WHERE message_kind = 'assistant_message'",
        )? + shard_count(
            db_path,
            "SELECT COUNT(*) FROM message WHERE message_kind = 'assistant_message'",
        )?,
        actions_with_null_normalized_action_count: query_count(
            conn,
            "SELECT COUNT(*) FROM action WHERE normalized_action IS NULL",
        )? + shard_count(
            db_path,
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
