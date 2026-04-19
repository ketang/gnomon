use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use super::{
    NormalizedMessage, NormalizedPart, NormalizedToolUsePartMetadata, SourceDescriptor,
    SourceFileKind, SourceProvider, Usage,
};
use crate::perf::{PerfLogger, PerfScope};

const PRIMARY_STREAM_SEQUENCE_NO: i64 = 0;
const SOURCE_LINE_PREVIEW_CHAR_LIMIT: usize = 160;
const WARNING_UNKNOWN_SOURCE_KIND: &str = "unknown_source_kind";
const WARNING_INVALID_JSON: &str = "invalid_json";

#[derive(Debug, Clone)]
pub struct NormalizeJsonlFileParams {
    pub project_id: i64,
    pub source_file_id: i64,
    pub import_chunk_id: i64,
    pub path: PathBuf,
    pub perf_logger: Option<PerfLogger>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizeJsonlFileResult {
    pub conversation_id: Option<i64>,
    pub stream_id: Option<i64>,
    pub record_count: usize,
    pub message_count: usize,
    pub turn_count: usize,
    pub history_event_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizeImportWarning {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizeJsonlFileOutcome {
    Imported(NormalizeJsonlFileResult),
    Skipped,
    Warning(NormalizeImportWarning),
}

#[derive(Debug, Default, Clone, Copy)]
struct NormalizeBreakdown {
    parse: Duration,
    sql: Duration,
    purge: Duration,
    finish_import: Duration,
    commit: Duration,
}

pub fn normalize_jsonl_file(
    conn: &mut Connection,
    params: &NormalizeJsonlFileParams,
) -> Result<NormalizeJsonlFileOutcome> {
    match load_source_descriptor(conn, params.source_file_id)? {
        Some(SourceDescriptor {
            provider: SourceProvider::Claude,
            kind: SourceFileKind::Transcript,
        }) => normalize_transcript_jsonl_file(conn, params),
        Some(SourceDescriptor {
            provider: SourceProvider::Claude,
            kind: SourceFileKind::History,
        }) => normalize_history_jsonl_file(conn, params),
        Some(SourceDescriptor {
            provider: SourceProvider::Codex,
            kind: SourceFileKind::Rollout,
        }) => normalize_codex_rollout_jsonl_file(conn, params),
        Some(_) => {
            purge_existing_import(conn, params)?;
            Ok(NormalizeJsonlFileOutcome::Skipped)
        }
        None => Ok(NormalizeJsonlFileOutcome::Warning(NormalizeImportWarning {
            code: WARNING_UNKNOWN_SOURCE_KIND,
            message: format!(
                "unable to determine source kind for source file id {}",
                params.source_file_id
            ),
        })),
    }
}

/// Normalize a JSONL file within an externally-managed transaction.
/// Caller is responsible for transaction/savepoint commit or rollback.
/// On `Warning` outcome, partial writes may have occurred — caller should
/// roll back the enclosing savepoint.
pub fn normalize_jsonl_file_in_tx(
    conn: &Connection,
    params: &NormalizeJsonlFileParams,
) -> Result<(NormalizeJsonlFileOutcome, Vec<NormalizedMessage>)> {
    match load_source_descriptor(conn, params.source_file_id)? {
        Some(SourceDescriptor {
            provider: SourceProvider::Claude,
            kind: SourceFileKind::Transcript,
        }) => {
            let mut scope = PerfScope::new(params.perf_logger.clone(), "import.normalize_jsonl");
            scope.field("path", params.path.display().to_string());
            let inner = normalize_transcript_jsonl_file_core(conn, params);
            match inner {
                Ok((outcome, breakdown, messages)) => {
                    scope.field("parse_ms", breakdown.parse.as_secs_f64() * 1000.0);
                    scope.field("sql_ms", breakdown.sql.as_secs_f64() * 1000.0);
                    scope.field("purge_ms", breakdown.purge.as_secs_f64() * 1000.0);
                    scope.field(
                        "finish_import_ms",
                        breakdown.finish_import.as_secs_f64() * 1000.0,
                    );
                    scope.field("commit_ms", 0.0f64);
                    match &outcome {
                        NormalizeJsonlFileOutcome::Imported(result) => {
                            scope.field("outcome", "imported");
                            scope.field("record_count", result.record_count);
                            scope.field("message_count", result.message_count);
                            scope.field("turn_count", result.turn_count);
                            scope.finish_ok();
                        }
                        NormalizeJsonlFileOutcome::Skipped => {
                            scope.field("outcome", "skipped");
                            scope.finish_ok();
                        }
                        NormalizeJsonlFileOutcome::Warning(_) => {
                            scope.field("outcome", "warning");
                            scope.finish_ok();
                        }
                    }
                    Ok((outcome, messages))
                }
                Err(err) => {
                    scope.finish_error(&err);
                    Err(err)
                }
            }
        }
        Some(SourceDescriptor {
            provider: SourceProvider::Claude,
            kind: SourceFileKind::History,
        }) => {
            let mut scope =
                PerfScope::new(params.perf_logger.clone(), "import.normalize_history_jsonl");
            scope.field("path", params.path.display().to_string());
            let result = normalize_history_jsonl_file_core(conn, params);
            match &result {
                Ok(NormalizeJsonlFileOutcome::Imported(outcome)) => {
                    scope.field("outcome", "imported");
                    scope.field("record_count", outcome.record_count);
                    scope.finish_ok();
                }
                Ok(NormalizeJsonlFileOutcome::Skipped) => {
                    scope.field("outcome", "skipped");
                    scope.finish_ok();
                }
                Ok(NormalizeJsonlFileOutcome::Warning(_)) => {
                    scope.field("outcome", "warning");
                    scope.finish_ok();
                }
                Err(err) => scope.finish_error(err),
            }
            result.map(|outcome| (outcome, Vec::new()))
        }
        Some(_) => {
            purge_existing_import(conn, params)?;
            Ok((NormalizeJsonlFileOutcome::Skipped, Vec::new()))
        }
        None => Ok((
            NormalizeJsonlFileOutcome::Warning(NormalizeImportWarning {
                code: WARNING_UNKNOWN_SOURCE_KIND,
                message: format!(
                    "unable to determine source kind for source file id {}",
                    params.source_file_id
                ),
            }),
            Vec::new(),
        )),
    }
}

/// Parse a JSONL file without any DB access (pure CPU work).
///
/// Reads the file, parses every line as JSON, extracts record metadata and
/// messages. Returns a [`ParseResult`](super::ParseResult) that can be fed
/// to [`write_parsed_file_in_tx`] for serial DB writes.
pub fn parse_jsonl_file(path: &Path, source_kind: super::SourceFileKind) -> super::ParseResult {
    match parse_jsonl_file_inner(path, source_kind) {
        Ok(result) => result,
        // Convert unexpected I/O errors into warnings so the caller can
        // roll back the savepoint without propagating a hard error that
        // aborts the entire chunk.
        Err(err) => super::ParseResult::Warning(NormalizeImportWarning {
            code: WARNING_INVALID_JSON,
            message: format!("unable to parse {}: {err:#}", path.display()),
        }),
    }
}

fn parse_jsonl_file_inner(
    path: &Path,
    source_kind: super::SourceFileKind,
) -> Result<super::ParseResult> {
    let file = File::open(path)
        .with_context(|| format!("unable to open jsonl source {}", path.display()))?;
    let reader = BufReader::new(file);

    let mut records = Vec::new();

    for (zero_based_line_no, line_result) in reader.lines().enumerate() {
        let line_no = (zero_based_line_no + 1) as i64;
        let line = line_result
            .with_context(|| format!("unable to read line {line_no} from {}", path.display()))?;

        let value: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => {
                return Ok(super::ParseResult::Warning(NormalizeImportWarning {
                    code: WARNING_INVALID_JSON,
                    message: format!(
                        "unable to parse json on line {line_no} from {} (preview: {})",
                        path.display(),
                        preview_source_line(&line)
                    ),
                }));
            }
        };

        let recorded_at_utc = extract_record_timestamp(&value).map(ToOwned::to_owned);
        let extracted_message = match source_kind {
            super::SourceFileKind::Transcript => extract_message(&value, line_no),
            super::SourceFileKind::History
            | super::SourceFileKind::Rollout
            | super::SourceFileKind::SessionIndex => None,
        };

        records.push(super::ParsedRecord {
            source_line_no: line_no,
            value,
            recorded_at_utc,
            extracted_message,
        });
    }

    // For transcript files: detect sessionless-metadata-only files.
    if matches!(source_kind, super::SourceFileKind::Transcript) {
        let has_session_id = records
            .iter()
            .any(|r| extract_session_id(&r.value).is_some());
        if !has_session_id {
            let all_metadata = !records.is_empty()
                && records
                    .iter()
                    .all(|r| is_sessionless_metadata_record(&r.value));
            if all_metadata {
                return Ok(super::ParseResult::SessionlessMetadata);
            }
            // No session ID and not all metadata — this will be an error
            // during the write phase (matches existing behavior).
        }
    }

    Ok(super::ParseResult::Parsed(super::ParsedFile { records }))
}

/// Write a pre-parsed file to the database within an externally-managed
/// transaction. This is the serial write counterpart to [`parse_jsonl_file`].
///
/// Caller is responsible for transaction/savepoint commit or rollback.
/// On `Warning` outcome, partial writes may have occurred — caller should
/// roll back the enclosing savepoint.
pub fn write_parsed_file_in_tx(
    conn: &Connection,
    params: &NormalizeJsonlFileParams,
    parsed: super::ParsedFile,
    descriptor: SourceDescriptor,
) -> Result<(NormalizeJsonlFileOutcome, Vec<NormalizedMessage>)> {
    match descriptor {
        SourceDescriptor {
            provider: SourceProvider::Claude,
            kind: super::SourceFileKind::Transcript,
        } => {
            let mut scope = PerfScope::new(params.perf_logger.clone(), "import.normalize_jsonl");
            scope.field("path", params.path.display().to_string());
            let inner = write_parsed_transcript_core(conn, params, parsed);
            match inner {
                Ok((outcome, breakdown, messages)) => {
                    scope.field("parse_ms", 0.0f64); // parsing done in parallel phase
                    scope.field("sql_ms", breakdown.sql.as_secs_f64() * 1000.0);
                    scope.field("purge_ms", breakdown.purge.as_secs_f64() * 1000.0);
                    scope.field(
                        "finish_import_ms",
                        breakdown.finish_import.as_secs_f64() * 1000.0,
                    );
                    scope.field("commit_ms", 0.0f64);
                    match &outcome {
                        NormalizeJsonlFileOutcome::Imported(result) => {
                            scope.field("outcome", "imported");
                            scope.field("record_count", result.record_count);
                            scope.field("message_count", result.message_count);
                            scope.field("turn_count", result.turn_count);
                            scope.finish_ok();
                        }
                        NormalizeJsonlFileOutcome::Skipped => {
                            scope.field("outcome", "skipped");
                            scope.finish_ok();
                        }
                        NormalizeJsonlFileOutcome::Warning(_) => {
                            scope.field("outcome", "warning");
                            scope.finish_ok();
                        }
                    }
                    Ok((outcome, messages))
                }
                Err(err) => {
                    scope.finish_error(&err);
                    Err(err)
                }
            }
        }
        SourceDescriptor {
            provider: SourceProvider::Claude,
            kind: super::SourceFileKind::History,
        } => {
            let mut scope =
                PerfScope::new(params.perf_logger.clone(), "import.normalize_history_jsonl");
            scope.field("path", params.path.display().to_string());
            let result = write_parsed_history_core(conn, params, parsed);
            match &result {
                Ok(NormalizeJsonlFileOutcome::Imported(outcome)) => {
                    scope.field("outcome", "imported");
                    scope.field("record_count", outcome.record_count);
                    scope.finish_ok();
                }
                Ok(NormalizeJsonlFileOutcome::Skipped) => {
                    scope.field("outcome", "skipped");
                    scope.finish_ok();
                }
                Ok(NormalizeJsonlFileOutcome::Warning(_)) => {
                    scope.field("outcome", "warning");
                    scope.finish_ok();
                }
                Err(err) => scope.finish_error(err),
            }
            result.map(|outcome| (outcome, Vec::new()))
        }
        SourceDescriptor {
            provider: SourceProvider::Codex,
            kind: super::SourceFileKind::Rollout,
        } => {
            let mut scope = PerfScope::new(
                params.perf_logger.clone(),
                "import.normalize_codex_rollout_jsonl",
            );
            scope.field("path", params.path.display().to_string());
            let result = write_parsed_codex_rollout_core(conn, params, parsed);
            match &result {
                Ok(NormalizeJsonlFileOutcome::Imported(outcome)) => {
                    scope.field("outcome", "imported");
                    scope.field("record_count", outcome.record_count);
                    scope.finish_ok();
                }
                Ok(NormalizeJsonlFileOutcome::Skipped) => {
                    scope.field("outcome", "skipped");
                    scope.finish_ok();
                }
                Ok(NormalizeJsonlFileOutcome::Warning(_)) => {
                    scope.field("outcome", "warning");
                    scope.finish_ok();
                }
                Err(err) => scope.finish_error(err),
            }
            result.map(|outcome| (outcome, Vec::new()))
        }
        _ => {
            purge_existing_import(conn, params)?;
            Ok((NormalizeJsonlFileOutcome::Skipped, Vec::new()))
        }
    }
}

/// Serial write phase for transcript files — fed from pre-parsed data.
fn write_parsed_transcript_core(
    conn: &Connection,
    params: &NormalizeJsonlFileParams,
    parsed: super::ParsedFile,
) -> Result<(
    NormalizeJsonlFileOutcome,
    NormalizeBreakdown,
    Vec<NormalizedMessage>,
)> {
    let mut breakdown = NormalizeBreakdown::default();

    let purge_start = Instant::now();
    purge_existing_import(conn, params)?;
    breakdown.purge += purge_start.elapsed();

    let mut state = ImportState::new(params.clone());

    for record in parsed.records {
        let sql_start = Instant::now();
        if state.conversation.is_none() {
            if extract_session_id(&record.value).is_some() {
                state.initialize_context(conn, &record.value)?;
                state.flush_buffered_records()?;
            } else {
                state.buffered_records.push(BufferedRecord {
                    source_line_no: record.source_line_no,
                    value: record.value,
                });
                breakdown.sql += sql_start.elapsed();
                continue;
            }
        }

        state.process_record_from_parsed(record)?;
        breakdown.sql += sql_start.elapsed();
    }

    if state.conversation.is_none() {
        // The parse phase already detects sessionless-metadata-only files and
        // returns SessionlessMetadata, so reaching here means a genuine
        // missing sessionId.
        bail!(
            "no sessionId found in {}; unable to normalize file",
            params.path.display()
        );
    }

    if !state.buffered_records.is_empty() {
        let sql_start = Instant::now();
        state.flush_buffered_records()?;
        breakdown.sql += sql_start.elapsed();
    }

    let mut turns_scope = PerfScope::new(params.perf_logger.clone(), "import.build_turns");
    let (normalized_messages, turn_count) =
        match persist_messages_with_turns(conn, params, &mut state) {
            Ok((msgs, count)) => {
                turns_scope.field("turn_count", count);
                turns_scope.finish_ok();
                (msgs, count)
            }
            Err(err) => {
                turns_scope.finish_error(&err);
                return Err(err);
            }
        };

    let finish_start = Instant::now();
    state.finish_import(conn, turn_count)?;
    breakdown.finish_import += finish_start.elapsed();

    Ok((
        NormalizeJsonlFileOutcome::Imported(NormalizeJsonlFileResult {
            conversation_id: Some(
                state
                    .conversation
                    .as_ref()
                    .map(|conversation| conversation.id)
                    .ok_or_else(|| anyhow!("conversation missing after import"))?,
            ),
            stream_id: Some(
                state
                    .stream
                    .as_ref()
                    .map(|stream| stream.id)
                    .ok_or_else(|| anyhow!("stream missing after import"))?,
            ),
            record_count: state.record_count,
            message_count: normalized_messages.len(),
            turn_count,
            history_event_count: 0,
        }),
        breakdown,
        normalized_messages,
    ))
}

/// Serial write phase for history files — fed from pre-parsed data.
fn write_parsed_history_core(
    conn: &Connection,
    params: &NormalizeJsonlFileParams,
    parsed: super::ParsedFile,
) -> Result<NormalizeJsonlFileOutcome> {
    purge_existing_import(conn, params)?;

    let mut history_event_count = 0usize;

    for record in parsed.records {
        insert_history_event(conn, params, &record.value, record.source_line_no)?;
        history_event_count += 1;
    }

    conn.prepare_cached(
        "
        UPDATE import_chunk
        SET imported_record_count = imported_record_count + ?2
        WHERE id = ?1
        ",
    )
    .and_then(|mut stmt| stmt.execute(params![params.import_chunk_id, history_event_count as i64]))
    .context("unable to update import chunk counters after history normalization")?;

    Ok(NormalizeJsonlFileOutcome::Imported(
        NormalizeJsonlFileResult {
            conversation_id: None,
            stream_id: None,
            record_count: history_event_count,
            message_count: 0,
            turn_count: 0,
            history_event_count,
        },
    ))
}

fn normalize_codex_rollout_jsonl_file(
    conn: &mut Connection,
    params: &NormalizeJsonlFileParams,
) -> Result<NormalizeJsonlFileOutcome> {
    let tx = conn
        .transaction()
        .context("unable to start transaction for codex rollout import")?;
    let parsed = match parse_jsonl_file_inner(&params.path, SourceFileKind::Rollout)? {
        super::ParseResult::Parsed(parsed) => parsed,
        super::ParseResult::SessionlessMetadata => {
            unreachable!("rollout files are never treated as sessionless metadata")
        }
        super::ParseResult::Warning(warning) => {
            tx.rollback()
                .context("unable to rollback codex rollout import transaction")?;
            return Ok(NormalizeJsonlFileOutcome::Warning(warning));
        }
    };
    let outcome = write_parsed_codex_rollout_core(&tx, params, parsed)?;
    tx.commit()
        .context("unable to commit codex rollout import transaction")?;
    Ok(outcome)
}

fn write_parsed_codex_rollout_core(
    conn: &Connection,
    params: &NormalizeJsonlFileParams,
    parsed: super::ParsedFile,
) -> Result<NormalizeJsonlFileOutcome> {
    purge_existing_import(conn, params)?;

    let metadata = CodexRolloutSessionDraft::from_records(&parsed.records)?;
    let rollout_session_id: i64 = conn.query_row(
        "
        INSERT INTO codex_rollout_session (
            project_id,
            source_file_id,
            import_chunk_id,
            session_id,
            raw_cwd_path,
            cli_version,
            originator,
            model_provider,
            model_name,
            started_at_utc,
            completed_at_utc,
            metadata_json
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        RETURNING id
        ",
        params![
            params.project_id,
            params.source_file_id,
            params.import_chunk_id,
            metadata.session_id,
            metadata.raw_cwd_path,
            metadata.cli_version,
            metadata.originator,
            metadata.model_provider,
            metadata.model_name,
            metadata.started_at_utc,
            metadata.completed_at_utc,
            metadata.metadata_json,
        ],
        |row| row.get(0),
    )?;

    let mut event_stmt = conn.prepare_cached(
        "
        INSERT INTO codex_rollout_event (
            codex_rollout_session_id,
            source_file_id,
            import_chunk_id,
            source_line_no,
            event_kind,
            recorded_at_utc,
            raw_json
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ",
    )?;
    for record in &parsed.records {
        event_stmt.execute(params![
            rollout_session_id,
            params.source_file_id,
            params.import_chunk_id,
            record.source_line_no,
            codex_rollout_event_kind(&record.value),
            record.recorded_at_utc,
            serde_json::to_string(&record.value)?,
        ])?;
    }

    Ok(NormalizeJsonlFileOutcome::Imported(
        NormalizeJsonlFileResult {
            conversation_id: None,
            stream_id: None,
            record_count: parsed.records.len(),
            message_count: 0,
            turn_count: 0,
            history_event_count: 0,
        },
    ))
}

fn load_source_descriptor(
    conn: &Connection,
    source_file_id: i64,
) -> Result<Option<SourceDescriptor>> {
    let descriptor: Option<(String, String)> = conn
        .query_row(
            "SELECT source_provider, source_kind FROM source_file WHERE id = ?1",
            [source_file_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .context("unable to load source descriptor for normalization")?;

    descriptor
        .map(|(provider, kind)| {
            let provider = SourceProvider::from_db_value(&provider)
                .with_context(|| format!("unknown source provider `{provider}`"))?;
            let kind = SourceFileKind::from_db_value(&kind)
                .with_context(|| format!("unknown source kind `{kind}`"))?;
            Ok(SourceDescriptor::new(provider, kind))
        })
        .transpose()
}

fn normalize_transcript_jsonl_file(
    conn: &mut Connection,
    params: &NormalizeJsonlFileParams,
) -> Result<NormalizeJsonlFileOutcome> {
    let mut scope = PerfScope::new(params.perf_logger.clone(), "import.normalize_jsonl");
    scope.field("path", params.path.display().to_string());
    let inner = normalize_transcript_jsonl_file_inner(conn, params);
    match inner {
        Ok((outcome, breakdown)) => {
            scope.field("parse_ms", breakdown.parse.as_secs_f64() * 1000.0);
            scope.field("sql_ms", breakdown.sql.as_secs_f64() * 1000.0);
            scope.field("purge_ms", breakdown.purge.as_secs_f64() * 1000.0);
            scope.field(
                "finish_import_ms",
                breakdown.finish_import.as_secs_f64() * 1000.0,
            );
            scope.field("commit_ms", breakdown.commit.as_secs_f64() * 1000.0);
            match &outcome {
                NormalizeJsonlFileOutcome::Imported(result) => {
                    scope.field("outcome", "imported");
                    scope.field("record_count", result.record_count);
                    scope.field("message_count", result.message_count);
                    scope.field("turn_count", result.turn_count);
                    scope.finish_ok();
                }
                NormalizeJsonlFileOutcome::Skipped => {
                    scope.field("outcome", "skipped");
                    scope.finish_ok();
                }
                NormalizeJsonlFileOutcome::Warning(_) => {
                    scope.field("outcome", "warning");
                    scope.finish_ok();
                }
            }
            Ok(outcome)
        }
        Err(err) => {
            scope.finish_error(&err);
            Err(err)
        }
    }
}

fn normalize_transcript_jsonl_file_core(
    conn: &Connection,
    params: &NormalizeJsonlFileParams,
) -> Result<(
    NormalizeJsonlFileOutcome,
    NormalizeBreakdown,
    Vec<NormalizedMessage>,
)> {
    let mut breakdown = NormalizeBreakdown::default();

    let purge_start = Instant::now();
    purge_existing_import(conn, params)?;
    breakdown.purge += purge_start.elapsed();

    let file = File::open(&params.path)
        .with_context(|| format!("unable to open jsonl source {}", params.path.display()))?;
    let reader = BufReader::new(file);

    let mut state = ImportState::new(params.clone());

    for (zero_based_line_no, line_result) in reader.lines().enumerate() {
        let line_no = zero_based_line_no + 1;
        let line = line_result.with_context(|| {
            format!(
                "unable to read line {line_no} from {}",
                params.path.display()
            )
        })?;

        let parse_start = Instant::now();
        let record: Value = match serde_json::from_str(&line) {
            Ok(record) => record,
            Err(_) => {
                breakdown.parse += parse_start.elapsed();
                // Caller is responsible for rollback
                return Ok((
                    NormalizeJsonlFileOutcome::Warning(NormalizeImportWarning {
                        code: WARNING_INVALID_JSON,
                        message: format!(
                            "unable to parse json on line {line_no} from {} (preview: {})",
                            params.path.display(),
                            preview_source_line(&line)
                        ),
                    }),
                    breakdown,
                    Vec::new(),
                ));
            }
        };
        breakdown.parse += parse_start.elapsed();

        let sql_start = Instant::now();
        if state.conversation.is_none() {
            if extract_session_id(&record).is_some() {
                state.initialize_context(conn, &record)?;
                state.flush_buffered_records()?;
            } else {
                state.buffered_records.push(BufferedRecord {
                    source_line_no: line_no as i64,
                    value: record,
                });
                breakdown.sql += sql_start.elapsed();
                continue;
            }
        }

        state.process_record(record, line_no as i64)?;
        breakdown.sql += sql_start.elapsed();
    }

    if state.conversation.is_none() {
        if file_contains_only_sessionless_metadata(&state.buffered_records) {
            return Ok((NormalizeJsonlFileOutcome::Skipped, breakdown, Vec::new()));
        }
        bail!(
            "no sessionId found in {}; unable to normalize file",
            params.path.display()
        );
    }

    if !state.buffered_records.is_empty() {
        let sql_start = Instant::now();
        state.flush_buffered_records()?;
        breakdown.sql += sql_start.elapsed();
    }

    let mut turns_scope = PerfScope::new(params.perf_logger.clone(), "import.build_turns");
    let (normalized_messages, turn_count) =
        match persist_messages_with_turns(conn, params, &mut state) {
            Ok((msgs, count)) => {
                turns_scope.field("turn_count", count);
                turns_scope.finish_ok();
                (msgs, count)
            }
            Err(err) => {
                turns_scope.finish_error(&err);
                return Err(err);
            }
        };

    let finish_start = Instant::now();
    state.finish_import(conn, turn_count)?;
    breakdown.finish_import += finish_start.elapsed();
    // No per-file commit: breakdown.commit stays at Duration::ZERO

    Ok((
        NormalizeJsonlFileOutcome::Imported(NormalizeJsonlFileResult {
            conversation_id: Some(
                state
                    .conversation
                    .as_ref()
                    .map(|conversation| conversation.id)
                    .ok_or_else(|| anyhow!("conversation missing after import"))?,
            ),
            stream_id: Some(
                state
                    .stream
                    .as_ref()
                    .map(|stream| stream.id)
                    .ok_or_else(|| anyhow!("stream missing after import"))?,
            ),
            record_count: state.record_count,
            message_count: normalized_messages.len(),
            turn_count,
            history_event_count: 0,
        }),
        breakdown,
        normalized_messages,
    ))
}

fn normalize_transcript_jsonl_file_inner(
    conn: &mut Connection,
    params: &NormalizeJsonlFileParams,
) -> Result<(NormalizeJsonlFileOutcome, NormalizeBreakdown)> {
    let tx = conn
        .transaction()
        .context("unable to start normalization transaction")?;
    let (outcome, mut breakdown, _messages) = normalize_transcript_jsonl_file_core(&tx, params)?;
    match &outcome {
        NormalizeJsonlFileOutcome::Warning(_) => {
            // tx drops, auto-rollback undoes purge + partial writes
        }
        NormalizeJsonlFileOutcome::Skipped => {
            let commit_start = Instant::now();
            tx.commit()
                .context("unable to commit skipped metadata-only import")?;
            breakdown.commit = commit_start.elapsed();
        }
        NormalizeJsonlFileOutcome::Imported(_) => {
            let commit_start = Instant::now();
            tx.commit().context("unable to commit normalized import")?;
            breakdown.commit = commit_start.elapsed();
        }
    }
    Ok((outcome, breakdown))
}

fn preview_source_line(line: &str) -> String {
    if line.is_empty() {
        return "<empty line>".to_string();
    }

    let mut preview = String::new();
    let mut chars = line.chars();

    for _ in 0..SOURCE_LINE_PREVIEW_CHAR_LIMIT {
        let Some(ch) = chars.next() else {
            return preview;
        };
        preview.extend(ch.escape_default());
    }

    if chars.next().is_some() {
        preview.push_str("...");
    }

    preview
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryInputKind {
    PlainPrompt,
    SlashCommand,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SkillInvocationDraft {
    session_id: String,
    recorded_at_utc: Option<String>,
    raw_project: Option<String>,
    skill_name: String,
}

impl HistoryInputKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PlainPrompt => "plain_prompt",
            Self::SlashCommand => "slash_command",
            Self::Other => "other",
        }
    }
}

fn normalize_history_jsonl_file(
    conn: &mut Connection,
    params: &NormalizeJsonlFileParams,
) -> Result<NormalizeJsonlFileOutcome> {
    let mut scope = PerfScope::new(params.perf_logger.clone(), "import.normalize_history_jsonl");
    scope.field("path", params.path.display().to_string());
    let result = normalize_history_jsonl_file_inner(conn, params);
    match &result {
        Ok(NormalizeJsonlFileOutcome::Imported(outcome)) => {
            scope.field("outcome", "imported");
            scope.field("record_count", outcome.record_count);
            scope.finish_ok();
        }
        Ok(NormalizeJsonlFileOutcome::Skipped) => {
            scope.field("outcome", "skipped");
            scope.finish_ok();
        }
        Ok(NormalizeJsonlFileOutcome::Warning(_)) => {
            scope.field("outcome", "warning");
            scope.finish_ok();
        }
        Err(err) => scope.finish_error(err),
    }
    result
}

fn normalize_history_jsonl_file_core(
    conn: &Connection,
    params: &NormalizeJsonlFileParams,
) -> Result<NormalizeJsonlFileOutcome> {
    purge_existing_import(conn, params)?;

    let file = File::open(&params.path)
        .with_context(|| format!("unable to open jsonl source {}", params.path.display()))?;
    let reader = BufReader::new(file);
    let mut history_event_count = 0usize;

    for (zero_based_line_no, line_result) in reader.lines().enumerate() {
        let line_no = zero_based_line_no + 1;
        let line = line_result.with_context(|| {
            format!(
                "unable to read line {line_no} from {}",
                params.path.display()
            )
        })?;

        let record: Value = match serde_json::from_str(&line) {
            Ok(record) => record,
            Err(_) => {
                // Caller is responsible for rollback
                return Ok(NormalizeJsonlFileOutcome::Warning(NormalizeImportWarning {
                    code: WARNING_INVALID_JSON,
                    message: format!(
                        "unable to parse json on line {line_no} from {} (preview: {})",
                        params.path.display(),
                        preview_source_line(&line)
                    ),
                }));
            }
        };

        insert_history_event(conn, params, &record, line_no as i64)?;
        history_event_count += 1;
    }

    conn.prepare_cached(
        "
        UPDATE import_chunk
        SET imported_record_count = imported_record_count + ?2
        WHERE id = ?1
        ",
    )
    .and_then(|mut stmt| stmt.execute(params![params.import_chunk_id, history_event_count as i64]))
    .context("unable to update import chunk counters after history normalization")?;

    Ok(NormalizeJsonlFileOutcome::Imported(
        NormalizeJsonlFileResult {
            conversation_id: None,
            stream_id: None,
            record_count: history_event_count,
            message_count: 0,
            turn_count: 0,
            history_event_count,
        },
    ))
}

fn normalize_history_jsonl_file_inner(
    conn: &mut Connection,
    params: &NormalizeJsonlFileParams,
) -> Result<NormalizeJsonlFileOutcome> {
    let tx = conn
        .transaction()
        .context("unable to start history normalization transaction")?;
    let result = normalize_history_jsonl_file_core(&tx, params)?;
    match &result {
        NormalizeJsonlFileOutcome::Warning(_) => {
            // tx drops, auto-rollback undoes purge + partial writes
        }
        _ => {
            tx.commit()
                .context("unable to commit normalized history import")?;
        }
    }
    Ok(result)
}

fn insert_history_event(
    conn: &Connection,
    params: &NormalizeJsonlFileParams,
    record: &Value,
    source_line_no: i64,
) -> Result<()> {
    let display_text = optional_string(record.get("display"));
    let (input_kind, slash_command_name) = classify_history_input(display_text.as_deref());
    let pasted_contents_json = record
        .get("pastedContents")
        .map(serde_json::to_string)
        .transpose()
        .with_context(|| {
            format!(
                "unable to serialize pastedContents on line {source_line_no} from {}",
                params.path.display()
            )
        })?;
    let raw_json = serde_json::to_string(record).with_context(|| {
        format!(
            "unable to serialize raw history event on line {source_line_no} from {}",
            params.path.display()
        )
    })?;

    conn.prepare_cached(
        "
        INSERT INTO history_event (
            import_chunk_id,
            source_file_id,
            source_line_no,
            session_id,
            recorded_at_utc,
            raw_project,
            display_text,
            pasted_contents_json,
            input_kind,
            slash_command_name,
            raw_json
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        ",
    )
    .and_then(|mut stmt| {
        stmt.execute(params![
            params.import_chunk_id,
            params.source_file_id,
            source_line_no,
            optional_string(record.get("sessionId")),
            optional_string(record.get("timestamp")),
            optional_string(record.get("project")),
            display_text,
            pasted_contents_json,
            input_kind.as_str(),
            slash_command_name,
            raw_json,
        ])
    })
    .with_context(|| {
        format!(
            "unable to insert normalized history event on line {source_line_no} from {}",
            params.path.display()
        )
    })?;
    let history_event_id = conn.last_insert_rowid();

    if let Some(invocation) = extract_skill_invocation(record) {
        insert_skill_invocation(conn, params, history_event_id, &invocation)?;
    }

    Ok(())
}

fn classify_history_input(display_text: Option<&str>) -> (HistoryInputKind, Option<String>) {
    let Some(display_text) = display_text.map(str::trim) else {
        return (HistoryInputKind::Other, None);
    };
    if display_text.is_empty() {
        return (HistoryInputKind::Other, None);
    }
    if let Some(rest) = display_text.strip_prefix('/') {
        let command_name = rest
            .split_whitespace()
            .next()
            .filter(|name| !name.is_empty())
            .map(ToOwned::to_owned);
        return (HistoryInputKind::SlashCommand, command_name);
    }
    (HistoryInputKind::PlainPrompt, None)
}

fn extract_skill_invocation(record: &Value) -> Option<SkillInvocationDraft> {
    let session_id = optional_string(record.get("sessionId"))?;
    let display_text = optional_string(record.get("display"))?;
    let stripped = display_text.trim().strip_prefix("/skill")?;
    let skill_name = stripped
        .split_whitespace()
        .next()
        .filter(|name| !name.is_empty())?
        .to_string();

    Some(SkillInvocationDraft {
        session_id,
        recorded_at_utc: optional_string(record.get("timestamp")),
        raw_project: optional_string(record.get("project")),
        skill_name,
    })
}

fn insert_skill_invocation(
    conn: &Connection,
    params: &NormalizeJsonlFileParams,
    history_event_id: i64,
    invocation: &SkillInvocationDraft,
) -> Result<()> {
    conn.prepare_cached(
        "
        INSERT INTO skill_invocation (
            import_chunk_id,
            history_event_id,
            source_file_id,
            session_id,
            recorded_at_utc,
            raw_project,
            skill_name,
            invocation_kind
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'explicit_history')
        ",
    )
    .and_then(|mut stmt| {
        stmt.execute(params![
            params.import_chunk_id,
            history_event_id,
            params.source_file_id,
            invocation.session_id,
            invocation.recorded_at_utc,
            invocation.raw_project,
            invocation.skill_name,
        ])
    })
    .context("unable to insert explicit skill invocation")?;
    Ok(())
}

pub(super) fn purge_existing_import(
    conn: &Connection,
    params: &NormalizeJsonlFileParams,
) -> Result<()> {
    let existing_conversation_id: Option<i64> = conn
        .prepare_cached("SELECT id FROM conversation WHERE source_file_id = ?1")
        .and_then(|mut stmt| {
            stmt.query_row([params.source_file_id], |row| row.get(0))
                .optional()
        })
        .context("unable to look up existing conversation for source file")?;

    if let Some(conversation_id) = existing_conversation_id {
        conn.prepare_cached("DELETE FROM conversation WHERE id = ?1")
            .and_then(|mut stmt| stmt.execute([conversation_id]))
            .context("unable to purge existing normalized conversation state")?;
    }

    conn.prepare_cached("DELETE FROM history_event WHERE source_file_id = ?1")
        .and_then(|mut stmt| stmt.execute([params.source_file_id]))
        .context("unable to purge existing normalized history event state")?;

    conn.prepare_cached("DELETE FROM skill_invocation WHERE source_file_id = ?1")
        .and_then(|mut stmt| stmt.execute([params.source_file_id]))
        .context("unable to purge existing normalized skill invocation state")?;

    conn.prepare_cached("DELETE FROM codex_rollout_session WHERE source_file_id = ?1")
        .and_then(|mut stmt| stmt.execute([params.source_file_id]))
        .context("unable to purge existing codex rollout raw state")?;

    conn.prepare_cached(
        "
        DELETE FROM import_warning
        WHERE import_chunk_id = ?1 AND source_file_id = ?2
        ",
    )
    .and_then(|mut stmt| stmt.execute(params![params.import_chunk_id, params.source_file_id]))
    .context("unable to clear prior import warnings for source file")?;

    Ok(())
}

#[derive(Debug, Clone)]
struct ConversationState {
    id: i64,
    started_at_utc: Option<String>,
    ended_at_utc: Option<String>,
}

#[derive(Debug, Clone)]
struct StreamState {
    id: i64,
    opened_at_utc: Option<String>,
    closed_at_utc: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct CodexRolloutSessionDraft {
    session_id: Option<String>,
    raw_cwd_path: Option<String>,
    cli_version: Option<String>,
    originator: Option<String>,
    model_provider: Option<String>,
    model_name: Option<String>,
    started_at_utc: Option<String>,
    completed_at_utc: Option<String>,
    metadata_json: String,
}

impl CodexRolloutSessionDraft {
    fn from_records(records: &[super::ParsedRecord]) -> Result<Self> {
        let mut draft = Self {
            metadata_json: "{}".to_string(),
            ..Self::default()
        };

        for record in records {
            if draft.started_at_utc.is_none() {
                draft.started_at_utc = record.recorded_at_utc.clone();
            }
            if record.recorded_at_utc.is_some() {
                draft.completed_at_utc = record.recorded_at_utc.clone();
            }

            if draft.session_id.is_none() {
                draft.session_id = optional_string(record.value.get("session_id"))
                    .or_else(|| optional_string(record.value.pointer("/payload/session_id")));
            }
            if draft.raw_cwd_path.is_none() {
                draft.raw_cwd_path = optional_string(record.value.get("cwd"))
                    .or_else(|| optional_string(record.value.pointer("/payload/cwd")));
            }

            if record.value.get("type").and_then(Value::as_str) == Some("session_meta") {
                draft.cli_version = draft
                    .cli_version
                    .or_else(|| optional_string(record.value.get("cli_version")))
                    .or_else(|| optional_string(record.value.pointer("/payload/cli_version")));
                draft.originator = draft
                    .originator
                    .or_else(|| optional_string(record.value.get("originator")))
                    .or_else(|| optional_string(record.value.pointer("/payload/originator")));
                draft.model_provider = draft
                    .model_provider
                    .or_else(|| optional_string(record.value.get("model_provider")))
                    .or_else(|| optional_string(record.value.pointer("/payload/model_provider")));
                draft.model_name = draft
                    .model_name
                    .or_else(|| optional_string(record.value.get("model")))
                    .or_else(|| optional_string(record.value.pointer("/payload/model")));
                draft.metadata_json = serde_json::to_string(&record.value)?;
            }
        }

        Ok(draft)
    }
}

#[derive(Debug, Clone)]
struct BufferedRecord {
    source_line_no: i64,
    value: Value,
}

#[derive(Debug, Clone)]
pub(super) struct ExtractedMessage {
    pub(super) external_id: String,
    pub(super) source_line_no: i64,
    pub(super) role: String,
    pub(super) message_kind: &'static str,
    pub(super) recorded_at_utc: Option<String>,
    pub(super) model_name: Option<String>,
    pub(super) stop_reason: Option<String>,
    pub(super) usage_source: Option<&'static str>,
    pub(super) usage: Usage,
    pub(super) parts: Vec<ExtractedMessagePart>,
}

/// Normalized message part fields extracted during import.
///
/// Only fields consumed by downstream modules (classify, query) are populated.
/// See [`IMPORT_SCHEMA_VERSION`](super::IMPORT_SCHEMA_VERSION) for the field
/// contract.
#[derive(Debug, Clone)]
pub(super) struct ExtractedMessagePart {
    pub(super) part_kind: String,
    pub(super) mime_type: Option<String>,
    pub(super) text_value: Option<String>,
    pub(super) tool_name: Option<String>,
    pub(super) tool_call_id: Option<String>,
    pub(super) metadata_json: Option<String>,
    pub(super) is_error: bool,
    pub(super) dedupe_key: String,
}

#[derive(Debug, Clone)]
struct MessageState {
    /// Database-assigned row ID; 0 until `persist_messages_with_turns` inserts
    /// this message.
    id: i64,
    stream_id: i64,
    sequence_no: i64,
    // Fields required for the INSERT INTO message statement.
    external_id: String,
    role: String,
    message_kind: String,
    created_at_utc: Option<String>,
    completed_at_utc: Option<String>,
    recorded_at_utc: Option<String>,
    model_name: Option<String>,
    stop_reason: Option<String>,
    usage_source: Option<&'static str>,
    usage: Usage,
    source_line_no: i64,
    // Part accumulator (pre-INSERT); converted to `parts` after INSERT.
    seen_part_keys: HashSet<String>,
    pending_parts: Vec<ExtractedMessagePart>,
    /// Populated by `persist_messages_with_turns` after the message part rows
    /// are written; consumed by `build_actions`.
    parts: Vec<NormalizedPart>,
    /// Set by `persist_messages_with_turns` once the message is assigned to a turn.
    turn_id: Option<i64>,
    turn_sequence_no: Option<i64>,
    ordinal_in_turn: Option<i64>,
}

#[derive(Debug)]
struct ImportState {
    params: NormalizeJsonlFileParams,
    conversation: Option<ConversationState>,
    stream: Option<StreamState>,
    buffered_records: Vec<BufferedRecord>,
    record_count: usize,
    message_count: usize,
    next_record_sequence_no: i64,
    next_message_sequence_no: i64,
    message_states: HashMap<String, MessageState>,
}

impl ImportState {
    fn new(params: NormalizeJsonlFileParams) -> Self {
        Self {
            params,
            conversation: None,
            stream: None,
            buffered_records: Vec::new(),
            record_count: 0,
            message_count: 0,
            next_record_sequence_no: 0,
            next_message_sequence_no: 0,
            message_states: HashMap::new(),
        }
    }

    fn initialize_context(&mut self, conn: &Connection, record: &Value) -> Result<()> {
        let session_id = extract_session_id(record)
            .ok_or_else(|| anyhow!("cannot initialize import context without sessionId"))?;
        let conversation_external_id =
            conversation_external_id(session_id, self.params.source_file_id);
        let timestamp = extract_record_timestamp(record).map(ToOwned::to_owned);

        conn.prepare_cached(
            "
                INSERT INTO conversation (
                    project_id,
                    source_file_id,
                    shared_session_id,
                    external_id,
                    started_at_utc,
                    ended_at_utc
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?5)
                ",
        )
        .and_then(|mut stmt| {
            stmt.execute(params![
                self.params.project_id,
                self.params.source_file_id,
                session_id,
                conversation_external_id,
                timestamp
            ])
        })
        .context("unable to insert conversation for normalized file")?;
        let conversation_id = conn.last_insert_rowid();

        let stream_kind = if record
            .get("isSidechain")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            "sidechain"
        } else {
            "primary"
        };
        let stream_external_id = record.get("agentId").and_then(Value::as_str);

        conn.prepare_cached(
            "
                INSERT INTO stream (
                    conversation_id,
                    import_chunk_id,
                    external_id,
                    stream_kind,
                    sequence_no,
                    opened_at_utc,
                    closed_at_utc
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
                ",
        )
        .and_then(|mut stmt| {
            stmt.execute(params![
                conversation_id,
                self.params.import_chunk_id,
                stream_external_id,
                stream_kind,
                PRIMARY_STREAM_SEQUENCE_NO,
                timestamp
            ])
        })
        .context("unable to insert primary stream for normalized file")?;
        let stream_id = conn.last_insert_rowid();

        self.conversation = Some(ConversationState {
            id: conversation_id,
            started_at_utc: timestamp.clone(),
            ended_at_utc: timestamp.clone(),
        });
        self.stream = Some(StreamState {
            id: stream_id,
            opened_at_utc: timestamp.clone(),
            closed_at_utc: timestamp,
        });

        Ok(())
    }

    fn flush_buffered_records(&mut self) -> Result<()> {
        let buffered_records = std::mem::take(&mut self.buffered_records);
        for record in buffered_records {
            self.process_record(record.value, record.source_line_no)?;
        }
        Ok(())
    }

    fn process_record(&mut self, record: Value, source_line_no: i64) -> Result<()> {
        let conversation = self
            .conversation
            .as_mut()
            .ok_or_else(|| anyhow!("conversation state missing while processing record"))?;
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| anyhow!("stream state missing while processing record"))?;

        let recorded_at_utc = extract_record_timestamp(&record).map(ToOwned::to_owned);
        update_bounds(
            &mut conversation.started_at_utc,
            &mut conversation.ended_at_utc,
            &recorded_at_utc,
        );
        update_bounds(
            &mut stream.opened_at_utc,
            &mut stream.closed_at_utc,
            &recorded_at_utc,
        );

        self.next_record_sequence_no += 1;
        self.record_count += 1;

        if let Some(message) = extract_message(&record, source_line_no) {
            self.upsert_message(message);
        }

        Ok(())
    }

    /// Like [`process_record`] but uses pre-extracted fields from the parallel
    /// parse phase, avoiding redundant JSON traversal.
    fn process_record_from_parsed(&mut self, record: super::ParsedRecord) -> Result<()> {
        let conversation = self
            .conversation
            .as_mut()
            .ok_or_else(|| anyhow!("conversation state missing while processing record"))?;
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| anyhow!("stream state missing while processing record"))?;

        update_bounds(
            &mut conversation.started_at_utc,
            &mut conversation.ended_at_utc,
            &record.recorded_at_utc,
        );
        update_bounds(
            &mut stream.opened_at_utc,
            &mut stream.closed_at_utc,
            &record.recorded_at_utc,
        );

        self.next_record_sequence_no += 1;
        self.record_count += 1;

        if let Some(message) = record.extracted_message {
            self.upsert_message(message);
        }

        Ok(())
    }

    /// Accumulate message state in memory.  No DB writes — the actual INSERT
    /// is deferred to `persist_messages_with_turns`, which can supply `turn_id`
    /// at INSERT time and avoid the join-table round-trip.
    fn upsert_message(&mut self, extracted: ExtractedMessage) {
        let stream_id = self.stream.as_ref().map(|s| s.id).unwrap_or(0);

        if let Some(state) = self.message_states.get_mut(&extracted.external_id) {
            // Subsequent occurrence of the same message (streaming update):
            // merge final-value fields in memory only.
            state.recorded_at_utc = extracted
                .recorded_at_utc
                .clone()
                .or_else(|| state.recorded_at_utc.clone());
            state.completed_at_utc = extracted
                .recorded_at_utc
                .clone()
                .or_else(|| state.completed_at_utc.clone());
            state.model_name = state.model_name.take().or(extracted.model_name);
            state.stop_reason = state.stop_reason.take().or(extracted.stop_reason);
            state.usage_source = state.usage_source.or(extracted.usage_source);
            if extracted.usage.has_any() {
                state.usage = extracted.usage;
            }
            for part in extracted.parts {
                if state.seen_part_keys.insert(part.dedupe_key.clone()) {
                    state.pending_parts.push(part);
                }
            }
            return;
        }

        // First occurrence: build a fresh MessageState (id = 0 until INSERT).
        let sequence_no = self.next_message_sequence_no;
        self.next_message_sequence_no += 1;
        self.message_count += 1;

        let mut seen_part_keys = HashSet::new();
        let mut pending_parts = Vec::new();
        for part in extracted.parts {
            if seen_part_keys.insert(part.dedupe_key.clone()) {
                pending_parts.push(part);
            }
        }

        self.message_states.insert(
            extracted.external_id.clone(),
            MessageState {
                id: 0,
                stream_id,
                sequence_no,
                external_id: extracted.external_id,
                role: extracted.role.to_string(),
                message_kind: extracted.message_kind.to_string(),
                created_at_utc: extracted.recorded_at_utc.clone(),
                completed_at_utc: extracted.recorded_at_utc.clone(),
                recorded_at_utc: extracted.recorded_at_utc,
                model_name: extracted.model_name,
                stop_reason: extracted.stop_reason,
                usage_source: extracted.usage_source,
                usage: extracted.usage,
                source_line_no: extracted.source_line_no,
                seen_part_keys,
                pending_parts,
                parts: Vec::new(),
                turn_id: None,
                turn_sequence_no: None,
                ordinal_in_turn: None,
            },
        );
    }

    fn finish_import(&self, conn: &Connection, turn_count: usize) -> Result<()> {
        let conversation = self
            .conversation
            .as_ref()
            .ok_or_else(|| anyhow!("conversation state missing while finalizing import"))?;
        let stream = self
            .stream
            .as_ref()
            .ok_or_else(|| anyhow!("stream state missing while finalizing import"))?;

        conn.prepare_cached(
            "
            UPDATE conversation
            SET started_at_utc = ?2, ended_at_utc = ?3
            WHERE id = ?1
            ",
        )
        .and_then(|mut stmt| {
            stmt.execute(params![
                conversation.id,
                conversation.started_at_utc,
                conversation.ended_at_utc
            ])
        })
        .context("unable to update conversation bounds after normalization")?;

        conn.prepare_cached(
            "
            UPDATE stream
            SET opened_at_utc = ?2, closed_at_utc = ?3
            WHERE id = ?1
            ",
        )
        .and_then(|mut stmt| {
            stmt.execute(params![
                stream.id,
                stream.opened_at_utc,
                stream.closed_at_utc
            ])
        })
        .context("unable to update stream bounds after normalization")?;

        conn.prepare_cached(
            "
            UPDATE import_chunk
            SET
                imported_record_count = imported_record_count + ?2,
                imported_message_count = imported_message_count + ?3,
                imported_conversation_count = imported_conversation_count + 1,
                imported_turn_count = imported_turn_count + ?4
            WHERE id = ?1
            ",
        )
        .and_then(|mut stmt| {
            stmt.execute(params![
                self.params.import_chunk_id,
                self.record_count as i64,
                self.message_count as i64,
                turn_count as i64
            ])
        })
        .context("unable to update import chunk counters after normalization")?;

        Ok(())
    }
}

/// Aggregated turn-level data accumulated during in-memory turn grouping.
#[derive(Debug)]
struct TurnDraft {
    stream_id: i64,
    started_at_utc: Option<String>,
    ended_at_utc: Option<String>,
    usage: Usage,
}

impl TurnDraft {
    fn new_from_state(ms: &MessageState) -> Self {
        let timestamp = ms
            .created_at_utc
            .clone()
            .or_else(|| ms.completed_at_utc.clone());
        Self {
            stream_id: ms.stream_id,
            started_at_utc: timestamp.clone(),
            ended_at_utc: ms.completed_at_utc.clone().or(timestamp),
            usage: ms.usage.clone(),
        }
    }

    fn push_from_state(&mut self, ms: &MessageState) {
        let start = ms
            .created_at_utc
            .clone()
            .or_else(|| ms.completed_at_utc.clone());
        let end = ms.completed_at_utc.clone().or_else(|| start.clone());

        update_bounds(&mut self.started_at_utc, &mut self.ended_at_utc, &start);
        update_end(&mut self.ended_at_utc, &end);

        Usage::add_field(&mut self.usage.input_tokens, ms.usage.input_tokens);
        Usage::add_field(
            &mut self.usage.cache_creation_input_tokens,
            ms.usage.cache_creation_input_tokens,
        );
        Usage::add_field(
            &mut self.usage.cache_read_input_tokens,
            ms.usage.cache_read_input_tokens,
        );
        Usage::add_field(&mut self.usage.output_tokens, ms.usage.output_tokens);
    }
}

/// A turn computed in memory, referencing indices into a sorted message slice.
struct TurnAssignment {
    /// Index of the root (first/user-prompt) message.
    root_idx: usize,
    /// Indices of all messages in this turn, root first, in sequence_no order.
    member_idxs: Vec<usize>,
    draft: TurnDraft,
}

fn insert_turn_row(
    conn: &Connection,
    conversation_id: i64,
    import_chunk_id: i64,
    sequence_no: i64,
    root_message_id: i64,
    draft: &TurnDraft,
) -> Result<i64> {
    conn.prepare_cached(
        "
        INSERT INTO turn (
            stream_id,
            conversation_id,
            import_chunk_id,
            root_message_id,
            sequence_no,
            started_at_utc,
            ended_at_utc,
            input_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            output_tokens
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        ",
    )?
    .execute(params![
        draft.stream_id,
        conversation_id,
        import_chunk_id,
        root_message_id,
        sequence_no,
        draft.started_at_utc,
        draft.ended_at_utc,
        draft.usage.input_tokens,
        draft.usage.cache_creation_input_tokens,
        draft.usage.cache_read_input_tokens,
        draft.usage.output_tokens,
    ])?;
    Ok(conn.last_insert_rowid())
}

/// INSERT a single message row and return its DB-assigned id.
fn insert_message_row(
    conn: &Connection,
    conversation_id: i64,
    import_chunk_id: i64,
    ms: &MessageState,
    turn_id: Option<i64>,
    ordinal_in_turn: Option<i64>,
    source_path: &Path,
) -> Result<i64> {
    conn.prepare_cached(
        "
        INSERT INTO message (
            stream_id,
            conversation_id,
            import_chunk_id,
            external_id,
            role,
            message_kind,
            sequence_no,
            created_at_utc,
            completed_at_utc,
            model_name,
            stop_reason,
            usage_source,
            input_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            output_tokens,
            turn_id,
            ordinal_in_turn
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
        ",
    )
    .and_then(|mut stmt| {
        stmt.execute(params![
            ms.stream_id,
            conversation_id,
            import_chunk_id,
            ms.external_id,
            ms.role,
            ms.message_kind,
            ms.sequence_no,
            ms.created_at_utc,
            ms.completed_at_utc,
            ms.model_name,
            ms.stop_reason,
            ms.usage_source,
            ms.usage.input_tokens,
            ms.usage.cache_creation_input_tokens,
            ms.usage.cache_read_input_tokens,
            ms.usage.output_tokens,
            turn_id,
            ordinal_in_turn,
        ])
    })
    .with_context(|| {
        format!(
            "unable to insert normalized message on line {} from {} (external_id={})",
            ms.source_line_no,
            source_path.display(),
            ms.external_id,
        )
    })?;
    Ok(conn.last_insert_rowid())
}

/// INSERT pending parts for a message (already inserted) and populate `ms.parts`.
fn insert_pending_parts(
    conn: &Connection,
    ms: &mut MessageState,
    source_path: &Path,
) -> Result<()> {
    for (ordinal, part) in ms.pending_parts.iter().enumerate() {
        let part_id = insert_message_part(
            conn,
            ms.id,
            ordinal as i64,
            part,
            ms.source_line_no,
            source_path,
        )?;
        ms.parts.push(NormalizedPart {
            id: part_id,
            part_kind: part.part_kind.clone(),
            tool_name: part.tool_name.clone(),
            tool_call_id: part.tool_call_id.clone(),
            metadata_json: part.metadata_json.clone(),
        });
    }
    ms.pending_parts.clear();
    Ok(())
}

/// Group messages into turns in memory, INSERT all messages and turns in the
/// correct order (root first, then turn row, then UPDATE root's turn_id, then
/// remaining members), and return the final sorted `NormalizedMessage` list.
///
/// This is the core of the D1b optimisation: `turn_id` is set at INSERT time
/// for all non-root messages, and only N_turns UPDATEs are needed for the root
/// messages (instead of N_messages turn_message INSERTs in the old schema).
fn persist_messages_with_turns(
    conn: &Connection,
    params: &NormalizeJsonlFileParams,
    state: &mut ImportState,
) -> Result<(Vec<NormalizedMessage>, usize)> {
    let conversation_id = state
        .conversation
        .as_ref()
        .ok_or_else(|| anyhow!("conversation missing during persist_messages_with_turns"))?
        .id;

    // Collect and sort messages by sequence_no (stable ordering).
    let mut messages: Vec<MessageState> = state.message_states.drain().map(|(_, v)| v).collect();
    messages.sort_by_key(|m| m.sequence_no);

    // Group into turns: a new turn starts whenever message_kind == "user_prompt".
    let mut turns: Vec<TurnAssignment> = Vec::new();
    let mut unassigned: Vec<usize> = Vec::new();
    let mut current_turn: Option<TurnAssignment> = None;

    for (idx, ms) in messages.iter().enumerate() {
        if ms.message_kind == "user_prompt" {
            if let Some(finished) = current_turn.take() {
                turns.push(finished);
            }
            current_turn = Some(TurnAssignment {
                root_idx: idx,
                member_idxs: vec![idx],
                draft: TurnDraft::new_from_state(ms),
            });
        } else if let Some(ref mut turn) = current_turn {
            turn.member_idxs.push(idx);
            turn.draft.push_from_state(ms);
        } else {
            unassigned.push(idx);
        }
    }
    if let Some(finished) = current_turn {
        turns.push(finished);
    }

    // INSERT unassigned messages (no turn membership).
    let source_path = &params.path;

    for &idx in &unassigned {
        let ms = &mut messages[idx];
        ms.id = insert_message_row(
            conn,
            conversation_id,
            params.import_chunk_id,
            ms,
            None,
            None,
            source_path,
        )?;
        insert_pending_parts(conn, ms, source_path)?;
    }

    // INSERT each turn: root message → turn row → UPDATE root → member messages.
    let mut next_turn_sequence_no: i64 = 0;
    for turn in &turns {
        // a. INSERT root message (turn_id = NULL initially, FK not yet set).
        {
            let root = &mut messages[turn.root_idx];
            root.id = insert_message_row(
                conn,
                conversation_id,
                params.import_chunk_id,
                root,
                None,
                None,
                source_path,
            )?;
            insert_pending_parts(conn, root, source_path)?;
        }

        // b. INSERT turn row referencing the root message.
        let root_id = messages[turn.root_idx].id;
        let turn_id = insert_turn_row(
            conn,
            conversation_id,
            params.import_chunk_id,
            next_turn_sequence_no,
            root_id,
            &turn.draft,
        )?;
        next_turn_sequence_no += 1;

        // c. UPDATE root message to set turn_id and ordinal_in_turn = 0.
        conn.prepare_cached("UPDATE message SET turn_id = ?1, ordinal_in_turn = 0 WHERE id = ?2")?
            .execute(params![turn_id, root_id])
            .context("unable to set turn_id on root message")?;
        {
            let root = &mut messages[turn.root_idx];
            root.turn_id = Some(turn_id);
            root.turn_sequence_no = Some(next_turn_sequence_no - 1);
            root.ordinal_in_turn = Some(0);
        }

        // d. INSERT remaining turn members with turn_id already set.
        for (ordinal, &idx) in turn.member_idxs.iter().enumerate().skip(1) {
            let ordinal_i64 = ordinal as i64;
            let ms = &mut messages[idx];
            ms.id = insert_message_row(
                conn,
                conversation_id,
                params.import_chunk_id,
                ms,
                Some(turn_id),
                Some(ordinal_i64),
                source_path,
            )?;
            insert_pending_parts(conn, ms, source_path)?;
            ms.turn_id = Some(turn_id);
            ms.turn_sequence_no = Some(next_turn_sequence_no - 1);
            ms.ordinal_in_turn = Some(ordinal_i64);
        }
    }

    // Build the final NormalizedMessage list, sorted by (sequence_no, id).
    let mut normalized: Vec<NormalizedMessage> = messages
        .iter()
        .map(|ms| NormalizedMessage {
            id: ms.id,
            stream_id: ms.stream_id,
            sequence_no: ms.sequence_no,
            message_kind: ms.message_kind.clone(),
            created_at_utc: ms.created_at_utc.clone(),
            completed_at_utc: ms.completed_at_utc.clone(),
            usage: ms.usage.clone(),
            parts: ms.parts.clone(),
            turn_id: ms.turn_id,
            turn_sequence_no: ms.turn_sequence_no,
            ordinal_in_turn: ms.ordinal_in_turn,
        })
        .collect();
    normalized.sort_by_key(|m| (m.sequence_no, m.id));

    Ok((normalized, turns.len()))
}

fn insert_message_part(
    conn: &Connection,
    message_id: i64,
    ordinal: i64,
    part: &ExtractedMessagePart,
    source_line_no: i64,
    source_path: &Path,
) -> Result<i64> {
    conn.prepare_cached(
        "
        INSERT INTO message_part (
            message_id,
            ordinal,
            part_kind,
            mime_type,
            text_value,
            tool_name,
            tool_call_id,
            metadata_json,
            is_error
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ",
    )
    .and_then(|mut stmt| {
        stmt.execute(params![
            message_id,
            ordinal,
            part.part_kind,
            part.mime_type,
            part.text_value,
            part.tool_name,
            part.tool_call_id,
            part.metadata_json,
            part.is_error,
        ])
    })
    .with_context(|| {
        format!(
            "unable to insert normalized message part on line {} from {}",
            source_line_no,
            source_path.display()
        )
    })?;

    Ok(conn.last_insert_rowid())
}

fn extract_message(record: &Value, source_line_no: i64) -> Option<ExtractedMessage> {
    match record.get("type").and_then(Value::as_str) {
        Some("assistant") => extract_top_level_assistant(record, source_line_no),
        Some("user") => extract_top_level_user(record, source_line_no),
        Some("progress")
            if record.pointer("/data/type").and_then(Value::as_str) == Some("agent_progress") =>
        {
            extract_relay_message(record, source_line_no)
        }
        _ => None,
    }
}

fn extract_top_level_assistant(record: &Value, source_line_no: i64) -> Option<ExtractedMessage> {
    let wrapper = record.get("message")?;
    let external_id = message_external_id(wrapper, record, source_line_no)?;
    let parts = extract_message_parts(wrapper.get("content"), source_line_no).ok()?;

    Some(ExtractedMessage {
        external_id,
        source_line_no,
        role: message_role(wrapper)?,
        message_kind: "assistant_message",
        recorded_at_utc: extract_record_timestamp(record).map(ToOwned::to_owned),
        model_name: optional_string(wrapper.get("model")),
        stop_reason: optional_string(wrapper.get("stop_reason")),
        usage_source: if wrapper.get("usage").is_some() {
            Some("message_usage")
        } else {
            None
        },
        usage: Usage::from_json(wrapper.get("usage")),
        parts,
    })
}

fn extract_top_level_user(record: &Value, source_line_no: i64) -> Option<ExtractedMessage> {
    let wrapper = record.get("message")?;
    let external_id = message_external_id(wrapper, record, source_line_no)?;
    let role = message_role(wrapper)?;
    let is_agent_run_summary = record.pointer("/toolUseResult/usage").is_some();
    let message_kind = if is_agent_run_summary {
        "agent_run_summary"
    } else if content_is_tool_result_only(wrapper.get("content")) {
        "user_tool_result"
    } else {
        "user_prompt"
    };
    let parts = extract_message_parts(wrapper.get("content"), source_line_no).ok()?;

    Some(ExtractedMessage {
        external_id,
        source_line_no,
        role,
        message_kind,
        recorded_at_utc: extract_record_timestamp(record).map(ToOwned::to_owned),
        model_name: None,
        stop_reason: None,
        usage_source: if is_agent_run_summary {
            Some("tool_use_result_usage")
        } else {
            None
        },
        usage: if is_agent_run_summary {
            Usage::from_json(record.pointer("/toolUseResult/usage"))
        } else {
            Usage::default()
        },
        parts,
    })
}

fn extract_relay_message(record: &Value, source_line_no: i64) -> Option<ExtractedMessage> {
    let relay_wrapper = record.pointer("/data/message")?;
    let message_wrapper = relay_wrapper.get("message")?;
    let role = message_role(message_wrapper)?;
    let external_id = message_external_id(message_wrapper, relay_wrapper, source_line_no)?;
    let message_kind = match role.as_str() {
        "assistant" => "relay_assistant_message",
        "user" if content_is_tool_result_only(message_wrapper.get("content")) => {
            "relay_user_tool_result"
        }
        "user" => "relay_user_prompt",
        _ => return None,
    };

    let usage_source = if role == "assistant" && message_wrapper.get("usage").is_some() {
        Some("relay_usage")
    } else {
        None
    };
    let usage = if role == "assistant" {
        Usage::from_json(message_wrapper.get("usage"))
    } else {
        Usage::default()
    };
    let parts = extract_message_parts(message_wrapper.get("content"), source_line_no).ok()?;

    Some(ExtractedMessage {
        external_id,
        source_line_no,
        role,
        message_kind,
        recorded_at_utc: relay_wrapper
            .get("timestamp")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| extract_record_timestamp(record).map(ToOwned::to_owned)),
        model_name: optional_string(message_wrapper.get("model")),
        stop_reason: optional_string(message_wrapper.get("stop_reason")),
        usage_source,
        usage,
        parts,
    })
}

fn extract_message_parts(
    content: Option<&Value>,
    source_line_no: i64,
) -> Result<Vec<ExtractedMessagePart>> {
    let Some(content) = content else {
        return Ok(Vec::new());
    };

    match content {
        Value::String(text) => Ok(vec![ExtractedMessagePart {
            part_kind: "text".to_string(),
            mime_type: None,
            text_value: Some(text.clone()),
            tool_name: None,
            tool_call_id: None,
            metadata_json: None,
            is_error: false,
            dedupe_key: format!("text:{text}"),
        }]),
        Value::Array(parts) => {
            let mut extracted_parts = Vec::with_capacity(parts.len());
            for part in parts {
                let part_type = part
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                let dedupe_key = serde_json::to_string(part).with_context(|| {
                    format!("unable to serialize message part on source line {source_line_no}")
                })?;

                let text_value = match part_type.as_str() {
                    "text" | "thinking" => part
                        .get("text")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    _ => None,
                };

                let tool_name = optional_string(part.get("name"));
                let tool_call_id = optional_string(part.get("id"))
                    .or_else(|| optional_string(part.get("tool_use_id")));
                let mime_type = None;
                let metadata_json = match part_type.as_str() {
                    "tool_use" => part.get("input").map(|input| {
                        serde_json::to_string(&NormalizedToolUsePartMetadata::from_input(input))
                    }),
                    _ => None,
                }
                .transpose()
                .with_context(|| {
                    format!(
                        "unable to serialize normalized tool input on source line {source_line_no}"
                    )
                })?;
                let is_error = part
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);

                extracted_parts.push(ExtractedMessagePart {
                    part_kind: part_type,
                    mime_type,
                    text_value,
                    tool_name,
                    tool_call_id,
                    metadata_json,
                    is_error,
                    dedupe_key,
                });
            }

            Ok(extracted_parts)
        }
        _ => Ok(Vec::new()),
    }
}

fn content_is_tool_result_only(content: Option<&Value>) -> bool {
    let Some(Value::Array(values)) = content else {
        return false;
    };

    !values.is_empty()
        && values
            .iter()
            .all(|value| value.get("type").and_then(Value::as_str) == Some("tool_result"))
}

fn message_external_id(
    message_wrapper: &Value,
    context: &Value,
    source_line_no: i64,
) -> Option<String> {
    optional_string(message_wrapper.get("id"))
        .or_else(|| optional_string(context.get("uuid")))
        .or_else(|| optional_string(context.get("messageId")))
        .or_else(|| optional_string(context.get("toolUseID")).map(|id| format!("tool-use:{id}")))
        .or_else(|| Some(format!("line:{source_line_no}")))
}

fn message_role(message_wrapper: &Value) -> Option<String> {
    optional_string(message_wrapper.get("role"))
}

fn extract_session_id(record: &Value) -> Option<&str> {
    record.get("sessionId").and_then(Value::as_str)
}

fn file_contains_only_sessionless_metadata(records: &[BufferedRecord]) -> bool {
    !records.is_empty()
        && records
            .iter()
            .all(|record| is_sessionless_metadata_record(&record.value))
}

fn is_sessionless_metadata_record(record: &Value) -> bool {
    matches!(
        record.get("type").and_then(Value::as_str),
        Some("file-history-snapshot")
    )
}

fn conversation_external_id(session_id: &str, source_file_id: i64) -> String {
    format!("source-file:{source_file_id}:session:{session_id}")
}

fn extract_record_timestamp(record: &Value) -> Option<&str> {
    record.get("timestamp").and_then(Value::as_str).or_else(|| {
        record
            .pointer("/snapshot/timestamp")
            .and_then(Value::as_str)
    })
}

fn optional_string(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(ToOwned::to_owned)
}

fn codex_rollout_event_kind(value: &Value) -> String {
    value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string()
}

fn update_bounds(start: &mut Option<String>, end: &mut Option<String>, candidate: &Option<String>) {
    if let Some(candidate) = candidate {
        if start.as_ref().is_none_or(|current| candidate < current) {
            *start = Some(candidate.clone());
        }
        if end.as_ref().is_none_or(|current| candidate > current) {
            *end = Some(candidate.clone());
        }
    }
}

fn update_end(end: &mut Option<String>, candidate: &Option<String>) {
    if let Some(candidate) = candidate
        && end.as_ref().is_none_or(|current| candidate > current)
    {
        *end = Some(candidate.clone());
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use rusqlite::{Connection, params};
    use tempfile::tempdir;

    use crate::db::Database;
    use crate::import::{NormalizedToolUsePartMetadata, SourceFileKind};

    use super::{NormalizeJsonlFileOutcome, NormalizeJsonlFileParams, normalize_jsonl_file};

    const MAIN_SESSION_FIXTURE: &str = concat!(
        "{\"type\":\"file-history-snapshot\",\"messageId\":\"snap-1\",\"snapshot\":{\"messageId\":\"snap-1\",\"trackedFileBackups\":{},\"timestamp\":\"2026-03-26T10:00:00Z\"},\"isSnapshotUpdate\":false}\n",
        "{\"type\":\"user\",\"uuid\":\"user-1\",\"timestamp\":\"2026-03-26T10:00:01Z\",\"sessionId\":\"session-1\",\"cwd\":\"/tmp/project\",\"message\":{\"role\":\"user\",\"content\":\"Investigate the failing test.\"}}\n",
        "{\"type\":\"assistant\",\"uuid\":\"assistant-1a\",\"timestamp\":\"2026-03-26T10:00:02Z\",\"sessionId\":\"session-1\",\"message\":{\"id\":\"msg-a\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"I will inspect the files first.\"}],\"usage\":{\"input_tokens\":3,\"cache_creation_input_tokens\":10,\"cache_read_input_tokens\":0,\"output_tokens\":2},\"model\":\"claude-opus\",\"stop_reason\":null}}\n",
        "{\"type\":\"assistant\",\"uuid\":\"assistant-1b\",\"timestamp\":\"2026-03-26T10:00:03Z\",\"sessionId\":\"session-1\",\"message\":{\"id\":\"msg-a\",\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu-read\",\"name\":\"Read\",\"input\":{\"file_path\":\"/tmp/project/src/lib.rs\"}}],\"usage\":{\"input_tokens\":3,\"cache_creation_input_tokens\":10,\"cache_read_input_tokens\":0,\"output_tokens\":7},\"model\":\"claude-opus\",\"stop_reason\":\"tool_use\"}}\n",
        "{\"type\":\"user\",\"uuid\":\"tool-result-1\",\"timestamp\":\"2026-03-26T10:00:04Z\",\"sessionId\":\"session-1\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-read\",\"content\":\"fn broken() {}\",\"is_error\":false}]},\"toolUseResult\":{\"stdout\":\"fn broken() {}\",\"stderr\":\"\",\"interrupted\":false}}\n",
        "{\"type\":\"progress\",\"uuid\":\"relay-1\",\"timestamp\":\"2026-03-26T10:00:05Z\",\"sessionId\":\"session-1\",\"data\":{\"type\":\"agent_progress\",\"agentId\":\"agent-2\",\"message\":{\"type\":\"assistant\",\"uuid\":\"relay-assistant-1\",\"timestamp\":\"2026-03-26T10:00:05Z\",\"message\":{\"id\":\"relay-msg-1\",\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu-bash\",\"name\":\"Bash\",\"input\":{\"command\":\"cargo test\"}}],\"usage\":{\"input_tokens\":5,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":8,\"output_tokens\":1},\"model\":\"claude-haiku\",\"stop_reason\":\"tool_use\"}}}}\n",
        "{\"type\":\"progress\",\"uuid\":\"relay-2\",\"timestamp\":\"2026-03-26T10:00:06Z\",\"sessionId\":\"session-1\",\"data\":{\"type\":\"agent_progress\",\"agentId\":\"agent-2\",\"message\":{\"type\":\"user\",\"uuid\":\"relay-user-1\",\"timestamp\":\"2026-03-26T10:00:06Z\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-bash\",\"content\":\"ok\",\"is_error\":false}]}}}}\n",
        "{\"type\":\"user\",\"uuid\":\"summary-1\",\"timestamp\":\"2026-03-26T10:00:07Z\",\"sessionId\":\"session-1\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-team\",\"content\":\"agent finished\",\"is_error\":false}]},\"toolUseResult\":{\"status\":\"completed\",\"agentId\":\"agent-2\",\"usage\":{\"input_tokens\":1,\"cache_creation_input_tokens\":2,\"cache_read_input_tokens\":3,\"output_tokens\":4}}}\n"
    );

    const SIDECHAIN_FIXTURE: &str = concat!(
        "{\"type\":\"user\",\"uuid\":\"side-user-1\",\"timestamp\":\"2026-03-26T11:00:00Z\",\"sessionId\":\"session-2\",\"isSidechain\":true,\"agentId\":\"agent-side\",\"cwd\":\"/tmp/project\",\"message\":{\"role\":\"user\",\"content\":\"Fix the bug in parser.rs\"}}\n",
        "{\"type\":\"assistant\",\"uuid\":\"side-assistant-1\",\"timestamp\":\"2026-03-26T11:00:01Z\",\"sessionId\":\"session-2\",\"isSidechain\":true,\"agentId\":\"agent-side\",\"message\":{\"id\":\"msg-side\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"Inspecting parser.rs\"}],\"usage\":{\"input_tokens\":4,\"cache_creation_input_tokens\":6,\"cache_read_input_tokens\":7,\"output_tokens\":1},\"model\":\"claude-haiku\",\"stop_reason\":null}}\n"
    );

    const SNAPSHOT_ONLY_FIXTURE: &str = "{\"type\":\"file-history-snapshot\",\"messageId\":\"snap-only-1\",\"snapshot\":{\"messageId\":\"snap-only-1\",\"trackedFileBackups\":{},\"timestamp\":\"2026-03-26T09:00:00Z\"},\"isSnapshotUpdate\":false}\n";

    const HISTORY_FIXTURE: &str = concat!(
        "{\"sessionId\":\"session-history-1\",\"timestamp\":\"2026-03-26T08:00:00Z\",\"project\":\"/tmp/project-a\",\"display\":\"Investigate the parser regression\",\"pastedContents\":[{\"type\":\"text\",\"text\":\"stack trace\"}]}\n",
        "{\"sessionId\":\"session-history-1\",\"timestamp\":\"2026-03-26T08:01:00Z\",\"project\":\"/tmp/project-a\",\"display\":\"/skill planner --fast\",\"pastedContents\":[]}\n",
        "{\"sessionId\":\"session-history-2\",\"timestamp\":\"2026-03-26T08:02:00Z\",\"project\":\"/tmp/project-b\"}\n"
    );

    type MessagePartRow = (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        i64,
    );
    type HistoryRow = (
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        Option<String>,
    );

    #[test]
    fn normalizes_main_session_and_deduplicates_assistant_usage() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let fixture_path = temp.path().join("session.jsonl");
        std::fs::write(&fixture_path, MAIN_SESSION_FIXTURE)?;

        let mut db = Database::open(&db_path)?;
        let ids = seed_import_context(db.connection_mut(), "session.jsonl")?;
        let result = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: ids.project_id,
                source_file_id: ids.source_file_id,
                import_chunk_id: ids.import_chunk_id,
                path: fixture_path,
                perf_logger: None,
            },
        )?;
        let NormalizeJsonlFileOutcome::Imported(result) = result else {
            panic!("main session fixture should import");
        };

        assert!(result.conversation_id.is_some());
        assert!(result.stream_id.is_some());
        assert_eq!(result.record_count, 8);
        assert_eq!(result.message_count, 6);
        assert_eq!(result.turn_count, 1);
        assert_eq!(result.history_event_count, 0);

        let conn = db.connection();

        let assistant_usage: (Option<i64>, Option<i64>, Option<i64>, Option<i64>, String) = conn.query_row(
            "
            SELECT input_tokens, cache_creation_input_tokens, cache_read_input_tokens, output_tokens, message_kind
            FROM message
            WHERE external_id = 'msg-a'
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )?;
        assert_eq!(assistant_usage.0, Some(3));
        assert_eq!(assistant_usage.1, Some(10));
        assert_eq!(assistant_usage.2, Some(0));
        assert_eq!(assistant_usage.3, Some(7));
        assert_eq!(assistant_usage.4, "assistant_message");

        let assistant_part_count: i64 = conn.query_row(
            "
            SELECT COUNT(*)
            FROM message_part
            WHERE message_id = (SELECT id FROM message WHERE external_id = 'msg-a')
            ",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(assistant_part_count, 2);

        let relay_kind: String = conn.query_row(
            "SELECT message_kind FROM message WHERE external_id = 'relay-msg-1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(relay_kind, "relay_assistant_message");

        let summary_usage: (Option<i64>, Option<i64>, Option<i64>, Option<i64>, String) = conn.query_row(
            "
            SELECT input_tokens, cache_creation_input_tokens, cache_read_input_tokens, output_tokens, usage_source
            FROM message
            WHERE external_id = 'summary-1'
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )?;
        assert_eq!(summary_usage.0, Some(1));
        assert_eq!(summary_usage.1, Some(2));
        assert_eq!(summary_usage.2, Some(3));
        assert_eq!(summary_usage.3, Some(4));
        assert_eq!(summary_usage.4, "tool_use_result_usage");

        let turn_totals: (Option<i64>, Option<i64>, Option<i64>, Option<i64>) = conn.query_row(
            "
            SELECT input_tokens, cache_creation_input_tokens, cache_read_input_tokens, output_tokens
            FROM turn
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(turn_totals.0, Some(9));
        assert_eq!(turn_totals.1, Some(12));
        assert_eq!(turn_totals.2, Some(11));
        assert_eq!(turn_totals.3, Some(12));

        Ok(())
    }

    #[test]
    fn preserves_missing_usage_as_null_and_marks_sidechain_streams() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let fixture_path = temp.path().join("side.jsonl");
        std::fs::write(&fixture_path, SIDECHAIN_FIXTURE)?;

        let mut db = Database::open(&db_path)?;
        let ids = seed_import_context(db.connection_mut(), "side.jsonl")?;
        let result = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: ids.project_id,
                source_file_id: ids.source_file_id,
                import_chunk_id: ids.import_chunk_id,
                path: fixture_path,
                perf_logger: None,
            },
        )?;
        let NormalizeJsonlFileOutcome::Imported(result) = result else {
            panic!("sidechain fixture should import");
        };

        assert!(result.conversation_id.is_some());
        assert!(result.stream_id.is_some());
        assert_eq!(result.record_count, 2);
        assert_eq!(result.message_count, 2);
        assert_eq!(result.turn_count, 1);
        assert_eq!(result.history_event_count, 0);

        let conn = db.connection();

        let prompt_usage: (Option<i64>, Option<i64>, Option<i64>, Option<i64>) = conn.query_row(
            "
            SELECT input_tokens, cache_creation_input_tokens, cache_read_input_tokens, output_tokens
            FROM message
            WHERE external_id = 'side-user-1'
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(prompt_usage, (None, None, None, None));

        let stream: (String, Option<String>) =
            conn.query_row("SELECT stream_kind, external_id FROM stream", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;
        assert_eq!(stream.0, "sidechain");
        assert_eq!(stream.1.as_deref(), Some("agent-side"));

        Ok(())
    }

    #[test]
    fn persists_only_consumed_message_part_fields_for_v1_import_schema() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let fixture_path = temp.path().join("session.jsonl");
        std::fs::write(&fixture_path, MAIN_SESSION_FIXTURE)?;

        let mut db = Database::open(&db_path)?;
        let ids = seed_import_context(db.connection_mut(), "session.jsonl")?;
        let result = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: ids.project_id,
                source_file_id: ids.source_file_id,
                import_chunk_id: ids.import_chunk_id,
                path: fixture_path,
                perf_logger: None,
            },
        )?;
        assert!(matches!(result, NormalizeJsonlFileOutcome::Imported(_)));

        let parts: Vec<MessagePartRow> = {
            let mut stmt = db.connection().prepare(
                "
                SELECT part_kind, mime_type, text_value, tool_name, metadata_json, is_error
                FROM message_part
                WHERE message_id = (SELECT id FROM message WHERE external_id = 'msg-a')
                ORDER BY ordinal
                ",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        assert_eq!(parts.len(), 2);
        assert_eq!(
            parts[0],
            (
                "text".to_string(),
                None,
                Some("I will inspect the files first.".to_string()),
                None,
                None,
                0,
            )
        );

        let expected_tool_use = serde_json::to_string(&NormalizedToolUsePartMetadata::from_input(
            &serde_json::json!({
                "file_path": "/tmp/project/src/lib.rs"
            }),
        ))?;
        assert_eq!(
            parts[1],
            (
                "tool_use".to_string(),
                None,
                None,
                Some("Read".to_string()),
                Some(expected_tool_use),
                0,
            )
        );

        let tool_result_part: (Option<String>, Option<String>, Option<String>) =
            db.connection().query_row(
                "
                SELECT mime_type, text_value, metadata_json
                FROM message_part
                WHERE part_kind = 'tool_result'
                LIMIT 1
                ",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
        assert_eq!(tool_result_part, (None, None, None));

        Ok(())
    }

    #[test]
    fn insert_message_errors_include_source_file_and_line() -> Result<()> {
        let temp = tempdir()?;
        let fixture_path = temp.path().join("session.jsonl");
        std::fs::write(&fixture_path, MAIN_SESSION_FIXTURE)?;

        let mut conn = Connection::open(temp.path().join("legacy.sqlite3"))?;
        conn.execute_batch(
            "
            CREATE TABLE source_file (
                id INTEGER PRIMARY KEY,
                project_id INTEGER NOT NULL,
                relative_path TEXT NOT NULL,
                source_provider TEXT NOT NULL,
                source_kind TEXT NOT NULL
            );

            CREATE TABLE conversation (
                id INTEGER PRIMARY KEY,
                project_id INTEGER NOT NULL,
                source_file_id INTEGER NOT NULL,
                shared_session_id TEXT,
                external_id TEXT,
                title TEXT,
                started_at_utc TEXT,
                ended_at_utc TEXT,
                imported_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE stream (
                id INTEGER PRIMARY KEY,
                conversation_id INTEGER NOT NULL,
                import_chunk_id INTEGER NOT NULL,
                external_id TEXT,
                stream_kind TEXT NOT NULL,
                sequence_no INTEGER NOT NULL,
                opened_at_utc TEXT,
                closed_at_utc TEXT
            );

            CREATE TABLE record (
                id INTEGER PRIMARY KEY,
                import_chunk_id INTEGER NOT NULL,
                source_file_id INTEGER NOT NULL,
                conversation_id INTEGER NOT NULL,
                stream_id INTEGER,
                source_line_no INTEGER NOT NULL,
                sequence_no INTEGER NOT NULL,
                record_kind TEXT NOT NULL,
                recorded_at_utc TEXT
            );

            CREATE TABLE message (
                id INTEGER PRIMARY KEY,
                conversation_id INTEGER NOT NULL,
                import_chunk_id INTEGER NOT NULL,
                external_id TEXT,
                role TEXT NOT NULL,
                message_kind TEXT NOT NULL,
                sequence_no INTEGER NOT NULL,
                created_at_utc TEXT,
                completed_at_utc TEXT,
                input_tokens INTEGER,
                cache_creation_input_tokens INTEGER,
                cache_read_input_tokens INTEGER,
                output_tokens INTEGER,
                model_name TEXT,
                stop_reason TEXT,
                usage_source TEXT
            );

            CREATE TABLE import_warning (
                id INTEGER PRIMARY KEY,
                import_chunk_id INTEGER NOT NULL,
                source_file_id INTEGER,
                conversation_id INTEGER,
                code TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'warning',
                message TEXT NOT NULL,
                created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE history_event (
                id INTEGER PRIMARY KEY,
                source_file_id INTEGER NOT NULL
            );

            CREATE TABLE skill_invocation (
                id INTEGER PRIMARY KEY,
                source_file_id INTEGER NOT NULL
            );

            CREATE TABLE codex_rollout_session (
                id INTEGER PRIMARY KEY,
                source_file_id INTEGER NOT NULL
            );
            ",
        )?;
        conn.execute(
            "
            INSERT INTO source_file (id, project_id, relative_path, source_provider, source_kind)
            VALUES (1, 1, 'session.jsonl', 'claude', 'transcript')
            ",
            [],
        )?;

        let error = normalize_jsonl_file(
            &mut conn,
            &NormalizeJsonlFileParams {
                project_id: 1,
                source_file_id: 1,
                import_chunk_id: 1,
                path: fixture_path.clone(),
                perf_logger: None,
            },
        )
        .expect_err("legacy message schema should fail during insert");

        let rendered = format!("{error:#}");
        assert!(rendered.contains(&fixture_path.display().to_string()));
        assert!(rendered.contains("line 2"));
        assert!(rendered.contains("unable to insert normalized message"));
        assert!(rendered.contains("stream_id"));

        Ok(())
    }

    #[test]
    fn parse_errors_return_warning_with_truncated_source_line_preview() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let fixture_path = temp.path().join("bad.jsonl");
        let malformed_line = format!(
            "{{\"type\":\"user\",\"uuid\":\"broken\",\"message\":{{\"role\":\"user\",\"content\":\"{}\"}} invalid tail",
            "x".repeat(200)
        );
        std::fs::write(
            &fixture_path,
            format!(
                "{{\"type\":\"user\",\"uuid\":\"ok\",\"timestamp\":\"2026-03-26T10:00:00Z\",\"sessionId\":\"session-1\",\"message\":{{\"role\":\"user\",\"content\":\"ok\"}}}}\n{malformed_line}\n"
            ),
        )?;

        let mut db = Database::open(&db_path)?;
        let ids = seed_import_context(db.connection_mut(), "bad.jsonl")?;
        let outcome = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: ids.project_id,
                source_file_id: ids.source_file_id,
                import_chunk_id: ids.import_chunk_id,
                path: fixture_path.clone(),
                perf_logger: None,
            },
        )?;

        let NormalizeJsonlFileOutcome::Warning(warning) = outcome else {
            panic!("malformed json fixture should return a warning");
        };
        assert_eq!(warning.code, "invalid_json");
        assert!(
            warning
                .message
                .contains(&fixture_path.display().to_string())
        );
        assert!(warning.message.contains("line 2"));
        assert!(
            warning
                .message
                .contains("preview: {\\\"type\\\":\\\"user\\\"")
        );
        assert!(warning.message.contains("..."));
        assert!(!warning.message.contains(&"x".repeat(200)));

        let conversation_count: i64 =
            db.connection()
                .query_row("SELECT COUNT(*) FROM conversation", [], |row| row.get(0))?;
        assert_eq!(conversation_count, 0);

        Ok(())
    }

    #[test]
    fn skips_snapshot_only_files_without_session_id() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let fixture_path = temp.path().join("snapshot-only.jsonl");
        std::fs::write(&fixture_path, SNAPSHOT_ONLY_FIXTURE)?;

        let mut db = Database::open(&db_path)?;
        let ids = seed_import_context(db.connection_mut(), "snapshot-only.jsonl")?;
        let result = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: ids.project_id,
                source_file_id: ids.source_file_id,
                import_chunk_id: ids.import_chunk_id,
                path: fixture_path,
                perf_logger: None,
            },
        )?;

        assert!(matches!(result, NormalizeJsonlFileOutcome::Skipped));

        let conn = db.connection();
        let conversation_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM conversation", [], |row| row.get(0))?;
        let stream_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM stream", [], |row| row.get(0))?;
        let message_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM message", [], |row| row.get(0))?;
        let turn_count: i64 = conn.query_row("SELECT COUNT(*) FROM turn", [], |row| row.get(0))?;

        assert_eq!(conversation_count, 0);
        assert_eq!(stream_count, 0);
        assert_eq!(message_count, 0);
        assert_eq!(turn_count, 0);

        Ok(())
    }

    #[test]
    fn normalization_allows_duplicate_session_ids_across_source_files() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let first_fixture_path = temp.path().join("first.jsonl");
        let second_fixture_path = temp.path().join("second.jsonl");
        std::fs::write(&first_fixture_path, MAIN_SESSION_FIXTURE)?;
        std::fs::write(&second_fixture_path, MAIN_SESSION_FIXTURE)?;

        let mut db = Database::open(&db_path)?;
        let first_ids = seed_import_context(db.connection_mut(), "first.jsonl")?;
        let second_ids = seed_second_source_file(
            db.connection_mut(),
            first_ids.project_id,
            "second.jsonl",
            first_ids.import_chunk_id,
        )?;

        let first_result = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: first_ids.project_id,
                source_file_id: first_ids.source_file_id,
                import_chunk_id: first_ids.import_chunk_id,
                path: first_fixture_path,
                perf_logger: None,
            },
        )?;
        let NormalizeJsonlFileOutcome::Imported(first_result) = first_result else {
            panic!("first fixture should import");
        };
        let second_result = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: second_ids.project_id,
                source_file_id: second_ids.source_file_id,
                import_chunk_id: second_ids.import_chunk_id,
                path: second_fixture_path,
                perf_logger: None,
            },
        )?;
        let NormalizeJsonlFileOutcome::Imported(second_result) = second_result else {
            panic!("second fixture should import");
        };

        let conversations: Vec<(i64, i64, String, String)> = {
            let mut stmt = db.connection().prepare(
                "
                SELECT id, source_file_id, shared_session_id, external_id
                FROM conversation
                ORDER BY source_file_id
                ",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        assert_eq!(first_result.conversation_id, Some(conversations[0].0));
        assert_eq!(second_result.conversation_id, Some(conversations[1].0));
        assert_eq!(conversations.len(), 2);
        assert_eq!(conversations[0].2, "session-1");
        assert_eq!(conversations[1].2, "session-1");
        assert_eq!(
            conversations[0].3,
            format!("source-file:{}:session:session-1", first_ids.source_file_id)
        );
        assert_eq!(
            conversations[1].3,
            format!(
                "source-file:{}:session:session-1",
                second_ids.source_file_id
            )
        );

        Ok(())
    }

    #[test]
    fn normalizes_history_jsonl_into_history_events() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let fixture_path = temp.path().join("history.jsonl");
        std::fs::write(&fixture_path, HISTORY_FIXTURE)?;

        let mut db = Database::open(&db_path)?;
        let ids = seed_import_context_with_kind(
            db.connection_mut(),
            "history.jsonl",
            SourceFileKind::History,
        )?;
        let result = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: ids.project_id,
                source_file_id: ids.source_file_id,
                import_chunk_id: ids.import_chunk_id,
                path: fixture_path,
                perf_logger: None,
            },
        )?;
        let NormalizeJsonlFileOutcome::Imported(result) = result else {
            panic!("history fixture should import");
        };

        assert_eq!(result.conversation_id, None);
        assert_eq!(result.stream_id, None);
        assert_eq!(result.record_count, 3);
        assert_eq!(result.message_count, 0);
        assert_eq!(result.turn_count, 0);
        assert_eq!(result.history_event_count, 3);

        let rows: Vec<HistoryRow> = {
            let mut stmt = db.connection().prepare(
                "
                SELECT session_id, recorded_at_utc, display_text, input_kind, slash_command_name
                FROM history_event
                ORDER BY source_line_no
                ",
            )?;
            stmt.query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].3, "plain_prompt");
        assert_eq!(rows[1].3, "slash_command");
        assert_eq!(rows[1].4.as_deref(), Some("skill"));
        assert_eq!(rows[2].3, "other");

        let pasted_contents_json: Option<String> = db.connection().query_row(
            "SELECT pasted_contents_json FROM history_event WHERE source_line_no = 1",
            [],
            |row| row.get(0),
        )?;
        assert!(pasted_contents_json.is_some());

        let invocations: Vec<(String, String, Option<String>)> = {
            let mut stmt = db.connection().prepare(
                "
                SELECT session_id, skill_name, raw_project
                FROM skill_invocation
                ORDER BY recorded_at_utc
                ",
            )?;
            stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        assert_eq!(invocations.len(), 1);
        assert_eq!(
            invocations[0],
            (
                "session-history-1".to_string(),
                "planner".to_string(),
                Some("/tmp/project-a".to_string()),
            )
        );

        Ok(())
    }

    struct SeededIds {
        project_id: i64,
        source_file_id: i64,
        import_chunk_id: i64,
    }

    fn seed_import_context(conn: &mut Connection, relative_path: &str) -> Result<SeededIds> {
        seed_import_context_with_kind(conn, relative_path, SourceFileKind::Transcript)
    }

    fn seed_import_context_with_kind(
        conn: &mut Connection,
        relative_path: &str,
        source_kind: SourceFileKind,
    ) -> Result<SeededIds> {
        let project_id = conn.query_row(
            "
            INSERT INTO project (identity_kind, canonical_key, display_name, root_path)
            VALUES ('path', 'project-key', 'project', '/tmp/project')
            RETURNING id
            ",
            [],
            |row| row.get(0),
        )?;

        let source_file_id = conn.query_row(
            "
            INSERT INTO source_file (project_id, relative_path, source_provider, source_kind, size_bytes)
            VALUES (?1, ?2, 'claude', ?3, 0)
            RETURNING id
            ",
            params![project_id, relative_path, source_kind.as_str()],
            |row| row.get(0),
        )?;

        let import_chunk_id = conn.query_row(
            "
            INSERT INTO import_chunk (project_id, chunk_day_local, state)
            VALUES (?1, '2026-03-26', 'running')
            RETURNING id
            ",
            [project_id],
            |row| row.get(0),
        )?;

        Ok(SeededIds {
            project_id,
            source_file_id,
            import_chunk_id,
        })
    }

    fn seed_second_source_file(
        conn: &mut Connection,
        project_id: i64,
        relative_path: &str,
        import_chunk_id: i64,
    ) -> Result<SeededIds> {
        let source_file_id = conn.query_row(
            "
            INSERT INTO source_file (project_id, relative_path, source_provider, source_kind, size_bytes)
            VALUES (?1, ?2, 'claude', 'transcript', 0)
            RETURNING id
            ",
            params![project_id, relative_path],
            |row| row.get(0),
        )?;

        Ok(SeededIds {
            project_id,
            source_file_id,
            import_chunk_id,
        })
    }
}
