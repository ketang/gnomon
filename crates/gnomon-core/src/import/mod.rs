use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Shared types used by both normalize and classify phases
// ---------------------------------------------------------------------------

/// Token usage counters shared between normalize and classify phases.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Usage {
    pub input_tokens: Option<i64>,
    pub cache_creation_input_tokens: Option<i64>,
    pub cache_read_input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
}

impl Usage {
    pub fn from_json(value: Option<&Value>) -> Self {
        let Some(value) = value else {
            return Self::default();
        };

        Self {
            input_tokens: usage_field(value, "input_tokens"),
            cache_creation_input_tokens: usage_field(value, "cache_creation_input_tokens"),
            cache_read_input_tokens: usage_field(value, "cache_read_input_tokens"),
            output_tokens: usage_field(value, "output_tokens"),
        }
    }

    pub fn has_any(&self) -> bool {
        self.input_tokens.is_some()
            || self.cache_creation_input_tokens.is_some()
            || self.cache_read_input_tokens.is_some()
            || self.output_tokens.is_some()
    }

    pub fn add_field(total: &mut Option<i64>, candidate: Option<i64>) {
        if let Some(candidate) = candidate {
            *total = Some(total.unwrap_or(0) + candidate);
        }
    }
}

fn usage_field(value: &Value, field_name: &str) -> Option<i64> {
    value.get(field_name).and_then(Value::as_i64)
}

/// A message part retained in memory after normalization for downstream
/// consumption by `build_actions`. Only fields needed by classify are kept.
#[derive(Debug, Clone)]
pub struct NormalizedPart {
    pub id: i64,
    pub part_kind: String,
    pub tool_name: Option<String>,
    pub tool_call_id: Option<String>,
    pub metadata_json: Option<String>,
}

/// A fully-normalized message retained in memory after normalization for
/// downstream consumption by `build_turns` and `build_actions`.
#[derive(Debug, Clone)]
pub struct NormalizedMessage {
    pub id: i64,
    pub stream_id: i64,
    pub sequence_no: i64,
    pub message_kind: String,
    pub created_at_utc: Option<String>,
    pub completed_at_utc: Option<String>,
    pub usage: Usage,
    pub parts: Vec<NormalizedPart>,
    /// Populated by `build_turns`; `None` for messages outside any turn.
    pub turn_id: Option<i64>,
    /// Populated by `build_turns`.
    pub turn_sequence_no: Option<i64>,
    /// Populated by `build_turns`.
    pub ordinal_in_turn: Option<i64>,
}

mod source;

pub use source::{
    ScanReport, ScanWarning, scan_source_manifest, scan_source_manifest_with_perf_logger,
    scan_source_manifest_with_policy,
};

pub const STARTUP_IMPORT_WINDOW_HOURS: i64 = 24;
pub const STARTUP_OPEN_DEADLINE_SECS: u64 = 10;
pub const IMPORT_CHUNK_UNIT: &str = "project x day";

/// Import schema version: controls when derived import data must be rebuilt.
///
/// Bumping this version triggers reimport of all previously imported source
/// files, since the stored normalized or action-derived data shape has changed.
///
/// ## v1 consumed fields (`message_part`)
///
/// | Column         | `tool_use`      | `tool_result`   | `text`/`thinking` |
/// |----------------|-----------------|-----------------|-------------------|
/// | `part_kind`    | stored          | stored          | stored            |
/// | `tool_name`    | stored          | —               | —                 |
/// | `tool_call_id` | stored          | stored          | —                 |
/// | `metadata_json`| `{"input":…}`   | `NULL`          | `NULL`            |
/// | `text_value`   | `NULL`          | `NULL`          | stored            |
/// | `mime_type`    | `NULL`          | `NULL`          | `NULL`            |
/// | `is_error`     | stored          | stored          | stored            |
/// | `ordinal`      | stored          | stored          | stored            |
///
/// Fields consumed by [`classify::build_actions`]:
/// - `part_kind` — distinguishes `tool_use` from `tool_result`
/// - `tool_name` — identifies which tool was invoked
/// - `tool_call_id` — joins `tool_use` with its `tool_result`
/// - `metadata_json` — only the `input` key, only for `tool_use` parts
/// - `text_value` — transcript text used for skill confirmation heuristics
/// ## v5 changes
///
/// The `record` table is no longer populated during import. All JSONL lines
/// are still counted (`imported_record_count`) but individual records are not
/// persisted. This eliminates ~490K INSERTs per full corpus import.
pub const IMPORT_SCHEMA_VERSION: i64 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceFileKind {
    Transcript,
    ClaudeHistory,
}

impl SourceFileKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transcript => "transcript",
            Self::ClaudeHistory => "claude_history",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "transcript" => Some(Self::Transcript),
            "claude_history" => Some(Self::ClaudeHistory),
            _ => None,
        }
    }
}

/// Stable normalized payload contract for persisted `tool_use` message parts.
///
/// This is the only structured `message_part.metadata_json` shape consumed by
/// `v1`. Any future expansion should add an explicit schema-version bump.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedToolUsePartMetadata {
    pub input: Value,
}

impl NormalizedToolUsePartMetadata {
    pub fn from_input(input: &Value) -> Self {
        Self {
            input: input.clone(),
        }
    }

    pub fn parse(raw_json: &str) -> Option<Self> {
        serde_json::from_str(raw_json).ok()
    }
}

// ---------------------------------------------------------------------------
// Pre-parsed file types for parallel parse → serial write pipeline
// ---------------------------------------------------------------------------

/// Result of parsing a single JSONL file (CPU-only, no DB access).
pub(in crate::import) enum ParseResult {
    /// File parsed successfully; ready for serial DB writes.
    Parsed(ParsedFile),
    /// File contains only sessionless metadata records (no session ID).
    SessionlessMetadata,
    /// A parse error occurred; caller should rollback the savepoint.
    Warning(normalize::NormalizeImportWarning),
}

/// Pre-parsed JSONL file — all CPU work done, ready for serial DB writes.
pub(in crate::import) struct ParsedFile {
    pub(in crate::import) records: Vec<ParsedRecord>,
}

/// A single parsed JSONL line with pre-extracted metadata.
pub(in crate::import) struct ParsedRecord {
    pub(in crate::import) source_line_no: i64,
    pub(in crate::import) value: serde_json::Value,
    pub(in crate::import) recorded_at_utc: Option<String>,
    pub(in crate::import) extracted_message: Option<normalize::ExtractedMessage>,
}

mod chunk;
mod normalize;

pub use chunk::{
    ImportExecutionReport, StartupImport, StartupImportMode, StartupOpenReason,
    StartupProgressUpdate, StartupWorkerEvent, import_all, import_all_with_perf_logger,
    start_startup_import, start_startup_import_with_mode_and_progress,
    start_startup_import_with_perf_logger, start_startup_import_with_progress,
};
pub use normalize::{
    NormalizeImportWarning, NormalizeJsonlFileOutcome, NormalizeJsonlFileParams,
    NormalizeJsonlFileResult, normalize_jsonl_file, normalize_jsonl_file_in_tx,
};

// Re-export shared types at crate level for external consumers
// (NormalizedMessage, NormalizedPart, Usage are defined at the top of this file)
