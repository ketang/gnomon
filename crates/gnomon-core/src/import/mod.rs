use serde::{Deserialize, Serialize};
use serde_json::Value;

mod source;

pub use source::{ScanReport, ScanWarning, scan_source_manifest};

pub const STARTUP_IMPORT_WINDOW_HOURS: i64 = 24;
pub const STARTUP_OPEN_DEADLINE_SECS: u64 = 10;
pub const IMPORT_CHUNK_UNIT: &str = "project x day";

/// Import schema version: controls what normalized fields are persisted.
///
/// Bumping this version triggers reimport of all previously imported source
/// files, since the stored data shape has changed.
///
/// ## v1 consumed fields (`message_part`)
///
/// | Column         | `tool_use`      | `tool_result`   | `text`/`thinking` |
/// |----------------|-----------------|-----------------|-------------------|
/// | `part_kind`    | stored          | stored          | stored            |
/// | `tool_name`    | stored          | —               | —                 |
/// | `tool_call_id` | stored          | stored          | —                 |
/// | `metadata_json`| `{"input":…}`   | `NULL`          | `NULL`            |
/// | `text_value`   | `NULL`          | `NULL`          | `NULL`            |
/// | `mime_type`    | `NULL`          | `NULL`          | `NULL`            |
/// | `is_error`     | stored          | stored          | stored            |
/// | `ordinal`      | stored          | stored          | stored            |
///
/// Fields consumed by [`classify::build_actions`]:
/// - `part_kind` — distinguishes `tool_use` from `tool_result`
/// - `tool_name` — identifies which tool was invoked
/// - `tool_call_id` — joins `tool_use` with its `tool_result`
/// - `metadata_json` — only the `input` key, only for `tool_use` parts
pub const IMPORT_SCHEMA_VERSION: i64 = 1;

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

mod chunk;
mod normalize;

pub use chunk::{
    ImportExecutionReport, StartupImport, StartupOpenReason, StartupProgressUpdate,
    StartupWorkerEvent, import_all, start_startup_import, start_startup_import_with_progress,
};
pub use normalize::{
    NormalizeImportWarning, NormalizeJsonlFileOutcome, NormalizeJsonlFileParams,
    NormalizeJsonlFileResult, normalize_jsonl_file,
};
