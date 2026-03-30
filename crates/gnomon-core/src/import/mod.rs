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

mod chunk;
mod normalize;

pub use chunk::{
    ImportExecutionReport, StartupImport, StartupOpenReason, StartupWorkerEvent, import_all,
    start_startup_import,
};
pub use normalize::{NormalizeJsonlFileParams, NormalizeJsonlFileResult, normalize_jsonl_file};
