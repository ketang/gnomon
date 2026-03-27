mod source;

pub use source::{ScanReport, ScanWarning, scan_source_manifest};

pub const STARTUP_IMPORT_WINDOW_HOURS: i64 = 24;
pub const STARTUP_OPEN_DEADLINE_SECS: u64 = 10;
pub const IMPORT_CHUNK_UNIT: &str = "project x day";

mod chunk;
mod normalize;

pub use chunk::{
    ImportExecutionReport, StartupImport, StartupOpenReason, StartupWorkerEvent, import_all,
    start_startup_import,
};
pub use normalize::{NormalizeJsonlFileParams, NormalizeJsonlFileResult, normalize_jsonl_file};
