use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use jiff::Timestamp;
use serde::Serialize;
use serde_json::{Map, Value, json};
use tracing::warn;

const PERF_LOG_ENV_VAR: &str = "GNOMON_PERF_LOG";
const PERF_LOG_FORMAT_ENV_VAR: &str = "GNOMON_PERF_LOG_FORMAT";
const PERF_LOG_GRANULARITY_ENV_VAR: &str = "GNOMON_PERF_LOG_GRANULARITY";
const DEFAULT_PERF_LOG_DIRNAME: &str = "logs";
const DEFAULT_PERF_LOG_FILENAME: &str = "perf.log";
const DEFAULT_PERF_LOG_MAX_BYTES: u64 = 10 * 1024 * 1024;
const DEFAULT_PERF_LOG_MAX_ARCHIVES: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PerfLogFormat {
    Human,
    Jsonl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PerfLogGranularity {
    Normal,
    Verbose,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PerfLoggerConfig {
    pub format: PerfLogFormat,
    pub granularity: PerfLogGranularity,
    pub max_bytes: u64,
    pub max_archives: usize,
}

impl Default for PerfLoggerConfig {
    fn default() -> Self {
        Self {
            format: PerfLogFormat::Human,
            granularity: PerfLogGranularity::Normal,
            max_bytes: DEFAULT_PERF_LOG_MAX_BYTES,
            max_archives: DEFAULT_PERF_LOG_MAX_ARCHIVES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PerfLogLevel {
    Normal,
    Verbose,
}

#[derive(Clone)]
pub struct PerfLogger {
    inner: Arc<PerfLoggerInner>,
}

struct PerfLoggerInner {
    path: PathBuf,
    config: PerfLoggerConfig,
    state: Mutex<PerfLogState>,
}

struct PerfLogState {
    file: File,
    current_size: u64,
}

impl PerfLogger {
    pub fn from_env(state_dir: &Path) -> Result<Option<Self>> {
        let raw = env::var(PERF_LOG_ENV_VAR).ok();
        let Some(path) = resolve_perf_log_path(raw.as_deref(), state_dir) else {
            return Ok(None);
        };
        let config = PerfLoggerConfig {
            format: resolve_perf_log_format(env::var(PERF_LOG_FORMAT_ENV_VAR).ok().as_deref()),
            granularity: resolve_perf_log_granularity(
                env::var(PERF_LOG_GRANULARITY_ENV_VAR).ok().as_deref(),
            ),
            ..PerfLoggerConfig::default()
        };

        Self::open_with_config(path, config).map(Some)
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_config(path, PerfLoggerConfig::default())
    }

    pub fn open_jsonl(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_config(
            path,
            PerfLoggerConfig {
                format: PerfLogFormat::Jsonl,
                granularity: PerfLogGranularity::Verbose,
                ..PerfLoggerConfig::default()
            },
        )
    }

    pub fn open_with_config(path: impl Into<PathBuf>, config: PerfLoggerConfig) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("unable to create parent directories for {}", path.display())
            })?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("unable to open perf log {}", path.display()))?;
        let current_size = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);

        Ok(Self {
            inner: Arc::new(PerfLoggerInner {
                path,
                config,
                state: Mutex::new(PerfLogState { file, current_size }),
            }),
        })
    }

    fn should_log(&self, level: PerfLogLevel) -> bool {
        matches!(
            (self.inner.config.granularity, level),
            (PerfLogGranularity::Verbose, _) | (PerfLogGranularity::Normal, PerfLogLevel::Normal)
        )
    }

    fn write_event(&self, event: &PerfEvent<'_>) {
        if !self.should_log(event.level) {
            return;
        }

        let serialized = match self.inner.config.format {
            PerfLogFormat::Human => format_human_event(event),
            PerfLogFormat::Jsonl => match format_jsonl_event(event) {
                Ok(serialized) => serialized,
                Err(_) => {
                    warn!("unable to serialize perf event");
                    return;
                }
            },
        };
        let bytes = serialized.as_bytes();

        let Ok(mut state) = self.inner.state.lock() else {
            warn!("unable to acquire perf log lock");
            return;
        };

        if should_rotate(
            state.current_size,
            bytes.len() as u64,
            self.inner.config.max_bytes,
        ) && rotate_log_files(&self.inner.path, &mut state, self.inner.config.max_archives)
            .is_err()
        {
            warn!("unable to rotate perf log {}", self.inner.path.display());
            return;
        }

        if let Err(error) = state.file.write_all(bytes) {
            warn!(
                "unable to write perf event to {}: {error:#}",
                self.inner.path.display()
            );
            return;
        }
        state.current_size = state.current_size.saturating_add(bytes.len() as u64);

        if let Err(error) = state.file.flush() {
            warn!(
                "unable to flush perf log {}: {error:#}",
                self.inner.path.display()
            );
        }
    }
}

pub struct PerfScope {
    logger: Option<PerfLogger>,
    started_at: Instant,
    operation: String,
    level: PerfLogLevel,
    fields: Map<String, Value>,
    finished: bool,
}

impl PerfScope {
    pub fn new(logger: Option<PerfLogger>, operation: impl Into<String>) -> Self {
        Self::with_level(logger, operation, PerfLogLevel::Normal)
    }

    pub fn new_verbose(logger: Option<PerfLogger>, operation: impl Into<String>) -> Self {
        Self::with_level(logger, operation, PerfLogLevel::Verbose)
    }

    fn with_level(
        logger: Option<PerfLogger>,
        operation: impl Into<String>,
        level: PerfLogLevel,
    ) -> Self {
        Self {
            logger,
            started_at: Instant::now(),
            operation: operation.into(),
            level,
            fields: Map::new(),
            finished: false,
        }
    }

    pub fn field<T>(&mut self, key: &str, value: T)
    where
        T: Serialize,
    {
        if self.logger.is_none() {
            return;
        }

        match serde_json::to_value(value) {
            Ok(value) => {
                self.fields.insert(key.to_string(), value);
            }
            Err(error) => {
                self.fields.insert(
                    key.to_string(),
                    json!(format!("serialization error: {error:#}")),
                );
            }
        }
    }

    pub fn finish_ok(mut self) {
        self.finish("ok", None);
    }

    pub fn finish_error(mut self, error: impl std::fmt::Display) {
        self.finish("error", Some(error.to_string()));
    }

    fn finish(&mut self, status: &str, error: Option<String>) {
        if self.finished {
            return;
        }
        self.finished = true;

        let Some(logger) = &self.logger else {
            return;
        };

        logger.write_event(&PerfEvent {
            timestamp_utc: Timestamp::now(),
            operation: &self.operation,
            status,
            duration_ms: self.started_at.elapsed().as_secs_f64() * 1000.0,
            error: error.as_deref(),
            fields: &self.fields,
            level: self.level,
        });
    }
}

fn resolve_perf_log_path(raw: Option<&str>, state_dir: &Path) -> Option<PathBuf> {
    let Some(raw) = raw.map(str::trim) else {
        return Some(default_perf_log_path(state_dir));
    };
    if raw.is_empty() || matches!(raw, "0" | "false" | "off") {
        return None;
    }

    if matches!(raw, "1" | "true" | "on" | "default") {
        return Some(default_perf_log_path(state_dir));
    }

    Some(PathBuf::from(raw))
}

fn resolve_perf_log_format(raw: Option<&str>) -> PerfLogFormat {
    match raw.map(str::trim).filter(|raw| !raw.is_empty()) {
        Some("json") | Some("jsonl") => PerfLogFormat::Jsonl,
        _ => PerfLogFormat::Human,
    }
}

fn resolve_perf_log_granularity(raw: Option<&str>) -> PerfLogGranularity {
    match raw.map(str::trim).filter(|raw| !raw.is_empty()) {
        Some("verbose") | Some("detail") | Some("trace") => PerfLogGranularity::Verbose,
        _ => PerfLogGranularity::Normal,
    }
}

fn default_perf_log_path(state_dir: &Path) -> PathBuf {
    state_dir
        .join(DEFAULT_PERF_LOG_DIRNAME)
        .join(DEFAULT_PERF_LOG_FILENAME)
}

fn should_rotate(current_size: u64, next_entry_bytes: u64, max_bytes: u64) -> bool {
    max_bytes > 0 && current_size > 0 && current_size.saturating_add(next_entry_bytes) > max_bytes
}

fn rotate_log_files(path: &Path, state: &mut PerfLogState, max_archives: usize) -> Result<()> {
    state.file.flush()?;
    for archive_index in (1..=max_archives).rev() {
        let archive_path = archived_log_path(path, archive_index);
        if archive_index == max_archives {
            if archive_path.exists() {
                fs::remove_file(&archive_path).with_context(|| {
                    format!(
                        "unable to remove old perf archive {}",
                        archive_path.display()
                    )
                })?;
            }
            continue;
        }
        let previous_path = archived_log_path(path, archive_index);
        if previous_path.exists() {
            let next_path = archived_log_path(path, archive_index + 1);
            fs::rename(&previous_path, &next_path).with_context(|| {
                format!(
                    "unable to rotate perf archive {} to {}",
                    previous_path.display(),
                    next_path.display()
                )
            })?;
        }
    }
    if path.exists() {
        fs::rename(path, archived_log_path(path, 1))
            .with_context(|| format!("unable to rotate perf log {} to archive", path.display()))?;
    }
    state.file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("unable to reopen perf log {}", path.display()))?;
    state.current_size = 0;
    Ok(())
}

fn archived_log_path(path: &Path, archive_index: usize) -> PathBuf {
    let mut archive = path.as_os_str().to_os_string();
    archive.push(format!(".{archive_index}"));
    PathBuf::from(archive)
}

struct PerfEvent<'a> {
    timestamp_utc: Timestamp,
    operation: &'a str,
    status: &'a str,
    duration_ms: f64,
    error: Option<&'a str>,
    fields: &'a Map<String, Value>,
    level: PerfLogLevel,
}

fn format_jsonl_event(event: &PerfEvent<'_>) -> serde_json::Result<String> {
    let mut payload = Map::new();
    payload.insert(
        "timestamp_utc".to_string(),
        Value::String(event.timestamp_utc.to_string()),
    );
    payload.insert(
        "operation".to_string(),
        Value::String(event.operation.to_string()),
    );
    payload.insert(
        "status".to_string(),
        Value::String(event.status.to_string()),
    );
    payload.insert("duration_ms".to_string(), json!(event.duration_ms));
    payload.insert(
        "granularity".to_string(),
        Value::String(
            match event.level {
                PerfLogLevel::Normal => "normal",
                PerfLogLevel::Verbose => "verbose",
            }
            .to_string(),
        ),
    );
    if let Some(error) = event.error {
        payload.insert("error".to_string(), Value::String(error.to_string()));
    }
    for (key, value) in event.fields {
        payload.insert(key.clone(), value.clone());
    }
    let mut serialized = serde_json::to_string(&payload)?;
    serialized.push('\n');
    Ok(serialized)
}

fn format_human_event(event: &PerfEvent<'_>) -> String {
    let mut fields = event
        .fields
        .iter()
        .map(|(key, value)| format!("{key}={}", format_human_value(value)))
        .collect::<Vec<_>>();
    fields.sort();
    if let Some(error) = event.error {
        fields.push(format!("error={}", format_human_scalar(error)));
    }
    let field_suffix = if fields.is_empty() {
        String::new()
    } else {
        format!(" {}", fields.join(" "))
    };
    format!(
        "{}  {:>5}  {:<6}  {}{}\n",
        event.timestamp_utc,
        format_duration_ms(event.duration_ms),
        event.status,
        event.operation,
        field_suffix,
    )
}

fn format_duration_ms(duration_ms: f64) -> String {
    if duration_ms >= 1000.0 {
        format!("{:.1}s", duration_ms / 1000.0)
    } else if duration_ms >= 10.0 {
        format!("{duration_ms:.0}ms")
    } else {
        format!("{duration_ms:.1}ms")
    }
}

fn format_human_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => format_human_scalar(value),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "\"serialization-error\"".to_string()),
    }
}

fn format_human_scalar(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | ':' | '+'))
    {
        value.to_string()
    } else {
        json!(value).to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use anyhow::{Context, Result};
    use serde_json::Value;
    use tempfile::tempdir;

    use super::{
        PerfLogFormat, PerfLogGranularity, PerfLogger, PerfLoggerConfig, PerfScope,
        default_perf_log_path, resolve_perf_log_path,
    };

    #[test]
    fn resolve_perf_log_path_defaults_to_state_log_directory() {
        let state_dir = Path::new("/tmp/gnomon-state");
        assert_eq!(
            resolve_perf_log_path(None, state_dir),
            Some(default_perf_log_path(state_dir))
        );
    }

    #[test]
    fn resolve_perf_log_path_uses_default_file_for_truthy_flag() {
        let state_dir = Path::new("/tmp/gnomon-state");
        assert_eq!(
            resolve_perf_log_path(Some("1"), state_dir),
            Some(default_perf_log_path(state_dir))
        );
        assert_eq!(
            resolve_perf_log_path(Some("true"), state_dir),
            Some(default_perf_log_path(state_dir))
        );
    }

    #[test]
    fn resolve_perf_log_path_respects_explicit_path_and_disabled_values() {
        let state_dir = Path::new("/tmp/gnomon-state");
        assert_eq!(
            resolve_perf_log_path(Some("/tmp/custom-perf.log"), state_dir),
            Some(PathBuf::from("/tmp/custom-perf.log"))
        );
        assert_eq!(resolve_perf_log_path(Some("0"), state_dir), None);
        assert_eq!(resolve_perf_log_path(Some("off"), state_dir), None);
    }

    #[test]
    fn perf_scope_writes_human_readable_line_by_default() -> Result<()> {
        let temp = tempdir()?;
        let log_path = temp.path().join("logs").join("perf.log");
        let logger = PerfLogger::open(log_path.clone())?;

        let mut scope = PerfScope::new(Some(logger), "query.browse");
        scope.field("row_count", 42usize);
        scope.field("root", "ProjectHierarchy");
        scope.finish_ok();

        let contents = fs::read_to_string(log_path)?;
        let line = contents.lines().next().context("missing perf log line")?;
        assert!(line.contains("query.browse"));
        assert!(line.contains("ok"));
        assert!(line.contains("row_count=42"));
        assert!(line.contains("root=ProjectHierarchy"));
        Ok(())
    }

    #[test]
    fn verbose_scope_is_suppressed_under_normal_granularity() -> Result<()> {
        let temp = tempdir()?;
        let log_path = temp.path().join("perf.log");
        let logger = PerfLogger::open(log_path.clone())?;

        PerfScope::new_verbose(Some(logger), "query.node_stats").finish_ok();

        let contents = fs::read_to_string(log_path)?;
        assert!(contents.is_empty());
        Ok(())
    }

    #[test]
    fn verbose_scope_is_logged_under_verbose_granularity() -> Result<()> {
        let temp = tempdir()?;
        let log_path = temp.path().join("perf.log");
        let logger = PerfLogger::open_with_config(
            log_path.clone(),
            PerfLoggerConfig {
                granularity: PerfLogGranularity::Verbose,
                ..PerfLoggerConfig::default()
            },
        )?;

        PerfScope::new_verbose(Some(logger), "query.node_stats").finish_ok();

        let contents = fs::read_to_string(log_path)?;
        assert!(contents.contains("query.node_stats"));
        Ok(())
    }

    #[test]
    fn perf_logger_rotates_when_file_exceeds_limit() -> Result<()> {
        let temp = tempdir()?;
        let log_path = temp.path().join("perf.log");
        let logger = PerfLogger::open_with_config(
            log_path.clone(),
            PerfLoggerConfig {
                max_bytes: 80,
                max_archives: 2,
                ..PerfLoggerConfig::default()
            },
        )?;

        for index in 0..4 {
            let mut scope = PerfScope::new(Some(logger.clone()), "query.filter_options");
            scope.field("entry", format!("line-{index}-with-extra-width"));
            scope.finish_ok();
        }

        assert!(log_path.exists());
        assert!(temp.path().join("perf.log.1").exists());
        Ok(())
    }

    #[test]
    fn jsonl_mode_preserves_structured_payloads() -> Result<()> {
        let temp = tempdir()?;
        let log_path = temp.path().join("perf.jsonl");
        let logger = PerfLogger::open_with_config(
            log_path.clone(),
            PerfLoggerConfig {
                format: PerfLogFormat::Jsonl,
                granularity: PerfLogGranularity::Verbose,
                ..PerfLoggerConfig::default()
            },
        )?;

        let mut scope = PerfScope::new(Some(logger), "query.browse");
        scope.field("row_count", 42usize);
        scope.field("root", "ProjectHierarchy");
        scope.finish_ok();

        let contents = fs::read_to_string(log_path)?;
        let line = contents.lines().next().context("missing perf log line")?;
        let payload: Value = serde_json::from_str(line)?;
        assert_eq!(payload["operation"], "query.browse");
        assert_eq!(payload["status"], "ok");
        assert_eq!(payload["row_count"], 42);
        assert_eq!(payload["root"], "ProjectHierarchy");
        assert_eq!(payload["granularity"], "normal");
        assert!(payload["duration_ms"].is_number());
        assert!(payload["timestamp_utc"].is_string());
        Ok(())
    }
}
