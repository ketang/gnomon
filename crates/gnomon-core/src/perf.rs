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
const DEFAULT_PERF_LOG_FILENAME: &str = "perf.jsonl";

#[derive(Clone)]
pub struct PerfLogger {
    inner: Arc<PerfLoggerInner>,
}

struct PerfLoggerInner {
    path: PathBuf,
    file: Mutex<File>,
}

impl PerfLogger {
    pub fn from_env(state_dir: &Path) -> Result<Option<Self>> {
        let raw = env::var(PERF_LOG_ENV_VAR).ok();
        let Some(path) = resolve_perf_log_path(raw.as_deref(), state_dir) else {
            return Ok(None);
        };

        Self::open(path).map(Some)
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
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

        Ok(Self {
            inner: Arc::new(PerfLoggerInner {
                path,
                file: Mutex::new(file),
            }),
        })
    }

    fn write_event(&self, payload: &Map<String, Value>) {
        let Ok(serialized) = serde_json::to_string(payload) else {
            warn!("unable to serialize perf event");
            return;
        };

        let Ok(mut file) = self.inner.file.lock() else {
            warn!("unable to acquire perf log lock");
            return;
        };

        if let Err(error) = writeln!(file, "{serialized}") {
            warn!(
                "unable to write perf event to {}: {error:#}",
                self.inner.path.display()
            );
            return;
        }

        if let Err(error) = file.flush() {
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
    fields: Map<String, Value>,
    finished: bool,
}

impl PerfScope {
    pub fn new(logger: Option<PerfLogger>, operation: impl Into<String>) -> Self {
        Self {
            logger,
            started_at: Instant::now(),
            operation: operation.into(),
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

        let mut payload = Map::new();
        payload.insert(
            "timestamp_utc".to_string(),
            Value::String(Timestamp::now().to_string()),
        );
        payload.insert(
            "operation".to_string(),
            Value::String(self.operation.clone()),
        );
        payload.insert("status".to_string(), Value::String(status.to_string()));
        payload.insert(
            "duration_ms".to_string(),
            json!(self.started_at.elapsed().as_secs_f64() * 1000.0),
        );
        if let Some(error) = error {
            payload.insert("error".to_string(), Value::String(error));
        }

        for (key, value) in &self.fields {
            payload.insert(key.clone(), value.clone());
        }

        logger.write_event(&payload);
    }
}

fn resolve_perf_log_path(raw: Option<&str>, state_dir: &Path) -> Option<PathBuf> {
    let raw = raw?.trim();
    if raw.is_empty() || matches!(raw, "0" | "false" | "off") {
        return None;
    }

    if matches!(raw, "1" | "true" | "on" | "default") {
        return Some(state_dir.join(DEFAULT_PERF_LOG_FILENAME));
    }

    Some(PathBuf::from(raw))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use anyhow::{Context, Result};
    use serde_json::Value;
    use tempfile::tempdir;

    use super::{PerfLogger, PerfScope, resolve_perf_log_path};

    #[test]
    fn resolve_perf_log_path_uses_default_file_for_truthy_flag() {
        let state_dir = Path::new("/tmp/gnomon-state");
        assert_eq!(
            resolve_perf_log_path(Some("1"), state_dir),
            Some(state_dir.join("perf.jsonl"))
        );
        assert_eq!(
            resolve_perf_log_path(Some("true"), state_dir),
            Some(state_dir.join("perf.jsonl"))
        );
    }

    #[test]
    fn resolve_perf_log_path_respects_explicit_path_and_disabled_values() {
        let state_dir = Path::new("/tmp/gnomon-state");
        assert_eq!(
            resolve_perf_log_path(Some("/tmp/custom-perf.jsonl"), state_dir),
            Some(PathBuf::from("/tmp/custom-perf.jsonl"))
        );
        assert_eq!(resolve_perf_log_path(Some("0"), state_dir), None);
        assert_eq!(resolve_perf_log_path(Some("off"), state_dir), None);
    }

    #[test]
    fn perf_scope_writes_a_json_line() -> Result<()> {
        let temp = tempdir()?;
        let log_path = temp.path().join("perf.jsonl");
        let logger = PerfLogger::open(log_path.clone())?;

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
        assert!(payload["duration_ms"].is_number());
        assert!(payload["timestamp_utc"].is_string());
        Ok(())
    }
}
