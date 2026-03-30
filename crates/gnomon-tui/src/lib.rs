mod app;
mod benchmark;
mod gnomon_sunburst;
mod sunburst;

use anyhow::Result;
use gnomon_core::config::RuntimeConfig;
use gnomon_core::import::{StartupOpenReason, StartupProgressUpdate, StartupWorkerEvent};
use gnomon_core::perf::PerfLogger;
use gnomon_core::query::{BrowsePath, RootView, SnapshotBounds};
use std::sync::mpsc::Receiver;

pub use benchmark::{SunburstBenchmarkOptions, SunburstBenchmarkReport, run_sunburst_benchmark};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupBrowseState {
    pub root: RootView,
    pub path: BrowsePath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupLoadProgressUpdate {
    pub phase: String,
    pub current: Option<usize>,
    pub total: Option<usize>,
}

// These entry points intentionally keep startup state explicit so callers can
// thread snapshot context, import progress, and prelaunch query progress
// without hiding them behind process-global state.
#[allow(clippy::too_many_arguments)]
pub fn run(
    config: &RuntimeConfig,
    snapshot: SnapshotBounds,
    startup_open_reason: StartupOpenReason,
    startup_status_message: Option<String>,
    startup_progress_update: Option<StartupProgressUpdate>,
    startup_browse_state: Option<StartupBrowseState>,
    status_updates: Option<Receiver<StartupWorkerEvent>>,
    perf_logger: Option<PerfLogger>,
) -> Result<()> {
    run_with_progress(
        config,
        snapshot,
        startup_open_reason,
        startup_status_message,
        startup_progress_update,
        startup_browse_state,
        status_updates,
        |_| {},
        perf_logger,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn run_with_progress<F>(
    config: &RuntimeConfig,
    snapshot: SnapshotBounds,
    startup_open_reason: StartupOpenReason,
    startup_status_message: Option<String>,
    startup_progress_update: Option<StartupProgressUpdate>,
    startup_browse_state: Option<StartupBrowseState>,
    status_updates: Option<Receiver<StartupWorkerEvent>>,
    mut on_progress: F,
    perf_logger: Option<PerfLogger>,
) -> Result<()>
where
    F: FnMut(StartupLoadProgressUpdate),
{
    app::App::new(
        config.clone(),
        snapshot,
        startup_open_reason,
        startup_status_message,
        startup_progress_update,
        startup_browse_state,
        status_updates,
        Some(&mut on_progress),
        perf_logger,
    )?
    .run()
}

/// Render the TUI once with the given viewport dimensions and return the
/// frame content as a newline-delimited string. Exits immediately without
/// entering an interactive event loop or requiring a terminal.
#[allow(clippy::too_many_arguments)]
pub fn render_snapshot(
    config: &RuntimeConfig,
    snapshot: SnapshotBounds,
    startup_open_reason: StartupOpenReason,
    startup_status_message: Option<String>,
    startup_progress_update: Option<StartupProgressUpdate>,
    startup_browse_state: Option<StartupBrowseState>,
    width: u16,
    height: u16,
) -> Result<String> {
    let mut app = app::App::new(
        config.clone(),
        snapshot,
        startup_open_reason,
        startup_status_message,
        startup_progress_update,
        startup_browse_state,
        None,
        None,
        None,
    )?;
    app.render_snapshot(width, height)
}

#[allow(clippy::too_many_arguments)]
pub fn probe_startup(
    config: &RuntimeConfig,
    snapshot: SnapshotBounds,
    startup_open_reason: StartupOpenReason,
    startup_status_message: Option<String>,
    startup_progress_update: Option<StartupProgressUpdate>,
    startup_browse_state: Option<StartupBrowseState>,
    status_updates: Option<Receiver<StartupWorkerEvent>>,
    perf_logger: Option<PerfLogger>,
) -> Result<()> {
    let _app = app::App::new(
        config.clone(),
        snapshot,
        startup_open_reason,
        startup_status_message,
        startup_progress_update,
        startup_browse_state,
        status_updates,
        None,
        perf_logger,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Result;
    use gnomon_core::config::RuntimeConfig;
    use gnomon_core::import::StartupOpenReason;
    use gnomon_core::perf::PerfLogger;
    use gnomon_core::query::{BrowsePath, RootView, SnapshotBounds};
    use serde_json::Value;
    use tempfile::tempdir;

    use super::{StartupBrowseState, probe_startup, render_snapshot};

    #[test]
    fn probe_startup_emits_tui_perf_events() -> Result<()> {
        let temp = tempdir()?;
        let log_path = temp.path().join("perf.jsonl");
        let logger = PerfLogger::open_jsonl(log_path.clone())?;

        probe_startup(
            &make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            Some(StartupBrowseState {
                root: RootView::CategoryHierarchy,
                path: BrowsePath::Root,
            }),
            None,
            Some(logger),
        )?;

        let operations = fs::read_to_string(log_path)?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|payload| {
                payload["operation"]
                    .as_str()
                    .map(std::string::ToString::to_string)
            })
            .collect::<Vec<_>>();

        assert!(operations.iter().any(|op| op == "tui.reload_view"));
        assert!(
            operations
                .iter()
                .any(|op| op == "tui.refresh_snapshot_status")
        );
        assert!(operations.iter().any(|op| op == "query.browse"));
        Ok(())
    }

    #[test]
    fn render_snapshot_produces_expected_row_count() -> Result<()> {
        let temp = tempdir()?;
        let width = 80;
        let height = 24;

        let output = render_snapshot(
            &make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            Some(StartupBrowseState {
                root: RootView::ProjectHierarchy,
                path: BrowsePath::Root,
            }),
            width,
            height,
        )?;

        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), height as usize);
        assert!(
            !output.trim().is_empty(),
            "snapshot output should not be blank"
        );
        Ok(())
    }

    fn make_test_config(root: &std::path::Path) -> RuntimeConfig {
        RuntimeConfig {
            app_name: "gnomon",
            state_dir: root.to_path_buf(),
            db_path: root.join("usage.sqlite3"),
            source_root: root.join("source"),
        }
    }
}
