mod app;
mod gnomon_sunburst;
mod sunburst;

use anyhow::Result;
use gnomon_core::config::RuntimeConfig;
use gnomon_core::import::{StartupOpenReason, StartupWorkerEvent};
use gnomon_core::perf::PerfLogger;
use gnomon_core::query::{BrowsePath, RootView, SnapshotBounds};
use std::sync::mpsc::Receiver;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupBrowseState {
    pub root: RootView,
    pub path: BrowsePath,
}

pub fn run(
    config: &RuntimeConfig,
    snapshot: SnapshotBounds,
    startup_open_reason: StartupOpenReason,
    startup_status_message: Option<String>,
    startup_browse_state: Option<StartupBrowseState>,
    status_updates: Option<Receiver<StartupWorkerEvent>>,
    perf_logger: Option<PerfLogger>,
) -> Result<()> {
    app::App::new(
        config.clone(),
        snapshot,
        startup_open_reason,
        startup_status_message,
        startup_browse_state,
        status_updates,
        perf_logger,
    )?
    .run()
}

pub fn probe_startup(
    config: &RuntimeConfig,
    snapshot: SnapshotBounds,
    startup_open_reason: StartupOpenReason,
    startup_status_message: Option<String>,
    startup_browse_state: Option<StartupBrowseState>,
    status_updates: Option<Receiver<StartupWorkerEvent>>,
    perf_logger: Option<PerfLogger>,
) -> Result<()> {
    let _app = app::App::new(
        config.clone(),
        snapshot,
        startup_open_reason,
        startup_status_message,
        startup_browse_state,
        status_updates,
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

    use super::{StartupBrowseState, probe_startup};

    #[test]
    fn probe_startup_emits_tui_perf_events() -> Result<()> {
        let temp = tempdir()?;
        let log_path = temp.path().join("perf.jsonl");
        let logger = PerfLogger::open(log_path.clone())?;

        probe_startup(
            &make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
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

    fn make_test_config(root: &std::path::Path) -> RuntimeConfig {
        RuntimeConfig {
            app_name: "gnomon",
            state_dir: root.to_path_buf(),
            db_path: root.join("usage.sqlite3"),
            source_root: root.join("source"),
        }
    }
}
