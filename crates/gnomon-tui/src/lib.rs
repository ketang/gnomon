mod app;

use anyhow::Result;
use gnomon_core::config::RuntimeConfig;
use gnomon_core::import::{StartupOpenReason, StartupWorkerEvent};
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
) -> Result<()> {
    app::App::new(
        config.clone(),
        snapshot,
        startup_open_reason,
        startup_status_message,
        startup_browse_state,
        status_updates,
    )?
    .run()
}
