mod app;

use anyhow::Result;
use gnomon_core::config::RuntimeConfig;
use gnomon_core::import::StartupOpenReason;
use gnomon_core::query::SnapshotBounds;

pub fn run(
    config: &RuntimeConfig,
    snapshot: SnapshotBounds,
    startup_open_reason: StartupOpenReason,
    startup_error: Option<String>,
) -> Result<()> {
    app::App::new(config.clone(), snapshot, startup_open_reason, startup_error)?.run()
}
