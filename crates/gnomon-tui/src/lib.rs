mod app;

use anyhow::Result;
use gnomon_core::config::RuntimeConfig;

pub fn run(config: &RuntimeConfig) -> Result<()> {
    app::App::new(config.clone())?.run()
}
