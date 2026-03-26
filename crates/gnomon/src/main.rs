use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use gnomon_core::config::{ConfigOverrides, RuntimeConfig};
use gnomon_core::db::Database;

#[derive(Debug, Parser)]
#[command(
    name = "gnomon",
    version,
    about = "Analyze Claude session history and explore token usage in the terminal."
)]
struct Cli {
    #[arg(long, env = "GNOMON_DB", value_name = "PATH")]
    db: Option<PathBuf>,

    #[arg(long, env = "GNOMON_SOURCE_ROOT", value_name = "PATH")]
    source_root: Option<PathBuf>,
}

fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let config = RuntimeConfig::load(ConfigOverrides {
        db_path: cli.db,
        source_root: cli.source_root,
    })?;
    config.ensure_dirs()?;
    let _database = Database::open(&config.db_path)?;

    gnomon_tui::run(&config)
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .try_init();
}
