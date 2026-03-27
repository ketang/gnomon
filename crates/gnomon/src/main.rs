use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use gnomon_core::config::{ConfigOverrides, RuntimeConfig};
use gnomon_core::db::Database;
use gnomon_core::import::{scan_source_manifest, start_startup_import};

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
    let mut database = Database::open(&config.db_path)?;
    let _scan_report = scan_source_manifest(&mut database, &config.source_root)?;
    let startup_import =
        start_startup_import(database.connection(), &config.db_path, &config.source_root)?;

    gnomon_tui::run(&config, startup_import.snapshot, startup_import.open_reason)
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .try_init();
}
