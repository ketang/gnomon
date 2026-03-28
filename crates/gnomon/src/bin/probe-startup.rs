use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use gnomon_core::config::RuntimeConfig;
use gnomon_core::db::Database;
use gnomon_core::import::StartupOpenReason;
use gnomon_core::perf::PerfLogger;
use gnomon_core::query::{BrowsePath, QueryEngine, RootView};
use gnomon_tui::{StartupBrowseState, probe_startup};

#[derive(Debug, Parser)]
#[command(
    name = "probe-startup",
    about = "Construct the TUI startup state without entering the interactive event loop."
)]
struct Cli {
    #[arg(long, env = "GNOMON_DB", value_name = "PATH")]
    db: PathBuf,

    #[arg(long, value_name = "PATH")]
    state_dir: Option<PathBuf>,

    #[arg(long, env = "GNOMON_SOURCE_ROOT", value_name = "PATH")]
    source_root: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t = RootViewArg::Project)]
    root: RootViewArg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RootViewArg {
    Project,
    Category,
}

impl RootViewArg {
    fn root_view(self) -> RootView {
        match self {
            Self::Project => RootView::ProjectHierarchy,
            Self::Category => RootView::CategoryHierarchy,
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let state_dir = cli.state_dir.unwrap_or_else(|| default_state_dir(&cli.db));
    let source_root = cli
        .source_root
        .unwrap_or_else(|| state_dir.join("probe-source"));
    let config = RuntimeConfig {
        app_name: "gnomon",
        state_dir,
        db_path: cli.db,
        source_root,
    };
    config.ensure_dirs()?;

    let perf_logger = PerfLogger::from_env(&config.state_dir)?;
    let snapshot = {
        let database = Database::open(&config.db_path)
            .with_context(|| format!("opening SQLite cache at {}", config.db_path.display()))?;
        QueryEngine::with_perf(database.connection(), perf_logger.clone())
            .latest_snapshot_bounds()?
    };

    probe_startup(
        &config,
        snapshot.clone(),
        StartupOpenReason::Last24hReady,
        None,
        Some(StartupBrowseState {
            root: cli.root.root_view(),
            path: BrowsePath::Root,
        }),
        None,
        perf_logger,
    )?;

    println!("db: {}", config.db_path.display());
    println!("state_dir: {}", config.state_dir.display());
    println!(
        "startup root: {}",
        match cli.root {
            RootViewArg::Project => "project",
            RootViewArg::Category => "category",
        }
    );
    println!(
        "snapshot: publish_seq <= {} ({} published chunk(s))",
        snapshot.max_publish_seq, snapshot.published_chunk_count
    );
    Ok(())
}

fn default_state_dir(db_path: &Path) -> PathBuf {
    db_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("startup-probe-state")
}
