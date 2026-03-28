use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use gnomon_core::benchmark::{QueryBenchmarkOptions, QueryBenchmarkReport, run_query_benchmark};
use gnomon_core::config::{ConfigOverrides, RuntimeConfig};
use gnomon_core::db::{Database, ResetReport, reset_sqlite_database};
use gnomon_core::import::{import_all, scan_source_manifest, start_startup_import};
use gnomon_core::perf::PerfLogger;
use gnomon_core::query::{
    ActionKey, BrowseFilters, BrowsePath, BrowseReport, BrowseRequest, ClassificationState,
    MetricLens, QueryEngine, RootView, TimeWindowFilter,
};

#[derive(Debug, Parser)]
#[command(
    name = "gnomon",
    version,
    about = "Analyze Claude session history and explore token usage in the terminal."
)]
struct Cli {
    #[command(flatten)]
    global: GlobalArgs,

    #[command(flatten)]
    startup: StartupArgs,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Clone, Args, Default, PartialEq, Eq)]
struct GlobalArgs {
    #[arg(long, env = "GNOMON_DB", value_name = "PATH", global = true)]
    db: Option<PathBuf>,

    #[arg(long, env = "GNOMON_SOURCE_ROOT", value_name = "PATH", global = true)]
    source_root: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Database maintenance commands: reset, rebuild.")]
    Db(DbCommand),
    #[command(
        about = "Run repeatable read-only query benchmarks against the current SQLite cache."
    )]
    Benchmark(Box<BenchmarkArgs>),
    #[command(about = "Return non-interactive aggregate rollups from the current snapshot.")]
    Report(Box<ReportArgs>),
}

#[derive(Debug, Args)]
struct DbCommand {
    #[command(subcommand)]
    command: DbSubcommand,
}

#[derive(Debug, Subcommand)]
enum DbSubcommand {
    /// Remove the derived SQLite cache file and its WAL sidecars.
    Reset(ResetArgs),
    /// Recreate the derived SQLite cache from the source manifest and session history.
    Rebuild,
}

#[derive(Debug, Clone, Args, Default)]
struct ResetArgs {
    /// Skip the destructive-operation confirmation check.
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Clone, Args, PartialEq, Eq)]
struct ReportArgs {
    /// Root hierarchy to browse.
    #[arg(long, value_enum, default_value_t = RootViewArg::Project)]
    root: RootViewArg,

    /// Metric lens used for sorting and indicators.
    #[arg(long, value_enum, default_value_t = MetricLensArg::UncachedInput)]
    lens: MetricLensArg,

    /// Aggregate path to browse within the chosen root hierarchy.
    #[arg(long, value_enum, default_value_t = BrowsePathKindArg::Root)]
    path: BrowsePathKindArg,

    /// Project id used by path or filters.
    #[arg(long)]
    project_id: Option<i64>,

    /// Category used by path or filters.
    #[arg(long)]
    category: Option<String>,

    /// Optional parent directory path used for path drill-down.
    #[arg(long)]
    parent_path: Option<String>,

    /// Start of the inclusive UTC time window filter.
    #[arg(long)]
    start_at_utc: Option<String>,

    /// End of the inclusive UTC time window filter.
    #[arg(long)]
    end_at_utc: Option<String>,

    /// Restrict rollups to a specific model name.
    #[arg(long)]
    model: Option<String>,

    /// Restrict rollups to a specific action filter category.
    #[arg(long = "filter-category")]
    filter_category: Option<String>,

    /// Classification state for action drill-down paths.
    #[arg(long, value_enum)]
    classification_state: Option<ClassificationStateArg>,

    /// Normalized action string for action drill-down paths.
    #[arg(long)]
    normalized_action: Option<String>,

    /// Command family for action drill-down paths.
    #[arg(long)]
    command_family: Option<String>,

    /// Base command for action drill-down paths.
    #[arg(long)]
    base_command: Option<String>,
}

#[derive(Debug, Clone, Args, PartialEq, Eq)]
struct BenchmarkArgs {
    /// Number of timing samples to collect for each benchmark scenario.
    #[arg(long, default_value_t = QueryBenchmarkOptions::default().iterations)]
    iterations: usize,
}

#[derive(Debug, Clone, Args, Default, PartialEq, Eq)]
struct StartupArgs {
    /// Root hierarchy to open on startup when explicitly requested.
    #[arg(long = "startup-root", value_enum)]
    root: Option<RootViewArg>,

    /// Aggregate path to open on startup when explicitly requested.
    #[arg(long = "startup-path", value_enum)]
    path: Option<BrowsePathKindArg>,

    /// Project id used by startup path drill-down.
    #[arg(long = "startup-project-id")]
    project_id: Option<i64>,

    /// Category used by startup path drill-down.
    #[arg(long = "startup-category")]
    category: Option<String>,

    /// Optional parent directory path used for startup path drill-down.
    #[arg(long = "startup-parent-path")]
    parent_path: Option<String>,

    /// Classification state for startup action drill-down paths.
    #[arg(long = "startup-classification-state", value_enum)]
    classification_state: Option<ClassificationStateArg>,

    /// Normalized action string for startup action drill-down paths.
    #[arg(long = "startup-normalized-action")]
    normalized_action: Option<String>,

    /// Command family for startup action drill-down paths.
    #[arg(long = "startup-command-family")]
    command_family: Option<String>,

    /// Base command for startup action drill-down paths.
    #[arg(long = "startup-base-command")]
    base_command: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RootViewArg {
    Project,
    Category,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum MetricLensArg {
    UncachedInput,
    GrossInput,
    Output,
    Total,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BrowsePathKindArg {
    Root,
    Project,
    ProjectCategory,
    ProjectAction,
    Category,
    CategoryAction,
    CategoryActionProject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ClassificationStateArg {
    Classified,
    Mixed,
    Unclassified,
}

fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    run(cli)
}

fn run(cli: Cli) -> Result<()> {
    let Cli {
        global,
        startup,
        command,
    } = cli;
    let config = RuntimeConfig::load(ConfigOverrides {
        db_path: global.db,
        source_root: global.source_root,
    })?;

    match command {
        None => run_app(&config, startup),
        Some(Command::Db(command)) => run_db_command(&config, command.command),
        Some(Command::Benchmark(args)) => run_benchmark_command(&config, &args),
        Some(Command::Report(args)) => run_report_command(&config, &args),
    }
}

fn run_app(config: &RuntimeConfig, startup_args: StartupArgs) -> Result<()> {
    config.ensure_dirs()?;
    let perf_logger = PerfLogger::from_env(&config.state_dir)?;
    let mut database = Database::open(&config.db_path)?;
    let _scan_report = scan_source_manifest(&mut database, &config.source_root)?;
    let mut startup_import =
        start_startup_import(database.connection(), &config.db_path, &config.source_root)?;
    let snapshot = startup_import.snapshot.clone();
    let open_reason = startup_import.open_reason;
    let startup_status_message = startup_import.startup_status_message.clone();
    let startup_browse_state = startup_args.build_startup_browse_state()?;
    let status_updates = startup_import.take_status_updates();

    gnomon_tui::run(
        config,
        snapshot,
        open_reason,
        startup_status_message,
        startup_browse_state,
        status_updates,
        perf_logger,
    )
}

fn run_db_command(config: &RuntimeConfig, command: DbSubcommand) -> Result<()> {
    config.ensure_dirs()?;

    match command {
        DbSubcommand::Reset(args) => {
            if !args.force {
                bail!(
                    "database reset is destructive; rerun with --force to delete {}",
                    config.db_path.display()
                );
            }

            let report = reset_sqlite_database(&config.db_path)?;
            print_reset_report(&config.db_path, &report);
            Ok(())
        }
        DbSubcommand::Rebuild => rebuild_database(config),
    }
}

fn run_report_command(config: &RuntimeConfig, args: &ReportArgs) -> Result<()> {
    config.ensure_dirs()?;
    let perf_logger = PerfLogger::from_env(&config.state_dir)?;
    let report = build_browse_report(config, args, perf_logger)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn build_browse_report(
    config: &RuntimeConfig,
    args: &ReportArgs,
    perf_logger: Option<PerfLogger>,
) -> Result<BrowseReport> {
    let database = Database::open(&config.db_path)?;
    let engine = QueryEngine::with_perf(database.connection(), perf_logger);
    let snapshot = engine.latest_snapshot_bounds()?;
    let request = BrowseRequest {
        snapshot,
        root: args.root.into(),
        lens: args.lens.into(),
        filters: args.filters(),
        path: args.build_path()?,
    };
    engine.browse_report(request)
}

fn run_benchmark_command(config: &RuntimeConfig, args: &BenchmarkArgs) -> Result<()> {
    let report = build_query_benchmark_report(config, args)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn build_query_benchmark_report(
    config: &RuntimeConfig,
    args: &BenchmarkArgs,
) -> Result<QueryBenchmarkReport> {
    run_query_benchmark(
        &config.db_path,
        QueryBenchmarkOptions {
            iterations: args.iterations,
        },
    )
}

fn rebuild_database(config: &RuntimeConfig) -> Result<()> {
    let reset_report = reset_sqlite_database(&config.db_path)?;
    let mut database = Database::open(&config.db_path)?;
    let scan_report = scan_source_manifest(&mut database, &config.source_root)?;
    let import_report = import_all(database.connection(), &config.db_path, &config.source_root)?;
    let completed_chunks = count_completed_chunks(&config.db_path)?;

    print_reset_report(&config.db_path, &reset_report);
    println!(
        "Rebuilt {} from {} discovered source files across {} completed chunks ({} startup, {} deferred).",
        config.db_path.display(),
        scan_report.discovered_source_files,
        completed_chunks,
        import_report.startup_chunk_count,
        import_report.deferred_chunk_count
    );

    Ok(())
}

fn print_reset_report(db_path: &Path, report: &ResetReport) {
    if report.removed_paths.is_empty() {
        println!(
            "No existing SQLite cache artifacts were found for {}.",
            db_path.display()
        );
    } else {
        println!(
            "Removed {} SQLite cache artifact(s) for {}.",
            report.removed_paths.len(),
            db_path.display()
        );
        for path in &report.removed_paths {
            println!("  deleted {}", path.display());
        }
    }
}

fn count_completed_chunks(db_path: &Path) -> Result<i64> {
    let database = Database::open(db_path)?;
    let completed_chunks = database.connection().query_row(
        "SELECT COUNT(*) FROM import_chunk WHERE state = 'complete'",
        [],
        |row| row.get(0),
    )?;
    Ok(completed_chunks)
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .try_init();
}

impl ReportArgs {
    fn filters(&self) -> BrowseFilters {
        let time_window = if self.start_at_utc.is_some() || self.end_at_utc.is_some() {
            Some(TimeWindowFilter {
                start_at_utc: self.start_at_utc.clone(),
                end_at_utc: self.end_at_utc.clone(),
            })
        } else {
            None
        };

        BrowseFilters {
            time_window,
            model: self.model.clone(),
            project_id: self.project_id,
            action_category: self.filter_category.clone(),
            action: None,
        }
    }

    fn build_path(&self) -> Result<BrowsePath> {
        match self.path {
            BrowsePathKindArg::Root => Ok(BrowsePath::Root),
            BrowsePathKindArg::Project => Ok(BrowsePath::Project {
                project_id: self.required_project_id("project path")?,
            }),
            BrowsePathKindArg::ProjectCategory => Ok(BrowsePath::ProjectCategory {
                project_id: self.required_project_id("project-category path")?,
                category: self.required_category("project-category path")?,
            }),
            BrowsePathKindArg::ProjectAction => Ok(BrowsePath::ProjectAction {
                project_id: self.required_project_id("project-action path")?,
                category: self.required_category("project-action path")?,
                action: self.required_action("project-action path")?,
                parent_path: self.parent_path.clone(),
            }),
            BrowsePathKindArg::Category => Ok(BrowsePath::Category {
                category: self.required_category("category path")?,
            }),
            BrowsePathKindArg::CategoryAction => Ok(BrowsePath::CategoryAction {
                category: self.required_category("category-action path")?,
                action: self.required_action("category-action path")?,
            }),
            BrowsePathKindArg::CategoryActionProject => Ok(BrowsePath::CategoryActionProject {
                category: self.required_category("category-action-project path")?,
                action: self.required_action("category-action-project path")?,
                project_id: self.required_project_id("category-action-project path")?,
                parent_path: self.parent_path.clone(),
            }),
        }
    }

    fn required_project_id(&self, context: &str) -> Result<i64> {
        self.project_id
            .with_context(|| format!("{context} requires --project-id"))
    }

    fn required_category(&self, context: &str) -> Result<String> {
        self.category
            .clone()
            .with_context(|| format!("{context} requires --category"))
    }

    fn required_action(&self, context: &str) -> Result<ActionKey> {
        Ok(ActionKey {
            classification_state: self
                .classification_state
                .map(Into::into)
                .with_context(|| format!("{context} requires --classification-state"))?,
            normalized_action: self.normalized_action.clone(),
            command_family: self.command_family.clone(),
            base_command: self.base_command.clone(),
        })
    }
}

impl StartupArgs {
    fn has_explicit_selection(&self) -> bool {
        self.root.is_some()
            || self.path.is_some()
            || self.project_id.is_some()
            || self.category.is_some()
            || self.parent_path.is_some()
            || self.classification_state.is_some()
            || self.normalized_action.is_some()
            || self.command_family.is_some()
            || self.base_command.is_some()
    }

    fn build_startup_browse_state(&self) -> Result<Option<gnomon_tui::StartupBrowseState>> {
        if !self.has_explicit_selection() {
            return Ok(None);
        }

        let path_kind = self.path.unwrap_or(BrowsePathKindArg::Root);
        if self.path.is_none() && self.has_path_arguments() {
            bail!(
                "startup drill-down requires --startup-path when specifying deeper startup scope"
            );
        }

        let path = self.build_path()?;
        let root = match self.root {
            Some(root) => {
                let root = root.into();
                if path_kind != BrowsePathKindArg::Root
                    && root != inferred_root_for_startup_path(path_kind)
                {
                    bail!("--startup-root does not match the requested --startup-path hierarchy")
                }
                root
            }
            None => inferred_root_for_startup_path(path_kind),
        };

        Ok(Some(gnomon_tui::StartupBrowseState { root, path }))
    }

    fn has_path_arguments(&self) -> bool {
        self.project_id.is_some()
            || self.category.is_some()
            || self.parent_path.is_some()
            || self.classification_state.is_some()
            || self.normalized_action.is_some()
            || self.command_family.is_some()
            || self.base_command.is_some()
    }

    fn build_path(&self) -> Result<BrowsePath> {
        match self.path.unwrap_or(BrowsePathKindArg::Root) {
            BrowsePathKindArg::Root => Ok(BrowsePath::Root),
            BrowsePathKindArg::Project => Ok(BrowsePath::Project {
                project_id: self.required_project_id("startup project path")?,
            }),
            BrowsePathKindArg::ProjectCategory => Ok(BrowsePath::ProjectCategory {
                project_id: self.required_project_id("startup project-category path")?,
                category: self.required_category("startup project-category path")?,
            }),
            BrowsePathKindArg::ProjectAction => Ok(BrowsePath::ProjectAction {
                project_id: self.required_project_id("startup project-action path")?,
                category: self.required_category("startup project-action path")?,
                action: self.required_action("startup project-action path")?,
                parent_path: self.parent_path.clone(),
            }),
            BrowsePathKindArg::Category => Ok(BrowsePath::Category {
                category: self.required_category("startup category path")?,
            }),
            BrowsePathKindArg::CategoryAction => Ok(BrowsePath::CategoryAction {
                category: self.required_category("startup category-action path")?,
                action: self.required_action("startup category-action path")?,
            }),
            BrowsePathKindArg::CategoryActionProject => Ok(BrowsePath::CategoryActionProject {
                category: self.required_category("startup category-action-project path")?,
                action: self.required_action("startup category-action-project path")?,
                project_id: self.required_project_id("startup category-action-project path")?,
                parent_path: self.parent_path.clone(),
            }),
        }
    }

    fn required_project_id(&self, context: &str) -> Result<i64> {
        self.project_id
            .with_context(|| format!("{context} requires --startup-project-id"))
    }

    fn required_category(&self, context: &str) -> Result<String> {
        self.category
            .clone()
            .with_context(|| format!("{context} requires --startup-category"))
    }

    fn required_action(&self, context: &str) -> Result<ActionKey> {
        Ok(ActionKey {
            classification_state: self
                .classification_state
                .map(Into::into)
                .with_context(|| format!("{context} requires --startup-classification-state"))?,
            normalized_action: self.normalized_action.clone(),
            command_family: self.command_family.clone(),
            base_command: self.base_command.clone(),
        })
    }
}

fn inferred_root_for_startup_path(path: BrowsePathKindArg) -> RootView {
    match path {
        BrowsePathKindArg::Root => RootView::ProjectHierarchy,
        BrowsePathKindArg::Project
        | BrowsePathKindArg::ProjectCategory
        | BrowsePathKindArg::ProjectAction => RootView::ProjectHierarchy,
        BrowsePathKindArg::Category
        | BrowsePathKindArg::CategoryAction
        | BrowsePathKindArg::CategoryActionProject => RootView::CategoryHierarchy,
    }
}

impl From<RootViewArg> for RootView {
    fn from(value: RootViewArg) -> Self {
        match value {
            RootViewArg::Project => Self::ProjectHierarchy,
            RootViewArg::Category => Self::CategoryHierarchy,
        }
    }
}

impl From<MetricLensArg> for MetricLens {
    fn from(value: MetricLensArg) -> Self {
        match value {
            MetricLensArg::UncachedInput => Self::UncachedInput,
            MetricLensArg::GrossInput => Self::GrossInput,
            MetricLensArg::Output => Self::Output,
            MetricLensArg::Total => Self::Total,
        }
    }
}

impl From<ClassificationStateArg> for ClassificationState {
    fn from(value: ClassificationStateArg) -> Self {
        match value {
            ClassificationStateArg::Classified => Self::Classified,
            ClassificationStateArg::Mixed => Self::Mixed,
            ClassificationStateArg::Unclassified => Self::Unclassified,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command as ProcessCommand;

    use anyhow::{Context, Result, anyhow};
    use clap::{CommandFactory, Parser};
    use rusqlite::OptionalExtension;
    use serde_json::json;
    use tempfile::tempdir;

    use super::{
        BenchmarkArgs, BrowsePathKindArg, ClassificationStateArg, Cli, Command, DbSubcommand,
        GlobalArgs, MetricLensArg, ReportArgs, ResetArgs, RootViewArg, StartupArgs,
        build_browse_report, build_query_benchmark_report, count_completed_chunks, run_db_command,
    };
    use gnomon_core::config::{ConfigOverrides, RuntimeConfig};
    use gnomon_core::db::Database;
    use gnomon_core::query::BrowsePath;
    use gnomon_core::validation::{ScaleValidationSpec, run_scale_validation};

    #[test]
    fn help_lists_db_subcommands() {
        let mut help = Vec::new();
        Cli::command()
            .write_long_help(&mut help)
            .expect("help output");
        let help = String::from_utf8(help).expect("utf8 help");

        assert!(help.contains("db"));
        assert!(help.contains("benchmark"));
        assert!(help.contains("report"));
        assert!(help.contains("reset"));
        assert!(help.contains("rebuild"));

        let mut db_help = Vec::new();
        Cli::command()
            .find_subcommand_mut("db")
            .expect("db subcommand")
            .write_long_help(&mut db_help)
            .expect("db help output");
        let db_help = String::from_utf8(db_help).expect("utf8 db help");

        assert!(db_help.contains("reset"));
        assert!(db_help.contains("rebuild"));
    }

    #[test]
    fn parse_accepts_global_overrides_before_subcommand() {
        let cli = Cli::parse_from([
            "gnomon",
            "--db",
            "/tmp/custom.sqlite3",
            "--source-root",
            "/tmp/source",
            "db",
            "reset",
            "--force",
        ]);

        assert_eq!(
            cli.global,
            GlobalArgs {
                db: Some(PathBuf::from("/tmp/custom.sqlite3")),
                source_root: Some(PathBuf::from("/tmp/source")),
            }
        );
        match cli.command {
            Some(Command::Db(command)) => match command.command {
                DbSubcommand::Reset(args) => assert!(args.force),
                DbSubcommand::Rebuild => panic!("expected reset subcommand"),
            },
            Some(Command::Benchmark(_)) => panic!("expected db command"),
            Some(Command::Report(_)) => panic!("expected db command"),
            None => panic!("expected db command"),
        }
    }

    #[test]
    fn parse_accepts_global_overrides_after_subcommand() {
        let cli = Cli::parse_from([
            "gnomon",
            "db",
            "rebuild",
            "--db",
            "/tmp/custom.sqlite3",
            "--source-root",
            "/tmp/source",
        ]);

        assert_eq!(
            cli.global,
            GlobalArgs {
                db: Some(PathBuf::from("/tmp/custom.sqlite3")),
                source_root: Some(PathBuf::from("/tmp/source")),
            }
        );
        match cli.command {
            Some(Command::Db(command)) => match command.command {
                DbSubcommand::Rebuild => {}
                DbSubcommand::Reset(_) => panic!("expected rebuild subcommand"),
            },
            Some(Command::Benchmark(_)) => panic!("expected db command"),
            Some(Command::Report(_)) => panic!("expected db command"),
            None => panic!("expected db command"),
        }
    }

    #[test]
    fn parse_accepts_report_arguments() {
        let cli = Cli::parse_from([
            "gnomon",
            "report",
            "--root",
            "category",
            "--path",
            "category-action",
            "--category",
            "editing",
            "--classification-state",
            "classified",
            "--normalized-action",
            "read file",
            "--db",
            "/tmp/custom.sqlite3",
        ]);

        assert_eq!(
            cli.global,
            GlobalArgs {
                db: Some(PathBuf::from("/tmp/custom.sqlite3")),
                source_root: None,
            }
        );
        match cli.command {
            Some(Command::Report(args)) => {
                assert_eq!(args.root, RootViewArg::Category);
                assert_eq!(args.path, BrowsePathKindArg::CategoryAction);
                assert_eq!(args.category.as_deref(), Some("editing"));
                assert_eq!(
                    args.classification_state,
                    Some(ClassificationStateArg::Classified)
                );
                assert_eq!(args.normalized_action.as_deref(), Some("read file"));
            }
            _ => panic!("expected report command"),
        }
    }

    #[test]
    fn parse_accepts_benchmark_arguments() {
        let cli = Cli::parse_from([
            "gnomon",
            "--db",
            "/tmp/custom.sqlite3",
            "benchmark",
            "--iterations",
            "7",
        ]);

        assert_eq!(
            cli.global,
            GlobalArgs {
                db: Some(PathBuf::from("/tmp/custom.sqlite3")),
                source_root: None,
            }
        );
        match cli.command {
            Some(Command::Benchmark(args)) => {
                assert_eq!(*args, BenchmarkArgs { iterations: 7 });
            }
            _ => panic!("expected benchmark command"),
        }
    }

    #[test]
    fn parse_accepts_startup_drill_down_arguments() {
        let cli = Cli::parse_from([
            "gnomon",
            "--startup-root",
            "project",
            "--startup-path",
            "project-category",
            "--startup-project-id",
            "7",
            "--startup-category",
            "editing",
        ]);

        assert_eq!(
            cli.startup,
            StartupArgs {
                root: Some(RootViewArg::Project),
                path: Some(BrowsePathKindArg::ProjectCategory),
                project_id: Some(7),
                category: Some("editing".to_string()),
                parent_path: None,
                classification_state: None,
                normalized_action: None,
                command_family: None,
                base_command: None,
            }
        );
        assert!(cli.command.is_none());
    }

    #[test]
    fn reset_requires_force() -> Result<()> {
        let temp = tempdir()?;
        let config = RuntimeConfig::load(ConfigOverrides {
            db_path: Some(temp.path().join("usage.sqlite3")),
            source_root: Some(temp.path().join("source")),
        })?;

        let err = run_db_command(&config, DbSubcommand::Reset(ResetArgs::default()))
            .expect_err("reset should require force");

        assert!(err.to_string().contains("database reset is destructive"));
        Ok(())
    }

    #[test]
    fn reset_deletes_database_file_when_forced() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let config = RuntimeConfig::load(ConfigOverrides {
            db_path: Some(db_path.clone()),
            source_root: Some(temp.path().join("source")),
        })?;
        config.ensure_dirs()?;
        fs::write(&db_path, "seed")?;

        run_db_command(&config, DbSubcommand::Reset(ResetArgs { force: true }))?;

        assert!(!db_path.exists());
        Ok(())
    }

    #[test]
    fn rebuild_recreates_database_from_source_history() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let source_root = temp.path().join("source");
        let project_root = temp.path().join("project");
        init_git_repo(&project_root)?;
        write_jsonl(&source_root.join("project/session.jsonl"), &project_root)?;

        let config = RuntimeConfig::load(ConfigOverrides {
            db_path: Some(db_path.clone()),
            source_root: Some(source_root),
        })?;

        run_db_command(&config, DbSubcommand::Rebuild)?;

        let database = Database::open(&db_path)?;
        let source_file_count: i64 =
            database
                .connection()
                .query_row("SELECT COUNT(*) FROM source_file", [], |row| row.get(0))?;
        let conversation_count: i64 =
            database
                .connection()
                .query_row("SELECT COUNT(*) FROM conversation", [], |row| row.get(0))?;

        assert_eq!(source_file_count, 1);
        assert_eq!(conversation_count, 1);
        assert_eq!(count_completed_chunks(&db_path)?, 1);

        Ok(())
    }

    #[test]
    fn rebuild_replaces_existing_database_contents() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let source_root = temp.path().join("source");
        let project_root = temp.path().join("project");
        init_git_repo(&project_root)?;
        write_jsonl(&source_root.join("project/session.jsonl"), &project_root)?;

        {
            let database = Database::open(&db_path)?;
            database.connection().execute(
                "INSERT INTO project (canonical_key, identity_kind, display_name, root_path) VALUES (?1, ?2, ?3, ?4)",
                ["stale", "path", "stale", "/tmp/stale"],
            )?;
        }

        let config = RuntimeConfig::load(ConfigOverrides {
            db_path: Some(db_path.clone()),
            source_root: Some(source_root),
        })?;

        run_db_command(&config, DbSubcommand::Rebuild)?;

        let database = Database::open(&db_path)?;
        let stale_project = database
            .connection()
            .query_row(
                "SELECT id FROM project WHERE canonical_key = 'stale'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;

        assert!(stale_project.is_none());
        Ok(())
    }

    #[test]
    fn report_returns_top_level_rollups_without_launching_tui() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let source_root = temp.path().join("source");
        let project_root = temp.path().join("project");
        init_git_repo(&project_root)?;
        write_jsonl(&source_root.join("project/session.jsonl"), &project_root)?;

        let config = RuntimeConfig::load(ConfigOverrides {
            db_path: Some(db_path),
            source_root: Some(source_root),
        })?;
        run_db_command(&config, DbSubcommand::Rebuild)?;

        let report = build_browse_report(
            &config,
            &ReportArgs {
                root: RootViewArg::Project,
                lens: MetricLensArg::UncachedInput,
                path: BrowsePathKindArg::Root,
                project_id: None,
                category: None,
                parent_path: None,
                start_at_utc: None,
                end_at_utc: None,
                model: None,
                filter_category: None,
                classification_state: None,
                normalized_action: None,
                command_family: None,
                base_command: None,
            },
            None,
        )?;

        assert_eq!(report.request.path, BrowsePath::Root);
        assert!(report.rows.is_empty());
        assert!(report.snapshot.max_publish_seq > 0);

        Ok(())
    }

    #[test]
    fn report_supports_project_drill_down_queries() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let source_root = temp.path().join("source");
        let project_root = temp.path().join("project");
        init_git_repo(&project_root)?;
        write_jsonl(&source_root.join("project/session.jsonl"), &project_root)?;

        let config = RuntimeConfig::load(ConfigOverrides {
            db_path: Some(db_path.clone()),
            source_root: Some(source_root),
        })?;
        run_db_command(&config, DbSubcommand::Rebuild)?;

        let project_id: i64 = Database::open(&db_path)?.connection().query_row(
            "SELECT id FROM project LIMIT 1",
            [],
            |row| row.get(0),
        )?;

        let report = build_browse_report(
            &config,
            &ReportArgs {
                root: RootViewArg::Project,
                lens: MetricLensArg::UncachedInput,
                path: BrowsePathKindArg::Project,
                project_id: Some(project_id),
                category: None,
                parent_path: None,
                start_at_utc: None,
                end_at_utc: None,
                model: None,
                filter_category: None,
                classification_state: None,
                normalized_action: None,
                command_family: None,
                base_command: None,
            },
            None,
        )?;

        assert_eq!(report.request.path, BrowsePath::Project { project_id });
        assert!(report.rows.is_empty());

        Ok(())
    }

    #[test]
    fn report_requires_classification_state_for_action_paths() {
        let args = ReportArgs {
            root: RootViewArg::Project,
            lens: MetricLensArg::UncachedInput,
            path: BrowsePathKindArg::ProjectAction,
            project_id: Some(1),
            category: Some("editing".to_string()),
            parent_path: None,
            start_at_utc: None,
            end_at_utc: None,
            model: None,
            filter_category: None,
            classification_state: None,
            normalized_action: Some("read file".to_string()),
            command_family: None,
            base_command: None,
        };

        let err = args
            .build_path()
            .expect_err("action paths should require a classification state");

        assert!(err.to_string().contains("--classification-state"));
    }

    #[test]
    fn benchmark_returns_query_report_without_launching_tui() -> Result<()> {
        let temp = tempdir()?;
        let validation = run_scale_validation(
            temp.path(),
            ScaleValidationSpec {
                project_count: 1,
                day_count: 2,
                sessions_per_day: 1,
            },
        )?;

        let config = RuntimeConfig::load(ConfigOverrides {
            db_path: Some(validation.db_path),
            source_root: Some(validation.source_root),
        })?;

        let report = build_query_benchmark_report(&config, &BenchmarkArgs { iterations: 2 })?;

        assert_eq!(report.iterations, 2);
        assert!(
            report
                .scenarios
                .iter()
                .any(|scenario| scenario.name == "project_root_refresh")
        );

        Ok(())
    }

    fn write_jsonl(path: &Path, cwd: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        fs::write(
            path,
            format!(
                "{}\n{}\n",
                json!({ "cwd": cwd }),
                json!({
                    "type": "user",
                    "uuid": "session-1-user",
                    "parentUuid": serde_json::Value::Null,
                    "timestamp": "2026-03-27T00:00:00Z",
                    "sessionId": "session-1",
                    "message": {
                        "id": "msg-session-1-user",
                        "role": "user",
                        "content": [{ "type": "text", "text": "hello" }]
                    }
                })
            ),
        )
        .with_context(|| format!("unable to write {}", path.display()))?;
        Ok(())
    }

    fn init_git_repo(repo_root: &Path) -> Result<()> {
        fs::create_dir_all(repo_root)?;
        run_git(repo_root, ["init"])?;
        run_git(repo_root, ["config", "user.email", "gnomon@example.com"])?;
        run_git(repo_root, ["config", "user.name", "Gnomon Tests"])?;
        fs::write(repo_root.join("README.md"), "seed\n")?;
        run_git(repo_root, ["add", "."])?;
        run_git(repo_root, ["commit", "-m", "seed"])?;
        Ok(())
    }

    fn run_git<const N: usize>(repo_root: &Path, args: [&str; N]) -> Result<()> {
        let output = ProcessCommand::new("git")
            .arg("-C")
            .arg(repo_root)
            .args(args)
            .output()
            .with_context(|| format!("unable to run git {:?}", args))?;
        if !output.status.success() {
            return Err(anyhow!(
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        Ok(())
    }
}
