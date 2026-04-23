use std::fmt::Write as _;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use gnomon_core::benchmark::{QueryBenchmarkOptions, QueryBenchmarkReport, run_query_benchmark};
use gnomon_core::browse_cache::{
    DerivedCacheResetReport, default_browse_cache_path, reset_derived_cache_artifacts,
};
use gnomon_core::config::{ConfigOverrides, RuntimeConfig};
use gnomon_core::db::{Database, ResetReport};
use gnomon_core::import::{
    StartupImportMode, StartupProgressUpdate, StartupWorkerEvent, import_all,
    scan_source_manifest_with_policy, start_startup_import,
    start_startup_import_with_mode_and_progress,
};
use gnomon_core::opportunity::{OpportunityCategory, OpportunityConfidence, OpportunitySummary};
use gnomon_core::perf::PerfLogger;
use gnomon_core::query::{
    ActionKey, BrowseFilters, BrowsePath, BrowseReport, BrowseRequest, ClassificationState,
    MetricLens, OpportunitiesFilters, QueryEngine, RootView, SkillsPath, SkillsReport,
    SnapshotBounds, TimeWindowFilter,
};
use rusqlite::OptionalExtension;

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
    #[command(about = "Return skill-oriented rollups with session-associated token metrics.")]
    Skills(Box<SkillsArgs>),
    #[command(
        about = "Emit opportunity annotations with supporting evidence for heuristic calibration."
    )]
    Opportunities(Box<OpportunitiesArgs>),
    #[command(
        about = "Render the TUI once to stdout and exit. For snapshot and golden-file testing."
    )]
    Snapshot(Box<SnapshotArgs>),
}

#[derive(Debug, Args)]
struct DbCommand {
    #[command(subcommand)]
    command: DbSubcommand,
}

#[derive(Debug, Subcommand)]
enum DbSubcommand {
    /// Remove the derived usage database and persisted browse-cache artifacts.
    Reset(ResetArgs),
    /// Recreate the derived usage database after clearing persisted cache artifacts.
    Rebuild,
    /// Report current import chunk state, phase, and recent failures.
    Status,
    /// Verify database integrity: SQLite page structure and foreign-key consistency.
    Check,
}

#[derive(Debug, Clone, Args, Default)]
struct ResetArgs {
    /// Skip the destructive-operation confirmation check.
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportChunkState {
    Pending,
    Running,
    Complete,
    Failed,
}

impl ImportChunkState {
    fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "complete" => Some(Self::Complete),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportChunkPhase {
    Startup,
    Deferred,
}

impl ImportChunkPhase {
    fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "startup" => Some(Self::Startup),
            "deferred" => Some(Self::Deferred),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Deferred => "deferred",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ImportStateCounts {
    pending: usize,
    running: usize,
    complete: usize,
    failed: usize,
}

impl ImportStateCounts {
    fn increment(&mut self, state: ImportChunkState, amount: usize) {
        match state {
            ImportChunkState::Pending => self.pending += amount,
            ImportChunkState::Running => self.running += amount,
            ImportChunkState::Complete => self.complete += amount,
            ImportChunkState::Failed => self.failed += amount,
        }
    }

    fn active_count(&self) -> usize {
        self.pending + self.running
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportPhaseCounts {
    phase: ImportChunkPhase,
    counts: ImportStateCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportChunkStatusRow {
    id: i64,
    project_key: String,
    chunk_day_local: String,
    state: ImportChunkState,
    phase: Option<ImportChunkPhase>,
    publish_seq: Option<i64>,
    started_at_utc: String,
    completed_at_utc: Option<String>,
    last_error_message: Option<String>,
}

impl ImportChunkStatusRow {
    fn label(&self) -> String {
        format!("{}:{}", self.project_key, self.chunk_day_local)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportStatusReport {
    db_path: PathBuf,
    db_exists: bool,
    snapshot: SnapshotBounds,
    counts: ImportStateCounts,
    phase_counts: Vec<ImportPhaseCounts>,
    active_chunk: Option<ImportChunkStatusRow>,
    latest_completed_chunk: Option<ImportChunkStatusRow>,
    recent_failures: Vec<ImportChunkStatusRow>,
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

#[derive(Debug, Clone, Args, PartialEq, Eq)]
struct SkillsArgs {
    /// Aggregate path to browse within the skills lens.
    #[arg(long, value_enum, default_value_t = SkillsPathArg::Root)]
    path: SkillsPathArg,

    /// Skill name used by non-root paths.
    #[arg(long)]
    skill: Option<String>,

    /// Project id used by skill-project paths.
    #[arg(long)]
    project_id: Option<i64>,
}

#[derive(Debug, Clone, Args, PartialEq)]
struct OpportunitiesArgs {
    /// Restrict to a single project by id.
    #[arg(long)]
    project_id: Option<i64>,

    /// Only show annotations matching this opportunity category.
    #[arg(long, value_enum)]
    category: Option<OpportunityCategoryArg>,

    /// Minimum confidence level to include (low < medium < high).
    #[arg(long, value_enum)]
    min_confidence: Option<OpportunityConfidenceArg>,

    /// Minimum score threshold (0.0–1.0).
    #[arg(long, default_value_t = 0.0)]
    min_score: f64,

    /// Start of the inclusive UTC time window filter.
    #[arg(long)]
    start_at_utc: Option<String>,

    /// End of the inclusive UTC time window filter.
    #[arg(long)]
    end_at_utc: Option<String>,

    /// Include conversations with no opportunity annotations.
    #[arg(long)]
    include_empty: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OpportunityCategoryArg {
    SessionSetup,
    TaskSetup,
    HistoryDrag,
    Delegation,
    ModelMismatch,
    PromptYield,
    SearchChurn,
    ToolResultBloat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OpportunityConfidenceArg {
    Low,
    Medium,
    High,
}

impl From<OpportunityCategoryArg> for OpportunityCategory {
    fn from(arg: OpportunityCategoryArg) -> Self {
        match arg {
            OpportunityCategoryArg::SessionSetup => OpportunityCategory::SessionSetup,
            OpportunityCategoryArg::TaskSetup => OpportunityCategory::TaskSetup,
            OpportunityCategoryArg::HistoryDrag => OpportunityCategory::HistoryDrag,
            OpportunityCategoryArg::Delegation => OpportunityCategory::Delegation,
            OpportunityCategoryArg::ModelMismatch => OpportunityCategory::ModelMismatch,
            OpportunityCategoryArg::PromptYield => OpportunityCategory::PromptYield,
            OpportunityCategoryArg::SearchChurn => OpportunityCategory::SearchChurn,
            OpportunityCategoryArg::ToolResultBloat => OpportunityCategory::ToolResultBloat,
        }
    }
}

impl From<OpportunityConfidenceArg> for OpportunityConfidence {
    fn from(arg: OpportunityConfidenceArg) -> Self {
        match arg {
            OpportunityConfidenceArg::Low => OpportunityConfidence::Low,
            OpportunityConfidenceArg::Medium => OpportunityConfidence::Medium,
            OpportunityConfidenceArg::High => OpportunityConfidence::High,
        }
    }
}

#[derive(Debug, Clone, Args, PartialEq, Eq)]
struct SnapshotArgs {
    /// Viewport width in columns.
    #[arg(long, default_value_t = 120)]
    width: u16,

    /// Viewport height in rows.
    #[arg(long, default_value_t = 40)]
    height: u16,

    #[command(flatten)]
    startup: StartupArgs,
}

#[derive(Debug, Clone, Args, Default, PartialEq, Eq)]
struct StartupArgs {
    /// Finish the full import before opening the TUI.
    #[arg(long = "startup-full-import")]
    full_import: bool,

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
    #[value(name = "all-input", alias = "gross-input")]
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
enum SkillsPathArg {
    Root,
    Skill,
    SkillProject,
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
        ..Default::default()
    })?;

    match command {
        None => run_app(&config, startup),
        Some(Command::Db(command)) => run_db_command(&config, command.command),
        Some(Command::Benchmark(args)) => run_benchmark_command(&config, &args),
        Some(Command::Report(args)) => run_report_command(&config, &args),
        Some(Command::Skills(args)) => run_skills_command(&config, &args),
        Some(Command::Opportunities(args)) => run_opportunities_command(&config, &args),
        Some(Command::Snapshot(args)) => run_snapshot_command(&config, &args),
    }
}

fn run_app(config: &RuntimeConfig, startup_args: StartupArgs) -> Result<()> {
    config.ensure_dirs()?;
    let perf_logger = PerfLogger::from_env(&config.state_dir)?;
    let mut startup_progress = StartupConsoleProgress::stderr();
    let mut database = Database::open(&config.db_path)?;
    let _scan_report = scan_source_manifest_with_policy(
        &mut database,
        &config.source_root,
        &config.project_identity,
        &config.project_filters,
    )?;
    let mut startup_import = start_startup_import_with_mode_and_progress(
        database.connection(),
        &config.db_path,
        &config.source_root,
        startup_args.import_mode(),
        |update| startup_progress.import_progress(update),
    )?;
    let snapshot = startup_import.snapshot.clone();
    let open_reason = startup_import.open_reason;
    let startup_progress_update = startup_import.startup_progress_update.clone();
    let startup_browse_state = startup_args.build_startup_browse_state()?;
    print_import_problem(startup_import.startup_status_message.as_deref());
    print_import_problem(startup_import.deferred_status_message.as_deref());
    let mut forwarded_updates =
        ForwardedStartupUpdates::spawn(startup_import.take_status_updates());
    let ui_updates = forwarded_updates.take_ui_updates();

    let run_result = gnomon_tui::run_with_progress(
        config,
        snapshot,
        open_reason,
        startup_progress_update,
        startup_browse_state,
        ui_updates,
        |update| startup_progress.query_progress(update),
        perf_logger,
    );
    print_import_problems(forwarded_updates.finish());
    run_result
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

            let report = reset_derived_cache_artifacts(&config.db_path, &config.state_dir)?;
            print_derived_reset_report(&config.state_dir, &config.db_path, &report);
            Ok(())
        }
        DbSubcommand::Rebuild => rebuild_database(config),
        DbSubcommand::Status => run_db_status_command(config),
        DbSubcommand::Check => run_db_check_command(config),
    }
}

fn run_report_command(config: &RuntimeConfig, args: &ReportArgs) -> Result<()> {
    config.ensure_dirs()?;
    let perf_logger = PerfLogger::from_env(&config.state_dir)?;
    let report = build_browse_report(config, args, perf_logger)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn run_snapshot_command(config: &RuntimeConfig, args: &SnapshotArgs) -> Result<()> {
    config.ensure_dirs()?;
    let mut database = Database::open(&config.db_path)?;
    let _scan_report = scan_source_manifest_with_policy(
        &mut database,
        &config.source_root,
        &config.project_identity,
        &config.project_filters,
    )?;
    let mut startup_import =
        start_startup_import(database.connection(), &config.db_path, &config.source_root)?;
    let snapshot = startup_import.snapshot.clone();
    let open_reason = startup_import.open_reason;
    let startup_progress_update = startup_import.startup_progress_update.clone();
    let startup_browse_state = args.startup.build_startup_browse_state()?;
    // Drain the import worker so it doesn't outlive the render.
    drop(startup_import.take_status_updates());
    print_import_problem(startup_import.startup_status_message.as_deref());

    let output = gnomon_tui::render_snapshot(
        config,
        snapshot,
        open_reason,
        startup_progress_update,
        startup_browse_state,
        args.width,
        args.height,
    )?;
    print!("{output}");
    Ok(())
}

fn run_skills_command(config: &RuntimeConfig, args: &SkillsArgs) -> Result<()> {
    config.ensure_dirs()?;
    let perf_logger = PerfLogger::from_env(&config.state_dir)?;
    let report = build_skills_report(config, args, perf_logger)?;
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

fn build_skills_report(
    config: &RuntimeConfig,
    args: &SkillsArgs,
    perf_logger: Option<PerfLogger>,
) -> Result<SkillsReport> {
    let database = Database::open(&config.db_path)?;
    let engine = QueryEngine::with_perf(database.connection(), perf_logger);
    let snapshot = engine.latest_snapshot_bounds()?;
    engine.skills_report(&snapshot, args.build_path()?)
}

fn run_opportunities_command(config: &RuntimeConfig, args: &OpportunitiesArgs) -> Result<()> {
    config.ensure_dirs()?;
    let perf_logger = PerfLogger::from_env(&config.state_dir)?;
    let database = Database::open(&config.db_path)?;
    let engine = QueryEngine::with_perf(database.connection(), perf_logger);
    let snapshot = engine.latest_snapshot_bounds()?;

    let filters = OpportunitiesFilters {
        project_id: args.project_id,
        start_at_utc: args.start_at_utc.clone(),
        end_at_utc: args.end_at_utc.clone(),
        include_empty: args.include_empty,
    };

    let mut report = engine.opportunities_report(&snapshot, &filters)?;

    let category_filter = args.category.map(OpportunityCategory::from);
    let min_confidence = args.min_confidence.map(OpportunityConfidence::from);
    let min_score = args.min_score;

    if category_filter.is_some() || min_confidence.is_some() || min_score > 0.0 {
        for row in &mut report.rows {
            row.opportunities = filter_opportunities(
                &row.opportunities,
                category_filter,
                min_confidence,
                min_score,
            );
        }
        if !args.include_empty {
            report.rows.retain(|row| !row.opportunities.is_empty());
        }
    }

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn filter_opportunities(
    summary: &OpportunitySummary,
    category: Option<OpportunityCategory>,
    min_confidence: Option<OpportunityConfidence>,
    min_score: f64,
) -> OpportunitySummary {
    let filtered: Vec<_> = summary
        .annotations
        .iter()
        .filter(|a| category.is_none_or(|c| a.category == c))
        .filter(|a| min_confidence.is_none_or(|mc| a.confidence >= mc))
        .filter(|a| a.score >= min_score)
        .cloned()
        .collect();
    OpportunitySummary::from_annotations(filtered)
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
    let reset_report = reset_derived_cache_artifacts(&config.db_path, &config.state_dir)?;
    let mut database = Database::open(&config.db_path)?;
    let scan_report = scan_source_manifest_with_policy(
        &mut database,
        &config.source_root,
        &config.project_identity,
        &config.project_filters,
    )?;
    let import_report = import_all(database.connection(), &config.db_path, &config.source_root)?;
    let completed_chunks = count_completed_chunks(&config.db_path)?;

    print_derived_reset_report(&config.state_dir, &config.db_path, &reset_report);
    println!(
        "Rebuilt {} from {} discovered source files across {} completed chunks ({} startup, {} deferred).",
        config.db_path.display(),
        scan_report.discovered_source_files,
        completed_chunks,
        import_report.startup_chunk_count,
        import_report.deferred_chunk_count
    );
    if let Some(summary) = import_report.deferred_failure_summary.as_deref() {
        println!("Warning: {summary}");
    }

    Ok(())
}

fn run_db_status_command(config: &RuntimeConfig) -> Result<()> {
    config.ensure_dirs()?;
    let report = build_import_status_report(config)?;
    print!("{}", render_import_status_report(&report));
    Ok(())
}

fn run_db_check_command(config: &RuntimeConfig) -> Result<()> {
    if !config.db_path.exists() {
        bail!(
            "database not found at {}; run an import first",
            config.db_path.display()
        );
    }

    let db = Database::open_read_only(&config.db_path)
        .with_context(|| format!("unable to open {}", config.db_path.display()))?;
    let conn = db.connection();

    let mut issues: Vec<String> = Vec::new();

    let integrity_rows: Vec<String> = conn
        .prepare("PRAGMA integrity_check")
        .and_then(|mut stmt| {
            stmt.query_map([], |row| row.get(0))?
                .collect::<rusqlite::Result<Vec<String>>>()
        })
        .context("unable to run integrity_check")?;

    if integrity_rows == ["ok"] {
        println!("integrity_check: ok");
    } else {
        for row in &integrity_rows {
            issues.push(format!("integrity_check: {row}"));
        }
    }

    let fk_rows: Vec<(String, i64, String, i64)> = conn
        .prepare("PRAGMA foreign_key_check")
        .and_then(|mut stmt| {
            stmt.query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
        })
        .context("unable to run foreign_key_check")?;

    if fk_rows.is_empty() {
        println!("foreign_key_check: ok");
    } else {
        for (table, rowid, parent, fkid) in &fk_rows {
            issues.push(format!(
                "foreign_key_check: {table} rowid={rowid} references {parent} (fk index {fkid})"
            ));
        }
    }

    if issues.is_empty() {
        Ok(())
    } else {
        for issue in &issues {
            eprintln!("{issue}");
        }
        bail!("{} integrity issue(s) found", issues.len());
    }
}

fn build_import_status_report(config: &RuntimeConfig) -> Result<ImportStatusReport> {
    if !config.db_path.exists() {
        return Ok(ImportStatusReport {
            db_path: config.db_path.clone(),
            db_exists: false,
            snapshot: SnapshotBounds::bootstrap(),
            counts: ImportStateCounts::default(),
            phase_counts: Vec::new(),
            active_chunk: None,
            latest_completed_chunk: None,
            recent_failures: Vec::new(),
        });
    }

    let database = Database::open(&config.db_path)?;
    let conn = database.connection();
    let snapshot = QueryEngine::new(conn).latest_snapshot_bounds()?;

    Ok(ImportStatusReport {
        db_path: config.db_path.clone(),
        db_exists: true,
        snapshot,
        counts: load_import_state_counts(conn)?,
        phase_counts: load_phase_counts(conn)?,
        active_chunk: load_active_import_chunk(conn)?,
        latest_completed_chunk: load_latest_completed_chunk(conn)?,
        recent_failures: load_recent_failed_chunks(conn)?,
    })
}

fn load_import_state_counts(conn: &rusqlite::Connection) -> Result<ImportStateCounts> {
    let mut stmt = conn.prepare(
        "
        SELECT state, COUNT(*)
        FROM import_chunk
        GROUP BY state
        ",
    )?;
    let mut rows = stmt.query([])?;
    let mut counts = ImportStateCounts::default();

    while let Some(row) = rows.next()? {
        let state_raw = row.get::<_, String>(0)?;
        let state = ImportChunkState::from_db_value(&state_raw)
            .ok_or_else(|| anyhow::anyhow!("unknown import chunk state {state_raw}"))?;
        let count = usize::try_from(row.get::<_, i64>(1)?)
            .context("import chunk count overflowed usize")?;
        counts.increment(state, count);
    }

    Ok(counts)
}

fn load_phase_counts(conn: &rusqlite::Connection) -> Result<Vec<ImportPhaseCounts>> {
    let mut stmt = conn.prepare(
        "
        SELECT last_attempt_phase, state, COUNT(*)
        FROM import_chunk
        WHERE last_attempt_phase IS NOT NULL
        GROUP BY last_attempt_phase, state
        ORDER BY
            CASE last_attempt_phase
                WHEN 'startup' THEN 0
                WHEN 'deferred' THEN 1
                ELSE 2
            END
        ",
    )?;
    let mut rows = stmt.query([])?;
    let mut startup = ImportStateCounts::default();
    let mut deferred = ImportStateCounts::default();
    let mut saw_startup = false;
    let mut saw_deferred = false;

    while let Some(row) = rows.next()? {
        let phase_raw = row.get::<_, String>(0)?;
        let state_raw = row.get::<_, String>(1)?;
        let phase = ImportChunkPhase::from_db_value(&phase_raw)
            .ok_or_else(|| anyhow::anyhow!("unknown import chunk phase {phase_raw}"))?;
        let state = ImportChunkState::from_db_value(&state_raw)
            .ok_or_else(|| anyhow::anyhow!("unknown import chunk state {state_raw}"))?;
        let count = usize::try_from(row.get::<_, i64>(2)?)
            .context("phase import chunk count overflowed usize")?;
        match phase {
            ImportChunkPhase::Startup => {
                startup.increment(state, count);
                saw_startup = true;
            }
            ImportChunkPhase::Deferred => {
                deferred.increment(state, count);
                saw_deferred = true;
            }
        }
    }

    let mut phase_counts = Vec::new();
    if saw_startup {
        phase_counts.push(ImportPhaseCounts {
            phase: ImportChunkPhase::Startup,
            counts: startup,
        });
    }
    if saw_deferred {
        phase_counts.push(ImportPhaseCounts {
            phase: ImportChunkPhase::Deferred,
            counts: deferred,
        });
    }
    Ok(phase_counts)
}

fn load_active_import_chunk(conn: &rusqlite::Connection) -> Result<Option<ImportChunkStatusRow>> {
    let mut stmt = conn.prepare(
        "
        SELECT
            import_chunk.id,
            project.canonical_key,
            import_chunk.chunk_day_local,
            import_chunk.state,
            import_chunk.last_attempt_phase,
            import_chunk.publish_seq,
            import_chunk.started_at_utc,
            import_chunk.completed_at_utc,
            import_chunk.last_error_message
        FROM import_chunk
        JOIN project ON project.id = import_chunk.project_id
        WHERE import_chunk.state IN ('running', 'pending')
        ORDER BY
            CASE import_chunk.state
                WHEN 'running' THEN 0
                WHEN 'pending' THEN 1
                ELSE 2
            END,
            import_chunk.started_at_utc DESC,
            import_chunk.id DESC
        LIMIT 1
        ",
    )?;
    Ok(stmt
        .query_row([], decode_import_chunk_status_row)
        .optional()?)
}

fn load_latest_completed_chunk(
    conn: &rusqlite::Connection,
) -> Result<Option<ImportChunkStatusRow>> {
    let mut stmt = conn.prepare(
        "
        SELECT
            import_chunk.id,
            project.canonical_key,
            import_chunk.chunk_day_local,
            import_chunk.state,
            import_chunk.last_attempt_phase,
            import_chunk.publish_seq,
            import_chunk.started_at_utc,
            import_chunk.completed_at_utc,
            import_chunk.last_error_message
        FROM import_chunk
        JOIN project ON project.id = import_chunk.project_id
        WHERE import_chunk.state = 'complete'
        ORDER BY import_chunk.publish_seq DESC, import_chunk.id DESC
        LIMIT 1
        ",
    )?;
    Ok(stmt
        .query_row([], decode_import_chunk_status_row)
        .optional()?)
}

fn load_recent_failed_chunks(conn: &rusqlite::Connection) -> Result<Vec<ImportChunkStatusRow>> {
    let mut stmt = conn.prepare(
        "
        SELECT
            import_chunk.id,
            project.canonical_key,
            import_chunk.chunk_day_local,
            import_chunk.state,
            import_chunk.last_attempt_phase,
            import_chunk.publish_seq,
            import_chunk.started_at_utc,
            import_chunk.completed_at_utc,
            import_chunk.last_error_message
        FROM import_chunk
        JOIN project ON project.id = import_chunk.project_id
        WHERE import_chunk.state = 'failed'
        ORDER BY import_chunk.completed_at_utc DESC, import_chunk.id DESC
        LIMIT 3
        ",
    )?;
    let rows = stmt.query_map([], decode_import_chunk_status_row)?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("unable to decode failed import chunk rows")
}

fn decode_import_chunk_status_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ImportChunkStatusRow> {
    let state_raw: String = row.get(3)?;
    let phase_raw: Option<String> = row.get(4)?;
    Ok(ImportChunkStatusRow {
        id: row.get(0)?,
        project_key: row.get(1)?,
        chunk_day_local: row.get(2)?,
        state: ImportChunkState::from_db_value(&state_raw).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unknown import chunk state {state_raw}"),
                )),
            )
        })?,
        phase: match phase_raw {
            Some(phase_raw) => {
                Some(ImportChunkPhase::from_db_value(&phase_raw).ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        4,
                        rusqlite::types::Type::Text,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unknown import chunk phase {phase_raw}"),
                        )),
                    )
                })?)
            }
            None => None,
        },
        publish_seq: row.get(5)?,
        started_at_utc: row.get(6)?,
        completed_at_utc: row.get(7)?,
        last_error_message: row.get(8)?,
    })
}

fn render_import_status_report(report: &ImportStatusReport) -> String {
    let mut output = String::new();
    let _ = writeln!(&mut output, "database: {}", report.db_path.display());

    if !report.db_exists {
        let _ = writeln!(&mut output, "status: no cache database found");
        let _ = writeln!(
            &mut output,
            "hint: run `cargo run -p gnomon -- db rebuild` or launch `gnomon` to populate the cache"
        );
        return output;
    }

    let _ = writeln!(&mut output, "status: {}", import_status_summary(report));
    let _ = writeln!(
        &mut output,
        "chunks: pending={}, running={}, complete={}, failed={}",
        report.counts.pending, report.counts.running, report.counts.complete, report.counts.failed
    );

    if !report.phase_counts.is_empty() {
        for phase_counts in &report.phase_counts {
            let _ = writeln!(
                &mut output,
                "{} chunks: pending={}, running={}, complete={}, failed={}",
                phase_counts.phase.as_str(),
                phase_counts.counts.pending,
                phase_counts.counts.running,
                phase_counts.counts.complete,
                phase_counts.counts.failed,
            );
        }
    }

    let snapshot_description = if report.snapshot.is_bootstrap() {
        "no published snapshot yet".to_string()
    } else {
        match report.snapshot.upper_bound_utc.as_deref() {
            Some(upper_bound) => format!(
                "publish_seq <= {} ({} published chunk(s), through {})",
                report.snapshot.max_publish_seq, report.snapshot.published_chunk_count, upper_bound
            ),
            None => format!(
                "publish_seq <= {} ({} published chunk(s))",
                report.snapshot.max_publish_seq, report.snapshot.published_chunk_count
            ),
        }
    };
    let _ = writeln!(&mut output, "latest snapshot: {snapshot_description}");

    if let Some(active_chunk) = &report.active_chunk {
        let phase = active_chunk
            .phase
            .map(ImportChunkPhase::as_str)
            .unwrap_or("unknown");
        let _ = writeln!(
            &mut output,
            "active chunk: {} [{} phase, state={}]",
            active_chunk.label(),
            phase,
            active_chunk.state.as_str()
        );
        let _ = writeln!(
            &mut output,
            "active started: {}",
            active_chunk.started_at_utc
        );
    }

    if let Some(latest_completed_chunk) = &report.latest_completed_chunk {
        let publish_seq = latest_completed_chunk
            .publish_seq
            .map(|seq| format!("#{seq}"))
            .unwrap_or_else(|| "unknown".to_string());
        let completed_at = latest_completed_chunk
            .completed_at_utc
            .as_deref()
            .unwrap_or("unknown");
        let _ = writeln!(
            &mut output,
            "latest completed chunk: {} (publish_seq {}, completed {})",
            latest_completed_chunk.label(),
            publish_seq,
            completed_at
        );
    }

    if report.recent_failures.is_empty() {
        let _ = writeln!(&mut output, "recent failures: none");
    } else {
        let _ = writeln!(&mut output, "recent failures:");
        for failure in &report.recent_failures {
            let phase = failure
                .phase
                .map(ImportChunkPhase::as_str)
                .unwrap_or("unknown");
            let completed_at = failure.completed_at_utc.as_deref().unwrap_or("unknown");
            let message = failure
                .last_error_message
                .as_deref()
                .unwrap_or("no stored error message");
            let _ = writeln!(
                &mut output,
                "- {} [{} phase, completed {}]: {}",
                failure.label(),
                phase,
                completed_at,
                message
            );
        }
    }

    output
}

fn import_status_summary(report: &ImportStatusReport) -> String {
    if let Some(active_chunk) = &report.active_chunk {
        let phase = active_chunk
            .phase
            .map(ImportChunkPhase::as_str)
            .unwrap_or("unknown");
        return match active_chunk.state {
            ImportChunkState::Running => format!("active ({phase} phase)"),
            ImportChunkState::Pending => format!("queued ({phase} phase)"),
            ImportChunkState::Complete | ImportChunkState::Failed => "idle".to_string(),
        };
    }

    if report.counts.failed > 0 {
        return "idle with failures".to_string();
    }
    if report.counts.complete > 0 {
        return "idle".to_string();
    }
    if report.counts.active_count() > 0 {
        return "queued".to_string();
    }

    "no imported chunks yet".to_string()
}

struct StartupConsoleProgress {
    enabled: bool,
    rendered_width: usize,
}

struct ForwardedStartupUpdates {
    ui_updates: Option<mpsc::Receiver<StartupWorkerEvent>>,
    captured_messages: Arc<Mutex<Vec<String>>>,
    worker: Option<JoinHandle<()>>,
}

impl ForwardedStartupUpdates {
    fn spawn(status_updates: Option<mpsc::Receiver<StartupWorkerEvent>>) -> Self {
        let Some(status_updates) = status_updates else {
            return Self {
                ui_updates: None,
                captured_messages: Arc::new(Mutex::new(Vec::new())),
                worker: None,
            };
        };

        let (sender, receiver) = mpsc::channel();
        let captured_messages = Arc::new(Mutex::new(Vec::new()));
        let worker_messages = Arc::clone(&captured_messages);
        let worker = thread::spawn(move || {
            while let Ok(update) = status_updates.recv() {
                match update {
                    StartupWorkerEvent::Progress { .. } | StartupWorkerEvent::Finished => {
                        if sender.send(update).is_err() {
                            break;
                        }
                    }
                    StartupWorkerEvent::StartupSettled {
                        startup_status_message,
                    } => push_import_problem(&worker_messages, startup_status_message),
                    StartupWorkerEvent::DeferredFailures {
                        deferred_status_message,
                    } => push_import_problem(&worker_messages, deferred_status_message),
                }
            }
        });

        Self {
            ui_updates: Some(receiver),
            captured_messages,
            worker: Some(worker),
        }
    }

    fn finish(mut self) -> Vec<String> {
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        take_import_problems(&self.captured_messages)
    }

    fn take_ui_updates(&mut self) -> Option<mpsc::Receiver<StartupWorkerEvent>> {
        self.ui_updates.take()
    }
}

fn push_import_problem(target: &Arc<Mutex<Vec<String>>>, message: Option<String>) {
    let Some(message) = message else {
        return;
    };
    if let Ok(mut problems) = target.lock() {
        problems.push(message);
    }
}

fn take_import_problems(source: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    source
        .lock()
        .map(|mut problems| std::mem::take(&mut *problems))
        .unwrap_or_default()
}

fn print_import_problem(message: Option<&str>) {
    let Some(message) = message else {
        return;
    };
    StartupConsoleProgress::clear_stderr_line();
    eprintln!("{message}");
}

fn print_import_problems(messages: Vec<String>) {
    for message in messages {
        print_import_problem(Some(&message));
    }
}

impl StartupConsoleProgress {
    fn stderr() -> Self {
        Self {
            enabled: io::stderr().is_terminal(),
            rendered_width: 0,
        }
    }

    fn import_progress(&mut self, update: &StartupProgressUpdate) {
        self.render_line(format!(
            "Starting gnomon: {} [{}/{}] {}",
            update.label, update.current, update.total, update.detail
        ));
    }

    fn query_progress(&mut self, update: gnomon_tui::StartupLoadProgressUpdate) {
        let progress = match (update.current, update.total) {
            (Some(current), Some(total)) => format!("[{current}/{total}] "),
            _ => String::new(),
        };
        self.render_line(format!(
            "Starting gnomon: precomputing queries {progress}{}",
            update.phase
        ));
    }

    fn render_line(&mut self, line: String) {
        if !self.enabled {
            return;
        }

        let padding = self.rendered_width.saturating_sub(line.len());
        eprint!("\r{line}{}", " ".repeat(padding));
        let _ = io::stderr().flush();
        self.rendered_width = line.len();
    }

    fn clear_stderr_line() {
        if !io::stderr().is_terminal() {
            return;
        }
        eprint!("\r\x1b[2K");
        let _ = io::stderr().flush();
    }
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

fn print_derived_reset_report(state_dir: &Path, db_path: &Path, report: &DerivedCacheResetReport) {
    print_reset_report(db_path, &report.database);
    let browse_cache_path = default_browse_cache_path(state_dir);
    print_reset_report(&browse_cache_path, &report.browse_cache);
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

impl SkillsArgs {
    fn build_path(&self) -> Result<SkillsPath> {
        match self.path {
            SkillsPathArg::Root => Ok(SkillsPath::Root),
            SkillsPathArg::Skill => Ok(SkillsPath::Skill {
                skill_name: self.required_skill("skill path")?,
            }),
            SkillsPathArg::SkillProject => Ok(SkillsPath::SkillProject {
                skill_name: self.required_skill("skill-project path")?,
                project_id: self.required_project_id("skill-project path")?,
            }),
        }
    }

    fn required_skill(&self, context: &str) -> Result<String> {
        self.skill
            .clone()
            .with_context(|| format!("{context} requires --skill"))
    }

    fn required_project_id(&self, context: &str) -> Result<i64> {
        self.project_id
            .with_context(|| format!("{context} requires --project-id"))
    }
}

impl StartupArgs {
    fn import_mode(&self) -> StartupImportMode {
        if self.full_import {
            StartupImportMode::Full
        } else {
            StartupImportMode::RecentFirst
        }
    }

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
    use std::sync::mpsc;

    use anyhow::{Context, Result, anyhow};
    use clap::{CommandFactory, Parser};
    use gnomon_core::opportunity::OpportunitySummary;
    use rusqlite::OptionalExtension;
    use serde_json::{Value, json};
    use tempfile::tempdir;

    use super::{
        BenchmarkArgs, BrowsePathKindArg, ClassificationStateArg, Cli, Command, DbSubcommand,
        GlobalArgs, MetricLensArg, OpportunityCategoryArg, OpportunityConfidenceArg, ReportArgs,
        ResetArgs, RootViewArg, SkillsArgs, SkillsPathArg, StartupArgs, build_browse_report,
        build_import_status_report, build_query_benchmark_report, count_completed_chunks,
        filter_opportunities, rebuild_database, render_import_status_report, run_db_command,
    };
    use clap::ValueEnum;
    use gnomon_core::config::{ConfigOverrides, RuntimeConfig};
    use gnomon_core::db::Database;
    use gnomon_core::import::StartupWorkerEvent;
    use gnomon_core::query::{
        BrowsePath, MetricIndicators, MetricTotals, RollupRow, RollupRowKind,
    };
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
        assert!(db_help.contains("status"));
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
                DbSubcommand::Status => panic!("expected reset subcommand"),
                DbSubcommand::Check => panic!("expected reset subcommand"),
            },
            Some(Command::Benchmark(_)) => panic!("expected db command"),
            Some(Command::Report(_)) => panic!("expected db command"),
            Some(Command::Skills(_)) => panic!("expected db command"),
            Some(Command::Opportunities(_)) => panic!("expected db command"),
            Some(Command::Snapshot(_)) => panic!("expected db command"),
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
                DbSubcommand::Status => panic!("expected rebuild subcommand"),
                DbSubcommand::Check => panic!("expected rebuild subcommand"),
            },
            Some(Command::Benchmark(_)) => panic!("expected db command"),
            Some(Command::Report(_)) => panic!("expected db command"),
            Some(Command::Skills(_)) => panic!("expected db command"),
            Some(Command::Opportunities(_)) => panic!("expected db command"),
            Some(Command::Snapshot(_)) => panic!("expected db command"),
            None => panic!("expected db command"),
        }
    }

    #[test]
    fn metric_lens_value_enum_uses_all_input_name() {
        let gross_input = MetricLensArg::value_variants()
            .iter()
            .find(|variant| matches!(variant, MetricLensArg::GrossInput))
            .expect("gross input variant");

        assert_eq!(
            gross_input
                .to_possible_value()
                .expect("possible value")
                .get_name(),
            "all-input"
        );
    }

    #[test]
    fn parse_accepts_all_input_metric_lens() {
        let cli = Cli::parse_from(["gnomon", "report", "--lens", "all-input"]);

        match cli.command {
            Some(Command::Report(args)) => assert_eq!(args.lens, MetricLensArg::GrossInput),
            Some(Command::Db(_)) => panic!("expected report command"),
            Some(Command::Benchmark(_)) => panic!("expected report command"),
            Some(Command::Skills(_)) => panic!("expected report command"),
            Some(Command::Opportunities(_)) => panic!("expected report command"),
            Some(Command::Snapshot(_)) => panic!("expected report command"),
            None => panic!("expected report command"),
        }
    }

    #[test]
    fn parse_accepts_legacy_gross_input_metric_lens_alias() {
        let cli = Cli::parse_from(["gnomon", "report", "--lens", "gross-input"]);

        match cli.command {
            Some(Command::Report(args)) => assert_eq!(args.lens, MetricLensArg::GrossInput),
            Some(Command::Db(_)) => panic!("expected report command"),
            Some(Command::Benchmark(_)) => panic!("expected report command"),
            Some(Command::Skills(_)) => panic!("expected report command"),
            Some(Command::Opportunities(_)) => panic!("expected report command"),
            Some(Command::Snapshot(_)) => panic!("expected report command"),
            None => panic!("expected report command"),
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
    fn parse_accepts_skills_arguments() {
        let cli = Cli::parse_from([
            "gnomon",
            "skills",
            "--path",
            "skill-project",
            "--skill",
            "planner",
            "--project-id",
            "7",
        ]);

        match cli.command {
            Some(Command::Skills(args)) => {
                assert_eq!(
                    *args,
                    SkillsArgs {
                        path: SkillsPathArg::SkillProject,
                        skill: Some("planner".to_string()),
                        project_id: Some(7),
                    }
                );
            }
            _ => panic!("expected skills command"),
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
                full_import: false,
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
    fn parse_accepts_startup_full_import_flag() {
        let cli = Cli::parse_from(["gnomon", "--startup-full-import"]);

        assert_eq!(
            cli.startup,
            StartupArgs {
                full_import: true,
                root: None,
                path: None,
                project_id: None,
                category: None,
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
    fn parse_opportunities_command_with_filters() {
        let cli = Cli::parse_from([
            "gnomon",
            "--db",
            "/tmp/custom.sqlite3",
            "opportunities",
            "--project-id",
            "3",
            "--category",
            "history-drag",
            "--min-confidence",
            "medium",
            "--min-score",
            "0.5",
            "--include-empty",
        ]);

        match cli.command {
            Some(Command::Opportunities(args)) => {
                assert_eq!(args.project_id, Some(3));
                assert_eq!(args.category, Some(OpportunityCategoryArg::HistoryDrag));
                assert_eq!(args.min_confidence, Some(OpportunityConfidenceArg::Medium));
                assert!((args.min_score - 0.5).abs() < f64::EPSILON);
                assert!(args.include_empty);
            }
            _ => panic!("expected opportunities command"),
        }
    }

    #[test]
    fn parse_opportunities_command_defaults() {
        let cli = Cli::parse_from(["gnomon", "opportunities"]);

        match cli.command {
            Some(Command::Opportunities(args)) => {
                assert_eq!(args.project_id, None);
                assert_eq!(args.category, None);
                assert_eq!(args.min_confidence, None);
                assert!((args.min_score).abs() < f64::EPSILON);
                assert!(!args.include_empty);
            }
            _ => panic!("expected opportunities command"),
        }
    }

    #[test]
    fn filter_opportunities_by_category_and_confidence() {
        use gnomon_core::opportunity::{
            OpportunityAnnotation, OpportunityCategory, OpportunityConfidence, OpportunitySummary,
        };

        let summary = OpportunitySummary::from_annotations(vec![
            OpportunityAnnotation {
                category: OpportunityCategory::HistoryDrag,
                score: 0.7,
                confidence: OpportunityConfidence::High,
                evidence: vec!["drag evidence".to_string()],
                recommendation: None,
            },
            OpportunityAnnotation {
                category: OpportunityCategory::SessionSetup,
                score: 0.3,
                confidence: OpportunityConfidence::Low,
                evidence: vec!["setup evidence".to_string()],
                recommendation: None,
            },
        ]);

        // Filter by category.
        let filtered =
            filter_opportunities(&summary, Some(OpportunityCategory::HistoryDrag), None, 0.0);
        assert_eq!(filtered.annotations.len(), 1);
        assert_eq!(
            filtered.annotations[0].category,
            OpportunityCategory::HistoryDrag
        );

        // Filter by min confidence.
        let filtered =
            filter_opportunities(&summary, None, Some(OpportunityConfidence::Medium), 0.0);
        assert_eq!(filtered.annotations.len(), 1);
        assert_eq!(
            filtered.annotations[0].confidence,
            OpportunityConfidence::High
        );

        // Filter by min score.
        let filtered = filter_opportunities(&summary, None, None, 0.5);
        assert_eq!(filtered.annotations.len(), 1);
        assert!((filtered.annotations[0].score - 0.7).abs() < f64::EPSILON);
    }

    #[test]
    fn reset_requires_force() -> Result<()> {
        let temp = tempdir()?;
        let config = load_test_config(
            temp.path(),
            temp.path().join("usage.sqlite3"),
            temp.path().join("source"),
        )?;

        let err = run_db_command(&config, DbSubcommand::Reset(ResetArgs::default()))
            .expect_err("reset should require force");

        assert!(err.to_string().contains("database reset is destructive"));
        Ok(())
    }

    #[test]
    fn reset_deletes_database_file_when_forced() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let config = load_test_config(temp.path(), db_path.clone(), temp.path().join("source"))?;
        config.ensure_dirs()?;
        fs::write(&db_path, "seed")?;

        run_db_command(&config, DbSubcommand::Reset(ResetArgs { force: true }))?;

        assert!(!db_path.exists());
        Ok(())
    }

    #[test]
    fn reset_deletes_browse_cache_file_when_forced() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let config = load_test_config(temp.path(), db_path, temp.path().join("source"))?;
        config.ensure_dirs()?;
        let browse_cache_path =
            gnomon_core::browse_cache::default_browse_cache_path(&config.state_dir);
        fs::write(&browse_cache_path, "seed")?;

        run_db_command(&config, DbSubcommand::Reset(ResetArgs { force: true }))?;

        assert!(!browse_cache_path.exists());
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

        let config = load_test_config(temp.path(), db_path.clone(), source_root)?;

        run_db_command(&config, DbSubcommand::Rebuild)?;

        let database = Database::open(&db_path)?;
        let source_file_count: i64 =
            database
                .connection()
                .query_row("SELECT COUNT(*) FROM source_file", [], |row| row.get(0))?;

        // conversation is in a shard; Database::open configures the TEMP VIEW that
        // surfaces it via the main DB's schema.
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
    fn rebuild_deletes_browse_cache_before_reimport() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let source_root = temp.path().join("source");
        let project_root = temp.path().join("project");
        init_git_repo(&project_root)?;
        write_jsonl(&source_root.join("project/session.jsonl"), &project_root)?;

        let config = load_test_config(temp.path(), db_path.clone(), source_root)?;
        config.ensure_dirs()?;
        let browse_cache_path =
            gnomon_core::browse_cache::default_browse_cache_path(&config.state_dir);
        fs::write(&browse_cache_path, "seed")?;

        run_db_command(&config, DbSubcommand::Rebuild)?;

        assert!(!browse_cache_path.exists());
        assert!(db_path.exists());
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

        let config = load_test_config(temp.path(), db_path.clone(), source_root)?;

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
    fn rebuild_skips_malformed_deferred_file_with_warning_and_completes() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let source_root = temp.path().join("source");
        let project_root = temp.path().join("project");
        init_git_repo(&project_root)?;
        write_jsonl(
            &source_root.join("project/recent/session.jsonl"),
            &project_root,
        )?;
        std::fs::create_dir_all(source_root.join("project/older"))?;
        let bad_path = source_root.join("project/older/bad.jsonl");
        let mut malformed_bytes = "{\"type\":\"user\",\"uuid\":\"bad-user\",\"timestamp\":\"2026-03-26T10:00:00Z\",\"sessionId\":\"bad-session\",\"message\":{\"role\":\"user\",\"content\":\"Inspect the project\"}}\n"
            .as_bytes()
            .to_vec();
        malformed_bytes.extend([0_u8; 256]);
        std::fs::write(&bad_path, malformed_bytes)?;
        set_file_modified_days_ago(&bad_path, 5)?;

        let config = load_test_config(temp.path(), db_path.clone(), source_root)?;

        rebuild_database(&config)?;

        let database = Database::open(&db_path)?;
        let counts: (i64, i64) = database.connection().query_row(
            "
            SELECT
                COUNT(*) FILTER (WHERE state = 'complete'),
                COUNT(*) FILTER (WHERE state = 'failed')
            FROM import_chunk
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(counts, (2, 0));

        // import_warning is in a shard; Database::open configures a TEMP VIEW that
        // aggregates it under the main DB's schema.
        let warning: (String, String) = database.connection().query_row(
            "SELECT code, message FROM import_warning LIMIT 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?;
        assert_eq!(warning.0, "invalid_json");
        assert!(warning.1.contains(&bad_path.display().to_string()));
        assert!(warning.1.contains("line 2"));

        Ok(())
    }

    #[test]
    fn db_status_reports_missing_cache() -> Result<()> {
        let temp = tempdir()?;
        let config = load_test_config(
            temp.path(),
            temp.path().join("usage.sqlite3"),
            temp.path().join("source"),
        )?;

        let report = build_import_status_report(&config)?;
        let rendered = render_import_status_report(&report);

        assert!(!report.db_exists);
        assert_eq!(report.counts, Default::default());
        assert!(rendered.contains("status: no cache database found"));
        Ok(())
    }

    #[test]
    fn db_status_reports_startup_running_chunk() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let config = load_test_config(temp.path(), db_path.clone(), temp.path().join("source"))?;
        let database = Database::open(&db_path)?;
        let project_id = insert_test_project(database.connection(), "git:/tmp/startup")?;
        insert_test_import_chunk(
            database.connection(),
            project_id,
            "2026-04-07",
            "running",
            Some("startup"),
            None,
            Some("2026-04-07 12:00:00"),
            None,
            None,
        )?;

        let report = build_import_status_report(&config)?;
        let rendered = render_import_status_report(&report);

        assert_eq!(report.counts.running, 1);
        assert_eq!(
            report
                .active_chunk
                .as_ref()
                .and_then(|chunk| chunk.phase)
                .expect("active startup phase")
                .as_str(),
            "startup"
        );
        assert!(rendered.contains("status: active (startup phase)"));
        Ok(())
    }

    #[test]
    fn db_status_reports_deferred_running_chunk() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let config = load_test_config(temp.path(), db_path.clone(), temp.path().join("source"))?;
        let database = Database::open(&db_path)?;
        let project_id = insert_test_project(database.connection(), "git:/tmp/deferred")?;
        insert_test_import_chunk(
            database.connection(),
            project_id,
            "2026-04-01",
            "running",
            Some("deferred"),
            None,
            Some("2026-04-07 12:00:00"),
            None,
            None,
        )?;

        let report = build_import_status_report(&config)?;
        let rendered = render_import_status_report(&report);

        assert_eq!(report.counts.running, 1);
        assert_eq!(
            report
                .active_chunk
                .as_ref()
                .and_then(|chunk| chunk.phase)
                .expect("active deferred phase")
                .as_str(),
            "deferred"
        );
        assert!(rendered.contains("status: active (deferred phase)"));
        Ok(())
    }

    #[test]
    fn db_status_reports_idle_after_completed_chunks() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let config = load_test_config(temp.path(), db_path.clone(), temp.path().join("source"))?;
        let database = Database::open(&db_path)?;
        let project_id = insert_test_project(database.connection(), "git:/tmp/completed")?;
        insert_test_import_chunk(
            database.connection(),
            project_id,
            "2026-04-07",
            "complete",
            Some("startup"),
            Some(7),
            Some("2026-04-07 12:00:00"),
            Some("2026-04-07 12:05:00"),
            None,
        )?;

        let report = build_import_status_report(&config)?;
        let rendered = render_import_status_report(&report);

        assert_eq!(report.counts.complete, 1);
        assert!(report.active_chunk.is_none());
        assert_eq!(report.snapshot.max_publish_seq, 7);
        assert!(rendered.contains("status: idle"));
        assert!(rendered.contains("latest completed chunk: git:/tmp/completed:2026-04-07"));
        Ok(())
    }

    #[test]
    fn db_status_reports_recent_failure_messages() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let config = load_test_config(temp.path(), db_path.clone(), temp.path().join("source"))?;
        let database = Database::open(&db_path)?;
        let project_id = insert_test_project(database.connection(), "git:/tmp/failure")?;
        insert_test_import_chunk(
            database.connection(),
            project_id,
            "2026-04-06",
            "failed",
            Some("deferred"),
            None,
            Some("2026-04-07 11:00:00"),
            Some("2026-04-07 11:01:00"),
            Some("unable to normalize source file /tmp/failure/session.jsonl"),
        )?;

        let report = build_import_status_report(&config)?;
        let rendered = render_import_status_report(&report);

        assert_eq!(report.counts.failed, 1);
        assert_eq!(report.recent_failures.len(), 1);
        assert_eq!(
            report.recent_failures[0].last_error_message.as_deref(),
            Some("unable to normalize source file /tmp/failure/session.jsonl")
        );
        assert!(rendered.contains("recent failures:"));
        assert!(rendered.contains("unable to normalize source file /tmp/failure/session.jsonl"));
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

        let config = load_test_config(temp.path(), db_path, source_root)?;
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
    fn report_json_includes_opportunity_annotation_fields() -> Result<()> {
        let report = gnomon_core::query::BrowseReport {
            snapshot: gnomon_core::query::SnapshotBounds::bootstrap(),
            request: gnomon_core::query::BrowseRequest {
                snapshot: gnomon_core::query::SnapshotBounds::bootstrap(),
                root: gnomon_core::query::RootView::ProjectHierarchy,
                lens: gnomon_core::query::MetricLens::UncachedInput,
                filters: gnomon_core::query::BrowseFilters::default(),
                path: gnomon_core::query::BrowsePath::Root,
            },
            rows: vec![RollupRow {
                kind: RollupRowKind::Project,
                key: "project:1".to_string(),
                label: "project-a".to_string(),
                metrics: MetricTotals {
                    uncached_input: 0.0,
                    cached_input: 0.0,
                    gross_input: 0.0,
                    output: 0.0,
                    total: 0.0,
                },
                indicators: MetricIndicators {
                    selected_lens_last_5_hours: 0.0,
                    selected_lens_last_week: 0.0,
                    uncached_input_reference: 0.0,
                },
                item_count: 0,
                opportunities: OpportunitySummary::default(),
                skill_attribution: None,
                project_id: Some(1),
                project_identity: None,
                category: None,
                action: None,
                full_path: None,
            }],
        };

        let json = serde_json::to_value(&report)?;
        assert_eq!(json["rows"][0]["opportunities"]["annotations"], json!([]));
        assert_eq!(
            json["rows"][0]["opportunities"]["top_category"],
            Value::Null
        );
        assert_eq!(json["rows"][0]["opportunities"]["total_score"], json!(0.0));
        assert_eq!(
            json["rows"][0]["opportunities"]["top_confidence"],
            Value::Null
        );

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

        let config = load_test_config(temp.path(), db_path.clone(), source_root)?;
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
    fn skills_path_requires_skill_name() {
        let args = SkillsArgs {
            path: SkillsPathArg::Skill,
            skill: None,
            project_id: None,
        };

        let err = args
            .build_path()
            .expect_err("skill path should require a skill name");

        assert!(err.to_string().contains("--skill"));
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

        let config = load_test_config(temp.path(), validation.db_path, validation.source_root)?;

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

    #[test]
    fn forwarded_startup_updates_capture_failures_outside_the_tui() -> Result<()> {
        let (sender, mut forwarded) = {
            let (sender, receiver) = mpsc::channel();
            let forwarded = super::ForwardedStartupUpdates::spawn(Some(receiver));
            (sender, forwarded)
        };

        sender.send(StartupWorkerEvent::StartupSettled {
            startup_status_message: Some("startup import failed for 1 chunk".to_string()),
        })?;
        sender.send(StartupWorkerEvent::DeferredFailures {
            deferred_status_message: Some("deferred import failed for 2 chunks".to_string()),
        })?;
        sender.send(StartupWorkerEvent::Progress {
            update: gnomon_core::import::StartupProgressUpdate {
                label: "startup import",
                current: 1,
                total: 2,
                detail: "chunk 1 of 2".to_string(),
            },
        })?;
        sender.send(StartupWorkerEvent::Finished)?;
        drop(sender);

        let receiver = forwarded
            .take_ui_updates()
            .context("missing forwarded ui receiver")?;
        assert!(matches!(
            receiver.recv()?,
            StartupWorkerEvent::Progress { .. }
        ));
        assert!(matches!(receiver.recv()?, StartupWorkerEvent::Finished));

        let captured = forwarded.finish();
        assert_eq!(
            captured,
            vec![
                "startup import failed for 1 chunk".to_string(),
                "deferred import failed for 2 chunks".to_string()
            ]
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

    fn set_file_modified_days_ago(path: &Path, days_ago: i64) -> Result<()> {
        let spec = format!("{days_ago} days ago");
        let output = ProcessCommand::new("touch")
            .arg("-d")
            .arg(&spec)
            .arg(path)
            .output()
            .with_context(|| format!("unable to backdate {}", path.display()))?;
        if !output.status.success() {
            return Err(anyhow!(
                "touch -d {:?} {} failed: {}",
                spec,
                path.display(),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
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
    fn insert_test_project(conn: &rusqlite::Connection, canonical_key: &str) -> Result<i64> {
        conn.execute(
            "
            INSERT INTO project (canonical_key, identity_kind, display_name, root_path)
            VALUES (?1, 'git', ?1, ?2)
            ",
            rusqlite::params![
                canonical_key,
                format!("/tmp/{}", canonical_key.replace('/', "_"))
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_test_import_chunk(
        conn: &rusqlite::Connection,
        project_id: i64,
        chunk_day_local: &str,
        state: &str,
        last_attempt_phase: Option<&str>,
        publish_seq: Option<i64>,
        started_at_utc: Option<&str>,
        completed_at_utc: Option<&str>,
        last_error_message: Option<&str>,
    ) -> Result<()> {
        conn.execute(
            "
            INSERT INTO import_chunk (
                project_id,
                chunk_day_local,
                state,
                publish_seq,
                started_at_utc,
                completed_at_utc,
                imported_message_count,
                imported_action_count,
                last_attempt_phase,
                last_error_message
            )
            VALUES (?1, ?2, ?3, ?4, COALESCE(?5, CURRENT_TIMESTAMP), ?6, 0, 0, ?7, ?8)
            ",
            rusqlite::params![
                project_id,
                chunk_day_local,
                state,
                publish_seq,
                started_at_utc,
                completed_at_utc,
                last_attempt_phase,
                last_error_message
            ],
        )?;
        Ok(())
    }

    fn load_test_config(
        root: &Path,
        db_path: PathBuf,
        source_root: PathBuf,
    ) -> Result<RuntimeConfig> {
        let config_path = root.join("config.toml");
        fs::write(&config_path, "")?;
        RuntimeConfig::load(ConfigOverrides {
            db_path: Some(db_path),
            source_root: Some(source_root),
            state_dir: Some(root.join("state")),
            config_path: Some(config_path),
        })
    }
}
