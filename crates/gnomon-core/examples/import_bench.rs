// Import benchmark harness for the import-perf project.
//
// Run with:
//   cargo run -p gnomon-core --example import_bench --release -- \
//     --corpus subset --mode full --perf-log /tmp/gnomon-perf.jsonl

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use clap::{Parser, ValueEnum};
use gnomon_core::db::Database;
use gnomon_core::import::{
    StartupImportMode, import_all_with_perf_logger, scan_source_manifest_with_perf_logger,
    start_startup_import_with_perf_logger,
};
use gnomon_core::perf::{PerfLogFormat, PerfLogGranularity, PerfLogger, PerfLoggerConfig};
use rusqlite::Connection;
use tempfile::TempDir;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CorpusChoice {
    Subset,
    Full,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ModeChoice {
    Full,
    Startup,
    WarmStartup,
    DeltaStartup,
}

#[derive(Parser, Debug)]
#[command(about = "Gnomon import benchmark harness")]
struct Args {
    #[arg(long, value_enum, default_value_t = CorpusChoice::Subset)]
    corpus: CorpusChoice,

    #[arg(long, value_enum, default_value_t = ModeChoice::Full)]
    mode: ModeChoice,

    #[arg(long, default_value_t = 1)]
    repeats: u32,

    #[arg(long)]
    perf_log: Option<PathBuf>,

    #[arg(long)]
    keep_db: bool,
}

const BENCH_PERF_LOG_MAX_BYTES: u64 = 200 * 1024 * 1024;

fn main() -> Result<()> {
    let args = Args::parse();

    let repo_root = find_repo_root()?;
    let corpus_path = match args.corpus {
        CorpusChoice::Subset => repo_root.join("tests/fixtures/import-corpus/subset.tar.zst"),
        CorpusChoice::Full => repo_root.join("tests/fixtures/import-corpus/full.tar.zst"),
    };
    if !corpus_path.exists() {
        return Err(anyhow!(
            "corpus tarball not found at {}; run tests/fixtures/import-corpus/capture.sh first",
            corpus_path.display()
        ));
    }

    for iteration in 1..=args.repeats {
        println!(
            "=== iteration {iteration}/{} ({:?} / {:?}) ===",
            args.repeats, args.corpus, args.mode
        );
        run_once(
            &corpus_path,
            args.mode,
            args.perf_log.as_deref(),
            args.keep_db,
        )?;
    }
    Ok(())
}

fn run_once(
    corpus_path: &Path,
    mode: ModeChoice,
    perf_log: Option<&Path>,
    keep_db: bool,
) -> Result<()> {
    let source_dir = TempDir::new().context("unable to create source tmpdir")?;
    let db_dir = TempDir::new().context("unable to create db tmpdir")?;

    let perf_logger = match perf_log {
        Some(path) => Some(
            PerfLogger::open_with_config(
                path,
                PerfLoggerConfig {
                    format: PerfLogFormat::Jsonl,
                    granularity: PerfLogGranularity::Verbose,
                    max_bytes: BENCH_PERF_LOG_MAX_BYTES,
                    ..PerfLoggerConfig::default()
                },
            )
            .with_context(|| format!("unable to open perf log at {}", path.display()))?,
        ),
        None => None,
    };

    println!("extracting {} ...", corpus_path.display());
    let extract_start = Instant::now();
    let status = Command::new("tar")
        .arg("-C")
        .arg(source_dir.path())
        .arg("-I")
        .arg("zstd")
        .arg("-xf")
        .arg(corpus_path)
        .status()
        .context("unable to spawn tar to extract corpus")?;
    if !status.success() {
        return Err(anyhow!("tar -xf failed for {}", corpus_path.display()));
    }
    let extract_elapsed = extract_start.elapsed();

    let projects_dir = source_dir.path().join("projects");
    let source_root = if projects_dir.exists() {
        projects_dir
    } else {
        source_dir.path().to_path_buf()
    };

    let db_path = db_dir.path().join("usage.sqlite3");

    let bytes = total_jsonl_bytes(&source_root)?;
    println!(
        "jsonl bytes: {:.2} MB   extract: {:.2}s",
        bytes as f64 / (1024.0 * 1024.0),
        extract_elapsed.as_secs_f64()
    );

    let import_start = Instant::now();
    let mut measured_start = None;

    match mode {
        ModeChoice::Full => {
            let mut database = Database::open(&db_path)
                .with_context(|| format!("unable to open db at {}", db_path.display()))?;
            scan_source_manifest_with_perf_logger(&mut database, &source_root, perf_logger.clone())
                .context("source scan failed")?;
            let report = import_all_with_perf_logger(
                database.connection(),
                &db_path,
                &source_root,
                perf_logger,
            )
            .context("import_all_with_perf_logger failed")?;
            println!(
                "startup chunks: {}, deferred chunks: {}, deferred failures: {}",
                report.startup_chunk_count,
                report.deferred_chunk_count,
                report.deferred_failure_count,
            );
        }
        ModeChoice::Startup => {
            let mut database = Database::open(&db_path)
                .with_context(|| format!("unable to open db at {}", db_path.display()))?;
            scan_source_manifest_with_perf_logger(&mut database, &source_root, perf_logger.clone())
                .context("source scan failed")?;
            let startup = start_startup_import_with_perf_logger(
                database.connection(),
                &db_path,
                &source_root,
                StartupImportMode::RecentFirst,
                perf_logger,
                |_| {},
            )
            .context("start_startup_import_with_perf_logger failed")?;
            drop(startup);
        }
        ModeChoice::WarmStartup => {
            {
                let mut database = Database::open(&db_path)
                    .with_context(|| format!("unable to open db at {}", db_path.display()))?;
                scan_source_manifest_with_perf_logger(
                    &mut database,
                    &source_root,
                    perf_logger.clone(),
                )
                .context("warm-startup prefill source scan failed")?;
                let report = import_all_with_perf_logger(
                    database.connection(),
                    &db_path,
                    &source_root,
                    perf_logger.clone(),
                )
                .context("warm-startup prefill import failed")?;
                println!(
                    "warm-startup prefill: startup chunks={}, deferred chunks={}, deferred failures={}",
                    report.startup_chunk_count,
                    report.deferred_chunk_count,
                    report.deferred_failure_count,
                );
            }

            measured_start = Some(Instant::now());
            let mut database = Database::open(&db_path)
                .with_context(|| format!("unable to reopen db at {}", db_path.display()))?;
            scan_source_manifest_with_perf_logger(&mut database, &source_root, perf_logger.clone())
                .context("warm-startup measured source scan failed")?;
            let startup = start_startup_import_with_perf_logger(
                database.connection(),
                &db_path,
                &source_root,
                StartupImportMode::RecentFirst,
                perf_logger,
                |_| {},
            )
            .context("warm-startup measured import failed")?;
            drop(startup);
        }
        ModeChoice::DeltaStartup => {
            let mutated_relative_path = {
                let mut database = Database::open(&db_path)
                    .with_context(|| format!("unable to open db at {}", db_path.display()))?;
                scan_source_manifest_with_perf_logger(
                    &mut database,
                    &source_root,
                    perf_logger.clone(),
                )
                .context("delta-startup prefill source scan failed")?;
                let report = import_all_with_perf_logger(
                    database.connection(),
                    &db_path,
                    &source_root,
                    perf_logger.clone(),
                )
                .context("delta-startup prefill import failed")?;
                println!(
                    "delta-startup prefill: startup chunks={}, deferred chunks={}, deferred failures={}",
                    report.startup_chunk_count,
                    report.deferred_chunk_count,
                    report.deferred_failure_count,
                );
                choose_delta_mutation_relative_path(database.connection())?
            };

            let mutated_file = touch_one_jsonl_file(&source_root, &mutated_relative_path)?;
            println!("delta-startup mutated: {}", mutated_file.display());

            measured_start = Some(Instant::now());
            let mut database = Database::open(&db_path)
                .with_context(|| format!("unable to reopen db at {}", db_path.display()))?;
            scan_source_manifest_with_perf_logger(&mut database, &source_root, perf_logger.clone())
                .context("delta-startup measured source scan failed")?;
            let startup = start_startup_import_with_perf_logger(
                database.connection(),
                &db_path,
                &source_root,
                StartupImportMode::RecentFirst,
                perf_logger,
                |_| {},
            )
            .context("delta-startup measured import failed")?;
            drop(startup);
        }
    }

    let import_elapsed = measured_start.unwrap_or(import_start).elapsed();

    let row_counts = count_rows(&db_path)?;
    let db_bytes = fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);

    println!("--- results ---");
    println!("wall: {:.3}s", import_elapsed.as_secs_f64());
    println!(
        "throughput: {:.2} MB/s parsed",
        (bytes as f64 / (1024.0 * 1024.0)) / import_elapsed.as_secs_f64().max(1e-6)
    );
    for (table, count) in &row_counts {
        println!("  {table:<20} {count}");
    }
    println!("db size: {:.2} MB", db_bytes as f64 / (1024.0 * 1024.0));

    if keep_db {
        let kept = db_path.clone();
        println!("db kept at: {}", kept.display());
        std::mem::forget(db_dir);
    }

    Ok(())
}

fn find_repo_root() -> Result<PathBuf> {
    let mut cur = env::current_dir()?;
    loop {
        if cur.join("Cargo.toml").exists() && cur.join("crates/gnomon-core").exists() {
            return Ok(cur);
        }
        if !cur.pop() {
            return Err(anyhow!("unable to find repo root with crates/gnomon-core"));
        }
    }
}

fn total_jsonl_bytes(root: &Path) -> Result<u64> {
    let mut total: u64 = 0;
    for entry in walkdir::WalkDir::new(root) {
        let entry = entry.context("walkdir entry failed")?;
        if entry.file_type().is_file()
            && entry.path().extension().and_then(|s| s.to_str()) == Some("jsonl")
        {
            total += entry.metadata().context("metadata failed")?.len();
        }
    }
    Ok(total)
}

fn choose_delta_mutation_relative_path(conn: &Connection) -> Result<String> {
    conn.query_row(
        "
        SELECT source_file.relative_path
        FROM conversation
        JOIN source_file ON source_file.id = conversation.source_file_id
        ORDER BY source_file.relative_path
        LIMIT 1
        ",
        [],
        |row| row.get(0),
    )
    .context("unable to select a conversation-backed source file for delta mutation")
}

fn touch_one_jsonl_file(root: &Path, relative_path: &str) -> Result<PathBuf> {
    thread::sleep(std::time::Duration::from_millis(1100));
    let path = root.join(relative_path);
    let contents = fs::read(&path)
        .with_context(|| format!("unable to read {} for mutation", path.display()))?;
    fs::write(&path, contents)
        .with_context(|| format!("unable to rewrite {} for mutation", path.display()))?;
    Ok(path)
}

fn count_rows(db_path: &Path) -> Result<Vec<(String, i64)>> {
    // Open the main DB via `Database::open` so the TEMP VIEWs unioning shard data tables
    // are configured. Unqualified counts (e.g. `SELECT COUNT(*) FROM message`) then resolve
    // to the view and include rows from all shards.
    let database = Database::open(db_path)
        .with_context(|| format!("unable to open db for row counts at {}", db_path.display()))?;
    let conn = database.connection();
    let tables = [
        "project",
        "source_file",
        "import_chunk",
        "conversation",
        "stream",
        "message",
        "message_part",
        "turn",
        "action",
    ];
    let mut out = Vec::new();
    for table in tables {
        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap_or(0);
        out.push((table.to_string(), count));
    }
    Ok(out)
}
