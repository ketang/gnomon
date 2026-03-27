use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use gnomon_core::validation::{ScaleValidationProfile, ScaleValidationSpec, run_scale_validation};
use tempfile::tempdir;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ProfileArg {
    Quick,
    TenX,
}

impl ProfileArg {
    fn profile(self) -> ScaleValidationProfile {
        match self {
            Self::Quick => ScaleValidationProfile::Quick,
            Self::TenX => ScaleValidationProfile::TenX,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "validate-scale",
    about = "Generate a synthetic corpus, run the importer/query pipeline, and report startup and query timings."
)]
struct Cli {
    #[arg(long, value_enum, default_value_t = ProfileArg::TenX)]
    profile: ProfileArg,

    #[arg(long, value_name = "PATH")]
    root: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    keep_artifacts: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let spec: ScaleValidationSpec = cli.profile.profile().spec();

    let temp_root;
    let root = if let Some(root) = cli.root {
        std::fs::create_dir_all(&root)?;
        root
    } else {
        temp_root = tempdir()?;
        if cli.keep_artifacts {
            temp_root.keep()
        } else {
            temp_root.path().to_path_buf()
        }
    };

    let report = run_scale_validation(&root, spec)?;

    println!(
        "Scale validation profile: {}",
        cli.profile.profile().as_str()
    );
    println!(
        "Corpus shape: {} projects x {} days x {} sessions/day = {} source files",
        report.spec.project_count,
        report.spec.day_count,
        report.spec.sessions_per_day,
        report.spec.expected_source_files()
    );
    println!("Artifacts root: {}", report.artifacts_root.display());
    println!("Source root: {}", report.source_root.display());
    println!("SQLite path: {}", report.db_path.display());
    println!();
    println!("Discovery:");
    println!("  fixture generation: {} ms", report.fixture_generation_ms);
    println!("  source scan + manifest shaping: {} ms", report.scan_ms);
    println!("  discovered projects: {}", report.discovered_projects);
    println!(
        "  discovered source files: {}",
        report.discovered_source_files
    );
    println!();
    println!("Import startup:");
    println!("  startup gate: {:?}", report.startup_open_reason);
    println!("  startup chunks: {}", report.startup_chunks);
    println!("  total chunks: {}", report.total_chunks);
    println!(
        "  time to first usable UI: {} ms",
        report.first_usable_ui_ms
    );
    println!("  time to last-24h ready: {} ms", report.last_24h_ready_ms);
    println!("  time to full backfill: {} ms", report.full_backfill_ms);
    println!(
        "  startup snapshot publish_seq <= {}",
        report.startup_snapshot.max_publish_seq
    );
    println!(
        "  final snapshot publish_seq <= {} ({} published chunks)",
        report.final_snapshot.max_publish_seq, report.final_snapshot.published_chunk_count
    );
    println!();
    println!("Query responsiveness:");
    println!("  filter options: {} ms", report.filter_options_ms);
    println!(
        "  project root browse: {} ms ({} rows)",
        report.project_root_browse_ms, report.project_root_row_count
    );
    println!(
        "  category root browse: {} ms ({} rows)",
        report.category_root_browse_ms, report.category_root_row_count
    );
    println!(
        "  project drill browse: {} ms ({} rows)",
        report.project_drill_ms, report.project_drill_row_count
    );

    Ok(())
}
