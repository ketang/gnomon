use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand};
use gnomon_core::config::{ConfigOverrides, RuntimeConfig};
use gnomon_core::db::{Database, ResetReport, reset_sqlite_database};
use gnomon_core::import::{import_all, scan_source_manifest, start_startup_import};

#[derive(Debug, Parser)]
#[command(
    name = "gnomon",
    version,
    about = "Analyze Claude session history and explore token usage in the terminal."
)]
struct Cli {
    #[command(flatten)]
    global: GlobalArgs,

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

fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    run(cli)
}

fn run(cli: Cli) -> Result<()> {
    let config = RuntimeConfig::load(ConfigOverrides {
        db_path: cli.global.db,
        source_root: cli.global.source_root,
    })?;

    match cli.command {
        None => run_app(&config),
        Some(Command::Db(command)) => run_db_command(&config, command.command),
    }
}

fn run_app(config: &RuntimeConfig) -> Result<()> {
    config.ensure_dirs()?;
    let mut database = Database::open(&config.db_path)?;
    let _scan_report = scan_source_manifest(&mut database, &config.source_root)?;
    let startup_import =
        start_startup_import(database.connection(), &config.db_path, &config.source_root)?;
    let snapshot = startup_import.snapshot.clone();
    let open_reason = startup_import.open_reason;
    let startup_error = startup_import.startup_error.clone();

    gnomon_tui::run(config, snapshot, open_reason, startup_error)
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
        Cli, Command, DbSubcommand, GlobalArgs, ResetArgs, count_completed_chunks, run_db_command,
    };
    use gnomon_core::config::{ConfigOverrides, RuntimeConfig};
    use gnomon_core::db::Database;

    #[test]
    fn help_lists_db_subcommands() {
        let mut help = Vec::new();
        Cli::command()
            .write_long_help(&mut help)
            .expect("help output");
        let help = String::from_utf8(help).expect("utf8 help");

        assert!(help.contains("db"));
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
            None => panic!("expected db command"),
        }
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
