mod migrations;

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use rayon::prelude::*;
use rusqlite::{Connection, OpenFlags, OptionalExtension};

pub const INITIAL_SCHEMA_VERSION: u32 = 15;
pub const DEFAULT_DB_FILENAME: &str = "usage.sqlite3";
pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

// Sharding: data tables are partitioned across SHARD_COUNT shard DBs alongside the main DB.
// Reads go through TEMP VIEWs that UNION ALL the data tables across shards.
// Writes go directly to the shard determined by `shard_index_for_project(project_id)`.
pub const SHARD_COUNT: usize = 9;
// Each shard's AUTOINCREMENT sequence is seeded at `shard_idx * SHARD_ID_STRIDE` so
// ids across shards don't collide (critical for correctness of JOINs through the views).
pub const SHARD_ID_STRIDE: i64 = 1_000_000_000;

// Data tables that live in shards. Reads see them via TEMP VIEWs that UNION across shards.
// Order matters: tables that are referenced by views of other tables must come first, but
// since we emit all `CREATE TEMP VIEW` statements in one batch, SQLite handles dependencies.
pub const SHARD_DATA_TABLES: &[&str] = &[
    "conversation",
    "stream",
    "message",
    "message_part",
    "turn",
    "turn_message",
    "action",
    "action_message",
    "action_skill_attribution",
    "path_node",
    "message_path_ref",
    "history_event",
    "skill_invocation",
    "import_warning",
    "chunk_action_rollup",
    "chunk_path_rollup",
];

// Tables with `INTEGER PRIMARY KEY AUTOINCREMENT` that need their sqlite_sequence
// pre-seeded so shard-local ids don't collide across shards.
const SHARD_AUTOINCREMENT_TABLES: &[&str] = &[
    "conversation",
    "stream",
    "message",
    "message_part",
    "turn",
    "action",
    "path_node",
    "message_path_ref",
    "history_event",
    "skill_invocation",
    "import_warning",
];

pub fn shard_dir_for_db(main_db_path: &Path) -> PathBuf {
    main_db_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("shards")
}

pub fn shard_path_for_index(main_db_path: &Path, shard_idx: usize) -> PathBuf {
    shard_dir_for_db(main_db_path).join(format!("shard{shard_idx}.sqlite3"))
}

pub fn shard_index_for_project(project_id: i64) -> usize {
    (project_id.unsigned_abs() as usize) % SHARD_COUNT
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetReport {
    pub removed_paths: Vec<PathBuf>,
    pub missing_paths: Vec<PathBuf>,
}

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut conn = Connection::open(path)
            .with_context(|| format!("unable to open sqlite database at {}", path.display()))?;

        configure_read_write_connection(&mut conn)?;
        apply_migrations(&mut conn)?;
        ensure_shards_exist(path)?;
        configure_read_view(&conn, path)?;

        Ok(Self { conn })
    }

    // Test-only escape hatch: open the MAIN DB without configuring shard views. Writes to
    // data tables land in the main DB (which is writable at the file level), and reads see
    // those same rows. Unit tests that predate sharding use this to keep their legacy
    // single-DB assertions working; production code paths use `open` / `open_for_import` /
    // `open_shard_for_import` / `open_read_only` as appropriate.
    #[cfg(test)]
    pub fn open_unsharded(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut conn = Connection::open(path)
            .with_context(|| format!("unable to open sqlite database at {}", path.display()))?;

        configure_read_write_connection(&mut conn)?;
        apply_migrations(&mut conn)?;

        Ok(Self { conn })
    }

    // Open the MAIN DB for import-time metadata writes (import_chunk, source_file updates).
    // Does NOT attach shards or create views — metadata writes go straight to main.
    pub fn open_for_import(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut conn = Connection::open(path)
            .with_context(|| format!("unable to open sqlite database at {}", path.display()))?;

        configure_import_connection(&mut conn)?;
        apply_migrations(&mut conn)?;

        Ok(Self { conn })
    }

    // Open a shard DB for import-time bulk data writes (conversation, message, action, etc.).
    // Runs migrations on the shard file (creates all tables but shards only populate data tables)
    // and seeds sqlite_sequence so shard-local ids stay in this shard's reserved range.
    pub fn open_shard_for_import(main_db_path: &Path, shard_idx: usize) -> Result<Self> {
        let shard_dir = shard_dir_for_db(main_db_path);
        fs::create_dir_all(&shard_dir)
            .with_context(|| format!("unable to create shard directory {}", shard_dir.display()))?;
        let shard_path = shard_path_for_index(main_db_path, shard_idx);
        let mut conn = Connection::open(&shard_path).with_context(|| {
            format!(
                "unable to open shard sqlite database at {}",
                shard_path.display()
            )
        })?;
        configure_import_connection(&mut conn)?;
        apply_migrations(&mut conn)?;
        seed_shard_sequences(&conn, shard_idx)?;
        Ok(Self { conn })
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| {
                format!(
                    "unable to open sqlite database in read-only mode at {}",
                    path.display()
                )
            })?;

        configure_read_only_connection(&mut conn)?;
        configure_read_view(&conn, path)?;

        Ok(Self { conn })
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }

    pub fn into_inner(self) -> Connection {
        self.conn
    }

    pub fn schema_version(&self) -> Result<u32> {
        let version = self
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .context("unable to read sqlite schema version")?;

        Ok(version)
    }
}

// Ensure all 9 shard DBs exist alongside the main DB. Creates any missing ones with the
// full schema migrated and sqlite_sequence seeded with their reserved id ranges. Idempotent.
// Shard creation is parallelized via rayon — each missing shard runs its migrations on its
// own thread with its own owned Connection (Connection is !Send, but creating it inside
// the rayon closure is fine).
fn ensure_shards_exist(main_db_path: &Path) -> Result<()> {
    let shard_dir = shard_dir_for_db(main_db_path);
    fs::create_dir_all(&shard_dir)
        .with_context(|| format!("unable to create shard directory {}", shard_dir.display()))?;
    (0..SHARD_COUNT).into_par_iter().try_for_each(|i| {
        let shard_path = shard_path_for_index(main_db_path, i);
        if !shard_path.exists() {
            let mut conn = Connection::open(&shard_path).with_context(|| {
                format!(
                    "unable to create shard database at {}",
                    shard_path.display()
                )
            })?;
            configure_import_connection(&mut conn)?;
            apply_migrations(&mut conn)?;
            seed_shard_sequences(&conn, i)?;
        }
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}

// Seed sqlite_sequence for this shard's AUTOINCREMENT tables so shard-local ids fall into
// [shard_idx * STRIDE + 1, (shard_idx+1) * STRIDE]. Idempotent: if a sequence already exists
// at or above the base value, leaves it alone. Relies on `sqlite_sequence` being created by
// the migration (since migration creates tables with AUTOINCREMENT, sqlite_sequence exists).
fn seed_shard_sequences(conn: &Connection, shard_idx: usize) -> Result<()> {
    let base = (shard_idx as i64) * SHARD_ID_STRIDE;
    for table in SHARD_AUTOINCREMENT_TABLES {
        let existing: Option<i64> = conn
            .query_row(
                "SELECT seq FROM sqlite_sequence WHERE name = ?1",
                [table],
                |row| row.get(0),
            )
            .optional()
            .with_context(|| format!("unable to read sqlite_sequence for {table}"))?;
        match existing {
            None => {
                conn.execute(
                    "INSERT INTO sqlite_sequence (name, seq) VALUES (?1, ?2)",
                    rusqlite::params![table, base],
                )
                .with_context(|| format!("unable to seed sqlite_sequence for {table}"))?;
            }
            Some(current) if current < base => {
                conn.execute(
                    "UPDATE sqlite_sequence SET seq = ?2 WHERE name = ?1",
                    rusqlite::params![table, base],
                )
                .with_context(|| format!("unable to seed sqlite_sequence for {table}"))?;
            }
            Some(_) => {}
        }
    }
    Ok(())
}

// Attach all SHARD_COUNT shards to `conn` and create TEMP VIEWs that shadow the main-DB
// data tables with UNION ALL across the shards. Unqualified queries (e.g. `SELECT FROM message`)
// resolve to the TEMP VIEW and see rows from all shards. Main-DB data tables stay empty.
fn configure_read_view(conn: &Connection, main_db_path: &Path) -> Result<()> {
    for i in 0..SHARD_COUNT {
        let shard_path = shard_path_for_index(main_db_path, i);
        // ATTACH uses literal path in SQL; escape any single quotes to prevent injection.
        let path_str = shard_path
            .to_str()
            .context("shard path is not valid UTF-8")?;
        let escaped = path_str.replace('\'', "''");
        conn.execute_batch(&format!("ATTACH DATABASE '{escaped}' AS shard{i};"))
            .with_context(|| format!("unable to attach shard{i} at {}", shard_path.display()))?;
    }
    for table in SHARD_DATA_TABLES {
        let unions: Vec<String> = (0..SHARD_COUNT)
            .map(|i| format!("SELECT * FROM shard{i}.{table}"))
            .collect();
        conn.execute_batch(&format!(
            "CREATE TEMP VIEW {table} AS {};",
            unions.join(" UNION ALL ")
        ))
        .with_context(|| format!("unable to create TEMP VIEW for {table}"))?;
    }
    Ok(())
}

pub fn reset_sqlite_database(path: impl AsRef<Path>) -> Result<ResetReport> {
    let mut report = ResetReport {
        removed_paths: Vec::new(),
        missing_paths: Vec::new(),
    };

    for candidate in sqlite_artifact_paths(path.as_ref()) {
        match fs::remove_file(&candidate) {
            Ok(()) => report.removed_paths.push(candidate),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                report.missing_paths.push(candidate);
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("unable to remove sqlite artifact {}", candidate.display())
                });
            }
        }
    }

    Ok(report)
}

pub fn sqlite_artifact_size_bytes(path: impl AsRef<Path>) -> Result<u64> {
    let mut total = 0u64;
    for candidate in sqlite_artifact_paths(path.as_ref()) {
        match fs::metadata(&candidate) {
            Ok(metadata) => {
                total = total
                    .checked_add(metadata.len())
                    .context("sqlite artifact size overflowed u64")?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("unable to stat sqlite artifact {}", candidate.display())
                });
            }
        }
    }
    Ok(total)
}

// All sqlite artifacts for the database: the main DB file, its WAL/SHM sidecars,
// and each shard DB file plus its WAL/SHM sidecars.
fn sqlite_artifact_paths(path: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::with_capacity(3 + 3 * SHARD_COUNT);
    paths.push(path.to_path_buf());
    paths.push(sqlite_sidecar_path(path, "wal"));
    paths.push(sqlite_sidecar_path(path, "shm"));
    for i in 0..SHARD_COUNT {
        let shard_path = shard_path_for_index(path, i);
        paths.push(shard_path.clone());
        paths.push(sqlite_sidecar_path(&shard_path, "wal"));
        paths.push(sqlite_sidecar_path(&shard_path, "shm"));
    }
    paths
}

fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| DEFAULT_DB_FILENAME.to_string());
    let sidecar_name = format!("{file_name}-{suffix}");
    path.with_file_name(sidecar_name)
}

fn configure_read_write_connection(conn: &mut Connection) -> Result<()> {
    conn.busy_timeout(DEFAULT_BUSY_TIMEOUT)
        .context("unable to configure sqlite busy timeout")?;
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;
        PRAGMA synchronous = NORMAL;
        PRAGMA cache_size = -64000;
        PRAGMA mmap_size = 268435456;
        PRAGMA temp_store = MEMORY;
        ",
    )
    .context("unable to configure sqlite connection pragmas")?;

    Ok(())
}

fn configure_import_connection(conn: &mut Connection) -> Result<()> {
    conn.busy_timeout(DEFAULT_BUSY_TIMEOUT)
        .context("unable to configure sqlite busy timeout")?;
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = OFF;
        PRAGMA synchronous = NORMAL;
        PRAGMA cache_size = -64000;
        PRAGMA mmap_size = 268435456;
        PRAGMA temp_store = MEMORY;
        ",
    )
    .context("unable to configure sqlite import connection pragmas")?;

    Ok(())
}

fn configure_read_only_connection(conn: &mut Connection) -> Result<()> {
    conn.busy_timeout(DEFAULT_BUSY_TIMEOUT)
        .context("unable to configure sqlite busy timeout")?;
    // query_only = ON is intentionally NOT set here; it blocks CREATE TEMP VIEW, which
    // `configure_read_view` needs to run after this. Callers that want the query-only
    // guardrail should set it themselves after views are configured (or rely on the
    // SQLITE_OPEN_READ_ONLY flag which already blocks writes to the main DB).
    conn.execute_batch(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA cache_size = -64000;
        PRAGMA mmap_size = 268435456;
        ",
    )
    .context("unable to configure read-only sqlite connection pragmas")?;

    Ok(())
}

fn apply_migrations(conn: &mut Connection) -> Result<()> {
    migrations::all()
        .to_latest(conn)
        .context("unable to apply sqlite migrations")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Result;
    use rusqlite::{Connection, OptionalExtension};
    use tempfile::tempdir;

    use super::{
        DEFAULT_BUSY_TIMEOUT, DEFAULT_DB_FILENAME, Database, INITIAL_SCHEMA_VERSION, SHARD_COUNT,
        reset_sqlite_database, shard_path_for_index, sqlite_sidecar_path,
    };

    const REQUIRED_TABLES: [&str; 20] = [
        "project",
        "source_file",
        "scan_source_cache",
        "pending_chunk_rebuild",
        "import_chunk",
        "import_warning",
        "history_event",
        "skill_invocation",
        "conversation",
        "stream",
        "message",
        "message_part",
        "turn",
        "action",
        "action_message",
        "action_skill_attribution",
        "chunk_action_rollup",
        "chunk_path_rollup",
        "path_node",
        "message_path_ref",
    ];

    #[test]
    fn fresh_database_is_created_and_migrations_are_idempotent() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join(DEFAULT_DB_FILENAME);

        {
            let db = Database::open_unsharded(&db_path)?;
            assert!(db_path.exists());
            assert_eq!(db.schema_version()?, INITIAL_SCHEMA_VERSION);
            assert_eq!(pragma_text(db.connection(), "journal_mode")?, "wal");
            assert_eq!(pragma_i64(db.connection(), "foreign_keys")?, 1);
            assert_eq!(
                pragma_i64(db.connection(), "busy_timeout")?,
                DEFAULT_BUSY_TIMEOUT.as_millis() as i64
            );
            assert_eq!(pragma_i64(db.connection(), "synchronous")?, 1); // NORMAL
            assert_eq!(pragma_i64(db.connection(), "cache_size")?, -64000);
            assert_eq!(pragma_i64(db.connection(), "mmap_size")?, 268435456);
            assert_eq!(pragma_i64(db.connection(), "temp_store")?, 2); // MEMORY

            for table in REQUIRED_TABLES {
                assert!(table_exists(db.connection(), table)?);
            }
        }

        let reopened = Database::open_unsharded(&db_path)?;
        assert_eq!(reopened.schema_version()?, INITIAL_SCHEMA_VERSION);

        Ok(())
    }

    #[test]
    fn reset_sqlite_database_removes_primary_and_sidecar_files() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join(DEFAULT_DB_FILENAME);
        fs::write(&db_path, "db")?;
        fs::write(temp.path().join("usage.sqlite3-wal"), "wal")?;
        fs::write(temp.path().join("usage.sqlite3-shm"), "shm")?;

        let report = reset_sqlite_database(&db_path)?;

        // Primary files were present; all shard files are absent (no shards were written).
        assert_eq!(
            report.removed_paths,
            vec![
                db_path.clone(),
                temp.path().join("usage.sqlite3-wal"),
                temp.path().join("usage.sqlite3-shm"),
            ]
        );
        assert_eq!(report.missing_paths.len(), 3 * SHARD_COUNT);
        for i in 0..SHARD_COUNT {
            let shard = shard_path_for_index(&db_path, i);
            assert!(report.missing_paths.contains(&shard));
            assert!(
                report
                    .missing_paths
                    .contains(&sqlite_sidecar_path(&shard, "wal"))
            );
            assert!(
                report
                    .missing_paths
                    .contains(&sqlite_sidecar_path(&shard, "shm"))
            );
        }
        assert!(!db_path.exists());
        assert!(!temp.path().join("usage.sqlite3-wal").exists());
        assert!(!temp.path().join("usage.sqlite3-shm").exists());

        Ok(())
    }

    #[test]
    fn reset_sqlite_database_reports_missing_files_gracefully() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join(DEFAULT_DB_FILENAME);

        let report = reset_sqlite_database(&db_path)?;

        // All primary and shard files absent; report lists them all as missing.
        assert!(report.removed_paths.is_empty());
        assert_eq!(report.missing_paths.len(), 3 + 3 * SHARD_COUNT);
        assert!(report.missing_paths.contains(&db_path));
        assert!(
            report
                .missing_paths
                .contains(&temp.path().join("usage.sqlite3-wal"))
        );
        assert!(
            report
                .missing_paths
                .contains(&temp.path().join("usage.sqlite3-shm"))
        );

        Ok(())
    }

    fn pragma_text(conn: &Connection, pragma_name: &str) -> Result<String> {
        let value = conn.pragma_query_value(None, pragma_name, |row| row.get(0))?;
        Ok(value)
    }

    fn pragma_i64(conn: &Connection, pragma_name: &str) -> Result<i64> {
        let value = conn.pragma_query_value(None, pragma_name, |row| row.get(0))?;
        Ok(value)
    }

    fn table_exists(conn: &Connection, table_name: &str) -> Result<bool> {
        let exists = conn
            .query_row(
                "
                SELECT 1
                FROM sqlite_master
                WHERE type = 'table' AND name = ?1
                ",
                [table_name],
                |row| row.get(0),
            )
            .optional()?;

        Ok(exists == Some(1))
    }

    #[test]
    fn schema_version_returns_initial_schema_version() -> Result<()> {
        let temp = tempdir()?;
        let db = Database::open_unsharded(temp.path().join(DEFAULT_DB_FILENAME))?;
        assert_eq!(db.schema_version()?, INITIAL_SCHEMA_VERSION);
        Ok(())
    }

    #[test]
    fn foreign_key_constraint_is_enforced() -> Result<()> {
        let temp = tempdir()?;
        let db = Database::open_unsharded(temp.path().join(DEFAULT_DB_FILENAME))?;

        // Insert a source_file referencing a project_id that does not exist.
        // Must fail because PRAGMA foreign_keys = ON is set at open time.
        let result = db.connection().execute(
            "INSERT INTO source_file (project_id, relative_path) VALUES (99999, 'orphan.jsonl')",
            [],
        );

        assert!(result.is_err(), "expected a foreign key violation error");
        Ok(())
    }

    #[test]
    fn database_can_be_reopened_and_retains_data() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join(DEFAULT_DB_FILENAME);

        {
            let db = Database::open_unsharded(&db_path)?;
            db.connection().execute(
                "INSERT INTO project (identity_kind, canonical_key, display_name, root_path)
                 VALUES ('path', 'path:/tmp/retain-test', 'retain-test', '/tmp/retain-test')",
                [],
            )?;
        } // db dropped here, connection closed

        let db = Database::open_unsharded(&db_path)?;
        let count: i64 = db
            .connection()
            .query_row("SELECT COUNT(*) FROM project", [], |row| row.get(0))?;
        assert_eq!(count, 1, "project row should persist after reopening");
        Ok(())
    }
}
