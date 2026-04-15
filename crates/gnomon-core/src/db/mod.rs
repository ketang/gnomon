mod migrations;

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};

pub const INITIAL_SCHEMA_VERSION: u32 = 11;
pub const DEFAULT_DB_FILENAME: &str = "usage.sqlite3";
pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

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

fn sqlite_artifact_paths(path: &Path) -> [PathBuf; 3] {
    let db_path = path.to_path_buf();
    let wal_path = sqlite_sidecar_path(path, "wal");
    let shm_path = sqlite_sidecar_path(path, "shm");
    [db_path, wal_path, shm_path]
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

fn configure_read_only_connection(conn: &mut Connection) -> Result<()> {
    conn.busy_timeout(DEFAULT_BUSY_TIMEOUT)
        .context("unable to configure sqlite busy timeout")?;
    conn.execute_batch(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA query_only = ON;
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
        DEFAULT_BUSY_TIMEOUT, DEFAULT_DB_FILENAME, Database, INITIAL_SCHEMA_VERSION, ResetReport,
        reset_sqlite_database,
    };

    const REQUIRED_TABLES: [&str; 18] = [
        "project",
        "source_file",
        "import_chunk",
        "import_warning",
        "history_event",
        "skill_invocation",
        "conversation",
        "stream",
        "record",
        "message",
        "message_part",
        "turn",
        "action",
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
            let db = Database::open(&db_path)?;
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

        let reopened = Database::open(&db_path)?;
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

        assert_eq!(
            report,
            ResetReport {
                removed_paths: vec![
                    db_path.clone(),
                    temp.path().join("usage.sqlite3-wal"),
                    temp.path().join("usage.sqlite3-shm"),
                ],
                missing_paths: Vec::new(),
            }
        );
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

        assert!(report.removed_paths.is_empty());
        assert_eq!(
            report.missing_paths,
            vec![
                db_path,
                temp.path().join("usage.sqlite3-wal"),
                temp.path().join("usage.sqlite3-shm"),
            ]
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
        let db = Database::open(temp.path().join(DEFAULT_DB_FILENAME))?;
        assert_eq!(db.schema_version()?, INITIAL_SCHEMA_VERSION);
        Ok(())
    }

    #[test]
    fn foreign_key_constraint_is_enforced() -> Result<()> {
        let temp = tempdir()?;
        let db = Database::open(temp.path().join(DEFAULT_DB_FILENAME))?;

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
            let db = Database::open(&db_path)?;
            db.connection().execute(
                "INSERT INTO project (identity_kind, canonical_key, display_name, root_path)
                 VALUES ('path', 'path:/tmp/retain-test', 'retain-test', '/tmp/retain-test')",
                [],
            )?;
        } // db dropped here, connection closed

        let db = Database::open(&db_path)?;
        let count: i64 = db
            .connection()
            .query_row("SELECT COUNT(*) FROM project", [], |row| row.get(0))?;
        assert_eq!(count, 1, "project row should persist after reopening");
        Ok(())
    }
}
