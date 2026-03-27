mod migrations;

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::Connection;

pub const INITIAL_SCHEMA_VERSION: u32 = 2;
pub const DEFAULT_DB_FILENAME: &str = "usage.sqlite3";
pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut conn = Connection::open(path)
            .with_context(|| format!("unable to open sqlite database at {}", path.display()))?;

        configure_connection(&mut conn)?;
        apply_migrations(&mut conn)?;

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

fn configure_connection(conn: &mut Connection) -> Result<()> {
    conn.busy_timeout(DEFAULT_BUSY_TIMEOUT)
        .context("unable to configure sqlite busy timeout")?;
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;
        ",
    )
    .context("unable to configure sqlite connection pragmas")?;

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
    use anyhow::Result;
    use rusqlite::{Connection, OptionalExtension};
    use tempfile::tempdir;

    use super::{DEFAULT_BUSY_TIMEOUT, DEFAULT_DB_FILENAME, Database, INITIAL_SCHEMA_VERSION};

    const REQUIRED_TABLES: [&str; 15] = [
        "project",
        "source_file",
        "import_chunk",
        "import_warning",
        "conversation",
        "stream",
        "record",
        "message",
        "message_part",
        "turn",
        "turn_message",
        "action",
        "action_message",
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

            for table in REQUIRED_TABLES {
                assert!(table_exists(db.connection(), table)?);
            }
        }

        let reopened = Database::open(&db_path)?;
        assert_eq!(reopened.schema_version()?, INITIAL_SCHEMA_VERSION);

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
}
