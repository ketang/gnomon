use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use jiff::Timestamp;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::db::{DEFAULT_BUSY_TIMEOUT, ResetReport, reset_sqlite_database};
use crate::query::{BrowseFilters, BrowsePath, BrowseRequest, MetricLens, RollupRow, RootView};

pub const DEFAULT_BROWSE_CACHE_FILENAME: &str = "browse-cache.sqlite3";
pub const DEFAULT_BROWSE_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;

const BROWSE_CACHE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BrowseCacheStats {
    pub entry_count: usize,
    pub total_payload_bytes: u64,
}

pub struct BrowseCacheStore {
    conn: Connection,
    max_bytes: u64,
}

impl BrowseCacheStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_max_bytes(path, DEFAULT_BROWSE_CACHE_MAX_BYTES)
    }

    pub fn load(&mut self, request: &BrowseRequest) -> Result<Option<Vec<RollupRow>>> {
        if request.snapshot.max_publish_seq == 0 {
            return Ok(None);
        }

        self.prune_superseded_snapshots(request.snapshot.max_publish_seq)?;
        let request_key = persisted_request_key(request)?;
        let snapshot_max_publish_seq = snapshot_publish_seq(request)?;

        let payload = self
            .conn
            .query_row(
                "
                SELECT payload_json
                FROM browse_entry
                WHERE snapshot_max_publish_seq = ?1
                  AND request_key = ?2
                ",
                params![snapshot_max_publish_seq, request_key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("unable to read persisted browse-cache entry")?;

        let Some(payload) = payload else {
            return Ok(None);
        };

        let rows = match serde_json::from_str::<Vec<RollupRow>>(&payload) {
            Ok(rows) => rows,
            Err(_) => {
                self.delete_entry(snapshot_max_publish_seq, &request_key)?;
                return Ok(None);
            }
        };

        let now = Timestamp::now().to_string();
        self.conn
            .execute(
                "
                UPDATE browse_entry
                SET last_accessed_at_utc = ?3
                WHERE snapshot_max_publish_seq = ?1
                  AND request_key = ?2
                ",
                params![snapshot_max_publish_seq, request_key, now],
            )
            .context("unable to update browse-cache access metadata")?;

        Ok(Some(rows))
    }

    pub fn store(&mut self, request: &BrowseRequest, rows: &[RollupRow]) -> Result<()> {
        if request.snapshot.max_publish_seq == 0 {
            return Ok(());
        }

        self.prune_superseded_snapshots(request.snapshot.max_publish_seq)?;
        let request_key = persisted_request_key(request)?;
        let request_json = serde_json::to_string(&PersistedBrowseRequest::from(request))
            .context("unable to serialize persisted browse-cache request")?;
        let payload_json =
            serde_json::to_string(rows).context("unable to serialize persisted browse rows")?;
        let payload_bytes = u64::try_from(payload_json.len())
            .context("persisted browse payload size overflowed u64")?;
        let snapshot_max_publish_seq = snapshot_publish_seq(request)?;

        if payload_bytes > self.max_bytes {
            self.delete_entry(snapshot_max_publish_seq, &request_key)?;
            return Ok(());
        }

        let row_count = i64::try_from(rows.len()).context("browse row count overflowed i64")?;
        let payload_bytes_i64 =
            i64::try_from(payload_bytes).context("browse payload size overflowed i64")?;
        let now = Timestamp::now().to_string();
        self.conn
            .execute(
                "
                INSERT INTO browse_entry (
                    snapshot_max_publish_seq,
                    request_key,
                    request_json,
                    row_count,
                    payload_json,
                    payload_bytes,
                    created_at_utc,
                    last_accessed_at_utc
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
                ON CONFLICT(snapshot_max_publish_seq, request_key) DO UPDATE SET
                    request_json = excluded.request_json,
                    row_count = excluded.row_count,
                    payload_json = excluded.payload_json,
                    payload_bytes = excluded.payload_bytes,
                    last_accessed_at_utc = excluded.last_accessed_at_utc
                ",
                params![
                    snapshot_max_publish_seq,
                    request_key,
                    request_json,
                    row_count,
                    payload_json,
                    payload_bytes_i64,
                    now
                ],
            )
            .context("unable to persist browse-cache entry")?;

        self.enforce_budget(request.snapshot.max_publish_seq)?;
        Ok(())
    }

    pub fn stats(&self) -> Result<BrowseCacheStats> {
        let stats = self
            .conn
            .query_row(
                "
                SELECT COUNT(*), COALESCE(SUM(payload_bytes), 0)
                FROM browse_entry
                ",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .context("unable to compute browse-cache stats")?;

        Ok(BrowseCacheStats {
            entry_count: usize::try_from(stats.0).context("browse-cache entry count overflowed")?,
            total_payload_bytes: u64::try_from(stats.1)
                .context("browse-cache payload bytes overflowed")?,
        })
    }

    pub fn clear(&mut self) -> Result<()> {
        self.conn
            .execute("DELETE FROM browse_entry", [])
            .context("unable to clear browse-cache entries")?;
        Ok(())
    }

    fn open_with_max_bytes(path: impl AsRef<Path>, max_bytes: u64) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("unable to create {}", parent.display()))?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("unable to open browse-cache store {}", path.display()))?;
        conn.busy_timeout(DEFAULT_BUSY_TIMEOUT)
            .context("unable to configure browse-cache sqlite busy timeout")?;
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;
            ",
        )
        .context("unable to configure browse-cache sqlite pragmas")?;

        let version = conn
            .pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
            .context("unable to read browse-cache schema version")?;
        match version {
            0 => {
                conn.execute_batch(
                    "
                    CREATE TABLE browse_entry (
                        snapshot_max_publish_seq INTEGER NOT NULL,
                        request_key TEXT NOT NULL,
                        request_json TEXT NOT NULL,
                        row_count INTEGER NOT NULL,
                        payload_json TEXT NOT NULL,
                        payload_bytes INTEGER NOT NULL,
                        created_at_utc TEXT NOT NULL,
                        last_accessed_at_utc TEXT NOT NULL,
                        PRIMARY KEY (snapshot_max_publish_seq, request_key)
                    );

                    CREATE INDEX browse_entry_snapshot_access_idx
                    ON browse_entry (snapshot_max_publish_seq, last_accessed_at_utc, request_key);

                    PRAGMA user_version = 1;
                    ",
                )
                .context("unable to initialize browse-cache schema")?;
            }
            BROWSE_CACHE_SCHEMA_VERSION => {}
            other => bail!("unsupported browse-cache schema version {other}"),
        }

        Ok(Self { conn, max_bytes })
    }

    fn prune_superseded_snapshots(&mut self, current_snapshot_max_publish_seq: u64) -> Result<()> {
        let snapshot_max_publish_seq = i64::try_from(current_snapshot_max_publish_seq)
            .context("browse-cache snapshot publish_seq overflowed i64")?;
        self.conn
            .execute(
                "
                DELETE FROM browse_entry
                WHERE snapshot_max_publish_seq < ?1
                ",
                [snapshot_max_publish_seq],
            )
            .context("unable to prune superseded browse-cache snapshots")?;
        Ok(())
    }

    fn enforce_budget(&mut self, current_snapshot_max_publish_seq: u64) -> Result<()> {
        let snapshot_max_publish_seq = i64::try_from(current_snapshot_max_publish_seq)
            .context("browse-cache snapshot publish_seq overflowed i64")?;
        loop {
            let total_bytes = self
                .conn
                .query_row(
                    "
                    SELECT COALESCE(SUM(payload_bytes), 0)
                    FROM browse_entry
                    WHERE snapshot_max_publish_seq = ?1
                    ",
                    [snapshot_max_publish_seq],
                    |row| row.get::<_, i64>(0),
                )
                .context("unable to measure browse-cache payload bytes")?;
            let total_bytes =
                u64::try_from(total_bytes).context("browse-cache payload bytes overflowed")?;
            if total_bytes <= self.max_bytes {
                return Ok(());
            }

            let oldest_key = self
                .conn
                .query_row(
                    "
                    SELECT request_key
                    FROM browse_entry
                    WHERE snapshot_max_publish_seq = ?1
                    ORDER BY last_accessed_at_utc ASC, request_key ASC
                    LIMIT 1
                    ",
                    [snapshot_max_publish_seq],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .context("unable to select browse-cache eviction candidate")?;
            let Some(oldest_key) = oldest_key else {
                return Ok(());
            };
            self.delete_entry(snapshot_max_publish_seq, &oldest_key)?;
        }
    }

    fn delete_entry(&mut self, snapshot_max_publish_seq: i64, request_key: &str) -> Result<()> {
        self.conn
            .execute(
                "
                DELETE FROM browse_entry
                WHERE snapshot_max_publish_seq = ?1
                  AND request_key = ?2
                ",
                params![snapshot_max_publish_seq, request_key],
            )
            .context("unable to delete browse-cache entry")?;
        Ok(())
    }
}

pub fn default_browse_cache_path(state_dir: &Path) -> PathBuf {
    state_dir.join(DEFAULT_BROWSE_CACHE_FILENAME)
}

pub fn reset_browse_cache(path: impl AsRef<Path>) -> Result<ResetReport> {
    reset_sqlite_database(path)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedBrowseRequest {
    root: RootView,
    lens: MetricLens,
    filters: BrowseFilters,
    path: BrowsePath,
}

impl From<&BrowseRequest> for PersistedBrowseRequest {
    fn from(request: &BrowseRequest) -> Self {
        Self {
            root: request.root,
            lens: request.lens,
            filters: request.filters.clone(),
            path: request.path.clone(),
        }
    }
}

fn persisted_request_key(request: &BrowseRequest) -> Result<String> {
    serde_json::to_string(&PersistedBrowseRequest::from(request))
        .context("unable to serialize browse-cache request key")
}

fn snapshot_publish_seq(request: &BrowseRequest) -> Result<i64> {
    i64::try_from(request.snapshot.max_publish_seq)
        .context("browse-cache snapshot publish_seq overflowed i64")
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tempfile::tempdir;

    use super::{BrowseCacheStats, BrowseCacheStore};
    use crate::query::{
        ActionKey, BrowseFilters, BrowsePath, BrowseRequest, ClassificationState, MetricIndicators,
        MetricLens, MetricTotals, RollupRow, RollupRowKind, RootView, SnapshotBounds,
    };

    #[test]
    fn browse_cache_round_trips_rows_for_same_snapshot() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("browse-cache.sqlite3");
        let mut store = BrowseCacheStore::open(&path)?;
        let request = sample_request(7, BrowsePath::Root);
        let rows = vec![sample_row("project-a")];

        store.store(&request, &rows)?;
        let loaded = store.load(&request)?;
        let stats = store.stats()?;

        assert_eq!(loaded, Some(rows));
        assert_eq!(
            stats,
            BrowseCacheStats {
                entry_count: 1,
                total_payload_bytes: stats.total_payload_bytes,
            }
        );
        Ok(())
    }

    #[test]
    fn browse_cache_prunes_superseded_snapshots_on_newer_store() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("browse-cache.sqlite3");
        let mut store = BrowseCacheStore::open(&path)?;
        let old_request = sample_request(1, BrowsePath::Root);
        let new_request = sample_request(2, BrowsePath::Project { project_id: 42 });

        store.store(&old_request, &[sample_row("old")])?;
        store.store(&new_request, &[sample_row("new")])?;

        assert_eq!(store.load(&old_request)?, None);
        assert_eq!(store.load(&new_request)?, Some(vec![sample_row("new")]));
        assert_eq!(store.stats()?.entry_count, 1);
        Ok(())
    }

    #[test]
    fn browse_cache_evicts_oldest_entries_when_budget_is_exceeded() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("browse-cache.sqlite3");
        let request_a = sample_request(4, BrowsePath::Project { project_id: 1 });
        let request_b = sample_request(4, BrowsePath::Project { project_id: 2 });
        let rows_a = vec![sample_row("alpha")];
        let rows_b = vec![sample_row("beta")];
        let payload_a = u64::try_from(serde_json::to_string(&rows_a)?.len())?;
        let payload_b = u64::try_from(serde_json::to_string(&rows_b)?.len())?;
        let budget = payload_a.max(payload_b) + 1;
        let mut store = BrowseCacheStore::open_with_max_bytes(&path, budget)?;

        store.store(&request_a, &rows_a)?;
        store.store(&request_b, &rows_b)?;

        let loaded_a = store.load(&request_a)?;
        let loaded_b = store.load(&request_b)?;

        assert!(loaded_a.is_none() || loaded_b.is_none());
        assert!(loaded_a.is_some() || loaded_b.is_some());
        assert!(store.stats()?.total_payload_bytes <= budget);
        Ok(())
    }

    fn sample_request(snapshot_max_publish_seq: u64, path: BrowsePath) -> BrowseRequest {
        BrowseRequest {
            snapshot: SnapshotBounds {
                max_publish_seq: snapshot_max_publish_seq,
                published_chunk_count: 1,
                upper_bound_utc: Some("2026-03-30T12:00:00Z".to_string()),
            },
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path,
        }
    }

    fn sample_row(label: &str) -> RollupRow {
        RollupRow {
            kind: RollupRowKind::Project,
            key: format!("project:{label}"),
            label: label.to_string(),
            metrics: MetricTotals {
                uncached_input: 12.0,
                cached_input: 0.0,
                gross_input: 12.0,
                output: 3.0,
                total: 15.0,
            },
            indicators: MetricIndicators {
                selected_lens_last_5_hours: 12.0,
                selected_lens_last_week: 12.0,
                uncached_input_reference: 12.0,
            },
            item_count: 1,
            opportunities: Default::default(),
            project_id: Some(1),
            project_identity: None,
            category: None,
            action: Some(ActionKey {
                classification_state: ClassificationState::Classified,
                normalized_action: Some("editing".to_string()),
                command_family: None,
                base_command: None,
            }),
            full_path: Some(format!("/tmp/{label}")),
        }
    }
}
