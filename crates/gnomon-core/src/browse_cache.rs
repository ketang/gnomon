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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedCacheResetReport {
    pub database: ResetReport,
    pub browse_cache: ResetReport,
}

impl DerivedCacheResetReport {
    pub fn removed_path_count(&self) -> usize {
        self.database.removed_paths.len() + self.browse_cache.removed_paths.len()
    }
}

pub fn reset_derived_cache_artifacts(
    db_path: impl AsRef<Path>,
    state_dir: impl AsRef<Path>,
) -> Result<DerivedCacheResetReport> {
    let browse_cache_path = default_browse_cache_path(state_dir.as_ref());

    Ok(DerivedCacheResetReport {
        // Clear the sidecar first so a later database-reset failure does not
        // leave stale persisted browse rows attached to an unchanged database.
        browse_cache: reset_browse_cache(&browse_cache_path)?,
        database: reset_sqlite_database(db_path)?,
    })
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

    use super::{
        BrowseCacheStats, BrowseCacheStore, default_browse_cache_path,
        reset_derived_cache_artifacts,
    };
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

    #[test]
    fn reset_derived_cache_artifacts_removes_database_and_browse_cache() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let browse_cache_path = default_browse_cache_path(temp.path());
        std::fs::write(&db_path, "db")?;
        std::fs::write(&browse_cache_path, "browse-cache")?;

        let report = reset_derived_cache_artifacts(&db_path, temp.path())?;

        assert_eq!(report.removed_path_count(), 2);
        assert!(!db_path.exists());
        assert!(!browse_cache_path.exists());
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
            provider_scope: crate::query::ProviderScope::Claude,
            opportunities: Default::default(),
            skill_attribution: None,
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

    // --- footprint measurement helpers and report ---

    use crate::query::ProjectIdentity;

    const CATEGORIES: &[&str] = &["coding", "review", "testing", "debugging", "documentation"];
    const ACTIONS: &[&str] = &["editing", "reading", "searching", "refactoring"];
    const PATH_PREFIXES: &[&str] = &[
        "src/components/auth/middleware",
        "src/services/api/handlers",
        "src/models/database/migrations",
        "lib/utils/formatting",
        "tests/integration/e2e",
    ];
    const FILE_NAMES: &[&str] = &["handler.rs", "mod.rs", "config.rs", "tests.rs", "types.rs"];

    fn project_row(id: i64, name: &str) -> RollupRow {
        RollupRow {
            kind: RollupRowKind::Project,
            key: format!("project:{id}"),
            label: name.to_string(),
            metrics: MetricTotals {
                uncached_input: 45_000.0,
                cached_input: 12_000.0,
                gross_input: 57_000.0,
                output: 8_500.0,
                total: 65_500.0,
            },
            indicators: MetricIndicators {
                selected_lens_last_5_hours: 15_000.0,
                selected_lens_last_week: 45_000.0,
                uncached_input_reference: 45_000.0,
            },
            item_count: 42,
            provider_scope: crate::query::ProviderScope::Claude,
            opportunities: Default::default(),
            skill_attribution: None,
            project_id: Some(id),
            project_identity: Some(ProjectIdentity {
                identity_kind: "git_origin".to_string(),
                root_path: format!("/home/user/projects/{name}"),
                git_root_path: Some(format!("/home/user/projects/{name}")),
                git_origin: Some(format!("git@github.com:user/{name}.git")),
                identity_reason: Some("git remote origin".to_string()),
            }),
            category: None,
            action: None,
            full_path: None,
        }
    }

    fn category_row(cat: &str) -> RollupRow {
        RollupRow {
            kind: RollupRowKind::ActionCategory,
            key: format!("category:{cat}"),
            label: cat.to_string(),
            metrics: MetricTotals {
                uncached_input: 20_000.0,
                cached_input: 5_000.0,
                gross_input: 25_000.0,
                output: 4_000.0,
                total: 29_000.0,
            },
            indicators: MetricIndicators {
                selected_lens_last_5_hours: 8_000.0,
                selected_lens_last_week: 20_000.0,
                uncached_input_reference: 20_000.0,
            },
            item_count: 15,
            provider_scope: crate::query::ProviderScope::Claude,
            opportunities: Default::default(),
            skill_attribution: None,
            project_id: None,
            project_identity: None,
            category: Some(cat.to_string()),
            action: None,
            full_path: None,
        }
    }

    fn action_row(cat: &str, action: &str) -> RollupRow {
        RollupRow {
            kind: RollupRowKind::Action,
            key: format!("action:{cat}:{action}"),
            label: action.to_string(),
            metrics: MetricTotals {
                uncached_input: 10_000.0,
                cached_input: 2_500.0,
                gross_input: 12_500.0,
                output: 2_000.0,
                total: 14_500.0,
            },
            indicators: MetricIndicators {
                selected_lens_last_5_hours: 4_000.0,
                selected_lens_last_week: 10_000.0,
                uncached_input_reference: 10_000.0,
            },
            item_count: 8,
            provider_scope: crate::query::ProviderScope::Claude,
            opportunities: Default::default(),
            skill_attribution: None,
            project_id: None,
            project_identity: None,
            category: Some(cat.to_string()),
            action: Some(ActionKey {
                classification_state: ClassificationState::Classified,
                normalized_action: Some(action.to_string()),
                command_family: Some("editor".to_string()),
                base_command: Some("vim".to_string()),
            }),
            full_path: None,
        }
    }

    fn path_row(kind: RollupRowKind, dir: &str, file: &str) -> RollupRow {
        let full = format!("{dir}/{file}");
        RollupRow {
            kind,
            key: format!("path:{full}"),
            label: file.to_string(),
            metrics: MetricTotals {
                uncached_input: 3_000.0,
                cached_input: 800.0,
                gross_input: 3_800.0,
                output: 600.0,
                total: 4_400.0,
            },
            indicators: MetricIndicators {
                selected_lens_last_5_hours: 1_200.0,
                selected_lens_last_week: 3_000.0,
                uncached_input_reference: 3_000.0,
            },
            item_count: 2,
            provider_scope: crate::query::ProviderScope::Claude,
            opportunities: Default::default(),
            skill_attribution: None,
            project_id: Some(1),
            project_identity: None,
            category: Some("coding".to_string()),
            action: Some(ActionKey {
                classification_state: ClassificationState::Classified,
                normalized_action: Some("editing".to_string()),
                command_family: Some("editor".to_string()),
                base_command: Some("vim".to_string()),
            }),
            full_path: Some(full),
        }
    }

    fn json_bytes(rows: &[RollupRow]) -> usize {
        serde_json::to_string(rows).expect("serialize").len()
    }

    #[test]
    #[ignore] // Run with: cargo test -p gnomon-core footprint_report -- --ignored --nocapture
    fn footprint_report() {
        // --- per-entry sizes by level ---
        let project_names = [
            "gnomon",
            "claude-code",
            "anthropic-sdk",
            "web-dashboard",
            "data-pipeline",
            "ml-training",
            "infra-deploy",
            "docs-site",
            "mobile-app",
            "shared-lib",
        ];

        let root_rows: Vec<RollupRow> = project_names
            .iter()
            .enumerate()
            .map(|(i, name)| project_row(i as i64 + 1, name))
            .collect();

        let cat_rows: Vec<RollupRow> = CATEGORIES.iter().map(|c| category_row(c)).collect();

        let action_rows: Vec<RollupRow> = ACTIONS.iter().map(|a| action_row("coding", a)).collect();

        let dir_rows: Vec<RollupRow> = PATH_PREFIXES
            .iter()
            .flat_map(|prefix| {
                FILE_NAMES
                    .iter()
                    .map(move |file| path_row(RollupRowKind::File, prefix, file))
            })
            .collect();

        // Sub-path rows (depth-1 children of a directory)
        let subdir_rows: Vec<RollupRow> = (0..10)
            .map(|i| {
                path_row(
                    RollupRowKind::File,
                    "src/components/auth/middleware/session/validators",
                    &format!("validator_{i}.rs"),
                )
            })
            .collect();

        let root_bytes = json_bytes(&root_rows);
        let cat_bytes = json_bytes(&cat_rows);
        let action_bytes = json_bytes(&action_rows);
        let dir_bytes = json_bytes(&dir_rows);
        let subdir_bytes = json_bytes(&subdir_rows);

        let root_per_entry = root_bytes / root_rows.len();
        let cat_per_entry = cat_bytes / cat_rows.len();
        let action_per_entry = action_bytes / action_rows.len();
        let dir_per_entry = dir_bytes / dir_rows.len();
        let subdir_per_entry = subdir_bytes / subdir_rows.len();

        println!("\n========================================");
        println!("  Browse Cache Footprint Report");
        println!("========================================\n");

        println!("## Per-Entry Payload Sizes (JSON serialized)\n");
        println!("| Level               | Rows | Total bytes | Bytes/entry |");
        println!("|---------------------|------|-------------|-------------|");
        println!(
            "| Root (projects)     | {:>4} | {:>11} | {:>11} |",
            root_rows.len(),
            root_bytes,
            root_per_entry
        );
        println!(
            "| Category            | {:>4} | {:>11} | {:>11} |",
            cat_rows.len(),
            cat_bytes,
            cat_per_entry
        );
        println!(
            "| Action              | {:>4} | {:>11} | {:>11} |",
            action_rows.len(),
            action_bytes,
            action_per_entry
        );
        println!(
            "| Path (dir/file)     | {:>4} | {:>11} | {:>11} |",
            dir_rows.len(),
            dir_bytes,
            dir_per_entry
        );
        println!(
            "| Sub-path (depth 1+) | {:>4} | {:>11} | {:>11} |",
            subdir_rows.len(),
            subdir_bytes,
            subdir_per_entry
        );

        // --- per-cache-entry sizes (one store() call = one JSON array) ---
        // Each cache entry is the JSON for the full Vec<RollupRow> for that parent.
        println!("\n## Per-Cache-Entry Sizes (one store() = one parent's children)\n");
        println!("| Entry type                     | Rows/entry | Bytes/entry |");
        println!("|--------------------------------|------------|-------------|");
        println!(
            "| Root → projects                | {:>10} | {:>11} |",
            root_rows.len(),
            root_bytes
        );
        println!(
            "| Project → categories           | {:>10} | {:>11} |",
            cat_rows.len(),
            cat_bytes
        );
        println!(
            "| Category → actions             | {:>10} | {:>11} |",
            action_rows.len(),
            action_bytes
        );
        println!(
            "| Action → paths (typical)       | {:>10} | {:>11} |",
            15,
            dir_per_entry * 15
        );
        println!(
            "| Path → sub-paths (depth 1)     | {:>10} | {:>11} |",
            subdir_rows.len(),
            subdir_bytes
        );

        // --- projection for different corpus sizes and depths ---
        println!("\n## Projected Total Cache Footprint\n");
        println!(
            "Assumptions: each project has {} categories, each category has {} actions,",
            CATEGORIES.len(),
            ACTIONS.len()
        );
        println!("each action has ~15 path children, each path has ~5 sub-paths at depth 1,");
        println!("each sub-path has ~3 sub-paths at depth 2.\n");

        for &num_projects in &[5usize, 10, 20, 50] {
            let num_categories = CATEGORIES.len();
            let num_actions = ACTIONS.len();
            let paths_per_action: usize = 15;
            let subpaths_depth1: usize = 5;
            let subpaths_depth2: usize = 3;

            // Grouped entries (root + project + category + action levels)
            let root_entry_count: usize = 1;
            let project_entries = num_projects;
            let category_entries = num_projects * num_categories;
            let action_entries = num_projects * num_categories * num_actions;

            let grouped_entries =
                root_entry_count + project_entries + category_entries + action_entries;

            let grouped_bytes = root_bytes
                + project_entries * cat_bytes
                + category_entries * action_bytes
                + action_entries * (dir_per_entry * paths_per_action);

            // Path entries (depth 0 = action children already counted above)
            let path_entries_d0 = num_projects * num_categories * num_actions * paths_per_action;
            let path_entries_d1 = path_entries_d0 * subpaths_depth1;
            let path_entries_d2 = path_entries_d1 * subpaths_depth2;

            let path_bytes_d0 = path_entries_d0 * (subdir_per_entry * subpaths_depth1);
            let path_bytes_d1 = path_entries_d1 * (subdir_per_entry * subpaths_depth2);

            // Depth 0: grouped only (no recursive path prefetch)
            let total_d0_entries = grouped_entries;
            let total_d0_bytes = grouped_bytes;

            // Depth 1: grouped + path depth-0 children
            let total_d1_entries = grouped_entries + path_entries_d0;
            let total_d1_bytes = grouped_bytes + path_bytes_d0;

            // Depth 2: grouped + depth-0 + depth-1 children
            let total_d2_entries = grouped_entries + path_entries_d0 + path_entries_d1;
            let total_d2_bytes = grouped_bytes + path_bytes_d0 + path_bytes_d1;

            let _ = path_entries_d2; // acknowledged but not stored at depth 2

            println!("### {} projects\n", num_projects);
            println!("| Depth | Cache entries | Payload bytes |    MiB |");
            println!("|-------|---------------|---------------|--------|");
            println!(
                "| No recursion | {:>13} | {:>13} | {:>6.2} |",
                total_d0_entries,
                total_d0_bytes,
                total_d0_bytes as f64 / (1024.0 * 1024.0)
            );
            println!(
                "| Depth 1      | {:>13} | {:>13} | {:>6.2} |",
                total_d1_entries,
                total_d1_bytes,
                total_d1_bytes as f64 / (1024.0 * 1024.0)
            );
            println!(
                "| Depth 2      | {:>13} | {:>13} | {:>6.2} |",
                total_d2_entries,
                total_d2_bytes,
                total_d2_bytes as f64 / (1024.0 * 1024.0)
            );
            println!();
        }

        // --- actual store/measure with BrowseCacheStore ---
        println!("## Actual SQLite Store Measurement\n");
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("footprint-test.sqlite3");
        let mut store =
            BrowseCacheStore::open_with_max_bytes(&db_path, u64::MAX).expect("open store");
        let snapshot_seq = 100u64;

        // Store representative entries
        let entries: Vec<(BrowsePath, Vec<RollupRow>)> = vec![
            (BrowsePath::Root, root_rows.clone()),
            (BrowsePath::Project { project_id: 1 }, cat_rows.clone()),
            (
                BrowsePath::ProjectCategory {
                    project_id: 1,
                    category: "coding".to_string(),
                },
                action_rows.clone(),
            ),
            (
                BrowsePath::ProjectAction {
                    project_id: 1,
                    category: "coding".to_string(),
                    action: ActionKey {
                        classification_state: ClassificationState::Classified,
                        normalized_action: Some("editing".to_string()),
                        command_family: Some("editor".to_string()),
                        base_command: Some("vim".to_string()),
                    },
                    parent_path: None,
                },
                dir_rows.clone(),
            ),
        ];

        for (path, rows) in &entries {
            let request = sample_request(snapshot_seq, path.clone());
            store.store(&request, rows).expect("store");
        }

        let stats = store.stats().expect("stats");
        let db_file_size = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);

        println!("| Metric              | Value        |");
        println!("|---------------------|--------------|");
        println!("| Cache entries       | {:>12} |", stats.entry_count);
        println!(
            "| Total payload bytes | {:>12} |",
            stats.total_payload_bytes
        );
        println!("| SQLite file size    | {:>12} |", db_file_size);
        println!(
            "| Overhead ratio      | {:>11.2}x |",
            db_file_size as f64 / stats.total_payload_bytes as f64
        );

        println!("\n## Recommendations\n");
        println!("See docs/browse-cache-footprint.md for full analysis.");
    }
}
