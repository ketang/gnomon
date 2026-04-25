# RTK Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Import RTK token-savings data at chunk-import time and surface per-project, per-action savings as conditional columns in the TUI and report output.

**Architecture:** At the end of each `project x day` chunk import, open RTK's `~/.local/share/rtk/history.db` read-only, load its rows for the project + day into a sorted Vec, then walk a cursor through that Vec in step with the chunk's Bash actions (matched by `original_cmd` + timestamp window). Matched savings are written to a new `action_rtk_match` FK table and rolled up into `chunk_action_rollup.rtk_saved_tokens`. The TUI renders "RTK saved" and "gross (w/RTK)" columns only when the current view has non-zero savings.

**Tech Stack:** Rust, rusqlite (already a dependency), serde/toml for config, existing migration framework.

**Spec:** `docs/specs/2026-04-18-rtk-integration-design.md`

---

## File Map

| File | Change |
|---|---|
| `crates/gnomon-core/src/db/migrations/0015_action_rtk_match.sql` | Create |
| `crates/gnomon-core/src/db/migrations.rs` | Add migration registration |
| `crates/gnomon-core/src/config.rs` | Add `RtkConfig` struct + `[rtk]` section |
| `crates/gnomon-core/src/import/rtk.rs` | Create — RTK db reader + cursor algorithm |
| `crates/gnomon-core/src/import/chunk.rs` | Thread `RtkConfig` through `ImportWorkerOptions`; call match phase in `finalize_chunk_import_core`; expose new public API |
| `crates/gnomon-core/src/import/mod.rs` | Declare `mod rtk`; re-export new public function |
| `crates/gnomon-core/src/rollup.rs` | Add `rtk_saved_tokens` to rollup INSERT SQL |
| `crates/gnomon-core/src/query/mod.rs` | Add `rtk_saved_tokens` to `MetricTotals`, `LoadedGroupedActionRollupRow`, all rollup queries |
| `crates/gnomon-tui/src/app.rs` | Add `RtkSaved` + `GrossWithRtk` optional columns; conditional display |
| `crates/gnomon/src/main.rs` | Pass `RtkConfig` to import entry points |

---

## Task 1: Migration — `action_rtk_match` + `chunk_action_rollup.rtk_saved_tokens`

**Files:**
- Create: `crates/gnomon-core/src/db/migrations/0015_action_rtk_match.sql`
- Modify: `crates/gnomon-core/src/db/migrations.rs`

- [ ] **Step 1: Write the migration SQL**

Create `crates/gnomon-core/src/db/migrations/0015_action_rtk_match.sql`:

```sql
CREATE TABLE action_rtk_match (
    action_id    INTEGER PRIMARY KEY REFERENCES action(id) ON DELETE CASCADE,
    rtk_row_id   INTEGER NOT NULL,
    saved_tokens INTEGER NOT NULL CHECK (saved_tokens >= 0),
    savings_pct  REAL    NOT NULL,
    exec_time_ms INTEGER NOT NULL CHECK (exec_time_ms >= 0)
);

ALTER TABLE chunk_action_rollup
    ADD COLUMN rtk_saved_tokens INTEGER NOT NULL DEFAULT 0
        CHECK (rtk_saved_tokens >= 0);
```

- [ ] **Step 2: Register the migration**

In `crates/gnomon-core/src/db/migrations.rs`, add the new `M::up(...)` line as the last entry in the `vec!`:

```rust
M::up(include_str!("migrations/0015_action_rtk_match.sql")),
```

- [ ] **Step 3: Verify migration applies**

```bash
cargo build -p gnomon-core 2>&1
```

Expected: compiles without errors.

- [ ] **Step 4: Commit**

```bash
git add crates/gnomon-core/src/db/migrations/0015_action_rtk_match.sql \
        crates/gnomon-core/src/db/migrations.rs
git commit -m "feat: migration 0015 — action_rtk_match table + rtk_saved_tokens rollup column"
```

---

## Task 2: Config — `RtkConfig` struct

**Files:**
- Modify: `crates/gnomon-core/src/config.rs`

- [ ] **Step 1: Write a failing test**

At the bottom of `crates/gnomon-core/src/config.rs`, in the `#[cfg(test)]` block, add:

```rust
#[test]
fn rtk_config_defaults_to_enabled_with_standard_db_path() {
    let cfg = RtkConfig::default();
    assert!(cfg.enabled);
    assert_eq!(cfg.pre_slack_ms, 2000);
    assert_eq!(cfg.post_slack_ms, 30000);
    // db_path default is checked at resolve time, not stored as PathBuf
}

#[test]
fn file_config_with_rtk_section_parses_correctly() {
    let toml = r#"
[rtk]
enabled = false
pre_slack_ms = 5000
"#;
    let file_cfg: FileConfig = toml::from_str(toml).unwrap();
    assert!(!file_cfg.rtk.enabled);
    assert_eq!(file_cfg.rtk.pre_slack_ms, 5000);
    assert_eq!(file_cfg.rtk.post_slack_ms, 30000); // default
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p gnomon-core config 2>&1 | tail -20
```

Expected: compile error — `RtkConfig` not defined.

- [ ] **Step 3: Add `RtkConfig` struct**

In `crates/gnomon-core/src/config.rs`, after the `ProjectFilterRule` struct, add:

```rust
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct RtkConfig {
    #[serde(default = "RtkConfig::default_enabled")]
    pub enabled: bool,
    #[serde(default = "RtkConfig::default_db_path")]
    pub db_path: String,
    #[serde(default = "RtkConfig::default_pre_slack_ms")]
    pub pre_slack_ms: u64,
    #[serde(default = "RtkConfig::default_post_slack_ms")]
    pub post_slack_ms: u64,
}

impl RtkConfig {
    fn default_enabled() -> bool { true }
    fn default_db_path() -> String {
        "~/.local/share/rtk/history.db".to_string()
    }
    fn default_pre_slack_ms() -> u64 { 2000 }
    fn default_post_slack_ms() -> u64 { 30000 }

    pub fn resolved_db_path(&self) -> Result<std::path::PathBuf> {
        expand_user_path(&self.db_path)
    }
}

impl Default for RtkConfig {
    fn default() -> Self {
        Self {
            enabled: Self::default_enabled(),
            db_path: Self::default_db_path(),
            pre_slack_ms: Self::default_pre_slack_ms(),
            post_slack_ms: Self::default_post_slack_ms(),
        }
    }
}
```

- [ ] **Step 4: Add to `FileConfig` and `RuntimeConfig`**

In `FileConfig` struct, add:
```rust
#[serde(default)]
rtk: RtkConfig,
```

In `RuntimeConfig` struct, add:
```rust
pub rtk: RtkConfig,
```

In `RuntimeConfig::load()`, in the `Ok(Self { ... })` block, add:
```rust
rtk: file_config.rtk,
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cargo test -p gnomon-core config 2>&1 | tail -20
```

Expected: all config tests pass.

- [ ] **Step 6: Quality gates**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -20
```

- [ ] **Step 7: Commit**

```bash
git add crates/gnomon-core/src/config.rs
git commit -m "feat: RtkConfig struct with enabled/db_path/slack defaults"
```

---

## Task 3: RTK reader module

**Files:**
- Create: `crates/gnomon-core/src/import/rtk.rs`
- Modify: `crates/gnomon-core/src/import/mod.rs`

- [ ] **Step 1: Write failing tests**

Create `crates/gnomon-core/src/import/rtk.rs` with test scaffolding:

```rust
use std::path::Path;
use anyhow::Result;
use rusqlite::{Connection, OpenFlags, params};

use crate::config::RtkConfig;

/// One RTK command row loaded from RTK's history.db.
#[derive(Debug)]
pub(crate) struct RtkRow {
    pub rtk_row_id: i64,
    pub timestamp_utc: String,
    pub original_cmd: String,
    pub saved_tokens: i64,
    pub savings_pct: f64,
    pub exec_time_ms: i64,
}

/// One match between a gnomon action and an RTK row.
#[derive(Debug)]
pub(crate) struct RtkMatch {
    pub action_id: i64,
    pub rtk_row_id: i64,
    pub saved_tokens: i64,
    pub savings_pct: f64,
    pub exec_time_ms: i64,
}

/// Runs the RTK match phase for a single import chunk.
///
/// Queries gnomon's DB for Bash actions in the chunk, loads the matching RTK
/// rows from RTK's db, runs the cursor algorithm, and batch-inserts results
/// into `action_rtk_match`.
///
/// Silently no-ops if RTK's db does not exist or `rtk_config.enabled` is false.
pub(crate) fn match_rtk_savings(
    conn: &Connection,
    import_chunk_id: i64,
    project_root_path: &str,
    chunk_day_local: &str,
    rtk_config: &RtkConfig,
) -> Result<()> {
    if !rtk_config.enabled {
        return Ok(());
    }
    let rtk_db_path = rtk_config.resolved_db_path()?;
    if !rtk_db_path.exists() {
        return Ok(());
    }

    let bash_actions = load_bash_actions(conn, import_chunk_id)?;
    if bash_actions.is_empty() {
        return Ok(());
    }

    let chunk_date = &chunk_day_local[..10]; // YYYY-MM-DD
    let rtk_rows = load_rtk_rows(&rtk_db_path, project_root_path, chunk_date)?;
    if rtk_rows.is_empty() {
        return Ok(());
    }

    let matches = run_cursor(
        &bash_actions,
        &rtk_rows,
        rtk_config.pre_slack_ms,
        rtk_config.post_slack_ms,
    );

    insert_matches(conn, &matches)
}

/// A Bash action row loaded from gnomon's DB.
#[derive(Debug)]
pub(crate) struct BashAction {
    pub action_id: i64,
    pub command: String,
    pub started_at_utc: String,
    pub completed_at_utc: String,
}

fn load_bash_actions(conn: &Connection, import_chunk_id: i64) -> Result<Vec<BashAction>> {
    let mut stmt = conn.prepare_cached(
        "
        SELECT
            a.id,
            json_extract(mp.metadata_json, '$.input.command'),
            a.started_at_utc,
            a.ended_at_utc
        FROM action a
        JOIN action_message am ON am.action_id = a.id
        JOIN message m ON m.id = am.message_id
        JOIN message_part mp ON mp.message_id = m.id
            AND mp.tool_name = 'Bash'
            AND mp.part_kind = 'tool_use'
            AND mp.metadata_json IS NOT NULL
        WHERE a.import_chunk_id = ?1
          AND json_extract(mp.metadata_json, '$.input.command') IS NOT NULL
        GROUP BY a.id
        ORDER BY a.started_at_utc
        ",
    )?;
    let rows = stmt.query_map(params![import_chunk_id], |row| {
        Ok(BashAction {
            action_id: row.get(0)?,
            command: row.get(1)?,
            started_at_utc: row.get(2).unwrap_or_default(),
            completed_at_utc: row.get(3).unwrap_or_default(),
        })
    })?;
    rows.map(|r| r.map_err(anyhow::Error::from)).collect()
}

fn load_rtk_rows(
    rtk_db_path: &Path,
    project_root_path: &str,
    chunk_date: &str,
) -> Result<Vec<RtkRow>> {
    let rtk_conn = Connection::open_with_flags(rtk_db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let path_prefix = format!("{}/", project_root_path);
    let date_start = format!("{chunk_date}T00:00:00");
    let date_end = format!("{chunk_date}T23:59:59");
    let mut stmt = rtk_conn.prepare(
        "
        SELECT id, timestamp, original_cmd, saved_tokens, savings_pct, exec_time_ms
        FROM commands
        WHERE (project_path = ?1 OR project_path LIKE ?2)
          AND timestamp >= ?3
          AND timestamp <= ?4
        ORDER BY timestamp ASC
        ",
    )?;
    let rows = stmt.query_map(
        params![project_root_path, path_prefix, date_start, date_end],
        |row| {
            Ok(RtkRow {
                rtk_row_id: row.get(0)?,
                timestamp_utc: row.get(1)?,
                original_cmd: row.get(2)?,
                saved_tokens: row.get(3)?,
                savings_pct: row.get(4)?,
                exec_time_ms: row.get(5)?,
            })
        },
    )?;
    rows.map(|r| r.map_err(anyhow::Error::from)).collect()
}

/// Parses an ISO 8601 timestamp string into milliseconds since Unix epoch.
/// Returns 0 on parse failure so the row is still considered but never matches
/// a tight window.
fn parse_timestamp_ms(ts: &str) -> i64 {
    // RTK timestamps: "2026-04-18T20:30:01.347531794+00:00"
    // Gnomon timestamps: "2026-04-18T20:30:01.387Z"
    // Parse the first 19 chars as naive datetime and ignore sub-second + offset.
    let s = ts.get(..19).unwrap_or("");
    if s.len() < 19 {
        return 0;
    }
    let year: i64 = s[0..4].parse().unwrap_or(0);
    let month: i64 = s[5..7].parse().unwrap_or(0);
    let day: i64 = s[8..10].parse().unwrap_or(0);
    let hour: i64 = s[11..13].parse().unwrap_or(0);
    let min: i64 = s[14..16].parse().unwrap_or(0);
    let sec: i64 = s[17..19].parse().unwrap_or(0);
    // Days-since-epoch approximation (good enough for ±30s window comparisons)
    let days = (year - 1970) * 365 + (year - 1969) / 4
        + [0i64, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
            [(month.saturating_sub(1) as usize).min(11)]
        + day - 1;
    days * 86_400_000 + hour * 3_600_000 + min * 60_000 + sec * 1_000
}

pub(crate) fn run_cursor(
    bash_actions: &[BashAction],
    rtk_rows: &[RtkRow],
    pre_slack_ms: u64,
    post_slack_ms: u64,
) -> Vec<RtkMatch> {
    let pre_slack = pre_slack_ms as i64;
    let post_slack = post_slack_ms as i64;

    let mut matches = Vec::new();
    let mut cursor: usize = 0;
    let mut consumed: std::collections::HashSet<i64> = std::collections::HashSet::new();

    for action in bash_actions {
        let action_start_ms = parse_timestamp_ms(&action.started_at_utc);
        let action_end_ms = parse_timestamp_ms(&action.completed_at_utc);

        // Advance cursor past rows that are definitively too old.
        while cursor < rtk_rows.len() {
            let rtk_ms = parse_timestamp_ms(&rtk_rows[cursor].timestamp_utc);
            if rtk_ms >= action_start_ms - pre_slack {
                break;
            }
            cursor += 1;
        }

        // Scan forward from cursor looking for first unused match.
        let window_end_ms = action_end_ms + post_slack;
        for i in cursor..rtk_rows.len() {
            let row = &rtk_rows[i];
            let rtk_ms = parse_timestamp_ms(&row.timestamp_utc);
            if rtk_ms > window_end_ms {
                break;
            }
            if consumed.contains(&row.rtk_row_id) {
                continue;
            }
            if row.original_cmd == action.command {
                consumed.insert(row.rtk_row_id);
                matches.push(RtkMatch {
                    action_id: action.action_id,
                    rtk_row_id: row.rtk_row_id,
                    saved_tokens: row.saved_tokens,
                    savings_pct: row.savings_pct,
                    exec_time_ms: row.exec_time_ms,
                });
                break;
            }
        }
    }

    matches
}

fn insert_matches(conn: &Connection, matches: &[RtkMatch]) -> Result<()> {
    if matches.is_empty() {
        return Ok(());
    }
    let mut stmt = conn.prepare_cached(
        "
        INSERT OR IGNORE INTO action_rtk_match
            (action_id, rtk_row_id, saved_tokens, savings_pct, exec_time_ms)
        VALUES (?1, ?2, ?3, ?4, ?5)
        ",
    )?;
    for m in matches {
        stmt.execute(params![
            m.action_id,
            m.rtk_row_id,
            m.saved_tokens,
            m.savings_pct,
            m.exec_time_ms
        ])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_action(id: i64, cmd: &str, started: &str, ended: &str) -> BashAction {
        BashAction {
            action_id: id,
            command: cmd.to_string(),
            started_at_utc: started.to_string(),
            completed_at_utc: ended.to_string(),
        }
    }

    fn make_rtk(id: i64, cmd: &str, ts: &str, saved: i64) -> RtkRow {
        RtkRow {
            rtk_row_id: id,
            timestamp_utc: ts.to_string(),
            original_cmd: cmd.to_string(),
            saved_tokens: saved,
            savings_pct: 90.0,
            exec_time_ms: 100,
        }
    }

    #[test]
    fn cursor_matches_exact_command_within_window() {
        let actions = vec![make_action(
            1,
            "git status",
            "2026-04-18T10:00:00.000Z",
            "2026-04-18T10:00:01.000Z",
        )];
        let rtk_rows = vec![make_rtk(
            42,
            "git status",
            "2026-04-18T10:00:00.500+00:00",
            150,
        )];
        let result = run_cursor(&actions, &rtk_rows, 2000, 30000);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].action_id, 1);
        assert_eq!(result[0].rtk_row_id, 42);
        assert_eq!(result[0].saved_tokens, 150);
    }

    #[test]
    fn cursor_does_not_double_match_same_rtk_row() {
        let actions = vec![
            make_action(1, "git status", "2026-04-18T10:00:00.000Z", "2026-04-18T10:00:01.000Z"),
            make_action(2, "git status", "2026-04-18T10:00:01.500Z", "2026-04-18T10:00:02.000Z"),
        ];
        let rtk_rows = vec![make_rtk(
            42,
            "git status",
            "2026-04-18T10:00:00.500+00:00",
            150,
        )];
        let result = run_cursor(&actions, &rtk_rows, 2000, 30000);
        // Only the first action matches; second finds the row already consumed.
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].action_id, 1);
    }

    #[test]
    fn cursor_skips_command_mismatch() {
        let actions = vec![make_action(
            1,
            "cargo build",
            "2026-04-18T10:00:00.000Z",
            "2026-04-18T10:00:05.000Z",
        )];
        let rtk_rows = vec![make_rtk(
            42,
            "git status",
            "2026-04-18T10:00:01.000+00:00",
            150,
        )];
        let result = run_cursor(&actions, &rtk_rows, 2000, 30000);
        assert!(result.is_empty());
    }

    #[test]
    fn cursor_skips_rtk_row_outside_window() {
        let actions = vec![make_action(
            1,
            "git status",
            "2026-04-18T10:00:00.000Z",
            "2026-04-18T10:00:01.000Z",
        )];
        // RTK row is 60s after action ended — beyond 30s post_slack.
        let rtk_rows = vec![make_rtk(
            42,
            "git status",
            "2026-04-18T10:01:01.000+00:00",
            150,
        )];
        let result = run_cursor(&actions, &rtk_rows, 2000, 30000);
        assert!(result.is_empty());
    }
}
```

- [ ] **Step 2: Declare module in `mod.rs`**

In `crates/gnomon-core/src/import/mod.rs`, add after `mod chunk;`:

```rust
pub(crate) mod rtk;
```

- [ ] **Step 3: Run tests to verify they pass**

```bash
cargo test -p gnomon-core import::rtk 2>&1 | tail -20
```

Expected: all 4 cursor tests pass.

- [ ] **Step 4: Quality gates**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -20
```

- [ ] **Step 5: Commit**

```bash
git add crates/gnomon-core/src/import/rtk.rs \
        crates/gnomon-core/src/import/mod.rs
git commit -m "feat: RTK reader module with cursor matching algorithm"
```

---

## Task 4: Import integration — thread `RtkConfig` through import pipeline

**Files:**
- Modify: `crates/gnomon-core/src/import/chunk.rs`
- Modify: `crates/gnomon-core/src/import/mod.rs`
- Modify: `crates/gnomon/src/main.rs`

- [ ] **Step 1: Add `rtk_config` to `ImportWorkerOptions`**

In `crates/gnomon-core/src/import/chunk.rs`, update the `ImportWorkerOptions` struct (around line 65):

```rust
struct ImportWorkerOptions {
    per_chunk_delay: Duration,
    perf_logger: Option<PerfLogger>,
    rtk_config: Option<crate::config::RtkConfig>,
}
```

The `Default` derive will set `rtk_config: None` automatically since `Option<T>` implements `Default`.

- [ ] **Step 2: Call `match_rtk_savings` in `finalize_chunk_import_core`**

In `finalize_chunk_import_core` (around line 1469), after `rebuild_chunk_action_rollups` and before `rebuild_chunk_path_rollups`, add the RTK match call. The function needs `project_root_path`; query it from the DB:

```rust
fn finalize_chunk_import_core(
    conn: &Connection,
    chunk: &PreparedChunk,
    options: &ImportWorkerOptions,
) -> Result<()> {
    // ... existing source_file UPDATE loop ...

    recompute_chunk_counts(conn, chunk.import_chunk_id)?;
    rebuild_chunk_action_rollups(conn, chunk.import_chunk_id, options.perf_logger.clone())?;

    // RTK match phase — runs after actions are committed, before path rollups.
    if let Some(rtk_config) = &options.rtk_config {
        let project_root_path: Option<String> = conn
            .query_row(
                "SELECT root_path FROM project WHERE id = ?1",
                params![chunk.project_id],
                |row| row.get(0),
            )
            .ok();
        if let Some(root_path) = project_root_path {
            if let Err(e) = crate::import::rtk::match_rtk_savings(
                conn,
                chunk.import_chunk_id,
                &root_path,
                &chunk.chunk_day_local,
                rtk_config,
            ) {
                // RTK match failure is non-fatal; log and continue.
                tracing::warn!("RTK match phase failed for chunk {}: {e}", chunk.import_chunk_id);
            }
        }
    }

    rebuild_chunk_path_rollups(conn, chunk.import_chunk_id, options.perf_logger.clone())?;
    clear_pending_chunk_rebuild(conn, chunk.project_id, &chunk.chunk_day_local)?;
    publish_import_chunk(conn, chunk)?;

    Ok(())
}
```

**Note:** `tracing` is already a dependency of `gnomon-core`. If the project uses a different logging crate, check `Cargo.toml` and use the correct macro. If no logger is available, replace the `tracing::warn!` with an `eprintln!`.

- [ ] **Step 3: Add public API function `import_all_with_rtk`**

After `import_all_with_perf_logger` in `chunk.rs`, add:

```rust
pub fn import_all_with_rtk(
    conn: &Connection,
    db_path: &Path,
    source_root: &Path,
    rtk_config: Option<crate::config::RtkConfig>,
) -> Result<ImportExecutionReport> {
    let state_dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    let perf_logger = PerfLogger::from_env(state_dir).ok().flatten();
    let options = ImportWorkerOptions {
        perf_logger,
        rtk_config,
        ..ImportWorkerOptions::default()
    };
    let now = jiff::Timestamp::now();
    let mut prepared = prepare_import(conn, source_root, now)?;
    let plan = build_import_plan(conn, now, prepared.startup_chunks.len());
    run_import_plan(conn, &mut prepared, plan, &options)
}
```

Similarly, add `start_startup_import_with_rtk` for the TUI startup path:

```rust
pub fn start_startup_import_with_rtk<F>(
    conn: &Connection,
    db_path: &Path,
    source_root: &Path,
    import_mode: StartupImportMode,
    rtk_config: Option<crate::config::RtkConfig>,
    mut on_progress: F,
) -> Result<StartupImport>
where
    F: FnMut(&StartupProgressUpdate),
{
    let options = ImportWorkerOptions {
        rtk_config,
        ..ImportWorkerOptions::default()
    };
    start_startup_import_with_options(
        conn,
        db_path,
        source_root,
        Duration::from_secs(STARTUP_OPEN_DEADLINE_SECS),
        import_mode,
        options,
        Some(&mut on_progress),
    )
}
```

- [ ] **Step 4: Re-export new public functions from `mod.rs`**

In `crates/gnomon-core/src/import/mod.rs`, update the `pub use chunk::{ ... }` block to include:

```rust
pub use chunk::{
    ImportExecutionReport, StartupImport, StartupImportMode, StartupOpenReason,
    StartupProgressUpdate, StartupWorkerEvent, import_all, import_all_with_perf_logger,
    import_all_with_rtk, start_startup_import, start_startup_import_with_mode_and_progress,
    start_startup_import_with_perf_logger, start_startup_import_with_progress,
    start_startup_import_with_rtk,
};
```

- [ ] **Step 5: Update `main.rs` call sites**

In `crates/gnomon/src/main.rs`:

1. Update `import { ... }` use statement to include the two new functions.

2. Near line 506, where `start_startup_import_with_mode_and_progress` is called for the interactive TUI path, replace with:

```rust
let mut startup_import = start_startup_import_with_rtk(
    database.connection(),
    &config.db_path,
    &config.source_root,
    startup_mode,
    Some(config.rtk.clone()),
    |update| { /* existing progress callback */ },
)?;
```

3. Near line 576, where `start_startup_import` is called for the rebuild path, replace with:

```rust
start_startup_import_with_rtk(
    database.connection(),
    &config.db_path,
    &config.source_root,
    StartupImportMode::default(),
    Some(config.rtk.clone()),
    |_| {},
)?;
```

4. Near line 717, where `import_all` is called, replace with:

```rust
let import_report = import_all_with_rtk(
    database.connection(),
    &config.db_path,
    &config.source_root,
    Some(config.rtk.clone()),
)?;
```

- [ ] **Step 6: Build and verify**

```bash
cargo build --workspace 2>&1 | tail -20
```

Expected: clean build.

- [ ] **Step 7: Smoke test**

```bash
cargo run -p gnomon -- --help 2>&1 | head -5
```

Expected: help text prints.

- [ ] **Step 8: Quality gates**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -20
```

- [ ] **Step 9: Commit**

```bash
git add crates/gnomon-core/src/import/chunk.rs \
        crates/gnomon-core/src/import/mod.rs \
        crates/gnomon/src/main.rs
git commit -m "feat: thread RtkConfig through import pipeline; call RTK match phase in finalize"
```

---

## Task 5: Rollup SQL — sum `rtk_saved_tokens` into `chunk_action_rollup`

**Files:**
- Modify: `crates/gnomon-core/src/rollup.rs`

- [ ] **Step 1: Update `INSERT_CHUNK_ACTION_ROLLUPS_SQL`**

In `crates/gnomon-core/src/rollup.rs`, replace `INSERT_CHUNK_ACTION_ROLLUPS_SQL` with:

```rust
const INSERT_CHUNK_ACTION_ROLLUPS_SQL: &str = "
INSERT INTO chunk_action_rollup (
    import_chunk_id,
    display_category,
    classification_state,
    normalized_action,
    command_family,
    base_command,
    input_tokens,
    cache_creation_input_tokens,
    cache_read_input_tokens,
    output_tokens,
    action_count,
    rtk_saved_tokens
)
SELECT
    action.import_chunk_id,
    CASE
        WHEN action.category IS NOT NULL THEN action.category
        WHEN action.classification_state = 'mixed' THEN '[mixed]'
        WHEN action.classification_state = 'unclassified' THEN '[unclassified]'
        ELSE 'classified'
    END AS display_category,
    action.classification_state,
    action.normalized_action,
    action.command_family,
    action.base_command,
    COALESCE(SUM(COALESCE(action.input_tokens, 0)), 0),
    COALESCE(SUM(COALESCE(action.cache_creation_input_tokens, 0)), 0),
    COALESCE(SUM(COALESCE(action.cache_read_input_tokens, 0)), 0),
    COALESCE(SUM(COALESCE(action.output_tokens, 0)), 0),
    COUNT(*),
    COALESCE(SUM(COALESCE(arm.saved_tokens, 0)), 0)
FROM action
LEFT JOIN action_rtk_match arm ON arm.action_id = action.id
WHERE action.import_chunk_id = ?1
GROUP BY
    action.import_chunk_id,
    display_category,
    action.classification_state,
    action.normalized_action,
    action.command_family,
    action.base_command
";
```

- [ ] **Step 2: Run existing rollup tests**

```bash
cargo test -p gnomon-core rollup 2>&1 | tail -20
```

Expected: all rollup tests pass (the new column has `DEFAULT 0` so old test data still works).

- [ ] **Step 3: Quality gates**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -20
```

- [ ] **Step 4: Commit**

```bash
git add crates/gnomon-core/src/rollup.rs
git commit -m "feat: include rtk_saved_tokens in chunk_action_rollup INSERT"
```

---

## Task 6: Query layer — `MetricTotals.rtk_saved_tokens` + rollup queries

**Files:**
- Modify: `crates/gnomon-core/src/query/mod.rs`

- [ ] **Step 1: Write a failing test**

In `crates/gnomon-core/src/query/mod.rs`, in the `#[cfg(test)]` block, add:

```rust
#[test]
fn metric_totals_rtk_saved_tokens_defaults_to_zero() {
    let m = MetricTotals::zero();
    assert_eq!(m.rtk_saved_tokens, 0.0);
}

#[test]
fn metric_totals_add_assign_includes_rtk_saved_tokens() {
    let mut a = MetricTotals::zero();
    a.rtk_saved_tokens = 100.0;
    let b = MetricTotals { rtk_saved_tokens: 50.0, ..MetricTotals::zero() };
    a += b;
    assert_eq!(a.rtk_saved_tokens, 150.0);
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p gnomon-core "metric_totals_rtk" 2>&1 | tail -10
```

Expected: compile error — field not found.

- [ ] **Step 3: Add `rtk_saved_tokens` to `MetricTotals`**

In `crates/gnomon-core/src/query/mod.rs`, update `MetricTotals`:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricTotals {
    pub uncached_input: f64,
    pub cached_input: f64,
    pub gross_input: f64,
    pub output: f64,
    pub total: f64,
    #[serde(default)]
    pub rtk_saved_tokens: f64,
}
```

Update `MetricTotals::zero()`:
```rust
fn zero() -> Self {
    Self {
        uncached_input: 0.0,
        cached_input: 0.0,
        gross_input: 0.0,
        output: 0.0,
        total: 0.0,
        rtk_saved_tokens: 0.0,
    }
}
```

Update the `AddAssign` impl (find the `add_assign` method):
```rust
fn add_assign(&mut self, other: Self) {
    self.uncached_input += other.uncached_input;
    self.cached_input += other.cached_input;
    self.gross_input += other.gross_input;
    self.output += other.output;
    self.total += other.total;
    self.rtk_saved_tokens += other.rtk_saved_tokens;
}
```

Update `divided_by` method:
```rust
pub fn divided_by(&self, divisor: f64) -> Self {
    Self {
        uncached_input: self.uncached_input / divisor,
        cached_input: self.cached_input / divisor,
        gross_input: self.gross_input / divisor,
        output: self.output / divisor,
        total: self.total / divisor,
        rtk_saved_tokens: self.rtk_saved_tokens / divisor,
    }
}
```

Any inline `MetricTotals { uncached_input, cached_input, gross_input, output, total }` struct literal in the file (not from `from_usage`) must also get `rtk_saved_tokens: 0.0`. Search for them:

```bash
grep -n "MetricTotals {" crates/gnomon-core/src/query/mod.rs
```

Add `rtk_saved_tokens: 0.0` to each literal that doesn't already have it.

- [ ] **Step 4: Add `rtk_saved_tokens` to `LoadedGroupedActionRollupRow`**

```rust
struct LoadedGroupedActionRollupRow {
    // ... existing fields ...
    action_count: i64,
    rtk_saved_tokens: i64,  // ADD THIS
}
```

- [ ] **Step 5: Add column 17 to all three rollup query shapes**

In `build_grouped_action_rollup_rows_query`, each of the three `GroupedActionRollupShape` variants (Project, Category, Action) has a SELECT ending with `COALESCE(SUM(car.action_count), 0)`. After each one, add:

```sql
COALESCE(SUM(car.rtk_saved_tokens), 0)
```

For example, the Project shape becomes:
```sql
COALESCE(SUM(car.input_tokens), 0),
COALESCE(SUM(car.cache_creation_input_tokens), 0),
COALESCE(SUM(car.cache_read_input_tokens), 0),
COALESCE(SUM(car.output_tokens), 0),
COALESCE(SUM(car.action_count), 0),
COALESCE(SUM(car.rtk_saved_tokens), 0)
```

Do this for all three shapes.

Also update the batch prefetch query near line 4275 to add the same column.

- [ ] **Step 6: Parse column 17 in the row mapper**

In the `query_map` closure near line 1993, add after `action_count: row.get(16)?`:

```rust
rtk_saved_tokens: row.get(17)?,
```

- [ ] **Step 7: Set `rtk_saved_tokens` in `grouped_action_rollup_row_to_rollup_row`**

After `let metrics = MetricTotals::from_usage(...)`, add:

```rust
let mut metrics = MetricTotals::from_usage(
    row.input_tokens,
    row.cache_creation_input_tokens,
    row.cache_read_input_tokens,
    row.output_tokens,
);
metrics.rtk_saved_tokens = row.rtk_saved_tokens as f64;
```

- [ ] **Step 8: Run all query tests**

```bash
cargo test -p gnomon-core query 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 9: Quality gates**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -20
```

- [ ] **Step 10: Commit**

```rust
git add crates/gnomon-core/src/query/mod.rs
git commit -m "feat: add rtk_saved_tokens to MetricTotals and rollup queries"
```

---

## Task 7: TUI — `RtkSaved` and `GrossWithRtk` optional columns

**Files:**
- Modify: `crates/gnomon-tui/src/app.rs`

The TUI has `OptionalColumn` enum, `priority()`, `short_label()`, `optional_column_spec()`, `render_column_value()`, and `default_enabled_columns()`. Each needs updating. The columns are hidden when the view has no RTK data.

- [ ] **Step 1: Add variants to `OptionalColumn` enum**

Find the `enum OptionalColumn` definition and add two new variants at the end of the list:

```rust
RtkSaved,
GrossWithRtk,
```

- [ ] **Step 2: Add to `priority()`**

In the `priority()` method, add after the existing last entry:

```rust
Self::RtkSaved => 9,       // after Items (7) and opportunity columns
Self::GrossWithRtk => 10,
```

Adjust the numbers to fit after the last existing entry without colliding.

- [ ] **Step 3: Add to `short_label()`**

```rust
Self::RtkSaved => "rtk saved",
Self::GrossWithRtk => "gross+rtk",
```

- [ ] **Step 4: Add to `optional_column_spec()`**

```rust
OptionalColumn::RtkSaved => ColumnSpec {
    key: ColumnKey::Optional(OptionalColumn::RtkSaved),
    title: "rtk saved".to_string(),
    constraint: Constraint::Length(10),
},
OptionalColumn::GrossWithRtk => ColumnSpec {
    key: ColumnKey::Optional(OptionalColumn::GrossWithRtk),
    title: "gross+rtk".to_string(),
    constraint: Constraint::Length(10),
},
```

- [ ] **Step 5: Add to `render_column_value()`**

```rust
ColumnKey::Optional(OptionalColumn::RtkSaved) => {
    format_metric(row.metrics.rtk_saved_tokens)
}
ColumnKey::Optional(OptionalColumn::GrossWithRtk) => {
    format_metric(row.metrics.gross_input + row.metrics.rtk_saved_tokens)
}
```

- [ ] **Step 6: Add to `default_enabled_columns()`**

Both columns default to enabled but will be suppressed by the data check:

```rust
OptionalColumn::RtkSaved,
OptionalColumn::GrossWithRtk,
```

Add them at the end of the returned vec.

- [ ] **Step 7: Add `has_rtk_data` helper and suppress columns when absent**

Add a helper method on `App` (or as a free function where the existing helpers live):

```rust
fn rows_have_rtk_data(rows: &[RollupRow]) -> bool {
    rows.iter().any(|r| r.metrics.rtk_saved_tokens > 0.0)
}
```

In `render_table`, update the `active_columns` call to filter out RTK columns when the current view has no RTK data:

```rust
fn render_table(&mut self, frame: &mut Frame<'_>, area: Rect) {
    let has_rtk = rows_have_rtk_data(&self.raw_rows);
    let effective_columns: Vec<OptionalColumn> = self
        .ui_state
        .enabled_columns
        .iter()
        .filter(|c| {
            has_rtk
                || !matches!(c, OptionalColumn::RtkSaved | OptionalColumn::GrossWithRtk)
        })
        .cloned()
        .collect();
    let visible_columns = active_columns(area.width, self.ui_state.lens, &effective_columns);
    // ... rest of render_table unchanged ...
}
```

- [ ] **Step 8: Write a test for conditional column suppression**

In the `#[cfg(test)]` block in `app.rs`:

```rust
#[test]
fn rtk_columns_hidden_when_no_rtk_data() {
    let rows: Vec<RollupRow> = vec![]; // empty = no rtk data
    assert!(!rows_have_rtk_data(&rows));
}

#[test]
fn rtk_columns_shown_when_rtk_data_present() {
    let mut row = RollupRow { /* use test helper or default */ ..Default::default() };
    row.metrics.rtk_saved_tokens = 500.0;
    assert!(rows_have_rtk_data(&[row]));
}
```

- [ ] **Step 9: Run TUI tests**

```bash
cargo test -p gnomon-tui 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 10: Quality gates**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -20
```

- [ ] **Step 11: Commit**

```bash
git add crates/gnomon-tui/src/app.rs
git commit -m "feat: add RtkSaved and GrossWithRtk TUI columns; hide when no data"
```

---

## Task 8: End-to-end verification and full test suite

- [ ] **Step 1: Run full workspace test suite**

```bash
cargo test --workspace 2>&1 | tail -30
```

Expected: all tests pass.

- [ ] **Step 2: Run clippy**

```bash
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -10
```

Expected: zero warnings.

- [ ] **Step 3: Smoke-test the binary**

```bash
cargo run -p gnomon -- --help 2>&1 | head -5
cargo run -p gnomon -- db status 2>&1
```

Expected: help prints; db status shows the existing snapshot.

- [ ] **Step 4: Live import run**

```bash
cargo run -p gnomon -- db rebuild 2>&1 | tail -10
```

Expected: import completes; no RTK match errors on stderr; db status shows complete chunks.

- [ ] **Step 5: Verify RTK savings appear in report**

```bash
cargo run -p gnomon -- report 2>&1 | python3 -c "
import sys, json
data = json.load(sys.stdin)
for row in data['rows'][:5]:
    print(row['metrics'].get('rtk_saved_tokens', 'MISSING'), row['label'])
"
```

Expected: `rtk_saved_tokens` key present in each row; non-zero values for projects that had Bash actions matching RTK history.

- [ ] **Step 6: Verify TUI launches**

```bash
cargo run -p gnomon 2>&1 &
sleep 3 && kill %1
```

Expected: no panics in stderr output.

- [ ] **Step 7: Final commit if any fixes were needed**

```bash
git add -p  # stage any fixups
git commit -m "fix: e2e verification fixes for RTK integration"
```

Only commit if there were actual changes. Skip if everything was clean.
