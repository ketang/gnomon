use std::path::Path;

use anyhow::Result;
use rusqlite::{Connection, OpenFlags, params};

use crate::config::RtkConfig;

/// One RTK command row loaded from RTK's history.db.
#[derive(Debug)]
#[allow(dead_code)]
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
#[allow(dead_code)]
pub(crate) struct RtkMatch {
    pub action_id: i64,
    pub rtk_row_id: i64,
    pub saved_tokens: i64,
    pub savings_pct: f64,
    pub exec_time_ms: i64,
}

/// A Bash action row loaded from gnomon's DB.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct BashAction {
    pub action_id: i64,
    pub command: String,
    pub started_at_utc: String,
    pub completed_at_utc: String,
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
    let path_prefix = format!("{project_root_path}/");
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

/// Parses an ISO 8601 timestamp string to milliseconds since Unix epoch.
///
/// Supports both RTK format ("2026-04-18T20:30:01.347531794+00:00") and
/// gnomon format ("2026-04-18T20:30:01.387Z"). Ignores sub-second precision
/// and timezone offset — only the date+time to-the-second matters for the
/// ±30s matching window.
///
/// Returns 0 on parse failure so the row is still considered but never falls
/// inside a tight window.
fn parse_timestamp_ms(ts: &str) -> i64 {
    let s = match ts.get(..19) {
        Some(s) if s.len() == 19 => s,
        _ => return 0,
    };
    let year: i64 = s[0..4].parse().unwrap_or(0);
    let month: i64 = s[5..7].parse().unwrap_or(0);
    let day: i64 = s[8..10].parse().unwrap_or(0);
    let hour: i64 = s[11..13].parse().unwrap_or(0);
    let min: i64 = s[14..16].parse().unwrap_or(0);
    let sec: i64 = s[17..19].parse().unwrap_or(0);
    // Cumulative days per month (non-leap-year; good enough for ±30s comparisons).
    const MONTH_DAYS: [i64; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let month_idx = (month.saturating_sub(1) as usize).min(11);
    let days = (year - 1970) * 365 + (year - 1969) / 4 + MONTH_DAYS[month_idx] + day - 1;
    days * 86_400_000 + hour * 3_600_000 + min * 60_000 + sec * 1_000
}

/// Matches Bash actions against RTK rows using a forward cursor.
///
/// Both slices must be sorted by time (ascending). The cursor advances past
/// RTK rows that are definitively older than the current action's window.
/// A consumed HashSet prevents the same RTK row from matching multiple actions.
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
    let mut consumed = std::collections::HashSet::<i64>::new();

    for action in bash_actions {
        let action_start_ms = parse_timestamp_ms(&action.started_at_utc);
        let action_end_ms = parse_timestamp_ms(&action.completed_at_utc);

        // Advance cursor past rows that can never match any future action.
        while cursor < rtk_rows.len() {
            let rtk_ms = parse_timestamp_ms(&rtk_rows[cursor].timestamp_utc);
            if rtk_ms >= action_start_ms - pre_slack {
                break;
            }
            cursor += 1;
        }

        // Scan forward from cursor for the first unused match in the window.
        let window_end_ms = action_end_ms + post_slack;
        for row in &rtk_rows[cursor..] {
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
            make_action(
                1,
                "git status",
                "2026-04-18T10:00:00.000Z",
                "2026-04-18T10:00:01.000Z",
            ),
            make_action(
                2,
                "git status",
                "2026-04-18T10:00:01.500Z",
                "2026-04-18T10:00:02.000Z",
            ),
        ];
        let rtk_rows = vec![make_rtk(
            42,
            "git status",
            "2026-04-18T10:00:00.500+00:00",
            150,
        )];
        let result = run_cursor(&actions, &rtk_rows, 2000, 30000);
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
