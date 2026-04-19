# RTK Integration Design

**Date:** 2026-04-18  
**Status:** Approved

## Goal

Import RTK (Rust Token Killer) savings data into gnomon at chunk-import time and
surface per-project, per-action token savings alongside existing consumption
metrics.

## Scope

RTK only. Headroom has no structured data source suitable for correlation today
(`headroom perf` reads a flat log file; the memory store is empty). Leave
headroom as explicit future scope.

## Data Sources

- **RTK db:** `~/.local/share/rtk/history.db` (SQLite)
- **Relevant table:** `commands`

```
commands(
    id INTEGER PRIMARY KEY,
    timestamp TEXT,          -- ISO 8601, nanosecond precision, UTC offset
    original_cmd TEXT,       -- command as invoked, without rtk prefix
    rtk_cmd TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    saved_tokens INTEGER,
    savings_pct REAL,
    exec_time_ms INTEGER,
    project_path TEXT        -- CWD at invocation time
)
```

The Claude Code hook rewrites `git status` → `rtk git status` at execution time.
Claude's transcript records the original command (without `rtk` prefix) in
`message_part.metadata_json` as `$.input.command`. RTK's `original_cmd` is also
without the prefix. The fields match directly.

## Correlation Strategy

### Join keys

1. **Project path** — `rtk.project_path` normalized to canonical project root
   matches gnomon's `project.root_path`. One gnomon project maps to multiple RTK
   `project_path` values (repo root + worktree paths); all are queried together.

2. **Command text** — `rtk.original_cmd == json_extract(mp.metadata_json,
   '$.input.command')` for `message_part` rows where `tool_name = 'Bash'` and
   `part_kind = 'tool_use'`.

3. **Timestamp window** — RTK records when the command ran; gnomon records
   `message.completed_at_utc` when the tool result was received. RTK timestamp
   is always before gnomon's completed timestamp. Match window:
   `rtk.timestamp ∈ [action.started_at_utc − pre_slack, action.completed_at_utc + post_slack]`.

Non-matches (RTK rows with no gnomon counterpart, gnomon actions with no RTK
row) are silently skipped. The match is opportunistic, not required.

### Cursor algorithm

Runs once per `project x day` chunk, after action rows are committed.

**Setup:**
```
rtk_rows: Vec<RtkRow>  -- all RTK rows for (project path variants, chunk day),
                          ORDER BY timestamp ASC
cursor: usize = 0
consumed: HashSet<i64>  -- RTK row IDs already matched in this chunk
matches: Vec<(action_id, RtkRow)>
```

**Per Bash action** (iterated in `started_at_utc` order):
1. Extract command text from `metadata_json`.
2. Advance `cursor` past any rows where
   `rtk.timestamp < action.started_at_utc − pre_slack` — those rows are now
   definitively too old to match any remaining action.
3. Scan forward from `cursor`, skipping IDs in `consumed`, until
   `rtk.timestamp > action.completed_at_utc + post_slack`.
4. On first row where `original_cmd == command`: record `(action_id, row)` in
   `matches`, insert RTK ID into `consumed`. Stop scanning for this action.

**After all actions:** batch-INSERT `matches` into `action_rtk_match` in one
transaction using a prepared statement.

This is a single sequential read of the RTK table per chunk. The cursor only
advances forward; consumed IDs prevent double-counting repeated commands within
a session.

## Schema Changes

### New table

```sql
CREATE TABLE action_rtk_match (
    action_id    INTEGER PRIMARY KEY REFERENCES action(id) ON DELETE CASCADE,
    rtk_row_id   INTEGER NOT NULL,
    saved_tokens INTEGER NOT NULL CHECK (saved_tokens >= 0),
    savings_pct  REAL    NOT NULL,
    exec_time_ms INTEGER NOT NULL CHECK (exec_time_ms >= 0)
);
```

`rtk_row_id` is stored for auditability; gnomon does not join back to RTK's db
at query time.

### `chunk_action_rollup`

Add one column:

```sql
rtk_saved_tokens INTEGER NOT NULL DEFAULT 0 CHECK (rtk_saved_tokens >= 0)
```

Populated by summing `action_rtk_match.saved_tokens` over the actions in each
rollup group during chunk rollup computation. Path rollup (`chunk_path_rollup`)
does not receive this column — RTK savings are command-level, not
file-attribution-level.

## Configuration

New `[rtk]` section in `config.toml`:

```toml
[rtk]
enabled = true
db_path = "~/.local/share/rtk/history.db"
pre_slack_ms  = 2000    # how far before action.started_at_utc to accept an RTK row
post_slack_ms = 30000   # how far after action.completed_at_utc to accept an RTK row
```

- `enabled = false` skips the match phase for all chunks.
- If the resolved `db_path` does not exist, the match phase is skipped silently.
- Both slack values are tunable; defaults cover normal latency variance.

## Display

### New columns (conditional)

RTK columns are **hidden when `rtk_saved_tokens` sums to zero** in the current
view. They appear only when at least one matched row contributes savings data.

| Column | Value |
|---|---|
| **RTK saved** | `rtk_saved_tokens` for the row |
| **Gross input** | `input_tokens + cache_creation_input_tokens + rtk_saved_tokens` |

"Gross input" shows what would have been consumed without RTK filtering, making
the reduction visible relative to actual consumption.

### Column placement

Follow the existing default column priority. RTK saved and gross input are
optional/secondary; they appear after the existing token columns when present.

### Report output

`gnomon report` JSON rollup rows gain `rtk_saved_tokens` alongside the existing
token fields. The field is always present in the JSON (value `0` when no match
data), consistent with how other token fields are emitted.

## Out of Scope

- Headroom integration (future — requires a structured data source)
- Surfacing which specific commands drove the most savings (future drill-down)
- Re-match pass independent of full chunk reimport (future — would require
  storing the extracted command text as a first-class column on `action`)
- RTK "missed opportunity" detection (commands that ran without RTK)
