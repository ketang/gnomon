# gnomon

`gnomon` is a terminal application for exploring Claude session history and finding the usage patterns that drive the highest token consumption.

## Current Status

This repository is bootstrapped as a Rust workspace with three crates:

- `gnomon-core`: configuration, source discovery, import, storage, classification, and query logic.
- `gnomon-tui`: the interactive terminal interface.
- `gnomon`: the executable entry point.

The current binary resolves runtime paths, scans the source manifest, schedules
`project x day` import chunks, normalizes and classifies actions into the
SQLite cache, and opens a pinned TUI against the latest published import
snapshot. The TUI now includes synchronized map and statistics panes, persistent
UI state, current-view filtering, global jump, and manual snapshot refresh.
Startup prioritizes the last 24 hours of chunks before the UI opens and
continues older imports in one background worker after launch. Startup import
errors are reported in the UI status area and do not abort launch; failed
chunks remain excluded from the pinned snapshot until a later successful
re-import. The checked-in design document captures the agreed `v1`
architecture and backlog.

## Workspace Layout

```text
crates/
  gnomon/
  gnomon-core/
  gnomon-tui/
docs/
  opportunity-stability.md
  v1-design.md
```

## Branch And Worktree Workflow

- Do not implement on `main`.
- Start each task on a feature branch or the existing task branch.
- Use a dedicated worktree for active implementation work whenever practical,
  and especially for parallel efforts.
- If you land on `main`, stop and switch to a feature branch before editing.
- Rebase finished feature branches onto `origin/main` before integration.
- Merge finished feature branches into `main` with `git merge --no-ff`.
- Do not fast-forward feature branch integrations into `main`.
- Repo-local worktrees live under `.worktrees/`. Treat that directory as local
  orchestration state, not product source.

## Issue Tracking

- GitHub Issues is the canonical issue tracker for this repository.
- If a task is already tracked, use the corresponding GitHub issue as the
  source of truth for task context.
- This repository does not use `beads`.

## Planned Core Stack

- TUI: `ratatui` + `crossterm`
- SQLite cache: `rusqlite` + `rusqlite_migration`
- Git root discovery: `gix`
- Time handling: `jiff`
- CLI/config: `clap`, `directories`
- Search/filtering: `nucleo-matcher`

## Running the Bootstrap

```bash
cargo run -p gnomon -- --help
cargo run -p gnomon
```

Press `q` or `Esc` to exit the bootstrap TUI.

Fresh launches open at the top level by default. Use `--startup-*` flags to
open directly into a narrower drill-down view when you want to skip the root
landing state.

## Performance Logs

Performance logging is enabled by default.

- Default path: `<state_dir>/logs/perf.log`
- Default format: human-readable line logs
- Default granularity: `normal`
- Rotation: `perf.log` plus `perf.log.1` through `perf.log.5`
- Active file size cap: `10 MiB`

The default `normal` granularity records user-visible phases such as startup,
filter loading, view reloads, jump-target generation, and similar waits.
Verbose per-node query logging is available when you need to identify which
specific drill-down or stats query is slow.

Useful overrides:

```bash
GNOMON_PERF_LOG=off cargo run -p gnomon
GNOMON_PERF_LOG_GRANULARITY=verbose cargo run -p gnomon
GNOMON_PERF_LOG_FORMAT=jsonl cargo run -p gnomon
GNOMON_PERF_LOG=/tmp/gnomon-perf.log cargo run -p gnomon
```

## Database Maintenance

The SQLite cache is derived data and can be maintained from the CLI:

```bash
cargo run -p gnomon -- db reset --force
cargo run -p gnomon -- db rebuild
```

Both commands honor the existing `--db` and `--source-root` overrides.
`reset` is destructive and requires `--force`. `rebuild` recreates the cache
from the source manifest and session history without opening the TUI.
If you pull a version that renames derived taxonomy labels, such as `Editing`
to `editing` or bracketed special-state labels like `[mixed]`, run `db rebuild`
to refresh existing cached aggregates and filters.
Run `db rebuild` after pulling a version that changes project identity
resolution as well. Identity fixes only affect newly imported manifest rows, so
an existing cache can keep stale project records until it is rebuilt.
This includes stale Claude worktree recovery: when a transcript `cwd` points at
`.../.claude/worktrees/...` and that worktree no longer exists, `gnomon` now
probes the repo root above the recognized worktree segment and re-attributes
the session to the canonical Git project when possible.
Apply the same rebuild step after pulling a version that bumps the importer
schema version. Import-schema bumps mean `gnomon` now consumes a different
normalized source-field set, so existing cached rows need reimport to match the
new contract.

Common stale-identity symptoms include:

- duplicate top-level project rows for what should be one repo
- ephemeral labels derived from worktree or agent directory names
- project metadata that still points at an old root path or fallback identity
- rows rooted under deleted Claude worktrees such as `.../.claude/worktrees/...`

Recovery path:

```bash
cargo run -p gnomon -- db rebuild
```

Use `db reset --force` only when you want to remove the cache artifacts first;
`db rebuild` is the normal recovery command after identity-related fixes.

## Browse Cache

The TUI now persists warmed browse results in a separate SQLite sidecar at
`<state_dir>/browse-cache.sqlite3`. The browse cache is scoped to the published
snapshot generation, reused across launches, pruned automatically when newer
snapshots appear, and bounded by a default `64 MiB` payload budget.

Implementation notes and retention details live in `docs/browse-cache.md`.

## Non-Interactive Reports

`gnomon` can emit stable JSON rollups from the current imported snapshot without
opening the TUI:

```bash
cargo run -p gnomon -- report
cargo run -p gnomon -- report --root category --path category
cargo run -p gnomon -- report --path project --project-id 1
```

The reporting mode reuses the same aggregate query engine as the TUI. Drill-down
paths use the existing hierarchy model, with `--project-id`, `--category`, and
action fields such as `--classification-state` and `--normalized-action`
supplying the path context when needed.
Each rollup row now reserves an `opportunities` object in the JSON output so
future heuristic annotations can ship without changing the hierarchy shape.

## Query Benchmarks

`gnomon` can also emit a read-only JSON benchmark report for the hot query
scenarios behind root browse, drill, refresh, filter-change, and jump-target
generation:

```bash
cargo run -p gnomon -- benchmark
cargo run -p gnomon -- --db /path/to/usage.sqlite3 benchmark --iterations 10
```

The benchmark report includes per-scenario timing samples plus `EXPLAIN QUERY
PLAN` output for the current snapshot, action-fact load, and path-fact load
queries. It also includes batched prefetch scenarios plus a `browse_footprint`
section that estimates serialized browse-cache payload sizes for shallow and
deep non-path and path-prefetch runs. Use the global `--db` override to point
at either the normal local cache or a synthetic cache generated by
`validate-scale`.

Footprint interpretation and current prefetch recommendations live in
`docs/browse-cache-footprint.md`.

## Scale Validation

Synthetic scale validation, the current Linux timing baseline from March 27,
2026, and release-build guidance live in `docs/scale-validation.md`.

Quick validation:

```bash
cargo run -p gnomon --bin validate-scale -- --profile quick
```

Larger issue-10 validation:

```bash
cargo run -p gnomon --release --bin validate-scale -- --profile ten-x --root /tmp/gnomon-scale
```

To benchmark the resulting synthetic cache afterward:

```bash
cargo run -p gnomon -- --db /tmp/gnomon-scale/validation.sqlite3 benchmark --iterations 10
```

## Opportunity Policy

Opportunity taxonomy and recommendation stability policy live in
`docs/opportunity-stability.md`. That policy separates stable mechanism
categories from time-sensitive recommendations so heuristic guidance can evolve
without renaming the underlying category labels.

## Agent Instructions

Shared agent standards are vendored via the `.agents/` git submodule:

```bash
git submodule update --init --recursive
```

- `CLAUDE.md` imports the project-appropriate shared rules from `.agents/rules/`
- `AGENTS.md` provides Codex-friendly project instructions and references the
  same shared standards directly

To refresh the shared standards later:

```bash
git submodule update --remote --merge .agents
```
