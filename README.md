# gnomon

`gnomon` is a terminal application for exploring Claude session history and finding the usage patterns that drive the highest token consumption.

## Current Status

This repository is bootstrapped as a Rust workspace with four crates:

- `gnomon-core`: configuration, source discovery, import, storage, classification, and query logic.
- `gnomon-tui`: the interactive terminal interface.
- `gnomon-web`: the local browser UI and embedded asset server.
- `gnomon`: the executable entry point.

The current binary resolves runtime paths, scans the source manifest, schedules
`project x day` import chunks, normalizes and classifies actions into the
SQLite cache, imports Claude `history.jsonl` as a first-class source when the
source root is the default `~/.claude/projects` tree, and opens a pinned TUI
against the latest published import snapshot. The TUI now includes synchronized
map and statistics panes, persistent UI state, current-view filtering, global
jump, and manual snapshot refresh.
Startup prioritizes the last 24 hours of chunks before the UI opens and
continues older imports in one background worker after launch by default. Use
`--startup-full-import` when you want the initial TUI snapshot to wait for the
entire import to finish instead. Startup and deferred import errors are
printed on stderr outside the TUI lifecycle and do not abort launch; failed
chunks remain excluded from the pinned snapshot until a later successful
re-import. The checked-in design document captures the agreed `v1`
architecture and backlog.

`gnomon-web` serves the same backend data locally over HTTP and embeds built
frontend assets into the Rust binary. The browser build lives under
`crates/gnomon-web/ui/`, while `crates/gnomon-web/build.rs` copies the built
`dist/` assets into the binary when they exist and falls back to the checked-in
placeholder assets when they do not.

## Workspace Layout

```text
crates/
  gnomon/
  gnomon-core/
  gnomon-tui/
  gnomon-web/
docs/
  opportunity-stability.md
  v1-design.md
```

## Branch And Worktree Workflow

- Do not implement on `main`.
- Start each task on a feature branch or the existing task branch.
- For any implementation task, automatically create or switch to a dedicated
  feature branch and linked worktree before making edits.
- Use a dedicated worktree for all implementation work. Creating only a feature
  branch in the repo root is not sufficient; create both the branch and the
  worktree before touching files.
- Treat branch and worktree setup as required preflight, not as a step that
  needs user approval.
- Exceptions are limited to read-only tasks and explicit branch-management or
  checkout-recovery tasks the user asked for.
- All parallel work must use separate worktrees.
- If you land on `main`, create a feature branch plus worktree before editing.
- Only pause for user input if branch or worktree setup would be destructive,
  ambiguous, or likely to interfere with existing uncommitted work.
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
cargo run -p gnomon -- --startup-full-import
```

Press `q` or `Esc` to exit the bootstrap TUI.

Fresh launches open at the top level by default. Use `--startup-*` flags to
open directly into a narrower drill-down view when you want to skip the root
landing state. `--startup-full-import` keeps the normal TUI launch path but
waits for the full import to finish before opening, unlike `gnomon db rebuild`
which is a maintenance command that only rebuilds the cache.

## Database Status Probe

Use the database status probe to verify whether import work is active without
opening SQLite manually.

```bash
cargo run -p gnomon -- db status
```

The probe reports:

- aggregate chunk counts by state (`pending`, `running`, `complete`, `failed`)
- startup/deferred phase counts when that metadata is available
- the currently active chunk, if one exists
- the latest published snapshot
- recent failed chunks with stored error text

## Skills Analytics

`gnomon` also exposes a non-interactive skills lens for explicit `/skill`
invocations imported from Claude history.

```bash
cargo run -p gnomon -- skills
cargo run -p gnomon -- skills --path skill --skill planner
cargo run -p gnomon -- skills --path skill-project --skill planner --project-id 1
```

The JSON output now reports both session-associated totals and explicit
action-attributed totals. Unmatched invocation counts are also included so you
can see when an explicit skill invocation did not join to a transcript-backed
session.

## TUI Screenshot Harness

A portable screenshot harness lives under `tools/tui-shot/`.

It runs `gnomon` in a PTY, renders the terminal via `xterm.js` inside headless
Chromium, and captures PNG screenshots after scripted navigation steps.

Bootstrap:

```bash
cd tools/tui-shot
npm install
npx playwright install chromium
```

Run the default drill-down scenario:

```bash
cd tools/tui-shot
node src/cli.mjs
```

Artifacts are written under `tools/tui-shot/artifacts/`.

## Web UI

`gnomon-web` is the local browser UI for the same derived data model used by the
terminal app. End users do not need Node.js at runtime because the Rust binary
serves embedded assets.

Frontend prerequisites and build loop:

```bash
cd crates/gnomon-web/ui
npm install
npm run build
```

The build writes `ui/dist/`, which `crates/gnomon-web/build.rs` embeds into the
binary at compile time. If `ui/dist/` is absent, the build script falls back to
the checked-in placeholder assets so the binary still runs.

Developer workflow:

```bash
cargo run -p gnomon-web -- --help
cargo run -p gnomon-web
```

For frontend-only iteration, keep the Rust server running and rebuild the UI
bundle in `crates/gnomon-web/ui/` as needed. The browser UI listens on loopback
by default, on port `4680` unless overridden with `--port` or `GNOMON_WEB_PORT`.

```bash
cd crates/gnomon-web/ui
npm run build
```

## Configuration

`gnomon` now boots a user config file automatically on first run.

- Linux config path: `~/.config/gnomon/config.toml`
- Default source root: `~/.claude/projects`
- When the source root follows the default Claude layout, gnomon also imports
  the sibling `~/.claude/history.jsonl` file automatically
- Default filter: exclude resolved project roots under `/tmp/`

The generated file starts with sensible defaults and comments. The default
filter keeps transient scratch directories such as smoke-test roots out of the
derived dataset unless you opt back in.

Example:

```toml
[source]
root = "~/.claude/projects"

[project_identity]
stale_claude_worktree_recovery = true
fallback_path_projects = true

[[project_filters]]
action = "exclude"
match_on = "resolved_root"
path_prefix = "/tmp/"
```

Supported project-filter matchers in `[[project_filters]]`:

- `path_prefix`
- `glob`
- `equals`

Supported `match_on` targets:

- `raw_cwd`
- `resolved_root`
- `identity_reason`

Rules are evaluated in order. The first matching rule wins.

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
`reset` is destructive and requires `--force`; it removes both the derived
usage database and the persisted browse-cache sidecar.
`rebuild` clears those persisted cache artifacts and recreates the usage
database from the source manifest and session history without opening the TUI.
If you pull a version that renames derived taxonomy labels, such as `Editing`
to `editing` or bracketed special-state labels like `[mixed]`, run `db rebuild`
to refresh existing cached aggregates and filters.
Apply the same rebuild step after pulling a version that adds skill
action-attribution support. Existing caches need a rebuild before the new
per-action skill totals will appear in the skills report.
Run `db rebuild` after pulling a version that changes project identity
resolution as well. Identity fixes only affect newly imported manifest rows, so
an existing cache can keep stale project records until it is rebuilt.
This includes stale worktree recovery: when a transcript `cwd` points at
`.../.claude/worktrees/...` or repo-local `.../.worktrees/...` and that
worktree no longer exists, `gnomon` probes the repo root above the recognized
worktree segment and re-attributes the session to the canonical Git project
when possible.
Project-filter changes also require a rebuild before the existing cache will
reflect the new include/exclude policy.
Apply the same rebuild step after pulling a version that bumps the importer
schema version. Import-schema bumps mean `gnomon` now consumes a different
normalized source-field set, so existing cached rows need reimport to match the
new contract.

Common stale-identity symptoms include:

- duplicate top-level project rows for what should be one repo
- ephemeral labels derived from worktree or agent directory names
- project metadata that still points at an old root path or fallback identity
- rows rooted under deleted worktrees such as `.../.claude/worktrees/...` or
  `.../.worktrees/...`

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
Database reset and rebuild clear the persisted sidecar before the next import so
old browse rows cannot survive a publish-sequence restart.

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

- `AGENTS.md` is the canonical project instruction entry point for Codex and
  other file-scoped agents
- `CLAUDE.md` contains the repo-local instructions for Claude
