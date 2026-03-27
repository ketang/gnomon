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
snapshot. The TUI now includes synchronized radial and table panes, persistent
UI state, current-view filtering, global jump, and manual snapshot refresh.
Startup prioritizes the last 24 hours of chunks before the UI opens and
continues older imports in one background worker after launch. The checked-in
design document captures the agreed `v1` architecture and backlog.

## Workspace Layout

```text
crates/
  gnomon/
  gnomon-core/
  gnomon-tui/
docs/
  v1-design.md
```

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

## Database Maintenance

The SQLite cache is derived data and can be maintained from the CLI:

```bash
cargo run -p gnomon -- db reset --force
cargo run -p gnomon -- db rebuild
```

Both commands honor the existing `--db` and `--source-root` overrides.
`reset` is destructive and requires `--force`. `rebuild` recreates the cache
from the source manifest and session history without opening the TUI.

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
