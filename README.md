# gnomon

`gnomon` is a terminal application for exploring Claude session history and finding the usage patterns that drive the highest token consumption.

## Current Status

This repository is bootstrapped as a Rust workspace with three crates:

- `gnomon-core`: configuration, source discovery, import, storage, classification, and query logic.
- `gnomon-tui`: the interactive terminal interface.
- `gnomon`: the executable entry point.

The current binary is a thin bootstrap that resolves runtime paths and opens a placeholder TUI. The checked-in design document captures the agreed `v1` architecture and backlog.

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
