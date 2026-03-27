# Agent Instructions

This repository keeps Claude-specific shared-rule imports in `CLAUDE.md`. For
Codex and other file-scoped agents, this `AGENTS.md` file is the canonical
project instruction entry point.

## Read First

- `README.md` for workspace overview and bootstrap commands
- `docs/v1-design.md` before architectural changes or feature planning
- `Cargo.toml` for workspace members, shared dependencies, and lint policy

## Shared Standards

Shared standards live under `.agents/rules/`. Open the relevant files when the
task touches these areas:

- `.agents/rules/workflow.md`
- `.agents/rules/testing.md`
- `.agents/rules/code-quality.md`
- `.agents/rules/learning.md`

Do not rely on Claude-style `@...` import behavior here. Treat these as files
to read when needed.

## Repo Facts

- Rust workspace with three crates: `crates/gnomon`, `crates/gnomon-core`, and
  `crates/gnomon-tui`
- `crates/gnomon-core` owns import, storage, query, classification, and VCS
  logic
- `crates/gnomon-tui` owns the interactive terminal UI
- `crates/gnomon` is the executable entry point
- `target/` is build output, not source
- `.worktrees/` is local worktree state, not product source

Do not edit or commit `target/` or `.worktrees/` unless the user explicitly
asks.

## Branch and Issue Workflow

- Do not implement on `main`
- If the current branch is `main`, stop and ask before editing
- Prefer feature branches or the existing task branch
- Prefer worktrees for parallel work
- This repo does not currently document a canonical issue tracker
- Do not assume `beads`
- If the user references an issue or tracker explicitly, follow that context;
  otherwise do not make tracker mutations on your own

## Rust Commands

- Format: `cargo fmt --all`
- Lint: `cargo clippy --workspace --all-targets -- -D warnings`
- Test: `cargo test --workspace`
- Build: `cargo build --workspace`
- CLI smoke test: `cargo run -p gnomon -- --help`
- Interactive bootstrap: `cargo run -p gnomon`

Press `q` or `Esc` to exit the bootstrap TUI.

## Working Agreements

- Match the architecture described in `docs/v1-design.md`
- If implementation requires changing the agreed design, update
  `docs/v1-design.md` in the same change
- Keep `README.md` current when build, run, config, or operator-facing behavior
  changes
- Preserve the workspace lint stance from `Cargo.toml`, including
  `unwrap_used = "deny"`, `todo = "deny"`, and `unsafe_code = "forbid"`
- New behavior should ship with tests
- Bug fixes should start with a failing reproduction test
- Do not hardcode machine-specific absolute paths
