# Agent Instructions

This repository keeps Claude-specific shared-rule imports in `CLAUDE.md`. For
Codex and other file-scoped agents, this `AGENTS.md` file is the canonical
project instruction entry point.

## Read First

- `README.md` for workspace overview and bootstrap commands
- `docs/v1-design.md` before architectural changes or feature planning
- `Cargo.toml` for workspace members, shared dependencies, and lint policy

## Shared Standards

This repository no longer vendors shared agent rules through a separate
submodule. Use the instructions in this file, `README.md`, and the design and
workspace files listed above as the canonical project guidance.

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
- For any implementation task, automatically create or switch to a dedicated
  feature branch and linked worktree before making edits
- Use a feature branch or the existing task branch for every implementation task
- Use a dedicated worktree for all implementation work. Creating only a feature
  branch in the repo root is not sufficient; create both the branch and the
  worktree before touching files.
- Treat branch and worktree setup as required preflight, not as a step that
  needs user approval
- Exceptions are limited to read-only tasks and explicit branch-management or
  checkout-recovery tasks the user asked for
- Only pause for user input if branch or worktree setup would be destructive,
  ambiguous, or likely to interfere with existing uncommitted work
- All parallel work must use separate worktrees
- Rebase finished feature branches onto `origin/main` before integration
- Merge finished feature branches into `main` with `git merge --no-ff`
- Do not fast-forward feature branch integrations into `main`
- GitHub Issues is the canonical issue tracker for this repo
- Do not assume `beads`
- If the user references a GitHub issue, follow that context
- Claim implementation work in GitHub Issues by adding the `in-progress` label
  before editing code when the task is tracked there
- Remove the `in-progress` label when work is handed off or completed unless the
  user explicitly asks to leave the issue state unchanged
- Do not create, materially edit, or close GitHub issues unless the user
  explicitly asks

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


<!-- headroom:rtk-instructions -->
# RTK (Rust Token Killer) - Token-Optimized Commands

When running shell commands, **always prefix with `rtk`**. This reduces context
usage by 60-90% with zero behavior change. If rtk has no filter for a command,
it passes through unchanged — so it is always safe to use.

## Key Commands
```bash
# Git (59-80% savings)
rtk git status          rtk git diff            rtk git log

# Files & Search (60-75% savings)
rtk ls <path>           rtk read <file>         rtk grep <pattern>
rtk find <pattern>      rtk diff <file>

# Test (90-99% savings) — shows failures only
rtk pytest tests/       rtk cargo test          rtk test <cmd>

# Build & Lint (80-90% savings) — shows errors only
rtk tsc                 rtk lint                rtk cargo build
rtk prettier --check    rtk mypy                rtk ruff check

# Analysis (70-90% savings)
rtk err <cmd>           rtk log <file>          rtk json <file>
rtk summary <cmd>       rtk deps                rtk env

# GitHub (26-87% savings)
rtk gh pr view <n>      rtk gh run list         rtk gh issue list

# Infrastructure (85% savings)
rtk docker ps           rtk kubectl get         rtk docker logs <c>

# Package managers (70-90% savings)
rtk pip list            rtk pnpm install        rtk npm run <script>
```

## Rules
- In command chains, prefix each segment: `rtk git add . && rtk git commit -m "msg"`
- For debugging, use raw command without rtk prefix
- `rtk proxy <cmd>` runs command without filtering but tracks usage
<!-- /headroom:rtk-instructions -->
