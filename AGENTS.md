# Agent Instructions

`gnomon` is a Rust workspace for analyzing Claude session history and
surfacing the usage patterns that drive the highest token consumption.

This file is the canonical project instruction entry point for all coding
agents. Claude Code reads it via `CLAUDE.md`, which imports this file.

## Read First

- `README.md` for bootstrap commands and human-facing project overview
- `docs/v1-design.md` for product shape, architecture, and backlog constraints
- `Cargo.toml` for workspace members, shared dependencies, and lint policy

## Repo Facts

- Rust workspace with three crates: `crates/gnomon`, `crates/gnomon-core`, and
  `crates/gnomon-tui`
- `crates/gnomon-core` owns configuration, import, storage, query,
  classification, and VCS logic
- `crates/gnomon-tui` owns the interactive terminal UI
- `crates/gnomon` is the executable entry point
- `target/` is build output, not source
- `.worktrees/` is local worktree state, not product source. Do not commit or
  clean it up unless the user explicitly asks.

## Branch and Issue Workflow

- Do not implement on `main`
- For any implementation task, automatically create or switch to a dedicated
  feature branch and linked worktree before making edits. Repo-local worktrees
  live under `.worktrees/`.
- Creating only a feature branch in the repo root is not sufficient; create
  both the branch and the worktree before touching files, regardless of which
  branch you are on
- Treat branch and worktree setup as required preflight, not as a step that
  needs user approval
- Exceptions are limited to read-only tasks and explicit branch-management or
  checkout-recovery tasks the user asked for
- Only pause for user input if branch or worktree setup would be destructive,
  ambiguous, or likely to interfere with existing uncommitted work
- All parallel work must use separate worktrees
- Rebase finished feature branches onto `origin/main` before integration
- Merge finished feature branches into `main` with `git merge --no-ff`; do not
  fast-forward
- GitHub Issues is the canonical issue tracker for this repo. Do not assume
  `beads`. If the user references a GitHub issue or branch naming convention,
  follow that context.
- Claim implementation work in GitHub Issues by adding the `in-progress` label
  before editing code when the task is tracked there; remove it on handoff or
  completion unless the user asks otherwise
- Do not create, materially edit, or close GitHub issues unless the user
  explicitly asks

## Rust Quality Gates

For code changes, run:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace
```

If CLI behavior changed, also smoke-test:

```bash
cargo run -p gnomon -- --help
```

Interactive bootstrap: `cargo run -p gnomon` (press `q` or `Esc` to exit).

### Import integration tests (required for perf optimization passes)

Before declaring any import performance optimization pass complete, you MUST
also run the full integration test suite:

```bash
cargo test -p gnomon-core --test import_corpus_integration -- --include-ignored
```

These tests are `#[ignore]`-tagged (they require local corpus fixture tarballs)
and are **not** included in `cargo test --workspace`. They verify end-to-end
import correctness, including aggregated counts written to `import_chunk`. A
change that passes unit tests but corrupts `imported_message_count` (e.g., by
reading a drained `HashMap`) will only be caught here.

Fixture tarballs live under `tests/fixtures/import-corpus/` in the primary
checkout or any worktree where they have previously been copied. Copy them
into your worktree if they are missing before running these tests.

## Working Agreements

- Match the architecture described in `docs/v1-design.md`. If implementation
  requires changing the agreed design, update `docs/v1-design.md` in the same
  change; do not let code silently diverge from the design doc.
- Keep `README.md` current when build, run, config, or operator-facing
  behavior changes
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
