# Gnomon - Claude Instructions

`gnomon` is a Rust workspace for analyzing Claude session history and surfacing
the usage patterns that drive the highest token consumption.

---

## Local Overrides

When this file conflicts with an imported shared rule, this file wins for this
repository.

### Branch and worktree policy

- Do not implement on `main`.
- Use a feature branch or the existing task branch for every implementation
  task.
- For any implementation task, automatically create or switch to a dedicated
  feature branch and linked worktree before making edits.
- Always use a dedicated worktree for implementation work. Repo-local worktrees
  live under `.worktrees/`. Creating a feature branch in the repo root is not a
  substitute — create both the branch and the worktree before touching any
  files, regardless of which branch you are on.
- Treat branch and worktree setup as required preflight, not as a step that
  needs user approval.
- Only pause for user input if branch or worktree setup would be destructive,
  ambiguous, or likely to interfere with existing uncommitted work.
- All parallel work must use separate worktrees.
- `.worktrees/` is local orchestration state, not product source. Do not commit
  or clean it up unless the user explicitly asks.

### Issue tracking

- GitHub Issues is the canonical issue tracker workflow for this repo.
- Do not assume `beads`.
- If the user references a GitHub issue or branch naming convention, follow
  that context.
- Do not create, edit, or close GitHub issues unless the user explicitly asks.

GitHub Issues is the tracker for `gnomon`; do not assume `beads`.

## Read First

- `README.md` for bootstrap commands and human-facing project overview
- `docs/v1-design.md` for product shape, architecture, and backlog constraints
- `Cargo.toml` for workspace members, shared dependencies, and lint policy

## Workspace Layout

- `crates/gnomon-core` owns configuration, import, storage, query,
  classification, and VCS logic
- `crates/gnomon-tui` owns the terminal UI
- `crates/gnomon` is the executable entry point

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
reading a drained HashMap) will only be caught here.

Fixture tarballs live under `tests/fixtures/import-corpus/` in the primary
checkout or any worktree where they have previously been copied. Copy them into
your worktree if they are missing before running these tests.

## Documentation

- Keep `README.md` updated when build, run, config, or workspace-shape behavior
  changes
- Keep `docs/v1-design.md` aligned with architecture decisions; do not let code
  silently diverge from the design doc
