# Gnomon - Claude Instructions

`gnomon` is a Rust workspace for analyzing Claude session history and surfacing
the usage patterns that drive the highest token consumption.

@.agents/rules/workflow.md
@.agents/rules/testing.md
@.agents/rules/code-quality.md
@.agents/rules/learning.md

---

## Local Overrides

When this file conflicts with an imported shared rule, this file wins for this
repository.

### Branch and worktree policy

- Do not implement on `main`.
- Use a feature branch or the existing task branch for every implementation
  task.
- If the current branch is `main`, stop and ask before editing files.
- Always use a dedicated worktree for implementation work. Repo-local worktrees
  live under `.worktrees/`. Creating a feature branch in the repo root is not a
  substitute — create both the branch and the worktree before touching any
  files, regardless of which branch you are on.
- All parallel work must use separate worktrees.
- `.worktrees/` is local orchestration state, not product source. Do not commit
  or clean it up unless the user explicitly asks.

The imported workflow rule contains a direct-to-`main` policy for repos that
use that model. `gnomon` does not. The local branch policy above overrides it.

### Issue tracking

- GitHub Issues is the canonical issue tracker workflow for this repo.
- Do not assume `beads`.
- If the user references a GitHub issue or branch naming convention, follow
  that context.
- Do not create, edit, or close GitHub issues unless the user explicitly asks.

GitHub Issues is the tracker for `gnomon`, so this repo does not import
`.agents/rules/beads.md`.

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

## Documentation

- Keep `README.md` updated when build, run, config, or workspace-shape behavior
  changes
- Keep `docs/v1-design.md` aligned with architecture decisions; do not let code
  silently diverge from the design doc
