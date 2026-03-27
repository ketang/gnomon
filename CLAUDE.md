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
- Use a dedicated worktree for implementation work whenever practical, and for
  all parallel work. Repo-local worktrees live under `.worktrees/`.
- `.worktrees/` is local orchestration state, not product source. Do not commit
  or clean it up unless the user explicitly asks.

The imported workflow rule contains a direct-to-`main` policy for repos that
use that model. `gnomon` does not. The local branch policy above overrides it.

### Issue tracking

- This repo does not currently document a canonical issue tracker workflow.
- Do not assume `beads`.
- If the user references a GitHub issue, branch naming convention, or another
  tracker explicitly, follow that context. Otherwise do not mutate tracker
  state on your own.

This is why this repo does not currently import `.agents/rules/beads.md`.

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
