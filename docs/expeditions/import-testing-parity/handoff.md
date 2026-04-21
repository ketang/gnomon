# import-testing-parity Expedition Handoff

- Expedition: `import-testing-parity`
- Base branch: `import-testing-parity`
- Base worktree: `/home/ketan/project/gnomon/.worktrees/import-testing-parity`
- Status: `ready_for_task`
- Active task branch: `none`
- Active task worktree: `none`
- Last completed: `import-testing-parity-03-codex-startup (kept)`
- Next action: Start task 04 (`codex-warnings-reimport`) from the expedition base branch.
- Primary branch: `main`

## Progress

- Task 01 (coverage-matrix) — kept. Produced
  `docs/expeditions/import-testing-parity/coverage-matrix.md`.
- Task 02 (claude-fixture-corpus) — kept. Added Claude fixture corpus,
  `src/import/test_fixtures.rs` shared helpers, and three fixture-driven
  provider-aware tests in `source.rs` and `chunk.rs`.
- Task 03 (codex-startup) — kept. Added three Codex startup tests in
  `crates/gnomon-core/src/import/chunk.rs`:
  - `codex_startup_import_opens_when_last_24h_slice_is_ready`
  - `codex_startup_timeout_still_allows_background_import_to_finish`
  - `codex_startup_full_import_waits_for_deferred_chunks_before_opening`
  - Helpers added in the same test module: `copy_codex_rollout_fixture`
    (copies the checked-in Codex rollout fixture into a writable tempdir) and
    `insert_seeded_codex_rollout_file` (pre-seeds a `source_file` row with
    `source_provider='codex'`, `source_kind='rollout'`, and a controlled
    `modified_at_utc`).
  - Imported `start_startup_import_with_options_and_sources` into the test
    module so the provider-aware startup path is exercised directly rather
    than through the legacy Claude-root wrapper.
  - Verified: `cargo fmt`, `cargo clippy --workspace --all-targets -- -D
    warnings`, `cargo test -p gnomon-core` all clean.

## Remaining Tasks (from `plan.md`)

1. **04-codex-warnings-reimport** — Malformed Codex input tests (missing
   `session_meta`, bad `session_index` JSON, truncated rollout); mtime-change
   reimport; schema-version reimport (Claude analog is
   `import_plan_reimports_files_when_import_schema_version_changes` in
   `chunk.rs`); deferred failure isolation. The task 03 helpers
   `copy_codex_rollout_fixture` + `insert_seeded_codex_rollout_file` are the
   right primitives for mtime-change reimport scenarios; for malformed
   cases, write a bespoke truncated rollout directly (analogous to
   `write_malformed_session_fixture` for Claude).
2. **05-rtk-source-aware** — Direct tests for `import_all_with_sources_and_rtk`
   and `start_startup_import_with_sources_and_mode_and_rtk` with mixed
   Claude+Codex `ConfiguredSources` and `rtk` enabled.
3. **06-cleanup** — De-duplicate the `codex_fixture_root()` helpers (one in
   `chunk.rs` test mod, one in `source.rs` test mod) in favor of the shared
   `test_fixtures::codex_fixture_root`. Also move the task-03
   `copy_codex_rollout_fixture` helper into `test_fixtures` so future tests
   can reuse it. Remove legacy-only tests clearly superseded by
   provider-aware equivalents.

## Session Resume Checklist

1. Read `plan.md`, `coverage-matrix.md`, this file.
2. Read the RESUME HERE block in `log.md` and the last few entries.
3. `expedition/scripts/expedition-verify.py --expedition import-testing-parity`
   from the base worktree.
4. `expedition/scripts/expedition-start-task.py --expedition
   import-testing-parity --slug codex-warnings-reimport --apply` to begin
   task 04.
