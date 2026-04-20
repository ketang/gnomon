# import-testing-parity Expedition Handoff

- Expedition: `import-testing-parity`
- Base branch: `import-testing-parity`
- Base worktree: `/home/ketan/project/gnomon/.worktrees/import-testing-parity`
- Status: `ready_for_task`
- Active task branch: `none`
- Active task worktree: `none`
- Last completed: `import-testing-parity-02-claude-fixture-corpus (kept)`
- Next action: Start task 03 (`codex-startup`) from the expedition base branch.
- Primary branch: `main`

## Progress

- Task 01 (coverage-matrix) — kept. Produced
  `docs/expeditions/import-testing-parity/coverage-matrix.md` with concrete
  file-line pointers and a gap summary.
- Task 02 (claude-fixture-corpus) — kept. Produced:
  - `crates/gnomon-core/tests/fixtures/claude/` with `README.md`,
    `projects/-tmp-redacted-project-a/session-claude-fixture-01.jsonl`, and
    `history.jsonl`.
  - `crates/gnomon-core/src/import/test_fixtures.rs` with
    `claude_fixture_root`, `codex_fixture_root`, `claude_fixture_sources`,
    `codex_fixture_sources`, `mixed_fixture_sources`.
  - Three new tests:
    - `scan_sources_manifest_discovers_checked_in_claude_fixture_corpus`
      (`source.rs`)
    - `scan_sources_manifest_discovers_checked_in_mixed_provider_corpus`
      (`source.rs`)
    - `import_all_with_sources_imports_checked_in_claude_fixture_corpus`
      (`chunk.rs`)
  - Verified: `cargo fmt`, `cargo clippy --workspace --all-targets -- -D
    warnings`, `cargo test --workspace` all clean.

## Remaining Tasks (from `plan.md`)

1. **03-codex-startup** — Add startup import tests for Codex covering
   recent-first mode, timeout/background completion, full-import modes.
   - Model after existing Claude startup tests in
     `crates/gnomon-core/src/import/chunk.rs` near `startup_import_opens_when_last_24h_slice_is_ready` (~line 2067)
     and `startup_timeout_still_allows_background_import_to_finish` (~line 2125).
   - Use `claude_fixture_sources()`/`codex_fixture_sources()` helpers where
     possible. For Codex, mtime-based recent-first behavior requires
     filesystem-backed copies (the checked-in fixture paths are read-only;
     copy into tempdir before touching mtime).
2. **04-codex-warnings-reimport** — Malformed Codex input tests (missing
   `session_meta`, bad `session_index` JSON, truncated rollout); mtime change
   reimport; schema-version reimport (`IMPORT_SCHEMA_VERSION` bump already
   covered for Claude at `chunk.rs` near `import_plan_reimports_files_when_import_schema_version_changes` line ~2588);
   deferred failure isolation.
3. **05-rtk-source-aware** — Direct tests for `import_all_with_sources_and_rtk`
   and `start_startup_import_with_sources_and_mode_and_rtk` at `chunk.rs` ~line 435.
   Mixed Claude+Codex `ConfiguredSources` with `rtk` enabled.
4. **06-cleanup** — De-duplicate the two existing `codex_fixture_root()`
   helpers (`chunk.rs:3060`, `source.rs:2545`) in favor of the shared
   `test_fixtures::codex_fixture_root`. Remove legacy-only tests that are
   clearly superseded by provider-aware equivalents.

## Session Resume Checklist

1. Read `plan.md`, `coverage-matrix.md`, this file.
2. Read the RESUME HERE block in `log.md` and the last few entries.
3. `expedition/scripts/expedition-verify.py --expedition import-testing-parity`
   from the base worktree.
4. `expedition/scripts/expedition-start-task.py --expedition
   import-testing-parity --slug codex-startup --apply` to begin task 03.
