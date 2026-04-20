# Import Testing Coverage Matrix: Claude vs Codex

Snapshot built from the workspace state on the
`import-testing-parity-01-coverage-matrix` branch. File paths are relative to
the worktree unless otherwise noted. Line numbers are drift-prone — always
re-grep before relying on them.

## 1. Checked-in Fixtures

| Fixture kind   | Claude                                     | Codex                                                                    |
|----------------|--------------------------------------------|--------------------------------------------------------------------------|
| Transcript     | NONE — tests generate synthetic JSONL via `write_session_fixture()` (`crates/gnomon-core/src/import/chunk.rs:3011`) | `crates/gnomon-core/tests/fixtures/codex/sessions/2026/04/18/rollout-2026-04-18T12-00-00Z.jsonl` (~740B, 9 event kinds, redacted) |
| History        | NONE (test-generated)                      | `crates/gnomon-core/tests/fixtures/codex/history.jsonl` (~137B)          |
| Session index  | N/A (Claude has no session index)          | `crates/gnomon-core/tests/fixtures/codex/session_index.jsonl` (~205B)    |
| Fixture README | N/A                                        | `crates/gnomon-core/tests/fixtures/codex/README.md`                      |

Corpus tarballs (external to repo, required for `#[ignore]` integration
tests): `tests/fixtures/import-corpus/subset.tar.zst` and `full.tar.zst`. Both
are Claude-only today.

## 2. Scan / Discovery

- Claude legacy: `scan_source_manifest` in
  `crates/gnomon-core/src/import/source.rs:153`
- Multi-provider: `scan_sources_manifest` at `source.rs:183`
- Policy variant: `scan_sources_manifest_with_policy` at `source.rs:258`
- `scan_discovers_sibling_claude_history_jsonl` at `source.rs:2076`
- `scan_sources_manifest_distinguishes_provider_and_kind_across_claude_and_codex`
  at `source.rs:2104` (covers both providers in one test)
- `scan_codex_rollout_uses_payload_cwd_for_project_attribution` at
  `source.rs:2205`

Gap: no dedicated Claude-only test that exercises
`scan_sources_manifest` with a checked-in, on-disk Claude fixture tree.

## 3. Normalization

Claude:
- Transcript: `normalize_claude_transcript_jsonl_file` (`normalize.rs` near :73–161 dispatch, body around :595)
- History: `normalize_claude_history_jsonl_file`

Codex:
- Rollout: `normalize_codex_rollout_jsonl_file` (`normalize.rs:636`)
- Session index: `normalize_codex_session_index_jsonl_file` (`normalize.rs:660`)
- History: shares `normalize_claude_history_jsonl_file` (`normalize.rs:595`)
- Tests: `normalizes_codex_history_jsonl_into_history_events` (~`normalize.rs:3541`),
  `normalizes_codex_session_index_jsonl_into_raw_entries` (~:3593),
  `normalizes_codex_rollout_into_raw_and_shared_models` (~:3653)

Gap: normalization tests exist for both providers and look roughly symmetric.

## 4. End-to-End Import Pipeline

- Claude integration: `subset_corpus_import_all_matches_expected_database_shape`
  (`crates/gnomon-core/tests/import_corpus_integration.rs:189`, `#[ignore]`)
- Claude integration: `full_corpus_import_all_matches_expected_database_shape`
  (`import_corpus_integration.rs:525`, `#[ignore]`)
- Mixed: `import_all_imports_codex_rollout_raw_sessions_without_blocking_claude_imports`
  (`chunk.rs:2689`) — synthetic Claude + checked-in Codex fixture

Gap: Claude has no fixture-driven end-to-end test on the provider-aware
(`ConfiguredSources`) path that uses a checked-in Claude corpus. All
provider-aware Claude coverage routes through synthetic `write_session_fixture`
or Claude-root legacy wrappers.

## 5. Startup / Deferred Import Behavior

- `start_startup_import` (legacy Claude-only wrapper): `chunk.rs:204`
- `start_startup_import_with_sources_and_perf_logger` (multi-provider): `chunk.rs:305`
- `startup_import_opens_when_last_24h_slice_is_ready` (`chunk.rs:2067`) — Claude
- `subset_corpus_recent_first_startup_import_defers_every_chunk_and_reaches_same_final_state`
  (`import_corpus_integration.rs:214`, `#[ignore]`) — Claude
- `subset_corpus_with_one_recent_file_imports_recent_chunk_before_opening`
  (`import_corpus_integration.rs:445`, `#[ignore]`) — Claude

**Gap (critical):** Zero Codex-sourced startup tests. Recent-first mode,
background completion, timeout, and full-import startup are untested with any
Codex source configured.

## 6. Warning / Failure Handling

- `import_all_completes_when_deferred_file_is_malformed_and_records_warning`
  (`chunk.rs:2512`) — Claude
- `invalid_jsonl_produces_scan_warning` (`source.rs:1917`) — Claude transcript
- `startup_import_records_warning_for_malformed_source_file_and_completes_chunk`
  (`chunk.rs:2125`) — Claude
- `deferred_import_records_warnings_without_failure_status_updates`
  (`chunk.rs:2212`) — Claude

**Gap:** No Codex-specific malformed input tests: missing `session_meta`,
invalid `function_call`, truncated rollout, bad `session_index` JSON, bad Codex
`history.jsonl` JSON. Code paths are shared but the shape of a Codex record is
distinct — shared-path coverage does not prove Codex-specific error routing.

## 7. Reimport / Schema-Bump

- `subset_corpus_reimports_only_the_touched_chunk_when_a_file_mtime_changes`
  (`chunk.rs:318`, `#[ignore]`) — Claude
- `import_plan_reimports_files_when_import_schema_version_changes`
  (`chunk.rs:2588`) — Claude
- `subset_corpus_reimport_is_a_no_op_when_files_are_unchanged`
  (`import_corpus_integration.rs:272`, `#[ignore]`) — Claude
- `subset_corpus_keeps_prior_rows_when_a_reimported_file_turns_malformed_and_recovers_after_restore`
  (`import_corpus_integration.rs:367`, `#[ignore]`) — Claude

**Gap:** No Codex mtime change, unchanged no-op, or schema-bump reimport
scenarios. Again, shared code path is assumed but not proven with Codex inputs.

## 8. RTK-Aware Paths

- `import_all_with_sources_and_rtk` (`chunk.rs:435`)
- `start_startup_import_with_sources_and_mode_and_progress` (multi-provider + RTK)
- `match_rtk_savings` in `crates/gnomon-core/src/import/rtk.rs:48`

**Gap:** `*_with_sources_and_rtk` helpers have no dedicated direct tests that
pass a non-legacy `ConfiguredSources` value. Existing RTK tests route through
Claude-root legacy wrappers.

## 9. Public Import API Surface

Legacy Claude-root wrappers:
- `scan_source_manifest` / `_with_perf_logger` / `_with_policy`
- `import_all` / `import_all_with_perf_logger`
- `import_all_with_rtk`
- `start_startup_import`

Multi-provider (`ConfiguredSources`):
- `scan_sources_manifest` / `_with_perf_logger` / `_with_policy`
- `import_all_with_sources_and_perf_logger`
- `import_all_with_sources_and_rtk`
- `start_startup_import_with_sources_and_perf_logger`
- `start_startup_import_with_sources_and_mode_and_progress`

`ConfiguredSources` entry points: `ConfiguredSources::new`,
`ConfiguredSources::legacy_claude`, `iter`, `resolve_path`,
`claude_transcript_root`.

## Gaps Summary

Concrete cells currently uncovered:

1. **Claude checked-in fixtures** — no redacted on-disk corpus; all tests
   generate JSONL at runtime.
2. **Claude provider-aware end-to-end** — no fixture-backed Claude import test
   that exercises `ConfiguredSources::new(...)` rather than
   `ConfiguredSources::legacy_claude(...)`.
3. **Codex startup / deferred** — no startup test at all with a Codex source.
4. **Codex warnings** — no malformed rollout / history / session-index
   warning tests.
5. **Codex reimport / schema-bump** — no mtime change, unchanged no-op, or
   schema-version bump test using Codex sources.
6. **RTK `*_with_sources_*` helpers** — no direct tests with non-legacy
   `ConfiguredSources`.

## Task Pointers

Expedition task plan consumes this matrix as follows:

- Task 02 addresses gaps 1 and 2 (Claude fixture corpus + provider-aware
  fixture-based tests + shared test helper).
- Task 03 addresses gap 3 (Codex startup).
- Task 04 addresses gaps 4 and 5 (Codex warnings + reimport).
- Task 05 addresses gap 6 (RTK `*_with_sources_*` direct tests).
- Task 06 removes legacy duplicated tests superseded by the above.
