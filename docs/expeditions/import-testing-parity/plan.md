# import-testing-parity Expedition Plan

## Goal

Bring Claude and Codex import testing to parity by lifting each provider where
the other is currently stronger:

- Claude gains reusable fixture-corpus coverage and provider-aware
  fixture-based tests.
- Codex gains richer end-to-end importer/startup/regression behavior coverage.

## Success Criteria

- Claude and Codex both have checked-in, redacted, reusable fixture-backed
  coverage where structural fidelity matters.
- Claude and Codex both have end-to-end import behavior coverage for startup,
  warnings, and reimport paths.
- New provider-aware source APIs (`*_with_sources_*` helpers introduced in the
  codex-integration merge) are directly tested.
- Mixed-provider import remains covered.
- `cargo test --workspace` and `cargo clippy --workspace --all-targets -- -D
  warnings` both pass on the rebased base branch.

## Task Sequence

1. **01-coverage-matrix** — Build a written coverage matrix for both providers
   across checked-in fixtures, scan/discovery, normalization, end-to-end import
   pipeline, startup/deferred import behavior, warning/failure handling,
   reimport/schema-bump, and RTK-aware paths. Output: a markdown matrix
   committed under `docs/expeditions/import-testing-parity/` identifying
   concrete test-file targets for later tasks.
2. **02-claude-fixture-corpus** — Create a small checked-in redacted Claude
   fixture corpus (transcript + history) at stable relative paths, plus a
   shared test helper for provider-aware source setup. Add fixture-driven
   provider-aware scan tests using `ConfiguredSources` and at least one
   fixture-driven end-to-end Claude import test on the provider-aware path.
3. **03-codex-startup** — Add startup import tests for Codex covering
   recent-first mode, timeout/background completion, and full-import modes.
4. **04-codex-warnings-reimport** — Add Codex warning-handling tests for
   malformed rollout/history/session-index inputs; add incremental reimport
   tests (changed vs unchanged Codex files); add schema-version/reimport
   regression tests; add deferred import tests proving Codex failures do not
   poison unrelated work.
5. **05-rtk-source-aware** — Add direct tests for `*_with_sources_*` RTK-aware
   helpers. Verify mixed-provider import continues to work end-to-end via the
   explicit `ConfiguredSources` path.
6. **06-cleanup** — Remove redundant legacy-only test paths superseded by the
   provider-aware tests. Keep inline synthetic generators only for narrow
   behavior-only cases. Final verification gate run.

## Experiment Register

None planned. If a task branch becomes a failed experiment (e.g., an approach
to fixture redaction that cannot keep test intent), record it here with:

- hypothesis
- success criteria
- discard criteria
- branch slug seed

## Verification Gates

Per-task (must pass before a kept task merges into the base branch):

- `cargo fmt --all`
- Focused import test targets touched by the task
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p gnomon-core`

Final landing gate (before rebasing base branch onto `main`):

- `cargo test --workspace`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo build --workspace`
- `cargo test -p gnomon-core --test import_corpus_integration --
  --include-ignored` (requires local corpus fixture tarballs; run only if the
  final task touched import correctness paths this suite exercises)
