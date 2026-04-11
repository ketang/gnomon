# Import Perf — Running Log

> Companion to `docs/specs/2026-04-10-import-perf-design.md`. Append-only phase log plus overwritten Resume Block. First action of any fresh session: read the Resume Block, then the last 2–3 phase entries, then continue.

## Environment
- Host: _(to be captured in Phase 1 — Task 10)_
- CPU: _(to be captured)_
- RAM: _(to be captured)_
- WSL filesystem for repo: ext2/ext3 (from capture script stat)
- WSL filesystem for DB path (`~/.local/share/gnomon/`): _(to be captured in Task 10)_
- Rust: _(to be captured)_
- SQLite (bundled): _(to be captured)_

## Corpus Snapshot
- Manifest: `tests/fixtures/import-corpus/MANIFEST.md`
- Captured: 2026-04-11T03:37:18Z
- Full corpus:
  - 1,626,942,271 bytes (1.55 GiB) uncompressed
  - 4,549 JSONL files
  - 52 projects
  - compressed to 139 MiB
  - SHA256: `5784682a90da345adc90beb5ce13fdb0d10a030de412b63aa3152c0c01c37b30`
- Subset:
  - 784,342,798 bytes (748 MiB) uncompressed
  - 1,650 JSONL files
  - 1 project (the single largest in the corpus)
  - compressed to 60 MiB
  - SHA256: `053ea32815905ee3936e9e596a7e759713defb198257740ad9e45c44ec35ed89`

## Baseline
_(to be captured in Phase 1 — Tasks 10-11)_

## Target
_(to be agreed with user at end of Phase 1 — Task 14)_

---

## Phase Log

### 2026-04-10 — Phase 1 started
Kicked off Phase 1 (measure). Design doc committed on `import-perf` (sha `dc136b5`). Phase 1 implementation plan committed (sha `cbd3516`). Running log initialized (sha `2c6e57a`). Fixture directory reserved and gitignored (sha `d49560c`). Capture script added (sha `1b1320c`).

### 2026-04-11 — Task 8 complete: parse vs SQL split inside per-record loop

Refactored `normalize_transcript_jsonl_file_inner` to track two `Duration` accumulators inside the JSONL loop: `parse_total` wraps the `serde_json::from_str` call, `sql_total` wraps the conversation-init / `flush_buffered_records` / `process_record` work (including the post-loop `flush_buffered_records` call). Inner now returns `(NormalizeJsonlFileOutcome, Duration, Duration)`; the outer wrapper attaches `parse_ms` and `sql_ms` as fields on the existing `import.normalize_jsonl` span before `finish_ok()`. No per-row events — one summary per file. Committed as sha `d539ff6`. Quality gates: fmt/clippy/tests/build all clean.

### 2026-04-11 — Tasks 5-7 complete: PerfLogger wired into import hot path

Added `Option<PerfLogger>` to `ImportWorkerOptions` and threaded it through `import_chunk` → `normalize_jsonl_file` → `build_turns` → `build_actions` → `finalize_chunk_import` → `rebuild_chunk_{action,path}_rollups` via new `perf_logger` fields on `NormalizeJsonlFileParams` and `BuildActionsParams`. Spans emitted at each phase: `import.chunk`, `import.normalize_jsonl` (+ `import.normalize_history_jsonl`), `import.build_turns`, `import.build_actions`, `import.finalize_chunk`, `import.rebuild_action_rollups`, `import.rebuild_path_rollups`. Opt-in via `GNOMON_PERF_LOG`. Added manual `Debug` impl to `PerfLogger` so it can live inside derived-`Debug` structs. Committed as sha `2a3a47a`. Quality gates: fmt/clippy/tests all clean.

### 2026-04-11 — Task 4 complete: corpus captured

Ran `tests/fixtures/import-corpus/capture.sh` against the live `~/.claude/projects`. Results recorded in Corpus Snapshot header above. Manifest committed (sha `9b1bd73`). Tarballs are local-only (gitignored).

**Finding — subset sizing.** The subset ended up at 48% of the full corpus by uncompressed bytes (748 MiB vs 1.55 GiB), not the intended ~5%. Cause: the largest project alone is 748 MiB, which exceeds both the 5% target (≈77 MiB) and the 100 MiB cap, and the selection rule takes whole projects only — so the loop picks that one project and stops.

Implications:
- Fast-iteration speedup from subset is much smaller than planned (~2×, not ~20×).
- Subset shape is "one giant project" rather than "a representative slice of the corpus distribution."
- Subset is still useful: it exercises the largest-project hot path (which is where most cold-import time lives anyway), and it's still smaller than full.
- Full-corpus runs remain authoritative either way.

Options to revisit (not fixing now):
- (a) Accept as-is — subset tests the hot-project shape.
- (b) Rewrite subset selection to "smallest projects first until target" — gives ~77 MiB of many small projects, tests the tail.
- (c) Rewrite to "include 2-3 medium-sized projects near the median" — needs a median computation.
- (d) Keep current subset as `subset-large.tar.zst`, add a second `subset-small.tar.zst` with option (b) — both served by the same harness via a flag.

Decision: defer to user checkpoint (Task 13) after baselines are in hand. If the subset run time on full mode is acceptable (<30s wall), the current subset is fine for iteration.

---

## RESUME HERE (if session was reset, read this first)

Last updated: 2026-04-11 (end of Task 8)
Current phase: Phase 1 — measure
Current branch: `import-perf`
Current worktree: `/home/ketan/project/gnomon/.worktrees/import-perf`
Primary repo root (do not implement here): `/home/ketan/project/gnomon`

### How to resume
1. `cd /home/ketan/project/gnomon/.worktrees/import-perf`
2. Verify: `git rev-parse --abbrev-ref HEAD` → must print `import-perf`
3. Read this log's Phase Log (latest entries first) for context.
4. Read `docs/specs/2026-04-10-import-perf-design.md` if you need the big picture.
5. Read `docs/specs/2026-04-10-import-perf-phase1-plan.md` for the task list — you are between Task 8 and Task 9.
6. Continue at the "Next action" below.

### Last completed
Task 8 — parse-vs-SQL split added inside `normalize_transcript_jsonl_file_inner`. Two local `Duration` accumulators wrap `serde_json::from_str` and the per-record SQL work (`initialize_context` / `flush_buffered_records` / `process_record`); the inner function now returns `(outcome, parse_total, sql_total)` and the outer wrapper attaches them as `parse_ms` / `sql_ms` fields on the `import.normalize_jsonl` span. Code commit sha `d539ff6`. All quality gates pass.

### Next action
**Task 9 (Phase 1 plan):** Write the benchmark harness as a `gnomon-core` example at `crates/gnomon-core/examples/import_bench.rs`. Full spec in `docs/specs/2026-04-10-import-perf-phase1-plan.md` starting at line 848.

After Task 9, Tasks 10-12 capture the baseline runs and CPU profiles. Task 13 is the first user checkpoint (review baseline, decide subset sizing).

### Uncommitted state
None. Working tree is clean on `import-perf`.

### Commits so far on `import-perf` (most recent last)
- `dc136b5` docs: add import performance optimization design
- `cbd3516` docs: add Phase 1 (measure) implementation plan
- `2c6e57a` log: initialize import perf running log
- `d49560c` chore: reserve tests/fixtures/import-corpus for perf snapshots
- `1b1320c` feat: add corpus capture script for import-perf benchmarks
- `9b1bd73` chore: commit import-corpus manifest for initial snapshot
- `2a3a47a` feat(import): wire PerfLogger into import hot path
- `d539ff6` feat(import): split parse vs SQL time in per-record loop

### Target status
Not set (pending Task 14, which requires Task 10-12 baseline data).

### Candidate ranking
See design doc Section 4 for the initial ranking (Tier A: A1, A2a, A2b, A3, A4, A5; Tier B: B1-B5; Tier C: C1-C3). Live re-ranking starts in Phase 2 after the baseline profile is in hand.

### Open questions for user at next checkpoint
1. **Subset sizing.** Is the 48%-of-full single-project subset OK for iteration, or do we want to rewrite `capture.sh` to produce a second "small" subset (smallest-first selection)?
2. **Target number.** Depends on baseline data from Tasks 10-12.

### Session-resumption sanity check
If you're reading this after a context reset, run:
```
git -C /home/ketan/project/gnomon/.worktrees/import-corpus rev-parse --abbrev-ref HEAD 2>/dev/null
```
No — correct path:
```
cd /home/ketan/project/gnomon/.worktrees/import-perf
git rev-parse --abbrev-ref HEAD
git log --oneline -10
ls tests/fixtures/import-corpus/
```
Expected: branch `import-perf`, 7 commits (design + plan + 5 log/chore/feat), `MANIFEST.md` + `full.tar.zst` + `subset.tar.zst` + `capture.sh` + `.gitkeep` present in fixtures dir.
