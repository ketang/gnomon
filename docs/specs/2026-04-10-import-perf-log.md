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

Last updated: 2026-04-11 (end of Task 4)
Current phase: Phase 1 — measure
Current branch: `import-perf`
Current worktree: `/home/ketan/project/gnomon/.worktrees/import-perf`
Primary repo root (do not implement here): `/home/ketan/project/gnomon`

### How to resume
1. `cd /home/ketan/project/gnomon/.worktrees/import-perf`
2. Verify: `git rev-parse --abbrev-ref HEAD` → must print `import-perf`
3. Read this log's Phase Log (latest entries first) for context.
4. Read `docs/specs/2026-04-10-import-perf-design.md` if you need the big picture.
5. Read `docs/specs/2026-04-10-import-perf-phase1-plan.md` for the task list — you are between Task 4 and Task 5.
6. Continue at the "Next action" below.

### Last completed
Task 4 — corpus captured, manifest committed (sha `9b1bd73`). Corpus snapshot header populated with SHAs and sizes. Subset-sizing finding logged (see phase entry above).

### Next action
**Task 5 (Phase 1 plan):** Add `perf_logger: Option<PerfLogger>` to `ImportWorkerOptions` in `crates/gnomon-core/src/import/chunk.rs` and thread it through `import_chunk`. Proceed then into Task 6 (normalize.rs spans) and Task 7 (classify + rollup spans) — these three tasks form one atomic change because the field additions break compilation until all three land. Single commit for Tasks 5-7. Task 8 (parse-vs-SQL split) is a separate commit.

All code for these tasks is fully specified in `docs/specs/2026-04-10-import-perf-phase1-plan.md`. Key integration points:
- `crates/gnomon-core/src/import/chunk.rs:66-69` — `ImportWorkerOptions` struct.
- `crates/gnomon-core/src/import/chunk.rs:239` — `import_all` public entry point; construct `PerfLogger::from_env(db_path.parent())` here.
- `crates/gnomon-core/src/import/chunk.rs:218` — `start_startup_import_with_mode_and_progress`; construct logger here too.
- `crates/gnomon-core/src/import/chunk.rs:828` — `import_chunk` inner function; wrap body in `PerfScope::new(...)` and pass `options.perf_logger.clone()` into `NormalizeJsonlFileParams` and `BuildActionsParams`.
- `crates/gnomon-core/src/import/mod.rs` — `NormalizeJsonlFileParams` definition; add `perf_logger: Option<PerfLogger>` field.
- `crates/gnomon-core/src/import/normalize.rs:78` — `normalize_transcript_jsonl_file`; add `import.normalize_jsonl` span and fields.
- `crates/gnomon-core/src/classify/mod.rs:21` — `build_actions`; add `perf_logger` to `BuildActionsParams`, wrap in `import.build_actions` span.
- `crates/gnomon-core/src/rollup.rs` — add `perf_logger` arg to `rebuild_chunk_action_rollups` and `rebuild_chunk_path_rollups`.

`PerfLogger` / `PerfScope` API is in `crates/gnomon-core/src/perf.rs` — `PerfLogger::from_env(state_dir) -> Result<Option<PerfLogger>>`, `PerfScope::new(Option<PerfLogger>, impl Into<String>)`, `.field(k, v)`, `.finish_ok()`, `.finish_error(&err)`.

Quality gates required before the Task 5-7 commit: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo build --workspace`.

After Task 5-7 commits, present the diff for user commit approval per design doc Section 8 (commit-on-approval workflow). The design says code commits need explicit user approval; log-only commits do not.

### Uncommitted state
None. Working tree is clean on `import-perf`.

### Commits so far on `import-perf` (most recent last)
- `dc136b5` docs: add import performance optimization design
- `cbd3516` docs: add Phase 1 (measure) implementation plan
- `2c6e57a` log: initialize import perf running log
- `d49560c` chore: reserve tests/fixtures/import-corpus for perf snapshots
- `1b1320c` feat: add corpus capture script for import-perf benchmarks
- `9b1bd73` chore: commit import-corpus manifest for initial snapshot
- `<next>`  log: record corpus SHAs and subset sizing finding (this commit)

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
