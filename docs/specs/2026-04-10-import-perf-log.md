# Import Perf — Running Log

> Companion to `docs/specs/2026-04-10-import-perf-design.md`. Append-only phase log plus overwritten Resume Block. First action of any fresh session: read the Resume Block, then the last 2–3 phase entries, then continue.

## Environment
- Host: `pontoon` — Linux 6.6.87.2-microsoft-standard-WSL2 #1 SMP PREEMPT_DYNAMIC Thu Jun 5 18:30:46 UTC 2025 x86_64 (WSL2 on Windows)
- CPU: AMD Ryzen 5 5600X 6-Core Processor (1 socket × 6 cores × 2 threads = 12 logical CPUs)
- RAM: 49,294,516 kB (≈48 GiB) available to the WSL VM
- Repo filesystem (`/home/ketan/project/gnomon/.worktrees/import-perf`): `ext2/ext3` (this is what `stat --file-system` reports for the WSL ext4 mount)
- DB filesystem (bench harness): `tmpfs` via `tempfile::TempDir` under `/tmp`. **Not** the production path `~/.local/share/gnomon/` (which doesn't exist on this host yet) — bench numbers reflect tmpfs IO, which is faster than a real disk-backed home dir. Production startup wall on a real disk will likely be slower.
- Rust: `rustc 1.93.1 (01f6ddf75 2026-02-11)`
- SQLite (bundled via `libsqlite3-sys 0.37.0`): `3.50.4`

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

### 2026-04-11 — Task 10 complete: subset baseline captured

Three back-to-back runs each of `import_bench --corpus subset` in `--mode full` and `--mode startup`, release build, on the environment captured in the Frozen Header above. Perf logs at `/tmp/gnomon-perf-subset-{full,startup}-{1,2,3}.jsonl`, run logs at `/tmp/gnomon-subset-{full,startup}-{1,2,3}.log`. The example was driven via the new `--perf-log` CLI flag (instead of the env-var path the plan suggested) so the workspace's `unsafe_code = "forbid"` lint stays satisfied.

**Subset, mode=full** (single-project corpus, 708 MB JSONL, 35 chunks: 1 startup + 34 deferred)
- Wall (3 runs): **69.324s / 68.430s / 65.017s — median 68.430s**
- Throughput at median: **10.35 MB/s parsed**
- Row counts (identical across runs): project=1, source_file=1649, import_chunk=35, conversation=1648, stream=1648, record=212788, message=130478, message_part=179412, turn=4915, action=50463
- DB size: 186.99 MB
- Per-phase split (median run = run 2, perf log `/tmp/gnomon-perf-subset-full-2.jsonl`, summed across all events):

  | operation | total ms | share of import.chunk wall |
  | --- | ---: | ---: |
  | `import.chunk` (outer) | 66346.6 | 100% |
  | `import.normalize_jsonl` | 37359.9 | 56.3% |
  | &nbsp;&nbsp;↳ `parse_ms` (serde_json::from_str) | 1633.8 | 2.5% |
  | &nbsp;&nbsp;↳ `sql_ms` (process_record + flush + init) | 9453.2 | 14.2% |
  | &nbsp;&nbsp;↳ unaccounted (commit, finish_import, line read, loop overhead) | ≈26273 | 39.6% |
  | `import.build_actions` | 26524.5 | 40.0% |
  | `import.build_turns` | 1020.2 | 1.5% |
  | `import.finalize_chunk` | 2147.0 | 3.2% |
  | `import.rebuild_path_rollups` | 559.7 | 0.8% |
  | `import.rebuild_action_rollups` | 67.5 | 0.1% |

**Subset, mode=startup** (24h slice of the subset corpus)
- Wall (3 runs): **2.485s / 2.452s / 2.513s — median 2.485s**
- Throughput at median: 285 MB/s parsed (the parse-ratio is misleading here — startup mode only loads the recent-24h slice, so most of the 708 MB JSONL is never read)
- Row counts (stable): conversation=27, stream=27, record=7166, message=4549, message_part=6160, turn=192, action=1670
- DB size: 5.94 MB
- Per-phase split (median run = run 2):

  | operation | total ms |
  | --- | ---: |
  | `import.chunk` (outer) | 1550.0 |
  | `import.normalize_jsonl` | 905.4 |
  | &nbsp;&nbsp;↳ `parse_ms` | 50.0 |
  | &nbsp;&nbsp;↳ `sql_ms` | 302.0 |
  | `import.build_actions` | 553.8 |
  | `import.build_turns` | 34.6 |
  | `import.finalize_chunk` | 39.4 |

  Wall (2.485s) − import.chunk total (1.55s) ≈ 0.94s of source-scan + Database::open + plan-build + harness extras outside the chunk loop. That's a real cost in startup mode and worth profiling separately.

**Findings, surprises, and load-bearing observations**

1. **`build_actions` is 40% of cold-import wall on the subset.** That's bigger than I expected — almost the same magnitude as the entire `normalize_jsonl` phase (which is the JSON-parsing + per-record-SQL + commit pipeline). The classification pass walks every persisted message and writes `action` rows; on the subset that's 130k messages → 50k actions. This is an obvious Tier-A target.
2. **`serde_json::from_str` is *not* the bottleneck.** Parse time is **2.5%** of `import.chunk` wall (1.6s out of 66s). Switching to a faster JSON parser (`simd-json`, `sonic-rs`, etc.) would save at most ~1.5s out of 66s on this corpus shape — not worth the dependency churn unless we can also amortize allocation. Candidate **A1 (faster JSON)** drops sharply in priority.
3. **`process_record` SQL inside the loop is 14%.** Not negligible, but it's smaller than the *unaccounted* portion of `normalize_jsonl` (26s ≈ 40%). The unaccounted slice covers `tx.commit()` (one big WAL flush per file), `state.finish_import`, the `BufRead::lines()` IO, and per-iteration loop overhead. Before optimizing `process_record` we should add a span around `tx.commit()` and `finish_import` so we know which one dominates.
4. **`import.rebuild_*_rollups` is rounding error** (0.6s of 66s, ~1%). Not worth touching in Phase 2.
5. **Run-to-run variance is meaningful** (~6% spread on full mode, ~2.5% on startup). Three runs is the minimum we can get away with. If a candidate produces a <5% improvement we'll need more runs to call it real.
6. **Startup mode wall has a non-`import.chunk` floor of ≈940 ms** (scan + DB open + plan build + harness overhead). On a target of "fast TUI gate," this floor is roughly 38% of total wall on the subset. Worth a span around `scan_source_manifest` and `build_import_plan` in Task 11 / Phase 2 instrumentation.
7. **Subset is unbalanced.** Because the subset is a single huge project, *all* 35 of its chunks land in *one* execution — and 34 of them are "deferred" (older than 24h). That mirrors the full-corpus shape where the bulk of work is also deferred chunks. Subset numbers should track full-corpus numbers proportionally for optimizations that target the per-chunk inner loop, but optimizations targeting the *plan builder* or *project-level overhead* will look near-zero on the subset and only show up on the full corpus.

Committed as sha `<filled in next commit>`. Quality gates: nothing rebuilt, this is data capture only.

### 2026-04-11 — Task 9 complete: import_bench example harness

Added `crates/gnomon-core/examples/import_bench.rs`. Extracts a corpus tarball into a tmpdir, opens a fresh SQLite database in another tmpdir, runs source scan, then either `import_all_with_perf_logger` (full mode) or `start_startup_import_with_perf_logger` (startup mode), and prints wall time, MB/s, per-table row counts, and final DB size. CLI: `--corpus subset|full --mode full|startup --repeats N --perf-log <path> --keep-db`.

The plan's draft used `unsafe { std::env::set_var(...) }` to drive `PerfLogger::from_env`, but the workspace has `unsafe_code = "forbid"` which cannot be overridden. Instead I added two pub helpers in `crates/gnomon-core/src/import/chunk.rs`: `import_all_with_perf_logger` and `start_startup_import_with_perf_logger`. The existing env-driven entry points (`import_all`, `start_startup_import_with_mode_and_progress`) now delegate to these and remain wire-compatible. The example constructs `PerfLogger::open_jsonl` directly and passes it in.

`clap` added as a `gnomon-core` dev-dependency (workspace version) for the example's `Parser` derive. `tempfile` was already a dev-dep.

Smoke test results on the subset corpus (single-project, 708 MB JSONL, release build):
- `--mode full`: wall **32.512s**, throughput **21.78 MB/s parsed**, 35 chunks (1 startup + 34 deferred, 0 failures), 212,788 records, 130,478 messages, 4,915 turns, 50,463 actions, DB 186.99 MB. Perf log at `/tmp/gnomon-perf-smoke.jsonl` contains 5,085 events across all expected operations: `import.chunk`, `import.normalize_jsonl` (with `parse_ms` and `sql_ms` fields populated, e.g. `parse_ms=2.07 sql_ms=9.64` on a 17.5ms file), `import.build_turns`, `import.build_actions`, `import.finalize_chunk`, `import.rebuild_action_rollups`, `import.rebuild_path_rollups`.
- `--mode startup`: wall **1.788s**, throughput 396 MB/s parsed (most of the 708 MB stays unparsed because startup mode only loads the recent-24h slice — 27 conversations / 7,166 records / 192 turns / 1,670 actions, DB 5.94 MB). This is the time-to-TUI-gate.

Committed as sha `0c24048`. Quality gates: fmt clean, clippy `-D warnings` clean, full workspace test suite passes (33 + 186 + 142 + 3 = 364 tests, 1 ignored).

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

Last updated: 2026-04-11 (end of Task 10)
Current phase: Phase 1 — measure
Current branch: `import-perf`
Current worktree: `/home/ketan/project/gnomon/.worktrees/import-perf`
Primary repo root (do not implement here): `/home/ketan/project/gnomon`

### How to resume
1. `cd /home/ketan/project/gnomon/.worktrees/import-perf`
2. Verify: `git rev-parse --abbrev-ref HEAD` → must print `import-perf`
3. Read this log's Phase Log (latest entries first) for context.
4. Read `docs/specs/2026-04-10-import-perf-design.md` if you need the big picture.
5. Read `docs/specs/2026-04-10-import-perf-phase1-plan.md` for the task list — you are between Task 10 and Task 11.
6. Continue at the "Next action" below.

### Last completed
Task 10 — subset baseline captured. Three runs each in full and startup mode against the single-project subset corpus. Median full wall **68.430s** (10.35 MB/s parsed), median startup wall **2.485s**. Per-phase split written into the Phase Log entry above. Headline finding: `import.build_actions` is **40%** of cold-import wall and `serde_json::from_str` is only **2.5%** — Tier-A candidate **A1 (faster JSON)** drops in priority and **`build_actions`** rises sharply. Environment fingerprint committed separately as sha `43d8421`. Subset baseline summary commit sha to be filled in by the next commit.

### Next action
**Task 11 (Phase 1 plan):** Capture the full-corpus baseline. Same recipe as Task 10 but `--corpus full`. Both modes, 3 runs each. Expect substantially longer full-mode walls (the full corpus is ~2.2× the subset by uncompressed bytes). Startup mode on the full corpus is the **primary metric** for the user-facing target. After capture, populate the `## Baseline` section of the Frozen Header with the median numbers from Tasks 10 and 11.

After Task 11, Task 12 captures one CPU profile each (full mode and startup mode) via `samply`. Then Task 13 is the first user checkpoint (review baseline, decide subset sizing) and Task 14 is the target-number checkpoint.

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
- `c876463` log: tick Task 8 step boxes and update resume block
- `0c24048` feat: add import_bench example for perf measurement
- `929a3d0` log: tick Task 9 step boxes and record bench harness numbers
- `43d8421` log: record environment fingerprint

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
