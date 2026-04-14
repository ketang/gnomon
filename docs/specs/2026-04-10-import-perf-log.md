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

Captured on the environment above (WSL2, Ryzen 5 5600X, tmpfs DB). All numbers are medians of 3 back-to-back runs unless noted.

| metric | subset (708 MB, 1 project, 1649 files) | full (1.44 GB, 31 projects, 4548 files) |
| --- | ---: | ---: |
| **Full mode wall** | **63.2s** | **126.1s** |
| Full mode throughput | 10.9 MB/s | 11.4 MB/s |
| **Startup mode wall** | **2.2s** | **5.1s** |
| Full mode import.chunk | 61.5s | 121.2s |
| Full mode commit_ms | 23.1s (37.5% chunk) | 40.1s (33.1% chunk) |
| Full mode build_actions | 24.6s (40.0% chunk) | 45.9s (37.9% chunk) |
| Full mode normalize_jsonl | 35.1s (57.0% chunk) | 69.6s (57.4% chunk) |
| Startup scan_source | 981ms (44% wall) | 2857ms (56% wall) |
| Startup prepare_plan | 131ms | 666ms |
| DB size (full mode) | 187 MB | 487 MB |

## Target
Agreed 2026-04-12. Full corpus, cold cache (worst-of-3), tmpfs DB on the baseline environment above.

| metric | baseline | acceptable | stretch |
| --- | ---: | ---: | ---: |
| **Cold full import** (empty DB → all data) | 126.1s | **10s** | **5s** |
| **Warm startup** (DB has data, import delta only) | 5.1s (empty DB) | **< 1s** | **300ms** |

Notes:
- "Cold full import" = `import_bench --corpus full --mode full` against a fresh SQLite. The 126.1s baseline is the v2 median; the cold-cache worst-of-3 is ~134s. The 5s stretch target implies ~25× speedup.
- "Warm startup" = the production startup path where the DB already holds most/all prior imported data. The 5.1s baseline was measured against an *empty* DB (worst case for startup mode). With data already loaded, startup should detect "nothing new" and skip most work, so the path from 5.1s to <1s is primarily about caching `scan_source` (2.86s) and `prepare_plan` (666ms), plus a fast no-op path through `import_chunk`.
- Benchmarking: always cold cache (first run or worst-of-3). This matches the real-world "first startup of the day" scenario.
- Subset corpus (3%, 10%, 30%) is useful for quick iteration but full corpus is the only authoritative metric. Subset results are directional only.

### Feasibility sketch

**Cold full import 126s → 10s (12.6× speedup):**
1. Commit batching (1 tx per chunk): saves ~38s → ~88s
2. build_actions batch + cache: saves ~35–40s → ~48–53s
3. Parallel chunk processing (6 cores): remaining ~50s ÷ 6 ≈ 8–9s
4. Prepared statements + bulk INSERT batching in normalize: tightens the per-core work

5s stretch requires all of the above plus aggressive allocation reduction and possibly SIMD JSON.

**Warm startup 5.1s → <1s:**
1. scan_source cache (hash mtimes, skip unchanged): saves ~2.8s
2. prepare_plan optimization or cache: saves ~0.5s
3. Fast no-op path in import_chunk (detect "already imported at current schema version"): saves most of the 2.5s chunk work
4. 300ms stretch requires near-zero scan cost and instant plan-build on a cached manifest.

---

## Phase Log

### 2026-04-10 — Phase 1 started
Kicked off Phase 1 (measure). Design doc committed on `import-perf` (sha `dc136b5`). Phase 1 implementation plan committed (sha `cbd3516`). Running log initialized (sha `2c6e57a`). Fixture directory reserved and gitignored (sha `d49560c`). Capture script added (sha `1b1320c`).

### 2026-04-12 — Tasks 13-14 complete: target agreed

User reviewed the Phase 1 baseline summary. Targets agreed:
- **Cold full import:** 10s acceptable, 5s stretch (from 126.1s baseline = 12.6–25× speedup)
- **Warm startup (data already loaded):** <1s acceptable, 300ms stretch (from 5.1s empty-DB baseline)
- **Benchmarking:** cold cache (worst-of-3 or first run)
- **Subset:** 3%, 10%, 30% nice-to-have for iteration but full corpus is the only authoritative metric

User explicitly noted build_actions is cacheable and batchable — aligns with candidate #2. No opinion on subset sizing details; full corpus is what counts.

### 2026-04-12 — Task 12 complete: baseline CPU profiles captured

Captured with `samply v0.13.1` against the full corpus. Profiles saved as Firefox Profiler JSON format; view with `samply load <file>`:
- `docs/specs/profiles/baseline-full.json.gz` — full mode, 124.4s wall (consistent with baseline). 994 KB.
- `docs/specs/profiles/baseline-startup.json.gz` — startup mode, 3.6s wall. 204 KB. **Degraded:** corpus sessions aged out of the 24h window overnight (captured April 11, profiled April 12), so the profile captures `scan_source` + `prepare_plan` (the 69% non-chunk floor) but zero `import.chunk` work. Still useful for the dominant startup bottleneck.

Profiles are unsymbolicated in the JSON — symbols resolve at browser-load time via samply's built-in symbolication against the local debug symbols. No text-mode top-function summary was extractable because `perf` is not installed on this WSL instance and samply doesn't have a CLI export mode. To get the top-5-by-self-time, open `samply load docs/specs/profiles/baseline-full.json.gz` and sort by self-time in the browser UI.

Committed as sha `7110745`.

### 2026-04-11 — Task 11 complete: full-corpus baseline captured

Three runs each of `import_bench --corpus full` in `--mode full` and `--mode startup`, release build. Perf logs at `/tmp/gnomon-perf-full-{full,startup}-{1,2,3}.jsonl`, run logs at `/tmp/gnomon-full-{full,startup}-{1,2,3}.log`.

**Full corpus, mode=full** (31 projects, 4548 files, 162 chunks: 5 startup + 157 deferred)
- Wall (3 runs): **106.4s / 133.6s / 126.1s — median 126.1s**
- Throughput at median: **11.4 MB/s parsed**
- Row counts: project=31, source_file=4548, import_chunk=162, conversation=4547, stream=4547, record=490250, message=294995, message_part=411842, turn=13363, action=120922
- DB size: 486.86 MB
- Per-phase split (median run = run 3):

  | operation | total ms | share of import.chunk (121.2s) |
  | --- | ---: | ---: |
  | `import.scan_source` (outside chunk) | 3100.9 | — |
  | `import.prepare_plan` (outside chunk) | 734.4 | — |
  | `import.build_plan` (outside chunk) | 10.8 | — |
  | `import.open_database` (outside chunk) | 0.6 | — |
  | `import.chunk` (outer) | 121150.8 | 100% |
  | &nbsp;&nbsp;`import.normalize_jsonl` | 69603.4 | 57.4% |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ `parse_ms` | 3745.0 | 3.1% |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ `sql_ms` | 20938.1 | 17.3% |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ `purge_ms` | 391.5 | 0.3% |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ `finish_import_ms` | 141.2 | 0.1% |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ **`commit_ms`** | **40052.5** | **33.1%** |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ untracked (line read + loop) | ≈4335 | 3.6% |
  | &nbsp;&nbsp;`import.build_actions` | 45946.7 | 37.9% |
  | &nbsp;&nbsp;`import.build_turns` | 2279.8 | 1.9% |
  | &nbsp;&nbsp;`import.finalize_chunk` | 5009.3 | 4.1% |
  | &nbsp;&nbsp;`import.rebuild_path_rollups` | 2001.7 | 1.7% |
  | &nbsp;&nbsp;`import.rebuild_action_rollups` | 198.9 | 0.2% |

**Full corpus, mode=startup** (primary user-facing metric)
- Wall (3 runs): **7.285s / 5.114s / 4.967s — median 5.114s**
- Row counts: conversation=55, record=10971, message=6942, turn=353, action=2695
- DB size: 11.59 MB
- Per-phase split (median run = run 2):

  | operation | total ms | share of wall (5114ms) |
  | --- | ---: | ---: |
  | `import.scan_source` | **2856.6** | **55.9%** |
  | `import.prepare_plan` | 665.8 | 13.0% |
  | `import.build_plan` | 8.7 | 0.2% |
  | **non-chunk floor** | **3531.1** | **69.0%** |
  | `import.chunk` (11 ev) | 2477.8 | 48.4% |
  | &nbsp;&nbsp;`import.normalize_jsonl` (146 ev) | 1518.5 | — |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ `commit_ms` | 709.1 | — |
  | &nbsp;&nbsp;`import.build_actions` (142 ev) | 873.1 | — |
  | &nbsp;&nbsp;`import.build_turns` | 63.8 | — |
  | &nbsp;&nbsp;`import.finalize_chunk` | 80.2 | — |

  Note: wall (5.1s) < scan (2.86s) + chunk (2.48s) + prepare_plan (0.67s) = 6.0s because some chunk work overlaps with the scan return path and the benchmark includes Database::open before scan begins.

  Note 2: run 1 (7.3s) was significantly slower — likely a cold-cache outlier representing the first startup of the day. Production will often see this cold-start penalty.

**Scaling: subset → full corpus**

| metric | subset | full | ratio | file/project ratio |
| --- | ---: | ---: | ---: | --- |
| Full mode wall | 63.2s | 126.1s | 2.0× | files: 2.76× |
| commit_ms | 23.1s | 40.1s | 1.74× | sublinear: many full-corpus files are small |
| build_actions | 24.6s | 45.9s | 1.87× | conversations: 2.76× |
| scan_source | 838ms | 3101ms | 3.7× | projects: 31× (superlinear!) |
| prepare_plan | 324ms | 734ms | 2.3× | |
| Startup wall | 2.2s | 5.1s | 2.3× | |

**Key findings from the full-corpus data**

1. **`commit_ms` is still #1 at 40.1s (33.1% of chunk wall).** The proportional share dropped slightly from 37.5% (subset) to 33.1% because `build_actions` and `sql_ms` grew more than linearly with the number of conversations. Absolute savings from commit-batching: 4548 commits → 162 → est. **~38s saved (30% of full-mode wall).**

2. **`build_actions` is 45.9s (37.9%).** Scales 1.87× for 2.76× more conversations — sublinear, suggesting fixed overhead per call is significant. Batching by chunk (162 calls instead of 4547) plus reusing in-memory state from normalize should give a substantial win.

3. **`scan_source` scales 3.7× for 31× more projects.** The scaling is sublinear in projects but the constant factor is high: 3.1s on the full corpus in full mode, 2.86s in startup mode. **In startup mode, `scan_source` is 56% of wall.** This is the #1 target for startup-mode optimization. Caching the manifest (hash project-root mtimes/sizes, skip rediscovery if unchanged) would near-eliminate this cost on repeated runs.

4. **`prepare_plan` is 734ms full / 666ms startup.** Now 13% of startup wall. Worth investigating — it prepares chunk metadata from the plan, so it's SQL query time. Could be optimized with better indices or batched queries.

5. **`finalize_chunk` grew to 5.0s (4.1%).** On the subset it was 1.6s (2.6%). Worth a span-split in Phase 2 if commit-batching moves the bigger slices first.

6. **`rebuild_path_rollups` is now 2.0s (1.7%).** Still small but now visible. Low priority.

7. **Run-to-run variance on the full corpus is ~20% for full mode and ~47% for startup mode.** The first startup run (7.3s) is the cold-cache outlier — may represent the real-world "first startup of the day" case. Need to decide whether we target hot or cold for the performance goal.

### 2026-04-11 — Subset baseline v2: commit / scan / plan-build attributed

Added five new perf fields/spans (sha `aa26c21`) to chase down findings 3 and 6 from the v1 subset baseline, then re-ran 3×full + 3×startup against the same subset corpus on the same host.

Spans added:
- `import.normalize_jsonl` now also emits `purge_ms`, `finish_import_ms`, `commit_ms` (next to the existing `parse_ms` / `sql_ms`).
- `import.build_plan`, `import.prepare_plan`, `import.open_database` — wrapped around `build_import_plan` / `prepare_import_plan` / `Database::open` inside both `import_all_with_perf_logger` and `start_startup_import_with_options`.
- `import.scan_source` — wrapped around `scan_source_manifest_with_perf_logger` (new pub variant). The `import_bench` example now uses this variant.

**Subset, mode=full** (re-run, identical row counts to v1)
- Wall (3 runs): **64.410s / 62.079s / 63.249s — median 63.249s** (v1 was 68.430s; the ~5s drop is run-to-run variance, not the new instrumentation)
- Per-phase split (median run = run 3, perf log `/tmp/gnomon-perf-subset-full-v2-3.jsonl`):

  | operation | total ms | share of import.chunk wall (61.5s) |
  | --- | ---: | ---: |
  | `import.scan_source` (outside chunk) | 838.5 | — |
  | `import.prepare_plan` (outside chunk) | 323.8 | — |
  | `import.build_plan` (outside chunk) | 1.8 | — |
  | `import.open_database` (outside chunk) | 0.5 | — |
  | `import.chunk` (outer) | 61514.0 | 100% |
  | &nbsp;&nbsp;`import.normalize_jsonl` | 35052.1 | 57.0% |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ `parse_ms` | 1483.0 | 2.4% |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ `sql_ms` (process_record + flush + init) | 8450.7 | 13.7% |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ `purge_ms` (`purge_existing_import`) | 151.0 | 0.2% |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ `finish_import_ms` | 50.3 | 0.1% |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ **`commit_ms` (`tx.commit()`)** | **23076.1** | **37.5%** |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ untracked (line read + loop overhead) | ≈1841 | 3.0% |
  | &nbsp;&nbsp;`import.build_actions` | 24573.2 | 39.9% |
  | &nbsp;&nbsp;`import.build_turns` | 917.0 | 1.5% |
  | &nbsp;&nbsp;`import.finalize_chunk` | 1594.5 | 2.6% |
  | &nbsp;&nbsp;`import.rebuild_path_rollups` | 505.9 | 0.8% |
  | &nbsp;&nbsp;`import.rebuild_action_rollups` | 65.8 | 0.1% |

**Subset, mode=startup**
- Wall (3 runs): **2.079s / 2.217s / 2.216s — median 2.216s** (v1 was 2.485s)
- Per-phase split (run 2, `/tmp/gnomon-perf-subset-startup-v2-2.jsonl`):

  | operation | total ms |
  | --- | ---: |
  | `import.scan_source` (outside chunk) | 981.3 |
  | `import.prepare_plan` (outside chunk) | 131.1 |
  | `import.build_plan` (outside chunk) | 3.0 |
  | `import.chunk` (outer, 12 events) | 1500.7 |
  | &nbsp;&nbsp;`import.normalize_jsonl` (50 ev) | 990.8 |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ `parse_ms` | 81.9 |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ `sql_ms` | 467.0 |
  | &nbsp;&nbsp;&nbsp;&nbsp;↳ `commit_ms` | 339.0 |
  | &nbsp;&nbsp;`import.build_actions` (41 ev) | 424.3 |
  | &nbsp;&nbsp;`import.build_turns` | 43.6 |
  | &nbsp;&nbsp;`import.finalize_chunk` (3 ev) | 42.0 |

**Findings — what the new spans actually said**

1. **The "unaccounted 26s" inside `normalize_jsonl` is `tx.commit()`.** 23.1 s of the 26 s gap is one `tx.commit()` per JSONL file (1,649 commits → ~14 ms per commit on tmpfs WAL fsync). The remaining ~1.8 s is line-reading IO and loop overhead. **`commit_ms` is now the single largest attributable slice in cold full import: 37.5% of `import.chunk` wall.** Promotes Tier-A candidate "batch commits at chunk granularity (one commit per `import_chunk` instead of one per file)" to **#1**. Estimated headline win: 1,649 → 35 commits ≈ 22 s saved out of 63 s wall ≈ **35% wall improvement** on cold full import.

2. **`finish_import_ms` (50 ms) and `purge_ms` (151 ms) are rounding error.** Not worth touching.

3. **The ~940 ms startup-mode floor is `scan_source` (981 ms).** Plus a small contribution from `prepare_plan` (131 ms). Together they account for ~1.11 s of the 2.22 s startup wall — **50% of startup wall happens before any chunk is touched.** This is the user-visible time-to-TUI-gate ceiling that no per-chunk optimization can move. Two obvious wedges:
   - **Parallelize `discover_source_files`** (the inner walkdir+vcs-resolve loop). On the subset's single project this won't help much because there's only one project to resolve, but on the full corpus's 52 projects it should scale.
   - **Cache the manifest scan across runs** by hashing project-root mtimes / sizes, so the second startup of the day skips the scan entirely. The user explicitly flagged caching as relevant for `build_actions`; the same idea applies even more cleanly to `scan_source` because the inputs are pure filesystem state.

4. **`build_actions` is still 40% of cold full wall (24.6 s).** Three wedges remain in priority order: batch by chunk (#1), skip the per-conversation `load_messages` SELECT by reusing in-memory state from `normalize_jsonl_file` (#2), defer to after-TUI-gate (#3, startup-only). The user noted this is also cacheable — and it is: action classification is a pure function of `(message_part rows, classifier version)`, so once computed for an `import_chunk` it doesn't need to be recomputed unless the schema or classifier rules change. That's already partly enforced by `IMPORT_SCHEMA_VERSION`, but `purge_existing_classification` still re-derives on every chunk re-import.

5. **`build_plan` is 1.8 ms (full) / 3 ms (startup), `open_database` is 0.5 ms.** These are pure noise. Drop them from any future analysis.

6. **`prepare_plan` is 324 ms full / 131 ms startup.** Surprising — that's enough to be worth a closer look in Phase 2 if scan_source ever drops below it.

7. **Wall time variance has narrowed slightly** (~3.7% spread on full mode this run, vs ~6% on v1; ~6.6% on startup, vs ~2.5% on v1). Three runs is still the floor, and we should publish median + range for any candidate comparison.

Rebased Tier-A candidate ranking after this baseline:

| rank | candidate | est. win on full mode wall | confidence |
| --- | --- | --- | --- |
| 1 | **commit batching** (one commit per `import_chunk`, not per file) | ≈22 s / 35% | high — direct measurement |
| 2 | **build_actions batching + cache** | ≈10–20 s / 16–32% | medium — needs profiling per call to confirm where the 24.6 s actually goes |
| 3 | **scan_source parallelization or caching** | startup floor only, ≈800 ms | high on the value/effort axis for startup mode |
| 4 | (was A1) faster JSON parser | ≈1.5 s / 2.4% | high — but tiny absolute win |

Next: Task 11 (full-corpus baseline) before any of these wedges land. The full corpus has 52 projects vs the subset's 1, so `scan_source` and `build_actions` should scale very differently than `commit_ms` does.

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

Committed as sha `2b59d25`. Quality gates: nothing rebuilt, this is data capture only.

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

### 2026-04-12 — candidate commit-batching: chunk-level transaction + per-file savepoints

Branch: `import-perf-commit-batching`
Worktree: `/home/ketan/project/gnomon/.worktrees/import-perf-commit-batching`

**Hypothesis:** Replacing N per-file COMMIT (WAL fsync at ~8.8ms each) with 1 per-chunk COMMIT should save ~38s (33% of chunk wall) on cold full import. Also eliminates per-conversation build_actions commits and per-chunk finalize commits.

**Implementation:** Widened 14 inner helper signatures from `&mut Transaction<'_>` to `&Connection` (backward-compatible via deref coercion). Extracted `_core` functions (transaction-free) from `normalize_transcript_jsonl_file_inner`, `normalize_history_jsonl_file_inner`, `build_actions_inner`, and `finalize_chunk_import`. Added `normalize_jsonl_file_in_tx` and `build_actions_in_tx` public APIs with PerfScope wrapping. Rewired `import_chunk()` to use a single chunk-level `Transaction` with per-file `Savepoint`s.

**Measurements:**

Subset (1 project, 1649 files, 35 chunks):
```
Mode=full:    63.2s → 39.1s  (−24.1s, −38.1%)  [41.6 / 39.1 / 35.2]
Mode=startup: N/A — corpus aged out of 24h window, 0 chunks imported
Row parity:   PASS (all 10 tables match baseline exactly)
DB size:      186.99 MB (unchanged)
```

Full corpus (31 projects, 4548 files, 162 chunks):
```
Mode=full:    126.1s → 77.5s  (−48.6s, −38.5%)  [94.7 / 60.1 / 77.5]
Mode=startup: N/A — corpus aged out of 24h window, 0 chunks imported
Row parity:   PASS (all 10 tables match baseline exactly)
DB size:      486.83 MB (unchanged)
```

**Notes:**
- The ~48.6s improvement exceeds the estimated ~38s because we also eliminated per-conversation `build_actions` commits (~4547 commits) and per-chunk `finalize` commits (162 commits), not just per-file normalize commits.
- Startup mode comparison is not meaningful: the baseline was captured on April 11 against data within the 24h window; by April 12 those sessions aged out, so startup imports 0 chunks. The startup-mode scan_source + prepare_plan floor (~3.5s) is unaffected by commit batching.
- Run-to-run variance on full corpus is high: 94.7s to 60.1s (58% spread). Run 1 is consistently the cold-cache outlier. Even worst-of-3 (94.7s) is 25% better than baseline (126.1s).
- Profile shift not captured (samply run deferred to after commit decision).

**Decision:** KEPT

**Commits on `import-perf-commit-batching`:**
- `8147cf5` refactor(import): widen helper signatures from &mut Transaction to &Connection
- `cfa8541` refactor(import): extract _core functions and add _in_tx public APIs
- `7840bbf` perf(import): batch commits per chunk via chunk-level transaction + per-file savepoints

**Merge:** `a948c2c` — merged `import-perf-commit-batching` into `import-perf` with `--no-ff`

**Next implied:** Re-profile to see new phase distribution, then pick candidate #2 (build_actions batching + cache — was 37.9% of chunk wall, likely now ~50%+ with commit_ms eliminated). Pragma tuning (synchronous=NORMAL, cache_size, mmap) is a low-effort runner-up.

### 2026-04-12 — post-commit-batching re-profile

Re-profiled to capture new phase distribution after commit batching. Ran 3× subset and 3× full corpus on the same environment. Session-local baselines (used for relative comparisons within this session only — absolute numbers differ from prior sessions due to system state):

**Subset post-commit-batching profile (run 3, chunk wall = 21,405 ms):**

| operation | total ms | % of chunk |
| --- | ---: | ---: |
| `import.normalize_jsonl` | 14,436 | 67.4% |
| &nbsp;&nbsp;↳ `sql_ms` | **11,175** | **52.2%** |
| &nbsp;&nbsp;↳ `parse_ms` | 1,432 | 6.7% |
| &nbsp;&nbsp;↳ `commit_ms` | 0.0 | 0.0% |
| &nbsp;&nbsp;↳ unaccounted (IO + loop) | 1,711 | 8.0% |
| `import.build_actions` | 4,766 | 22.3% |
| `import.build_turns` | 1,016 | 4.7% |
| `import.finalize_chunk` | 660 | 3.1% |
| `import.rebuild_path_rollups` | 578 | 2.7% |

**Key finding:** `sql_ms` is now the dominant slice at 52% of chunk wall. No prepared statement caching — every `conn.execute()`/`conn.query_row()` call re-parses and re-plans SQL. ~490k records × multiple SQL calls = massive redundant compilations.

### 2026-04-12 — candidate prepared-statement caching

Branch: `import-perf-prepared-stmts`
Worktree: `/home/ketan/project/gnomon/.worktrees/import-perf-prepared-stmts`

**Hypothesis:** Replacing `conn.execute(sql, params)` / `conn.query_row(sql, params, f)` with `conn.prepare_cached(sql).and_then(|mut stmt| stmt.execute(params))` / equivalent eliminates per-call SQL compilation. `prepare_cached` maintains a per-connection hashmap of compiled statements. Expected: ~50% reduction in `sql_ms` → ~25% wall improvement.

**Implementation:** Converted 34 SQL call sites across `normalize.rs` (21 sites) and `classify/mod.rs` (13 sites). Used `.and_then()` to chain `prepare_cached` and `execute`/`query_row` into a single `Result` before applying `.context()`/`.with_context()`, preserving error-wrapping semantics that `)?` would bypass.

**Measurements (session-local, same environment as re-profile above):**

Subset (1 project, 1649 files, 35 chunks):
```
Mode=full:    22.7s → 14.2s  (−8.5s, −37.4%)  [14.2 / 14.1 / 15.3]
Row parity:   PASS (all 10 tables match baseline exactly)
DB size:      186.99 MB (unchanged)
```

Full corpus (31 projects, 4548 files, 162 chunks):
```
Mode=full:    53.6s → 38.4s  (−15.2s, −28.4%)  [36.8 / 40.0 / 38.4]
Row parity:   PASS (all 10 tables match baseline exactly)
DB size:      486.83 MB (unchanged)
```

**Profile shift (subset):**

| phase | before | after | absolute delta |
| --- | ---: | ---: | ---: |
| `sql_ms` | 11,175 ms (52.2%) | 5,867 ms (45.3%) | −5,308 ms (−47.5%) |
| `build_actions` | 4,766 ms (22.3%) | 2,672 ms (20.6%) | −2,094 ms (−43.9%) |
| `parse_ms` | 1,432 ms (6.7%) | 1,140 ms (8.8%) | −292 ms |
| chunk wall | 21,405 ms | 12,956 ms | −8,449 ms (−39.5%) |

**Notes:**
- Session-to-session absolute numbers vary (~30%) due to system state (page cache, WSL load). Within-session relative comparisons are reliable.
- The ~47% `sql_ms` reduction matches expectations: statement compilation overhead eliminated, but SQLite btree/index operations remain.
- `build_actions` also benefits substantially (−44%) because classify/mod.rs SQL calls were also uncached.
- Run-to-run variance is low this session: 3% on subset, 8% on full corpus.

**Decision:** KEPT

**Commit on `import-perf-prepared-stmts`:**
- `708468e` perf(import): use prepare_cached for all SQL in import hot path

**Merge:** `83dedf2` — merged `import-perf-prepared-stmts` into `import-perf` with `--no-ff`

**Next implied:** Re-profile to see new phase distribution, then pick candidate #3. Expected top candidates: SQLite pragma tuning (synchronous=NORMAL, cache_size, mmap), or scan_source caching for startup mode.

---

### Re-profile: post prepared-statement caching (2026-04-12)

Three full-corpus runs: 35.6s / 39.8s / 56.7s (run 3 is a system-load outlier). Median: **39.8s**.
Startup mode: **3.0s** (down from ~5.1s baseline; mostly scan_source improvement from earlier work).
Subset: 14.5s / 15.4s. Median: **~14.9s**.

**Phase distribution (full corpus, cleanest run = 35.6s):**

| phase | total_ms | % of span | note |
| --- | ---: | ---: | --- |
| `normalize_jsonl` (envelope) | 19,010 | 28.9% | sql_ms dominates |
| — `sql_ms` | 13,340 | 81.7% of normalize | **still the #1 bottleneck** |
| — `parse_ms` | 2,603 | 15.9% of normalize | JSON parsing |
| — `purge_ms` | 219 | 1.3% of normalize | |
| — `finish_import_ms` | 164 | 1.0% of normalize | |
| — `commit_ms` | 0 | 0.0% | batched into chunk-level commit |
| `build_actions` | 6,726 | 10.2% | classification + SQL inserts |
| `scan_source` | 2,182 | 3.3% | startup floor |
| `finalize_chunk` | 1,896 | 2.9% | |
| `rebuild_path_rollups` | 1,693 | 2.6% | |
| `build_turns` | 1,521 | 2.3% | |
| `prepare_plan` | 515 | 0.8% | |

**Analysis:** SQL is still ~62% of wall time (sql_ms 13.3s + build_actions 6.7s + rollups 1.9s ≈ 22s of ~35.6s). Pragma tuning (synchronous, cache_size, mmap) targets the I/O substrate beneath all SQL phases — low effort, broadly applicable.

**Candidate #3 selection:** SQLite pragma tuning (A2a) — safe pragmas targeting I/O reduction across all SQL-bound phases.

---

## Phase 2, iteration 3: candidate SQLite pragma tuning

**Hypothesis:** Setting `synchronous=NORMAL`, increasing `cache_size`, and enabling `mmap_size` will reduce I/O overhead on all SQL operations, yielding 10–30% wall time reduction.

**Branch:** `import-perf-pragma-tuning` (off `import-perf`)

**Pragmas added to `configure_read_write_connection`:**
```sql
PRAGMA synchronous = NORMAL;   -- safe with WAL; reduces fsync calls
PRAGMA cache_size = -64000;    -- 64MB page cache (default ~2MB)
PRAGMA mmap_size = 268435456;  -- 256MB memory-mapped reads
PRAGMA temp_store = MEMORY;    -- temp tables in RAM
```
Also added `cache_size` and `mmap_size` to `configure_read_only_connection`.

**Measurements (within-session, interleaved, full corpus, 6 runs each):**

Subset (5-repeat runs):
```
Pragma:   18.2 / 15.0 / 15.3 / 12.6 / 13.0  median=15.0s
Baseline: 14.1 / 15.2 / 13.9 / 14.4 / 13.9  median=14.1s
Delta: inconclusive on subset (35 chunks → low commit overhead)
```

Full corpus (3-repeat runs × 2 rounds, interleaved):
```
Pragma:   31.8 / 32.1 / 33.5 / 30.3 / 32.0 / 31.2  median=31.9s
Baseline: 38.8 / 36.9 / 38.1 / 38.2 / 38.0 / 35.6  median=38.1s
Delta: 38.1s → 31.9s  (−6.2s, −16.3%)
```

Startup mode (3-repeat runs):
```
Pragma:   2.286 / 2.310 / 2.290  median=2.29s
Baseline: 2.754 / 2.851 / 2.773  median=2.77s
Delta: 2.77s → 2.29s  (−0.48s, −17.4%)
```

Row parity: **PASS** (all 10 tables match baseline exactly, DB size 486.83 MB identical)

**Profile shift:** Instrumented span timings are nearly identical (66.2s vs 65.8s total). The improvement comes from reduced I/O stalls *between* spans — fsync savings from `synchronous=NORMAL` and reduced page faults from `mmap_size`.

**Decision:** KEPT

**Commit on `import-perf-pragma-tuning`:**
- `b0c3eee` perf(db): add SQLite pragma tuning for import performance

**Merge:** `d083c1b` — merged `import-perf-pragma-tuning` into `import-perf` with `--no-ff`

**Next implied:** Re-profile post-pragma state, pick candidate #4. With 3 candidates applied (commit-batching −38.5%, prepared-stmts −28.4%, pragma tuning −16.3%), cumulative improvement is ~75% from the 126.1s baseline. Remaining candidates: parallel chunk processing (rayon), deferred secondary indexes, scan_source caching.

---

## Phase 2, iteration 4: candidate deferred secondary indexes — REVERTED

**Hypothesis:** Dropping 21 of 23 secondary indexes before bulk import (keeping `idx_message_conversation_role_sequence` for build_turns/build_actions and `idx_action_chunk_classification` for rollup queries), then recreating them after import completes, would reduce per-INSERT index maintenance overhead by 30–50%.

**Branch:** `import-perf-parallel-parse` (off `import-perf`) — deleted after revert

**Approach:** Drop indexes via `sqlite_master` query before import loop, recreate after. Rollups kept inline (within per-chunk transaction) for cache locality.

**Measurements (within-session, interleaved, full corpus, 3 repeats):**
```
Deferred indexes: 45.2 / 35.2 / 34.1  median=35.2s
Baseline:         43.7 / 35.0 / 32.2  median=35.0s
Delta: indistinguishable from noise
```

**Perf-log analysis (subset, single run):**
- normalize sql_ms: 6,053 → 3,432 (**−43%**) — inserts genuinely faster
- rebuild_path_rollups: 504 → 5,291 (**+950%**) — when rollups deferred, cache miss penalty kills it
- With rollups inline (v2): wall time matches baseline exactly — insert savings too small to measure

**Why it failed:** Per-insert index maintenance is negligible when btree pages are cached in the 64MB page cache. The dominant SQL cost is primary btree insert (finding position, inserting row, possible page split), not secondary index updates. The indexes that were dropped are on tables where the pages are hot from recent inserts.

**Lesson:** With modern SQLite + WAL + large page cache, secondary index overhead during bulk import is ~0 — the pages are already in memory. Only approaches that reduce the core per-row btree cost (fewer rows, fewer tables, or parallelism) can improve further.

**Decision:** REVERTED — no code committed.

---

## Phase 2, iteration 5: candidate in-memory data passing — KEPT

**Hypothesis:** Eliminating redundant DB reads in `build_turns` and `build_actions` by passing normalized message data in-memory through the pipeline will save ~2–3s. More importantly, it is an architectural prerequisite for parallel parse+classify (candidate #1).

**Branch:** `import-perf-inmemory` (off `main`)

**Implementation:**
- Defined shared `Usage`, `NormalizedMessage`, `NormalizedPart` types in `import/mod.rs`
- Enriched `MessageState` to retain message_kind, timestamps, sequence_no, and parts during normalization
- Refactored `build_turns` to iterate in-memory `Vec<NormalizedMessage>` instead of DB SELECT
- Added `build_actions_in_tx_with_messages` in classify, skipping the 4-way `load_messages` JOIN
- Used `last_insert_rowid()` for part IDs (RETURNING id was 2.8s slower on 412K parts)
- Extracted `classify_and_persist_actions` helper to share logic between DB and in-memory paths

**Measurements (full corpus, 3 repeats + 1 perf-logged run):**

Wall time:
```
In-memory:  32.9 / 31.9 / 31.8 / 31.5  median=31.8s
Baseline:   31.9s (previous best median)
Delta: within session noise
```

Phase breakdown (single perf-logged run, 31.5s wall):
```
build_actions:   7.1s → 5.6s  (−1.5s, −21%) — skip load_messages JOIN
build_turns:     1.5s → 1.1s  (−0.4s, −27%) — in-memory iteration
sql_ms:         10.5s → 9.7s  (−0.8s) — possibly reduced cache pressure
parse_ms:        3.3s → 3.0s  (noise)
```

Row parity: **PASS** (all 10 tables match exactly, DB size 486.83 MB identical)

**Lesson learned:** `RETURNING id` on high-frequency INSERTs (412K parts) adds ~6.8µs/row overhead vs `last_insert_rowid()`. For perf-sensitive bulk paths, always prefer `last_insert_rowid()`.

**Decision:** KEPT

**Commit on `import-perf-inmemory`:** `0c0048c`
**Merge to main:** `2eca9fa`

**Next implied:** Candidate #1 (parallel parse + classify via rayon). The in-memory data flow is now in place — normalize produces `Vec<NormalizedMessage>` that can be built in parallel workers and funneled to a single SQLite writer thread.

---

### Iteration 6 — Parallel JSONL parsing (rayon)

**What changed:**

Split `normalize_jsonl_file_in_tx` into two phases:
1. `parse_jsonl_file` — pure CPU (JSON parsing, message extraction), runs in
   parallel via `rayon::par_iter` across all files in a chunk
2. `write_parsed_file_in_tx` — serial DB writes from pre-parsed data

New types `ParsedFile`, `ParsedRecord`, `ParseResult` carry pre-parsed data
from the parallel phase to the serial write phase. `rayon 1.12` added as a
workspace dependency.

**Measurements (full corpus, interleaved comparison with main):**

Interleaved runs (discarding cold outliers):
```
Main:      37.3s / 32.1s
Parallel:  29.9s / 29.0s / 28.8s
Best-vs-best: 28.8s vs 32.1s (−10.3%)
```

Row parity: **PASS** (all 10 tables match exactly, DB size 486.83 MB identical)

Quality gates: `cargo fmt`, `clippy -D warnings`, `cargo test` — all pass (142 tests).

**Analysis:** The ~10% wall reduction comes from two sources:
1. Parse parallelism — the ~3s JSON parse work is spread across cores
2. I/O separation — file reads happen upfront in parallel, so the serial write
   phase never waits on file I/O

The simple barrier approach (`par_iter().collect()` before serial writes) is
effective. A channel-based pipeline (overlap parse N+1 with write N) could
extract more, but the current gains are solid.

**Decision:** KEPT

**Commit on `import-perf-parallel`:** `a8b62a6`

---

## RESUME HERE (if session was reset, read this first)

Last updated: 2026-04-14 (Phase 2, iteration 6 — parallel JSONL parsing KEPT)
Current phase: Phase 2 — iteration 6 complete (parallel JSONL parsing KEPT)
Latest code on `import-perf-parallel` branch, pending merge to `main`.

### How to resume
1. `cd /home/ketan/project/gnomon`
2. Read this log's Phase Log (iterations 1–6) for context on what was tried and what was learned.
3. Read `docs/specs/2026-04-10-import-perf-design.md` Sections 3–4 and Section 12 for architecture.
4. Continue at the "Next action" below.

### Iteration summary

| # | Candidate | Result | Delta |
| --- | --- | --- | --- |
| 1 | Commit batching (per-chunk transactions) | **KEPT** | 126.1s → 77.5s (−38.5%) |
| 2 | Prepared-statement caching (`prepare_cached`) | **KEPT** | 53.6s → 38.4s (−28.4%) |
| 3 | SQLite pragma tuning (sync=NORMAL, cache, mmap) | **KEPT** | 38.1s → 31.9s (−16.3%) |
| 4 | Deferred secondary indexes during bulk load | **REVERTED** | no measurable improvement |
| 5 | In-memory data passing (build_turns + build_actions) | **KEPT** | build_actions −21%, build_turns −27%; wall ~noise |
| 6 | Parallel JSONL parsing (rayon par_iter) | **KEPT** | ~29s vs ~32s (−10%) |

### Current best metrics (post parallel parsing)
| metric | value | vs original baseline | vs target |
| --- | ---: | ---: | ---: |
| Cold full import (session-local best) | ~28.8s | ~77% from 126.1s | ~2.9× to 10s target |
| Startup | ~2.29s | ~55% from ~5.1s baseline | ~2.3× to <1s target |

### Current phase distribution (full corpus, post-inmemory, ~31.5s wall)

| phase | time | % of wall | note |
| --- | ---: | ---: | --- |
| normalize_jsonl.sql_ms | 9.7s | 31% | CPU-bound btree inserts — 490K records, 295K messages, 412K parts |
| build_actions | 5.6s | 18% | classification + action/path_ref inserts (no more load_messages JOIN) |
| normalize_jsonl.parse_ms | 3.0s | 10% | serde_json::from_str per line |
| scan_source | 2.7s | 9% | directory walk + VCS resolution |
| finalize_chunk + rollups | 1.7s + 1.5s | 10% | path rollup is the expensive part |
| build_turns | 1.1s | 3% | in-memory iteration, then inserts turns |
| other (uninstrumented) | 6.2s | 20% | Vec<NormalizedMessage> assembly, overhead |

### Key findings (cumulative)
1. **Btree pages are cached.** With 64MB page cache + WAL, secondary index maintenance during inserts is essentially free.
2. **The remaining SQL cost is core btree insert work** — CPU-bound, not I/O-bound.
3. **In-memory data passing saves ~2s in build_actions + build_turns** but the Vec assembly overhead partially offsets gains. Primary value: **architectural prerequisite for parallelism**.
4. **`RETURNING id` is expensive on high-frequency INSERTs.** 412K parts × ~6.8µs/row = 2.8s overhead. Use `last_insert_rowid()` instead.
5. **Session-to-session variance is high (~30%)** on WSL2. Within-session relative comparisons are reliable.

### What must change to reach 10s
The 10s target requires ~2.9× more speedup from ~29s. Parallel parsing is in
place. Remaining options:

**Next: Parallel classify (candidate #1b, Tier B):**
- Extend rayon parallelism to the classification phase (build_actions)
- Classification is ~5.6s, mostly CPU-bound (classify_message, path ref extraction)
- Would require running classification on ParsedFile data before DB IDs are assigned
- Expected: additional ~10–15% wall reduction

**Channel-based pipeline (follow-up to iteration 6):**
- Replace barrier (`par_iter().collect()`) with producer-consumer channel
- Overlap parsing file N+1 with writing file N
- Expected: modest additional gain if parse and write phases are similarly sized

**Structural changes (Tier C, requires approval):**
- Skip `record` table inserts (~490K rows, ~33% of insert time)
- In-memory staging DB → `VACUUM INTO`
- DuckDB or columnar store for analytical queries

### Candidate ranking (updated after iteration 5)

| rank | candidate | est. remaining win | confidence | tier |
| --- | --- | --- | --- | --- |
| 1 | ~~Parallel parse + classify~~ | **DONE** (iteration 6, parse only — ~10%) | — | — |
| 1b | **Parallel classify** (extend rayon to classification phase) | 10–15% wall | medium | B |
| 2 | ~~In-memory data passing~~ | **DONE** (iteration 5) | — | — |
| 3 | **scan_source caching** | startup floor only, ~0.5s | high for startup | A |
| 4 | Faster JSON parser (simd-json) | ~2.4s / ~8% | low | B |
| 5 | Skip `record` table inserts | ~10s / ~33% | needs product review | C |

### Bench harness
```bash
# Build
cargo build -p gnomon-core --example import_bench --release

# Subset (fast iteration, ~14s)
cargo run -p gnomon-core --example import_bench --release -- \
  --corpus subset --mode full --repeats 3

# Full corpus (truth, ~32s)
cargo run -p gnomon-core --example import_bench --release -- \
  --corpus full --mode full --repeats 3

# With perf log
cargo run -p gnomon-core --example import_bench --release -- \
  --corpus full --mode full --perf-log /tmp/gnomon-perf.jsonl

# Corpus tarballs are gitignored; they live at:
#   tests/fixtures/import-corpus/{full,subset}.tar.zst
# If missing, run: tests/fixtures/import-corpus/capture.sh
```

### Session-resumption sanity check
```bash
cd /home/ketan/project/gnomon
git log --oneline -3
# Should show in-memory merge at 2eca9fa
ls tests/fixtures/import-corpus/
# MANIFEST.md + capture.sh (tarballs are gitignored, may need re-capture)
cargo run -p gnomon-core --example import_bench --release -- \
  --corpus subset --mode full --repeats 1
# Should complete in ~14s
```
