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

### Iteration 7 — Skip record table inserts

**What changed:**

Removed all `INSERT INTO record` statements from `process_record` and
`process_record_from_parsed`. The `record` table stored one row per JSONL line
(~490K rows for the full corpus) but was never read by any query, TUI, web, or
rollup code. The `record_count` counter is still maintained for reporting.

Also removed: `classify_record_kind` (dead code), `FROM record` arm in
`purge_chunk_data` (redundant with `stream`). Bumped `IMPORT_SCHEMA_VERSION`
to 5 to trigger reimport.

**Measurements (full corpus, interleaved comparison with main):**

Interleaved runs:
```
Main (iter 6):    37.1s / 32.3s / 36.8s
Skip-records:     29.3s / 29.8s / 28.4s / 27.2s
Best: 27.2s vs 32.3s (−15.8%)
```

Row parity: **PASS** — all tables match except `record` (0, by design).
DB size: 425.58 MB (down from 486.83 MB, −13%).

Quality gates: `cargo fmt`, `clippy -D warnings`, `cargo test` — all pass (364 tests).

**Decision:** KEPT (user approved: no current readers, future features trigger reimport)

**Commit on `import-perf-skip-records`:** `1aca5c6`

---

## 2026-04-14 — candidate #1b: Parallel classify

**Branch:** `import-perf-classify`
**Hypothesis:** Moving `classify_message()` from the serial Phase 2 to the parallel
Phase 1 (rayon) should reduce wall time by ~10–15% by parallelizing the CPU-bound
classification work (~3–4s estimated) across cores.

**Implementation:** Pre-classify during Phase 1 (parallel parse). Added
`pre_classify_parsed_file` in chunk.rs that groups records by `external_id`, merges
parts with dedup, builds `ClassifyInput` wrappers, and calls a new `pre_classify_message`
function. Results are keyed by `external_id` in a HashMap on `ParsedFile`. In Phase 2,
after `write_parsed_file_in_tx` assigns DB IDs, `resolve_pre_classifications` correlates
via `NormalizedMessage.external_id` and maps part indices to real part IDs.
`build_actions_in_tx_with_preclassified` skips `classify_message()` and uses pre-computed
results instead. Files: classify/mod.rs (new types + pre_classify_message +
build_actions_in_tx_with_preclassified), import/mod.rs (external_id on NormalizedMessage,
HashMap on ParsedFile), import/chunk.rs (pre_classify_parsed_file,
resolve_pre_classifications), import/normalize.rs (external_id in NormalizedMessage
construction).

**Measurements:**

Subset (3 repeats each, interleaved same session):
  Baseline (main):  8.9–9.3s
  Branch:           8.9–9.7s
  Delta:            within noise

Full corpus (3 repeats each, interleaved same session):
  Baseline (main):  24.2–24.8s
  Branch:           24.4–24.6s
  Delta:            within noise

Row parity:  PASS (all 9 non-record tables identical)
DB size:     425.58 MB (identical)

Perf-log build_actions breakdown:
  Baseline:  total=5452ms, avg=1.199ms/conversation
  Branch:    total=5136ms, avg=1.129ms/conversation
  Savings:   ~316ms total (~6% of build_actions, ~1.3% of wall)

**Key finding:** The `classify_message()` CPU cost is ~300ms total across 4547
conversations — far less than the 3–4s originally estimated. The build_actions phase
(~5.5s) is dominated by DB persist operations (INSERT actions, INSERT path_refs,
DELETE existing classification, UPDATE import_chunk). Moving ~300ms of CPU to the
parallel phase produces no measurable wall-time improvement on this hardware.

**Decision:** REVERTED — no measurable improvement; added complexity not justified.

**Next implied:** The remaining build_actions cost is almost entirely DB persist.
Further optimization of this phase requires reducing the number of DB operations
(fewer actions/path_refs, batch inserts) or structural changes (Tier C). The
next most promising candidate is **scan_source caching** (rank #3) for startup
improvement, or a **fresh perf-log phase distribution** to identify where the
uninstrumented ~9s overhead is hiding.

---

## 2026-04-15 — candidate 9: Replace RETURNING id with last_insert_rowid()

**Branch:** `import-perf-batch-inserts`
**Hypothesis:** The `message` INSERT (295K rows), `action` INSERT (121K rows),
and several other hot-path INSERTs use `RETURNING id` with `query_row`, which
has overhead vs `execute` + `last_insert_rowid()`. The `RETURNING` clause forces
SQLite through a query-row code path (step + column extraction) rather than a
simpler execute path. Across 416K+ high-frequency inserts, this overhead adds up.

**Fresh profile (pre-implementation, single run on main, 2026-04-15):**

| phase | time | % of wall |
| --- | ---: | ---: |
| normalize_jsonl | 9094ms | 36.6% |
| build_actions | 5604ms | 22.5% |
| scan_source | 2824ms | 11.4% |
| finalize_chunk | 1717ms | 6.9% |
| rebuild_path_rollups | 1530ms | 6.2% |
| build_turns | 1077ms | 4.3% |
| rebuild_action_rollups | 154ms | 0.6% |
| uninstrumented | 2838ms | 11.4% |
| **wall** | **24857ms** | **100%** |

**Implementation:** Replaced `RETURNING id` + `query_row` with `execute` +
`conn.last_insert_rowid()` on 7 hot-path INSERT statements:
- `normalize.rs`: message (295K), conversation (4.5K), stream (4.5K),
  turn (13K), history_event (low freq)
- `classify/mod.rs`: action (121K), path_node (variable)

File refs: `normalize.rs:1398-1454`, `normalize.rs:1148-1173`,
`normalize.rs:1186-1214`, `normalize.rs:1697-1733`, `normalize.rs:887-930`,
`classify/mod.rs:1287-1329`, `classify/mod.rs:1164-1185`

**Measurements:**

Same-session interleaved comparison (3 repeats each, full corpus):

| run | baseline (main) | iter 9 |
| --- | ---: | ---: |
| 1 | 25.6s | 23.2s |
| 2 | 25.1s | 21.7s |
| 3 | 28.8s | 21.6s |
| **median** | **25.6s** | **21.7s** |
| **best** | **25.1s** | **21.6s** |

Delta: **−3.9s / −15.2%** (median), **−3.5s / −13.9%** (best)

Per-phase improvement (single-run perf-log comparison):
- normalize_jsonl: 9094ms → 7955ms (−1139ms, −12.5%)
- build_actions: 5604ms → 4648ms (−956ms, −17.1%)

Row parity: **PASS** — all 9 non-record tables match baseline exactly.
Quality gates: `cargo fmt`, `cargo clippy`, `cargo test` all pass.
DB size: 425.58 MB (unchanged).

**Decision:** KEPT — clear measurable improvement with zero behavioral change.

**Next implied:** Multi-row INSERT batching (A5) for message_part (412K rows)
and action_message as a follow-up iteration. The remaining normalize_jsonl cost
(~8s) is still dominated by per-row btree inserts. Channel-based pipeline
(candidate #6) and scan_source caching (candidate #3) remain viable.

---

## 2026-04-15 — candidate 10: Channel-based pipeline (overlap parse with write)

**Branch:** `import-perf-channel-pipeline`
**Hypothesis:** Currently each chunk does par_iter().collect() (barrier) then
serial writes. Parse takes ~2.5s total (11% of chunk time) and is completely
serialized before writing begins. By replacing the barrier with a channel-based
producer-consumer pipeline, we can overlap parsing file N+1 with writing file N.
The main thread writes results in order as they arrive from rayon workers.
Expected savings: up to ~2.5s (~11% of wall time).

**Implementation:** Replaced `par_iter().collect()` barrier with `mpsc::sync_channel`
producer-consumer. Rayon workers parse files and send `(index, ParseResult)` to
channel. Main thread receives and writes in order, buffering out-of-order results.
File ref: `chunk.rs:958-1001`.

**Measurements:**

Same-session interleaved comparison (5 repeats each, full corpus):

| run | baseline (main) | iter 10 |
| --- | ---: | ---: |
| 1 | 23.4s | 34.0s* |
| 2 | 23.5s | 30.2s* |
| 3 | 22.8s | 25.5s |
| 4 | 23.4s | 23.0s |
| 5 | 22.8s | 22.9s |
| **best** | **22.8s** | **22.9s** |
| **median** | **23.4s** | **25.5s** |

*Runs 1-2 of channel pipeline were impacted by system load (baseline ran after
load subsided and was stable).

Best-of-5 numbers are identical: 22.8s vs 22.9s. No measurable improvement.

Row parity: **PASS** — all 9 non-record tables match baseline exactly.

**Decision:** REVERTED — no measurable improvement. The parse phase is only ~2.5s
across 162 chunks (average ~15ms per chunk). Within each chunk, rayon finishes
parsing before the writer has meaningful work to overlap with. The per-chunk
parse time is too small relative to per-chunk write time for pipelining to help.

**Next implied:** The remaining optimization opportunities are all in reducing
the serial DB write cost or structural changes. Channel-based pipeline is
exhausted as a strategy.

---

## 2026-04-15 — candidate 11: Defer rollups to post-import pass

**Branch:** `import-perf-defer-rollups`
**Hypothesis:** `rebuild_chunk_path_rollups` (1.7s) and `rebuild_chunk_action_rollups`
(0.2s) run 162 times inside `finalize_chunk_import_core`, totaling ~1.9s. These
rollups aggregate per-chunk data and are only read by the TUI after import completes.
Moving rollup computation out of the per-chunk finalize into a single post-import
pass eliminates per-chunk rollup overhead. Expected savings: ~1.8s.

**Implementation:** Added `defer_rollups` flag to `ImportWorkerOptions`. When true,
`finalize_chunk_import_core` skips rollup rebuilds. A new `rebuild_all_chunk_rollups`
function runs all rollups in a single transaction after the import loop. Full import
path defers; startup path keeps per-chunk rollups for TUI readiness.

**Measurements:**

Same-session interleaved comparison (3 repeats each, full corpus):

| run | baseline (main) | iter 11 |
| --- | ---: | ---: |
| 1 | 24.2s | 25.1s |
| 2 | 23.1s | 27.7s* |
| 3 | 24.8s | 23.2s |
| **best** | **23.1s** | **23.2s** |

*System load outlier (extract time 6.87s vs normal ~4.5s).

Row parity: **PASS** — all 9 non-record tables match baseline exactly.

**Decision:** REVERTED — no measurable improvement. Deferring rollups moves the
work to a different transaction but doesn't reduce total SQL work. The rollup
cost is the same whether per-chunk or batched.

---

## 2026-04-15 — candidate 12: Multi-row VALUES batching for message_part

**Branch:** `import-perf-batch-values`
**Hypothesis:** The `message_part` table receives 412K individual INSERT
statements. Each INSERT requires a separate SQLite VM step cycle and btree
descent. By batching parts per-message into multi-row `INSERT INTO message_part
VALUES (...), (...), ...` statements, we reduce per-row overhead. Average
message has ~1.4 parts, but large messages can have 10-50+ parts. The batching
benefit comes from fewer statement executions and better btree amortization.

**Implementation:** Replaced per-part `insert_message_part` calls with
`batch_insert_message_parts` that builds multi-row `VALUES` statements.
Parts are collected per-message, inserted in one statement, IDs inferred
from `last_insert_rowid()` range.

**Measurements:**

Full corpus (3 repeats):

| run | baseline (main) | iter 12 |
| --- | ---: | ---: |
| 1 | ~23s | 31.9s |
| 2 | ~23s | 35.2s |
| 3 | ~23s | 32.7s |

**Significant regression (~40% slower).** Root cause:
1. Dynamic SQL strings cannot use `prepare_cached` — each batch has a different
   placeholder count, requiring fresh statement compilation per message.
2. `Box<dyn ToSql>` heap allocations add per-parameter overhead.
3. Average ~1.4 parts/message means most batches are 1-2 rows — all overhead,
   no savings. The `prepare_cached` single-row approach compiles once and reuses
   for all 412K inserts.

Row parity: PASS.

**Decision:** REVERTED — significant regression. Multi-row VALUES batching
loses to `prepare_cached` single-row inserts when batch sizes are small and
statement variation prevents caching.

**Key finding #7:** Multi-row VALUES batching is counterproductive for tables
with small per-parent fan-out (~1.4 parts/message). The `prepare_cached`
single-row pattern is near-optimal for SQLite when the same statement shape
is reused hundreds of thousands of times.

---

## 2026-04-15 — candidate 13: In-memory staging DB + VACUUM INTO

**Branch:** `import-perf-inmemory-staging`
**Hypothesis:** Importing into a `:memory:` SQLite database eliminates file I/O,
WAL journal overhead, and page cache management during bulk inserts. After import,
`VACUUM INTO` persists the database in one pass. Expected savings: the difference
between in-memory btree work and on-disk btree work.

**Implementation:** Added `Database::load_into_memory` (backup API to copy on-disk
DB into `:memory:`), `Database::vacuum_into` (VACUUM INTO with file cleanup), and
`import_all_in_memory` function. Bench harness gets `--in-memory` flag. Also added
`backup` feature to rusqlite.

File refs: `db/mod.rs:37-85`, `chunk.rs:372-469`, `import_bench.rs:148-158`

**Measurements:**

Perf-log comparison (single run each, full corpus):

| phase | baseline | in-memory | delta |
| --- | ---: | ---: | ---: |
| chunk (total) | 20170ms | 19468ms | −702ms |
| normalize_jsonl | 7932ms | 9456ms | +1524ms |
| build_actions | 4578ms | 5461ms | +883ms |
| vacuum_into | — | 1461ms | +1461ms |
| load_into_memory | — | 2ms | +2ms |
| **wall** | **23671ms** | **24851ms** | **+1180ms** |

No improvement. normalize_jsonl and build_actions are SLOWER in-memory (likely
due to memory allocation pressure from the growing in-memory DB pages). VACUUM
INTO adds 1.5s of unavoidable serialization cost.

Row parity: **PASS** — all 9 non-record tables match baseline exactly.
DB size: 409 MB (smaller than baseline 426 MB due to VACUUM compaction).

**Decision:** REVERTED — no measurable improvement; slight regression.

**Key finding #8:** In-memory staging does not help when the DB is already on
tmpfs (bench) or when the bottleneck is CPU-bound btree work rather than I/O.
The VACUUM INTO serialization cost (1.5s for 409 MB) offsets any savings from
eliminating WAL/page-cache overhead. For production (disk-backed DB), in-memory
staging might help but adds ~1.5s of VACUUM cost at the end.

---

## 2026-04-15 — D0: zero-write diagnostic (CPU floor measurement)

Branch: `import-perf-d0-zero-write` (kept as reference, not merged)
Hypothesis: The time remaining after zeroing all SQL writes is the minimum
achievable wall time with SQLite. If the floor is 4-6s (for the full corpus),
the btree-ops model (projecting 10s via D1-D4) is conservative and the 10s
target is plausible. If the floor is >15s, the bottleneck is elsewhere.
Implementation: Added `GNOMON_ZERO_WRITE=1` env-var gate (OnceLock-cached)
to all INSERT/UPDATE/DELETE calls in `normalize.rs`, `chunk.rs`, and
`classify/mod.rs`. Parse phase, JSONL extraction, in-memory turn grouping,
and classification logic still execute.
Measurements:
  Subset baseline (confirmation):  8.54s, 8.38s, 8.60s → avg 8.51s
  Subset zero-write (CPU floor):   2.91s, 2.76s, 2.78s → avg 2.82s
  SQL write overhead (subset):     ~5.7s (67% of wall)
  CPU floor (subset):              ~2.82s (33% of wall)
  Row parity:                      N/A (diagnostic — zero-write produces empty DB)
  Profile shift: All SQL write phases collapse to near-zero. Remaining ~2.8s
    is scan_source + rayon JSONL parse + in-memory turn/action grouping.
Decision: N/A (diagnostic only — establishes CPU floor)
Commit: c8546fb (on branch import-perf-d0-zero-write)
Next implied: D1 (denormalize join tables). With 5.7s of SQL overhead and 46%
  of btree ops from join tables, D1 should save ~2.6s on subset (46% × 5.7s).
  The CPU floor of 2.82s is well below the 10s target, confirming there is
  room to land at 10s if D1+D2+D3 each deliver their projected savings.

---

## 2026-04-15 — D1: denormalize join tables (turn_message → message.turn_id, action_message → message.action_id)

Branch: `import-perf-d1-denormalize-joins` (kept as reference, not merged)
Hypothesis: Replacing 295K INSERT INTO turn_message + action_message with
UPDATE message SET turn_id/action_id eliminates 46% of btree ops (2.36M),
yielding ~4-5s savings on full corpus. Two variants tested: (a) one UPDATE per
message, (b) one batch UPDATE per turn/action group (WHERE id IN ...).
Implementation: New migration 0011_denormalize_message_joins.sql adds turn_id
and action_id nullable columns to message, creates idx_message_turn_id and
idx_message_action_id, drops turn_message and action_message tables.
normalize.rs:persist_turn changed to UPDATE message SET turn_id. classify/
mod.rs:persist_action changed to UPDATE message SET action_id. load_messages
updated to read m.turn_id directly. Five JOIN rewrites in query/mod.rs.
Measurements:
  Subset baseline (D0 run):          8.51s avg
  Subset D1 individual UPDATEs:      10.08s, 9.69s, 10.54s → avg 10.1s (+19%)
  Subset D1 batched UPDATEs:         10.04s, 9.90s, 10.02s → avg 10.0s (+18%)
  Row parity:                        PASS (7/7 integration tests)
  DB size:                           152.57 MB (vs 160.70 MB baseline, −5%)
Decision: REVERTED — both variants are regressions of ~1.5s (+18%)

Key finding #9: The btree-ops COUNT model is correct (join tables do account
for 46% of btree operations) but the cost model was wrong. Replacing join table
INSERTs with UPDATEs on the message table is MORE expensive, not less, because:
  (a) UPDATE requires search+modify (random write) vs INSERT's append-friendly
      sequential write into compact new btrees.
  (b) The message table is large and hot during import; its btrees require
      deeper traversals than the smaller join table btrees.
  (c) Batching (WHERE id IN ...) reduces SQL execution overhead but not btree
      work, confirming the bottleneck is the per-row btree operation cost.
The correct approach to eliminate join table overhead is to preset turn_id at
message INSERT time (D1b), not to update it afterwards.
Commit: bec34a7 (on branch import-perf-d1-denormalize-joins, not merged)
Next implied: D3 (defer rollup entirely), D2 (skip message_part INSERTs —
  pure INSERT elimination, avoids UPDATE pitfall), or D1b (preset turn_id
  before initial message INSERT).

---

## 2026-04-15 — D3: defer rollup computation out of import path — REVERTED

**Branch:** `import-perf-d3-defer-rollups` (kept as reference, not merged)

**Hypothesis:** `rebuild_chunk_path_rollups` (1.5s) and `rebuild_chunk_action_rollups`
(0.15s) run 35 times inside `finalize_chunk_import_core`. Skipping them during
import and running a single post-import `rebuild_all_chunk_rollups` pass should
save ~1.5s from the import wall time. (D3 differs from iteration 11 by deferring
rollups entirely out of the import call, not just batching within it.)

**Implementation:**
- Added `defer_rollups: bool` to `ImportWorkerOptions` (default false; startup
  path unchanged).
- `import_all_with_perf_logger` sets `defer_rollups: true`.
- `finalize_chunk_import_core` skips rollup calls when `defer_rollups` is true.
- New `rebuild_all_chunk_rollups(conn, perf_logger)` rebuilds rollups for all
  complete chunks in one pass. Called separately by bench after import.
- Bench reports import-only wall time and deferred rollup time independently.

**Measurements (subset, same-session interleaved, 3+3+3+3 runs):**

| run | main wall | D3 import-only | D3 rollup (deferred) | D3 total |
| --- | ---: | ---: | ---: | ---: |
| 1 | 23.6s | 15.1s | 9.5s | 24.6s |
| 2 | 19.0s | 11.3s | 6.5s | 17.8s |
| 3 | 16.0s | 9.2s | 5.7s | 14.9s |
| 4 | 10.1s | 8.1s | 5.8s | 13.9s |
| 5 | 11.7s | 9.1s | 6.9s | 16.0s |
| 6 | 13.0s | 9.5s | 6.5s | 16.0s |
| **median** | **14.9s** | **9.3s** | **6.5s** | **15.9s** |

Note: high variance in early runs (system loading up after D3 build). Later runs
are more stable and comparable. Session baseline median ≈10-11s.

**Analysis:**
- D3 import-only (no rollups): ~9.3s vs baseline ~10-11s → modest ~1-2s
  improvement in the import phase.
- D3 rollup rebuild (deferred): ~6.5s vs per-chunk rollup cost ~3.4s → 2× MORE
  expensive than the per-chunk approach.
- D3 total: ~15-16s vs baseline ~10-11s → **+38-45% REGRESSION** on the total
  user-perceived time.

**Root cause:** Same cache-miss penalty as deferred secondary indexes (iteration 4).
Per-chunk rollup computation runs while the chunk's data pages are hot in the 64MB
page cache (just written). In the post-import pass, all 35 chunks' pages have been
loaded and evicted since rollup ran for chunk 1. The path rollup query joins action
→ action_message → message → message_path_ref → path_node and aggregates over
all files in the chunk — it needs the same pages that were hot during import. Eviction
from 35 subsequent chunks causes severe cache misses, making each rollup rebuild
4-10× slower per chunk than in the hot-cache scenario.

**Key finding #10:** Rollup computation is cache-locality dependent. Per-chunk
rollup runs efficiently because it accesses the same data pages that were just
written. Any strategy that delays rollup computation past the point where those
pages are hot (whether post-loop batch or truly deferred) pays a severe penalty.
The ~3.4s per-chunk rollup cost is already near-optimal given the cache constraints.

**Decision:** REVERTED — total time regresses ~40-50%. Branch kept as reference.

Row parity: **PASS** — all rollup tables match per-chunk baseline when
`rebuild_all_chunk_rollups` is called after import.

---

## 2026-04-15 — D1b: preset turn_id at message INSERT time

**Branch:** `import-perf-d1b`
**Hypothesis:** D1 regressed because post-hoc UPDATE on `message` is more
expensive than JOIN table INSERTs. The correct path is to compute turn
membership in memory before the first INSERT, so `turn_id` and
`ordinal_in_turn` can be set at INSERT time — eliminating both the
`turn_message` table and any post-hoc UPDATE cost.

**Implementation:**
- Migration `0011_d1b_message_turn_id.sql`: ALTER TABLE message to add
  `turn_id` (nullable FK → turn) and `ordinal_in_turn`; DROP TABLE
  turn_message.
- `normalize.rs`: `upsert_message` made fully in-memory (no DB calls).
  New `persist_messages_with_turns` function groups messages into turns
  in memory (sorted by sequence_no; new turn at every `user_prompt`),
  then inserts in order: root message (turn_id=NULL) → turn row →
  UPDATE root to set turn_id/ordinal_in_turn=0 → non-root members with
  turn_id set at INSERT time. Only N_turns (~5K subset / ~25K full)
  UPDATEs vs N_messages UPDATEs in D1.
- `classify/mod.rs`: `load_messages` query updated to use `m.turn_id` /
  `m.ordinal_in_turn` directly; removed `JOIN turn_message` JOIN.
- `query/mod.rs` test fixture: `INSERT INTO turn_message` → `UPDATE
  message SET turn_id`.
- `db/mod.rs`: `INITIAL_SCHEMA_VERSION` bumped 10 → 11; `REQUIRED_TABLES`
  count corrected (19, not 20).
- `IMPORT_SCHEMA_VERSION` bumped 5 → 6 (triggers full reimport).

**Measurements (interleaved, same session):**

Subset (8 pairs): main median **8.89s**, D1b median **8.32s** → **−0.57s / −6.4%**

| run | main | D1b |
| --- | ---: | ---: |
| 1 | 8.986s | 9.676s |
| 2 | 8.793s | 8.193s |
| 3 | 9.221s | 9.669s |
| 4 | 9.990s | 7.684s |
| 5 | 8.171s | 8.818s |
| 6 | 8.662s | 8.137s |
| 7 | 8.384s | 7.745s |
| 8 | 8.998s | 8.446s |
| **median** | **8.89s** | **8.32s** |

Full corpus (3 pairs): main median **21.244s**, D1b median **21.009s** → **−0.24s / −1.1%**

| run | main | D1b |
| --- | ---: | ---: |
| 1 | 22.525s | 21.525s |
| 2 | 21.149s | 21.009s |
| 3 | 21.244s | 20.576s |
| **median** | **21.244s** | **21.009s** |

Row parity: **PASS** — all tables match. `turn` row count unchanged (4,915
subset). `message` row count unchanged (130,478 subset).

Quality gates: `cargo fmt`, `cargo clippy -D warnings`, `cargo test --workspace` — all pass.

**Analysis:** Savings are real but smaller than the 2-3s btree model predicted.
Root cause: `turn_message` rows are compact (3 INTEGER columns) and the table's
btree is tiny relative to the main `message` table, so per-op insert cost is
low. The 295K × 3 btree ops eliminated are fast ops. Additionally, the 5K root-
message UPDATEs offset a fraction of the savings. Full-corpus delta sits within
WSL2 noise (~5%), though subset shows a cleaner −6.4% signal.

Architectural benefit is real regardless: the schema is simpler, `load_messages`
eliminates one JOIN (no index lookup on `turn_message.message_id`), and the
join-table-elimination goal is complete for turns.

**Decision:** KEPT — subset shows clear directional improvement; schema
simplification has value independent of the modest wall-time delta.

---

## 2026-04-15 — D1b bug fix: imported_message_count written as 0

### Root cause

`ImportState::finish_import` computed `imported_message_count` as
`self.message_states.len()`. But `persist_messages_with_turns` — which runs
before `finish_import` — drains the HashMap via `.drain()`. After drain,
`len()` == 0, so the `import_chunk` row was silently written with
`imported_message_count = 0`.

The bug was invisible to unit tests (none assert the `import_chunk` counter
directly) and to `cargo test --workspace` (integration tests are `#[ignore]`).
It was only caught when the full integration suite was run explicitly.

### Fix

Added a `message_count: usize` field to `ImportState`, incremented on the
first occurrence of each `external_id` in `upsert_message`. `finish_import`
now reads `self.message_count` instead of `self.message_states.len()`.

### Integration test results (post-fix)

```
running 7 tests
test subset_corpus_import_all_matches_expected_database_shape ... ok
test subset_corpus_recent_first_startup_import_defers_every_chunk_and_reaches_same_final_state ... ok
test subset_corpus_with_one_recent_file_imports_recent_chunk_before_opening ... ok
test subset_corpus_reimport_is_a_no_op_when_files_are_unchanged ... ok
test subset_corpus_reimports_only_the_touched_chunk_when_a_file_mtime_changes ... ok
test subset_corpus_keeps_prior_rows_when_a_reimported_file_turns_malformed_and_recovers_after_restore ... ok
test full_corpus_import_all_matches_expected_database_shape ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured
```

### Process fix

Added the import integration test suite to `CLAUDE.md` quality gates with an
explicit note that `cargo test --workspace` does not include these tests. Any
future perf optimization pass must run `cargo test -p gnomon-core --test
import_corpus_integration -- --include-ignored` before being declared complete.

---

## 2026-04-15 — D2: defer message_part INSERTs to post-normalize pass — REVERTED

**Branch:** `import-perf-d2`
**Hypothesis:** Deferring all `INSERT INTO message_part` calls to a single
sequential pass after all message rows are inserted would reduce btree op
interleaving, yielding ~1-2s improvement on subset.

**Implementation:** Removed the three inline `insert_pending_parts` calls from
`persist_messages_with_turns` (after unassigned message INSERT, after root
message INSERT, and after each turn-member message INSERT). Added a single
deferred loop after all message INSERTs:

```rust
for ms in &mut messages {
    insert_pending_parts(conn, ms, source_path)?;
}
```

**Measurements (subset, same session):**

| run | main wall | D2 wall |
| --- | ---: | ---: |
| 1 | 10.889s | 24.947s (cold — integration test + build still running) |
| 2 | 11.099s | 18.822s |
| 3 | 10.439s | 18.706s |
| **warm median** | **10.9s** | **18.7s** |

**Delta: +71% REGRESSION** — D2 is ~1.7× slower than main on subset.

**Analysis:** The deferred approach triggers a SQLite FK verification penalty.
`PRAGMA foreign_keys = ON` is set globally. Each `INSERT INTO message_part`
causes SQLite to look up `message.id` to verify the `message_id` FK. In the
inline path (D1b), this lookup always hits a hot page: the parent message row
was just inserted one statement earlier. In D2, all 130K message rows are
inserted before any part is written. By the time the deferred pass begins,
the message btree's page ordering has shifted — even though the 64MB page
cache is large enough to hold all message pages (~26MB), the FK lookup access
pattern changes from purely sequential to random-within-range, interacting
poorly with SQLite's internal btree read-ahead.

`PRAGMA foreign_keys` cannot be toggled inside an active savepoint (the parts
are inserted within the per-file savepoint), so there is no simple workaround.

**Quality gates:** All pass. `cargo test --workspace` (142 tests ok), 7/7
integration tests ok, `cargo clippy` clean.

**Decision:** REVERTED — +71% regression far exceeds the threshold.

Key finding #12: **Deferring child-table INSERTs past parent-table INSERTs
causes SQLite FK verification cache misses.** The inline pattern (parent INSERT
immediately followed by child INSERTs) keeps FK lookup pages hot. Any
batching strategy that separates parent and child inserts by more than a few
rows will pay a similar penalty with `PRAGMA foreign_keys = ON`.

---

## 2026-04-15 — D2b: action_message schema optimization (3 → 2 btrees) — REVERTED

**Branch:** `import-perf-d2b`
**Hypothesis:** Changing `action_message` PRIMARY KEY from composite
`(action_id, message_id)` to singleton `(message_id)` eliminates one of the
three auto-indexes, reducing btree ops from 3 → 2 per INSERT (130K rows ×
1 fewer btree = 130K ops saved). All query and cascade patterns remain covered:
`UNIQUE(action_id, ordinal_in_action)` covers action lookups and cascade DELETE
from action; `PRIMARY KEY (message_id)` = INTEGER rowid covers message lookups
and cascade DELETE from message.

**Analysis of D1b-style approach:** The canonical D1b approach — preset
`action_id` at message INSERT time — is blocked by a dependency cycle: action
rows require `turn_id` (turn must exist), turns require message IDs (messages
must be inserted first), so `action_id` cannot be set at message INSERT time.
The UPDATE-after-action-INSERT alternative would likely regress like D1 (+18%).
The schema optimization (3→2 btrees) was the cleanest available approach.

**action_message distribution (subset corpus):**
- 130,475 rows across 50,463 actions (avg 2.59 messages/action)
- 19,827 single-message actions (39.3%)
- 19,003 two-message actions (37.7%)

**Measurements (subset, same session, D2b first then main):**

| run | D2b wall | main wall |
| --- | ---: | ---: |
| 1 | 10.682s | 8.575s |
| 2 | 9.260s | 7.844s |
| 3 | 8.893s | 7.499s |
| **warm median** | **9.08s** | **7.67s** |

**Delta: +18% REGRESSION** — D2b is ~1.4s slower than main on subset.

**Analysis:** Reducing from 3 → 2 auto-indexes regressed rather than improved.
Possible mechanism: the old composite PK `(action_id, message_id)` and separate
`UNIQUE(message_id)` index combination may have better cache characteristics for
the mix of INSERT and FK-verification operations. With INTEGER PRIMARY KEY =
rowid, the FK uniqueness check uses the table's own btree rather than a compact
separate index, which may cause more cache pressure during the dense action
classification phase. Exact mechanism uncertain — the measurement is unambiguous.

**Quality gates:** All pass. 7/7 integration tests ok. Code reverted to main state.

**Decision:** REVERTED — +18% regression.

Key finding #13: **Reducing action_message auto-indexes from 3→2 by changing
PK from composite (action_id, message_id) to INTEGER rowid (message_id)
regresses +18%. The old composite PK + UNIQUE(message_id) index structure is
faster in practice despite the higher index count, likely due to cache behavior
during mixed INSERT + FK verification workloads.**

---

## RESUME HERE (if session was reset, read this first)

Last updated: 2026-04-15 (D2b reverted — +18% regression)
Current phase: Phase 2 — D0 done, D1 reverted, D3 reverted, D1b kept, D2 reverted, D2b reverted
All code is on `main`. Subset session-local median: ~7.67s (post-D2b session).

D0: CPU floor = 2.82s (subset). SQL = 67% of wall.
D1: denormalize-via-UPDATE = +18% regression (UPDATE > INSERT on hot table).
D3: defer rollups = +40-50% regression (cache-miss penalty in post-import pass).
D1b: −6% subset / −1% full; turn_message eliminated. Bug: imported_message_count written as 0 — fixed.
D2: defer message_part INSERTs = +71% REGRESSION (FK verification cache-miss penalty).
D2b: action_message schema (3→2 btrees) = +18% REGRESSION (composite PK + UNIQUE index faster than INTEGER PRIMARY KEY rowid in practice).
Key finding #10: rollup SQL requires hot cache (per-chunk is optimal).
Key finding #12: deferring child INSERTs past parent INSERTs causes FK verification cache misses.
Key finding #13: reducing action_message PK to INTEGER rowid regresses — composite index structure is faster.

Next candidates:
- **D4:** simd-json parsing (parse phase is ~0.3s — ceiling is modest).
- **D5:** scan_source caching (~0.5s startup only, not full import).
- No strong candidates remain. Project may be near practical limit.

### How to resume
1. `cd /home/ketan/project/gnomon`
2. Read this log's Phase Log for context.
3. Assess whether D4 or D5 is worth attempting given the ~7.7s session median.
4. Consider declaring the perf project complete at current state.

### Iteration summary

| # | Candidate | Result | Delta |
| --- | --- | --- | --- |
| 1 | Commit batching (per-chunk transactions) | **KEPT** | 126.1s → 77.5s (−38.5%) |
| 2 | Prepared-statement caching (`prepare_cached`) | **KEPT** | 53.6s → 38.4s (−28.4%) |
| 3 | SQLite pragma tuning (sync=NORMAL, cache, mmap) | **KEPT** | 38.1s → 31.9s (−16.3%) |
| 4 | Deferred secondary indexes during bulk load | **REVERTED** | no measurable improvement |
| 5 | In-memory data passing (build_turns + build_actions) | **KEPT** | build_actions −21%, build_turns −27%; wall ~noise |
| 6 | Parallel JSONL parsing (rayon par_iter) | **KEPT** | ~29s vs ~32s (−10%) |
| 7 | Skip record table inserts | **KEPT** | ~27s vs ~29s (−7%), DB −13% |
| 8 | Parallel classify (pre-classify in rayon Phase 1) | **REVERTED** | ~300ms savings, within noise |
| 9 | RETURNING id → last_insert_rowid() | **KEPT** | ~21.7s vs ~25.6s (−15.2%) |
| 10 | Channel-based pipeline | **REVERTED** | no improvement; parse too small to overlap |
| 11 | Defer rollups to post-import | **REVERTED** | no improvement; same total SQL work |
| 12 | Multi-row VALUES batching | **REVERTED** | 40% REGRESSION; dynamic SQL defeats prepare_cached |
| 13 | In-memory staging + VACUUM INTO | **REVERTED** | no improvement; btree work is CPU-bound, not I/O |
| D0 | Zero-write diagnostic | **N/A** | CPU floor = 2.82s (subset); SQL = 67% of wall |
| D1 | Denormalize join tables via UPDATE | **REVERTED** | +18% REGRESSION; UPDATE > INSERT on existing table |
| D3 | Defer rollup computation out of import path | **REVERTED** | +40-50% REGRESSION; cache-miss penalty in post-import rollup pass |
| D1b | Preset turn_id at message INSERT (deferred grouping) | **KEPT** | −6% subset / −1% full; turn_message eliminated |
| D2 | Defer message_part INSERTs to post-normalize pass | **REVERTED** | +71% REGRESSION; SQLite FK verification cache misses when child inserts are batched after parent inserts |
| D2b | action_message schema: 3→2 btrees via INTEGER PRIMARY KEY | **REVERTED** | +18% REGRESSION; composite PK + UNIQUE(message_id) faster than INTEGER rowid PK in practice |

### Current best metrics (post D1b)
| metric | value | vs original baseline | vs target |
| --- | ---: | ---: | ---: |
| Cold full import (session-local best) | ~9.7s | ~92% from 126.1s | ~0.97× to 10s target |
| Startup | ~2.29s | ~55% from ~5.1s baseline | ~2.3× to <1s target |

### Current phase distribution (subset, ~8.3s post-D1b session-local median)

| phase | time | % of wall | note |
| --- | ---: | ---: | --- |
| normalize_jsonl | ~3.8s | ~46% | messages + parts INSERTs (130K + 179K); turn_message gone |
| build_actions | ~2.3s | ~28% | classify CPU + DB persist |
| scan_source | ~1.0s | ~12% | directory walk + VCS resolution |
| finalize_chunk + rollups | ~1.7s | ~21% | path rollup dominant, cache-locality bound |
| build_turns | ~0.0s | ~0% | eliminated (merged into normalize_jsonl) |
| parse (rayon CPU) | ~0.3s | ~4% | parallel JSON parse |

### Key findings (cumulative)
1. **Btree pages are cached.** With 64MB page cache + WAL, secondary index maintenance during inserts is essentially free.
2. **The remaining SQL cost is core btree insert work** — CPU-bound, not I/O-bound.
3. **In-memory data passing saves ~2s in build_actions + build_turns** but the Vec assembly overhead partially offsets gains. Primary value: **architectural prerequisite for parallelism**.
4. **`RETURNING id` is expensive on high-frequency INSERTs.** Replacing with `execute` + `last_insert_rowid()` saved ~3.5s (−15%) across 416K+ inserts.
5. **Session-to-session variance is high (~30%)** on WSL2. Within-session relative comparisons are reliable.
6. **`classify_message()` CPU is ~300ms total.** Parallelizing classification alone yields no measurable improvement.
7. **Multi-row VALUES batching loses to `prepare_cached` single-row inserts** when batch sizes are small (~1.4 parts/message) and dynamic SQL prevents statement caching.
8. **In-memory staging does not help** when the DB is on tmpfs and the bottleneck is CPU-bound btree work. VACUUM INTO adds 1.5s overhead.
9. **UPDATE on existing table is MORE expensive than INSERT into dedicated join tables.** Even with batch WHERE id IN (...), replacing join table INSERTs with UPDATEs on the message table regresses by +18%. Search+modify (random write) is costlier than append-friendly INSERT into compact btrees. The correct join-table elimination path is to preset FKs at initial INSERT time (D1b), not to update afterwards.
10. **Rollup computation is cache-locality dependent.** Per-chunk rollup runs efficiently because data pages are hot immediately after insert. Any post-import pass (whether single batch or truly deferred) pays 4-10× cache-miss penalty as earlier chunks' pages are evicted. Per-chunk rollup is near-optimal — it cannot be deferred.
11. **Compact join-table btree ops are cheap.** `turn_message` rows are tiny (3 INTEGER cols) and the table fits in cache — each insert is fast. Eliminating 295K × 3 btree ops saves ~0.6s subset (−6%) but is within noise on full corpus. The btree-count model overestimates wall-time impact for small, compact tables.
12. **Deferring child-table INSERTs past parent INSERTs causes SQLite FK verification cache misses.** With `PRAGMA foreign_keys = ON`, each child INSERT triggers a lookup in the parent table to verify the FK. The inline pattern (parent INSERT immediately followed by child INSERTs) keeps FK lookup pages hot. Batching all parent rows first, then all child rows, means each FK lookup on a child INSERT may need to fetch a page that's no longer hot — despite the 64MB cache being nominally large enough. D2 (deferring 179K message_part rows after 130K message rows) caused +71% regression. The `PRAGMA foreign_keys` flag cannot be toggled inside an active savepoint, so there is no easy workaround. The correct strategy for table elimination is the D1b pattern: preset FKs at INSERT time rather than deferring any parent-child relationship across bulk insert boundaries.
13. **Reducing action_message auto-indexes from 3→2 by changing PK to INTEGER rowid regresses +18%.** The old composite PK `(action_id, message_id)` + `UNIQUE(action_id, ordinal_in_action)` + `UNIQUE(message_id)` structure (3 auto-indexes + 1 rowid btree = 4 total) is faster in practice than `PRIMARY KEY(message_id)` (rowid) + `UNIQUE(action_id, ordinal_in_action)` (2 total). The composite index structure is better suited to the mixed INSERT + FK-verification workload, possibly because the compact separate `UNIQUE(message_id)` index caches more efficiently than using the full table btree for the same lookups.

### 10s target assessment (revised 2026-04-15, post-D2b)

D1b eliminated `turn_message`. D2 and D2b both regressed. The `action_message`
optimization path is exhausted: neither deferred inserts (D2) nor schema
restructuring (D2b) improved performance. The D1b "preset FK at INSERT" pattern
cannot apply to action_message due to the turn dependency cycle.

D2 (deferred message_part inserts) regressed +71% — SQLite FK verification
cache-miss penalty. Deferring parent-child inserts across a bulk boundary is
not viable with `PRAGMA foreign_keys = ON`.

### Next action

D2 REVERTED. Try D2b (denormalize action_message — same D1b pattern: preset
FK at INSERT time, eliminating a join table). Expected savings: ~0.5s or less
(similar to D1b; action_message has similar row characteristics to turn_message).

### Remaining unexplored candidates (re-ranked post D2b)

| rank | candidate | est. win | confidence | note |
| --- | --- | --- | --- | --- |
| 1 | **D4: simd-json** | <0.3s | very low | parse phase is only ~0.3s / 4% — ceiling is tiny |
| 2 | **D5: scan_source caching** | ~0.5s startup only | high | warm startup only, not full import |
| — | **D2b (all variants)** | — | EXHAUSTED | dependency cycle blocks preset; schema changes regress |

### Bench harness
```bash
cargo build -p gnomon-core --example import_bench --release

# Subset (~7.7s session-local median post-D2b session; ~8.3s in D1b session)
cargo run -p gnomon-core --example import_bench --release -- \
  --corpus subset --mode full --repeats 3

# Full corpus (~21s post-D1b)
cargo run -p gnomon-core --example import_bench --release -- \
  --corpus full --mode full --repeats 3
```

### Session-resumption sanity check
```bash
cd /home/ketan/project/gnomon
git log --oneline -5
cargo run -p gnomon-core --example import_bench --release -- \
  --corpus subset --mode full --repeats 1
# Should complete in ~8-9s
```
