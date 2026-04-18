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

## 2026-04-16 — candidate E1+E2+E3: Pragma hardening

**Branch:** `import-perf`
**Hypothesis:** Disabling FK verification (`foreign_keys=OFF`), taking an
exclusive writer lock (`locking_mode=EXCLUSIVE`), and disabling auto-checkpoint
(`wal_autocheckpoint=0` with a manual `wal_checkpoint(TRUNCATE)` after import)
should reduce cold full-import wall time by ~2–4s without changing schema or
read-path behavior.

**Implementation:** Added importer-specific SQLite connection profiles in
`crates/gnomon-core/src/db/mod.rs`. Background startup imports now open with
`foreign_keys=OFF` and `wal_autocheckpoint=0`; foreground full import reuses
the caller's `Database` handle and applies `foreign_keys=OFF`,
`locking_mode=EXCLUSIVE`, and `wal_autocheckpoint=0` before chunk execution.
Manual WAL checkpointing was added after import completion in
`crates/gnomon-core/src/import/chunk.rs`. The foreground `import_all` path was
refactored to accept `&mut Database` so `locking_mode=EXCLUSIVE` does not
deadlock against the previous dual-connection planner/writer topology. Call
sites and integration tests were updated accordingly.

**Measurements:**

Full corpus, full mode (3 repeats, same session):

| run | E1+E2+E3 |
| --- | ---: |
| 1 | 28.4s |
| 2 | 26.5s |
| 3 | 27.1s |
| **median** | **27.1s** |
| **best** | **26.5s** |

Comparison point: current pre-E session-local best was ~21.6s. On the
authoritative 3-repeat run, E1+E2+E3 regressed by **~5.5s / ~25%**.

Perf-log follow-up (single isolated run, full corpus):
- wall: **21.0s**
- `scan_source`: **2407ms**
- `normalize_jsonl`: **7178ms**
  - `sql_ms`: **5170ms**
  - `purge_ms`: **224ms**
  - `finish_import_ms`: **184ms**
  - **`commit_ms`: `0.0ms`**
- `build_actions`: **4260ms**
- `build_turns`: **872ms**
- `finalize_chunk`: **2009ms**
  - `rebuild_path_rollups`: **1777ms**
  - `rebuild_action_rollups`: **195ms**

Row parity: **PASS** — full-corpus row counts matched the pre-E shape exactly
(`project=31`, `source_file=4548`, `import_chunk=162`, `conversation=4547`,
`stream=4547`, `message=294995`, `message_part=411842`, `turn=13363`,
`action=120922`, `record=0`). DB size remained **425.58 MB**.

**Decision:** REVERTED — the bundle did not produce a repeatable win. The
isolated perf-log run was promising, but the 3-repeat benchmark regressed badly
enough that this candidate is not defensible as a kept change.

**Rationale:** Two distinct findings emerged:
1. `foreign_keys=OFF` appears to remove the old commit stall entirely
   (`commit_ms = 0` on the perf-log run), so the hypothesis was not directionally
   wrong.
2. `locking_mode=EXCLUSIVE` is topology-sensitive. The original full-import path
   deadlocked because it planned on one SQLite connection and wrote on another.
   Fixing that required a single-connection foreground refactor. The startup
   worker cannot safely use the same exclusive mode because it intentionally
   coexists with an open reader/TUI connection.

Given the benchmark variance and the architectural caveat on E2, this bundle is
not reliable enough to keep. If pragma work is revisited later, it should be as
**E1+E3 only** or as part of the sharded merge path, not as a standalone kept
optimization.

**Key finding #14:** `locking_mode=EXCLUSIVE` is incompatible with the existing
dual-connection import topology. It can only be tested cleanly on a
single-connection foreground import path and is not a drop-in setting for the
background startup importer.

**Key finding #15:** `foreign_keys=OFF` can erase commit cost on an isolated
run, but the full pragma bundle still failed to deliver a stable wall-time win.
The remaining cold-import problem is still dominated by overall serial write
volume, not just fsync/FK overhead.

**Next implied:** **F2a: parallel import -> sequential merge.** E was the
lowest-risk cold-import candidate and did not move the 10s target reliably. The
remaining candidate with a clear multi-second ceiling is parallel write via
per-project shards followed by a sequential merge into the single production DB.

---

## 2026-04-16 — timing coverage hardening

**Branch:** `import-perf`
**Hypothesis:** The E1+E2+E3 result was not interpretable enough because the
perf log still had blind spots. Specifically, the end-of-import manual WAL
checkpoint was uninstrumented, the foreground import path reused the old
`import.open_database` label for connection reconfiguration, chunk-local parse
time was unlabeled after the rayon split, and the benchmark harness's default
10 MiB perf-log cap was truncating fully-instrumented full-corpus runs.

**Implementation:** Added timing coverage for:
- `import.total`
- `import.configure_connection`
- `import.checkpoint_wal`
- `import.begin_chunk`
- `import.parse_phase`
- `import.chunk_commit`
- `import.savepoint_open`
- `import.savepoint_release`
- `import.savepoint_rollback`

Also split `build_actions` into internal subspans:
- `import.build_actions.load_context`
- `import.build_actions.purge_existing`
- `import.build_actions.load_messages`
- `import.build_actions.build_tool_lookup`
- `import.build_actions.group_turns`
- `import.build_actions.classify_messages`
- `import.build_actions.persist_path_refs`
- `import.build_actions.persist_actions`
- `import.build_actions.update_chunk_count`

Finally, `crates/gnomon-core/examples/import_bench.rs` now opens JSONL perf logs
with a 200 MiB cap so full-corpus verbose runs are not truncated.

**Measurements:** Clean full-corpus coverage run (single run, full mode):
- wall: **26.193s**
- `import.total`: **23906ms**
- `import.scan_source`: **2212ms**
- `import.build_plan`: **5ms**
- `import.prepare_plan`: **15ms**
- `import.configure_connection`: **619ms**
- `import.begin_chunk`: **139ms**
- `import.parse_phase`: **1630ms**
- `import.chunk`: **19281ms**
- `import.normalize_jsonl`: **8348ms**
  - `sql_ms`: **5908ms**
  - `purge_ms`: **284ms**
  - `finish_import_ms`: **231ms**
  - `commit_ms`: **0ms**
- `import.build_actions`: **5210ms**
  - `load_context`: **260ms**
  - `purge_existing`: **310ms**
  - `load_messages`: not emitted on the in-memory import path
  - `build_tool_lookup`: **307ms**
  - `group_turns`: **14ms**
  - `classify_messages`: **3435ms**
  - `persist_path_refs`: **3481ms**
  - `persist_actions`: **3506ms**
  - `update_chunk_count`: **361ms**
- `import.finalize_chunk`: **2165ms**
  - `rebuild_path_rollups`: **1900ms**
  - `rebuild_action_rollups`: **213ms**
- `import.chunk_commit`: **1595ms**
- `import.checkpoint_wal`: **3843ms**
- `import.savepoint_open`: **10ms**
- `import.savepoint_release`: **149ms**

**Decision:** KEPT as instrumentation. This is not a product optimization; it is
measurement hardening so subsequent experiments are diagnosable.

**Rationale:** The earlier ambiguity is now resolved:
1. The "missing time" from E1+E2+E3 was primarily **`import.checkpoint_wal`**
   rather than unexplained work elsewhere. `commit_ms` moved to ~0, but the
   WAL checkpoint absorbed multi-second cost at the end of import.
2. Savepoint lifecycle overhead is negligible (`~159ms` total across 4547 files)
   and can be ruled out as a material bottleneck.
3. Chunk-local parse time is real but small (`~1.6s` total), so parse remains a
   secondary lever.
4. The fully split `build_actions` subspans are informative but **not
   additive**: `classify_messages`, `persist_path_refs`, and `persist_actions`
   overlap structurally because the current implementation does those steps
   interleaved inside the same per-message loop.

**Key finding #16:** The end-of-import manual WAL checkpoint is the largest
newly-attributed blind spot. In the fully instrumented run it cost **3.84s**,
which explains why `commit_ms` could drop to zero without producing a stable
end-to-end wall-time win.

**Key finding #17:** Savepoint overhead is negligible. `import.savepoint_open`
+ `import.savepoint_release` is ~159ms across the entire full corpus and is not
worth targeting.

**Key finding #18:** Full-corpus verbose perf runs were previously being
truncated by the benchmark harness's 10 MiB log cap. The harness now preserves
complete logs for future coverage-heavy experiments.

**Next implied:** F2a remains the next cold-import experiment. The importer is
now instrumented well enough to evaluate parallel shard import and sequential
merge without hand-waving about where the time moved.

---

## 2026-04-16 — F2a parallel import -> sequential merge

**Branch:** `import-perf`
**Hypothesis:** Per-project shard databases would let the importer parallelize
the remaining serial SQLite write volume without changing the production read
path. The expected shape was: import each project's chunks into its own temp
SQLite in parallel, then merge those shard DBs sequentially into one target DB.
If the parallel write phase collapsed toward the existing parse floor and the
merge stayed under a few seconds, this was the last plausible cold-import
candidate with a multi-second ceiling toward the 10s target.

**Implementation:** Added a benchmark-only sharded import path:
- plan chunks once on the main DB, then group prepared chunks by `project_id`
- seed one temp shard DB per project with the required `project`,
  `source_file`, and `import_chunk` metadata
- import each project's chunks against its shard DB in parallel
- merge shard tables back into the target DB sequentially with reserved rowid
  offsets, then reassign global `publish_seq`
- expose the path through `import_bench --strategy sharded`

The product path is unchanged. This was an experiment harness for measuring the
parallel import + sequential merge topology only.

**Measurements:**

Subset corpus, full mode, single run:
- serial wall: **12.632s**
- sharded wall: **10.466s**
- delta: **-2.166s / -17.1%**
- row parity: **PASS**
  (`project=1`, `source_file=1649`, `import_chunk=35`, `conversation=1648`,
  `stream=1648`, `message=130478`, `message_part=179412`, `turn=4915`,
  `action=50463`, `record=0`)
- DB size: **160.62 MB serial** vs **160.70 MB sharded**

Full corpus, full mode, authoritative single-run comparison:
- serial wall: **20.599s**
- sharded wall: **24.563s**
- sharded perf-log wall: **24.070s**
- delta vs serial: **+3.47s to +3.96s / +16.8% to +19.2%**
- row parity: **PASS**
  (`project=31`, `source_file=4548`, `import_chunk=162`, `conversation=4547`,
  `stream=4547`, `message=294995`, `message_part=411842`, `turn=13363`,
  `action=120922`, `record=0`)
- DB size: **425.74 MB serial** vs **425.50 MB sharded**

Sharded perf-log attribution (full corpus):
- `import.scan_source`: **2085ms**
- `import.shard_setup`: **866ms**
- `import.shard_import_parallel`: **14407ms**
- cumulative `import.shard_import`: **30131ms** across 31 projects
- `import.shard_merge_total`: **3341ms**
- cumulative `import.checkpoint_wal`: **6171ms** across 32 databases
- `import.reassign_publish_seq`: **5ms**

Largest individual merges were concentrated in the biggest projects:
- one project merge at **1543ms**
- one project merge at **1295ms**
- one project merge at **232ms**
- the remaining project merges were small

**Decision:** REVERTED. F2a preserves exact row counts and helps on the single-
project subset, but it regresses the only authoritative metric: full-corpus
cold import wall time.

**Rationale:** The perf log makes the failure mode explicit:
1. Parallel shard import does reduce the write wall inside the shard stage
   (`import.shard_import_parallel` at ~14.4s), but not enough to outrun the
   cost of creating, checkpointing, and later merging 31 shard databases.
2. Sequential merge alone costs ~3.3s on the full corpus, which is already a
   large fraction of the serial path's total remaining gap.
3. Cumulative checkpoint cost grows to ~6.2s because each shard pays its own
   WAL/commit lifecycle before the target DB pays it again.
4. The subset win was misleading because the subset contains only one project,
   so it avoided the full-corpus fan-out and most of the sequential merge tax.

This means the F-track topology does not beat a well-tuned single SQLite on the
full corpus. The remaining cold-import problem is not "lack of project-level
parallelism" in isolation; it is the overhead of maintaining and reconciling
many SQLite files.

**Key finding #19:** F2a can look good on a single-project subset and still
lose on the full corpus. Cross-project fan-out and merge costs dominate once
the shard count grows.

**Key finding #20:** Repeated WAL lifecycle costs matter at shard scale. The
sharded full-corpus run paid ~6.2s of cumulative checkpoint time across 32
databases, which erased the benefit of the shorter per-shard write phase.

**Next implied:** **G2 path_node memoization** is now the best remaining
cold-import candidate. E and F were the only candidates with an obvious
multi-second ceiling; both failed. The next defensible cold-import lever is the
import-local `path_node` reduction work because it is low-risk, product-safe,
and targets a still-hot write-side path without introducing merge topology or
read-path churn.

---

## 2026-04-16 — G2 path_node memoization

**Branch:** `import-perf`
**Hypothesis:** `persist_path_refs()` still walks the same
`ensure_path_node_chain() -> ensure_path_node()` path for repeated file paths.
An import-local memoization table keyed by canonical full path might collapse a
meaningful share of the repeated `SELECT id FROM path_node ...` lookups into
cheap in-memory hits, especially on the large single-project subset where path
prefix reuse is high.

**Implementation:** Added a transient importer-local cache from `full_path` to
`path_node.id` and threaded it through the `build_actions` write path so
multiple conversations imported in the same chunk could reuse resolved path
nodes without extra SQLite lookups. No schema or read-path changes.

**Measurements:**

Subset corpus, full mode, single run:
- wall: **7.743s**
- row parity: **PASS**
  (`project=1`, `source_file=1649`, `import_chunk=35`, `conversation=1648`,
  `stream=1648`, `message=130478`, `message_part=179412`, `turn=4915`,
  `action=50463`, `record=0`)
- DB size: **160.70 MB**

Full corpus, full mode, authoritative single run:
- serial wall to beat: **20.599s**
- G2 wall: **24.553s**
- delta vs serial: **+3.954s / +19.2%**
- row parity: **PASS**
  (`project=31`, `source_file=4548`, `import_chunk=162`, `conversation=4547`,
  `stream=4547`, `message=294995`, `message_part=411842`, `turn=13363`,
  `action=120922`, `record=0`)
- DB size: **425.74 MB**

**Decision:** REVERTED. G2 preserves exact row counts and looks attractive on
the subset, but it materially regresses the authoritative full-corpus cold
import benchmark.

**Rationale:** The cache reduced enough repeated work inside the single-project
subset to produce a fast directional result, but that locality did not survive
the full-corpus shape. With 31 projects and 162 chunks, the importer still
spends most of its time on the underlying path/action writes rather than on the
`path_node` lookup overhead alone. G2 therefore joins E and F in the category
of experiments that can look promising on a directional run yet fail the only
metric that matters.

**Key finding #21:** Import-local `path_node` memoization has strong locality on
the single-project subset but does not translate into a full-corpus wall-time
win. The remaining cold-import bottleneck is larger than repeated path-node
lookups alone.

**Next implied:** **G3 inline rollup materialization** is now the next
defensible cold-import candidate. G2 was the last low-risk write-side reduction
without topology changes; if cold-import work continues, the remaining
experiment needs to attack the reread/rebuild cost in rollups directly. If the
goal shifts to startup latency instead, G1 and G4 remain the strongest warm-
start candidates.

---

## 2026-04-16 — G3 inline rollup materialization

**Branch:** `import-perf`
**Hypothesis:** The remaining cold-import reread is concentrated in
`rebuild_chunk_path_rollups()` plus the smaller action-rollup rebuild. If the
import path materializes those chunk rollups while classification already has
the normalized messages, grouped action descriptors, and explicit file refs in
memory, the importer should be able to remove the finalize-time reread entirely
and shave roughly the old rollup cost from cold full import without changing
the read schema.

**Implementation:** Added a temporary chunk-local rollup accumulator and wired
`build_actions_in_tx_with_messages()` to feed it during classification, then
changed chunk finalization to flush those in-memory rollup rows instead of
calling the existing SQL rebuild helpers. A dedicated parity test compared the
inline rollups against the old rebuild path on the same imported fixture before
benchmarking. After measurement, the code changes were reverted because the
benchmark regressed.

**Measurements:**

Subset corpus, full mode, single run:
- wall: **12.221s**
- row parity: **PASS**
  (`project=1`, `source_file=1649`, `import_chunk=35`, `conversation=1648`,
  `stream=1648`, `message=130478`, `message_part=179412`, `turn=4915`,
  `action=50463`, `record=0`)
- DB size: **160.83 MB**

Full corpus, full mode, authoritative single run:
- serial wall to beat: **20.599s**
- G3 wall: **26.573s**
- delta vs serial: **+5.974s / +29.0%**
- row parity: **PASS**
  (`project=31`, `source_file=4548`, `import_chunk=162`, `conversation=4547`,
  `stream=4547`, `message=294995`, `message_part=411842`, `turn=13363`,
  `action=120922`, `record=0`)
- DB size: **426.00 MB**

**Decision:** REVERTED. G3 preserves exact row counts but materially regresses
both the single-project subset and the authoritative full-corpus cold-import
benchmark.

**Rationale:** The finalize-time SQL reread looked wasteful in isolation, but
the existing rollup rebuild runs against a hot cache after the write phase. By
moving rollup materialization into classification, G3 pushed extra Rust-side
aggregation and per-path fan-out bookkeeping into the already hot write path.
That additional synchronous work cost more than the old reread it removed.
G3 therefore reinforces the earlier D3 result from the opposite direction:
chunk rollups want a hot-cache post-write rebuild rather than more importer-side
work inside classification.

**Key finding #22:** Inline rollup materialization is slower than the existing
chunk rollup reread. The remaining rollup rebuild cost is smaller than the
extra per-message and per-path bookkeeping required to materialize those rows in
the hot write path.

**Next implied:** **G1 scan_source delta cache + parallel header extraction** is
now the next sensible experiment. G3 exhausted the last remaining
product-preserving cold-import structural candidate; the remaining leverage is
in startup latency (`scan_source` / invalidation) unless the project is willing
to pursue behavior-changing options such as deferring heavy tables or background
streaming import.

---

## 2026-04-16 — G4 finer-grained invalidation than whole `project x day` chunks

**Branch:** `import-perf`
**Hypothesis:** Once G1 removes almost all no-delta startup work, the remaining
warm-startup cost is self-inflicted chunk replay: a single changed file still
causes the importer to purge and rebuild the entire `project x day` chunk. If
the planner can distinguish changed files from unchanged siblings and the
executor can purge only the affected file-backed rows while recomputing chunk
rollups/counts afterward, changed-file startup should fall from multi-chunk
replay toward "one changed file + one old-day cleanup chunk" cost without
changing query results.

**Implementation:** Added a new `pending_chunk_rebuild` table to remember old
chunk days that must be revisited after a file moves or disappears. The planner
now emits per-chunk change sets with separate `import_source_files` and
`remove_only_source_files` lists instead of treating any dirty file as "rebuild
the whole day." Chunk execution no longer purges the entire day up front.
Instead it:
1. purges only the changed file's prior rows from the target chunk before
   rewriting that file,
2. runs remove-only chunks for prior day membership or deleted files,
3. recomputes per-chunk counts and rollups after the selective purge/write, and
4. clears any satisfied `pending_chunk_rebuild` rows.

The bench harness also gained a `delta-startup` mode. Its measured pass now
bumps `mtime` by rewriting the same bytes, rather than appending a newline, so
the benchmark exercises a real one-file delta instead of a warning-only parse
artifact.

**Measurements:**

Subset corpus, `delta-startup`, 3 repeats:

| run | wall |
| --- | ---: |
| 1 | 0.391s |
| 2 | 0.244s |
| 3 | 0.161s |
| **median** | **0.244s** |

Full corpus, `delta-startup`, 3 repeats:

| run | wall |
| --- | ---: |
| 1 | 0.541s |
| 2 | 0.555s |
| 3 | 0.453s |
| **median** | **0.541s** |

Perf-log follow-up, full corpus `delta-startup` (single corrected measured pass):
- end-to-end measured wall: **0.394s**
- `import.scan_source`: **191.389ms**
  - `collect_candidates`: **38.479ms**
  - `load_cache`: **7.071ms**
  - `resolve_cache_misses`: **0.591ms**
  - `persist_scan_source_cache`: **90.244ms**
  - `cache_hit_count`: **4547**
  - `cache_miss_count`: **1**
  - `updated_source_files`: **1**
- `import.build_plan`: **4.577ms**
- `import.prepare_plan`: **0.274ms**
- startup chunk (`1` changed file on the new day): **194.333ms**
  - `parsed_file_count`: **1**
  - `parsed_record_count`: **39**
  - `normalize_jsonl.purge_ms`: **190.628ms**
- deferred remove-only chunk (old day membership cleanup): **3.483ms**
- `import.worker_total`: **198.638ms**

Row parity: **PASS** — authoritative full-corpus `delta-startup` runs retained
the established row counts (`project=31`, `source_file=4548`, `import_chunk=163`,
`conversation=4547`, `stream=4547`, `message=294995`, `message_part=411842`,
`turn=13363`, `action=120922`, `record=0`). The extra `import_chunk` row is the
expected one-file delta replay chunk created after the measured mutation. Full
DB size remained ~**429.7 MB**.

Focused verification:
- `cargo test -p gnomon-core import::chunk -- --nocapture` — PASS
- `cargo test -p gnomon-core import::source -- --nocapture` — PASS
- `cargo test -p gnomon-core` — PASS (`191 passed, 8 ignored`)

**Decision:** **KEPT.** G4 turns changed-file warm startup from whole-day replay
into a true file-scoped delta path. The authoritative full corpus now measures
at **0.541s median** for a one-file delta, while retaining exact row parity.

**Rationale:** G1 solved the no-delta case, but startup still paid for coarse
chunk invalidation whenever any file changed. G4 removes that structural waste:
the measured pass touched exactly one imported file on the new day, rebuilt the
old day in a fast remove-only chunk, and left unchanged siblings untouched. The
perf log also shows what now dominates the changed-file path: selective purge of
the changed file's prior rows (~191ms), not scan or planning. That is a much
smaller and more truthful steady-state cost than replaying an entire day chunk.

**Key finding #25:** File-granular invalidation is enough to keep changed-file
warm startup in the sub-second range on the authoritative full corpus. The
remaining steady-state cost is mostly row purge/rewrite for the changed file,
not chunk scheduling or sibling reimport.

**Key finding #26:** Once G1 and G4 are both in place, the startup-oriented
structural work is largely exhausted. Remaining meaningful wins now require
behavior-changing choices such as deferring heavy derived tables or opening the
UI before background import finishes.

**Next implied:** **C3 background streaming import** is now the next sensible
experiment if the goal is further startup/user-perceived latency improvement.
G1 and G4 solved no-delta and one-file-delta warm startup without changing
product behavior; the remaining leverage is to change when heavy import work
blocks the UI, not to keep shrinking planner granularity.

## 2026-04-17 — candidate C3: background streaming import

**Branch:** `import-perf`
**Hypothesis:** Opening the TUI immediately on the current published snapshot
while import continues in the background should remove nearly all remaining
startup blocking. Warm startup should stay near the G1 floor, and one-file delta
startup should fall well below the current G4 median because the changed chunk
no longer blocks open.

**Implementation:** Prototyped a streaming startup gate in the importer so the
default startup path returned immediately after plan preparation and worker
spawn, instead of waiting for the startup window to settle. The visible
snapshot stayed pinned and still required manual refresh, so the experiment was
strictly about when the TUI opened, not about auto-applying live updates.

**Measurements:**

Subset corpus, `startup`, 1 repeat:

| run | wall |
| --- | ---: |
| 1 | 1.094s |

Full corpus, `startup`, 3 repeats:

| run | wall |
| --- | ---: |
| 1 | 0.629s |
| 2 | 0.530s |
| 3 | 0.613s |
| **median** | **0.613s** |

Observed startup-shape change on the full corpus:
- startup returned before any import chunks published
- row counts at benchmark exit were still bootstrap-only:
  `project=31`, `source_file=4548`, `import_chunk=162`,
  `conversation=0`, `stream=0`, `message=0`, `message_part=0`,
  `turn=0`, `action=0`, `record=0`
- DB size at benchmark exit was only **5.47 MB**

Full corpus, `warm-startup`, 3 repeats:

| run | wall |
| --- | ---: |
| 1 | 0.233s |
| 2 | 0.231s |
| 3 | 0.250s |
| **median** | **0.233s** |

Full corpus, `delta-startup`, 3 repeats:

| run | wall |
| --- | ---: |
| 1 | 0.284s |
| 2 | 0.242s |
| 3 | 0.215s |
| **median** | **0.242s** |

Focused verification while the prototype was present:
- `cargo test -p gnomon-core startup_streaming_opens_immediately_and_finishes_in_background` — PASS
- `cargo test -p gnomon-tui streaming_refresh_copy` — PASS

**Decision:** **REVERTED.** The startup-speed win is real, but the experiment
breaks the current product contract by opening on an empty or stale snapshot.
On a cold/full-corpus startup, the UI would open in ~**0.61s** while still
showing zero imported conversations until the user manually refreshed later.

**Rationale:** C3 proves that the remaining startup wall is mostly optional UI
blocking, not mandatory import work. But the current `v1` design says startup
imports the last 24 hours before opening, or times out after 10 seconds. This
prototype violates that contract in the worst possible case: first launch on an
empty DB opens into an empty view even though importable data exists. That is a
meaningful product change, not just a perf optimization, so it should not land
without an explicit product decision and accompanying design update.

**Key finding #27:** C3 can cut authoritative full-corpus cold startup-open
wall time to roughly **0.53-0.63s median** by eliminating startup-window
blocking entirely.

**Key finding #28:** The price of C3 is not correctness but initial coverage:
the first visible snapshot can be empty or stale, so the user sees less data at
open time and must refresh manually to pick up the published chunks.

**Next implied:** If the startup contract stays unchanged, **skip/defer
action+path_ref tables** is now the next ranked experiment. C3 demonstrated the
mechanical ceiling for "open sooner", but it did so only by relaxing the agreed
visibility semantics.

---

## RESUME HERE (if session was reset, read this first)

Last updated: 2026-04-17 (C3 evaluated, reverted)
Current phase: Phase 3 — behavior-changing startup experiment evaluated; remaining work is either deeper product change or cold-import deferral
Current branch/worktree: `import-perf` at `/home/ketan/project/gnomon/.worktrees/import-perf`
Current best full-corpus metric is now **20.599s** (serial full-corpus run
captured while evaluating F2a). Subset session-local median remains ~7.67s
(post-D2b session).

D0: CPU floor = 2.82s (subset). SQL = 67% of wall.
D1: denormalize-via-UPDATE = +18% regression (UPDATE > INSERT on hot table).
D3: defer rollups = +40-50% regression (cache-miss penalty in post-import pass).
D1b: −6% subset / −1% full; turn_message eliminated. Bug: imported_message_count written as 0 — fixed.
D2: defer message_part INSERTs = +71% REGRESSION (FK verification cache-miss penalty).
D2b: action_message schema (3→2 btrees) = +18% REGRESSION (composite PK + UNIQUE index faster than INTEGER PRIMARY KEY rowid in practice).
Key finding #10: rollup SQL requires hot cache (per-chunk is optimal).
Key finding #12: deferring child INSERTs past parent INSERTs causes FK verification cache misses.
Key finding #13: reducing action_message PK to INTEGER rowid regresses — composite index structure is faster.
Key finding #14: `locking_mode=EXCLUSIVE` requires a single writer connection and is incompatible with the old dual-connection import path.
Key finding #15: `foreign_keys=OFF` can zero `commit_ms`, but E1+E2+E3 still failed to produce a stable full-corpus wall-time win.
Key finding #16: the end-of-import WAL checkpoint is a real multi-second cost (~3.8s in the fully instrumented run).
Key finding #17: savepoint lifecycle overhead is negligible (~159ms across the full corpus).
Key finding #18: the benchmark harness needed a larger perf-log cap to keep full-corpus verbose traces complete.
Key finding #19: F2a can win on a single-project subset and still regress on the full corpus because shard fan-out and merge costs dominate at scale.
Key finding #20: sharded import paid ~6.2s of cumulative checkpoint time across 32 databases; repeated WAL lifecycle cost erased the benefit of the shorter parallel write phase.
Key finding #21: import-local `path_node` memoization helps on the single-project subset but still loses on the full corpus; repeated path-node lookups are not the dominant remaining cold-import cost.
Key finding #22: inline rollup materialization regresses; the hot-cache reread is cheaper than pushing rollup aggregation into the classifier/write path.
Key finding #23: a policy-aware persisted scan cache is enough to push no-delta full-corpus warm startup to ~230ms.
Key finding #24: after G1, no-delta warm startup is mostly directory walk + cache reconciliation, not import work.
Key finding #25: file-granular invalidation keeps one-file delta warm startup in the ~0.4-0.5s range on the full corpus.
Key finding #26: after G1+G4, further meaningful startup wins require behavior changes, not finer structural invalidation.
Key finding #27: C3 can drive cold startup-open wall time to ~0.6s on the full corpus by opening before any chunks publish.
Key finding #28: that C3 win comes from serving an empty/stale initial snapshot, so it is a product trade, not a free perf gain.

## 2026-04-16 — candidate G1: scan_source delta cache + parallel header extraction

**Branch:** `import-perf`
**Hypothesis:** Reusing cached `scan_source` results for unchanged files and
parallelizing first-record header extraction for cache misses should collapse
warm-startup scan time from the current multi-second floor to well under the
`<1s` target, with no product behavior change. Cold full import should stay
roughly flat or improve slightly because the first scan still benefits from
parallel header extraction.

**Implementation:** Added a new SQLite table `scan_source_cache` keyed by
`(source_root_path, policy_fingerprint, source_kind, relative_path)`. Each row
stores the file metadata used for invalidation (`modified_at_utc`, `size_bytes`)
plus the resolved scan outcome: `raw_cwd_path`, warnings JSON, exclusion flag,
and the fully resolved project identity payload. `scan_source_manifest_with_policy`
now fingerprints `ProjectIdentityPolicy + project_filters`, loads matching cache
rows, reuses unchanged results without reopening the JSONL file, and resolves
cache misses in parallel with a shared `cwd -> ResolvedProject` memo. The cache
is reconciled in the same transaction as the manifest update so stale rows are
removed deterministically. The benchmark harness gained a `warm-startup` mode
that prefills the DB once, then measures only the second startup pass against a
fully populated DB and scan cache.

**Measurements:**

Subset corpus, `warm-startup`, 3 repeats:

| run | wall |
| --- | ---: |
| 1 | 0.083s |
| 2 | 0.079s |
| 3 | 0.086s |
| **median** | **0.083s** |

Full corpus, `warm-startup`, 3 repeats:

| run | wall |
| --- | ---: |
| 1 | 0.231s |
| 2 | 0.230s |
| 3 | 0.227s |
| **median** | **0.230s** |

Perf-log follow-up, full corpus `warm-startup` (single measured pass):
- `import.scan_source`: **179.562ms**
  - `collect_candidates`: **40.307ms**
  - `load_cache`: **7.467ms**
  - `resolve_cache_misses`: **0.003ms**
  - `persist_scan_source_cache`: **79.374ms**
  - `cache_hit_count`: **4548**
  - `cache_miss_count`: **0**
- `import.build_plan`: **4.634ms**
- `import.prepare_plan`: **0.001ms**
- `import.load_snapshot`: **0.087ms**
- end-to-end measured wall: **0.217s**

Cold full-import control, full corpus `full` mode (single run, non-authoritative):
- wall: **18.731s**
- comparison point: current serial reference is **20.599s**
- interpretation: no evidence of a cold-path regression; any cold-path win from
  G1 should still be treated as directional until repeated under the normal
  full-import benchmark cadence.

Row parity: **PASS** — subset/full warm-startup and the cold full-import
control all produced the established authoritative row counts
(`project=31`, `source_file=4548`, `import_chunk=162`, `conversation=4547`,
`stream=4547`, `message=294995`, `message_part=411842`, `turn=13363`,
`action=120922`, `record=0` on full corpus). Full DB size remained
**429.69 MB**.

**Decision:** **KEPT.** G1 reaches the startup stretch target on the
authoritative full corpus (`0.230s` median vs `300ms` stretch) and drives the
single-project subset to `83ms`. The measured warm-startup pass is a true
no-delta path: all `4548` files hit the scan cache, `prepare_plan` is
effectively zero, and startup opens from the existing snapshot immediately.

**Rationale:** This is the first experiment to directly solve the remaining
user-facing startup problem instead of chasing diminishing cold-import wins.
The perf log confirms the mechanism is working exactly as intended:
1. The first scan on a fresh corpus still parallelizes the miss path and
   repopulates cache rows.
2. The measured warm-startup pass avoids file reads entirely (`0` misses).
3. `scan_source` now costs ~180-200ms on the full corpus instead of multiple
   seconds.
4. The residual wall is now dominated by directory walk + cache reconciliation,
   not header parsing or plan preparation.

**Key finding #23:** A policy-aware persisted scan cache is enough to move
full-corpus warm startup into the stretch-target range without touching the
import schema or read path.

**Key finding #24:** After G1, warm startup is no longer bottlenecked by
`prepare_plan` or import work. The remaining steady-state cost is mostly the
directory walk plus reconciling the cache rows for unchanged files.

**Next implied:** **G4 finer-grained invalidation than `project x day` chunks**
if the user wants to optimize warm startup when *some* files changed. G1 solves
the no-delta warm-startup path; the remaining startup/product opportunity is to
avoid scheduling and replaying whole chunk days when only a small number of
source files changed. If behavior-changing work is out of scope, the perf
project can reasonably stop here.

### Phase 3 plan (2026-04-16)

This plan section captured the post-D-series experiment queue before E and F
were executed. As of the current session state, E, F, G2, and G3 have all been
tried and reverted. The remaining work in this document is primarily startup
oriented (G1/G4) unless the project chooses a behavior-changing cold-import
strategy.

**E1+E2+E3: Pragma hardening (untried, low risk)**
- `PRAGMA foreign_keys = OFF` on the import connection (set at connection
  open time, not mid-transaction — avoids the savepoint limitation that
  blocked D2). Eliminates ~2.5M FK verification btree lookups. Est. 2–4s.
- `PRAGMA locking_mode = EXCLUSIVE` on the import connection. Est. 0.3–0.5s.
- `PRAGMA wal_autocheckpoint = 0` + manual checkpoint post-import. Est. 0.3–0.5s.
- Total estimate: 2–4s. Zero schema or read-path changes.
- Also re-opens D2 (deferred message_part): the +71% regression was
  entirely FK verification cache misses. With FK=OFF, the D2 hypothesis
  can be tested cleanly.

**F2a: Parallel import → sequential merge (if E doesn't reach 10s)**
- Import: one temp SQLite per project, written in parallel via rayon.
- Merge: `ATTACH` + `INSERT INTO target SELECT * FROM shard.table` per
  shard. Sequential scan, no per-row Rust overhead.
- Read path unchanged (single production DB).
- Estimated: parallel_import ~3–4s + merge ~2–3s = ~5–7s write phase.
- Cross-project query audit completed: root browse and filter_options()
  are the only cross-project queries; all drill-down is per-project.
  `history_event` and `skill_invocation` lack project_id (needs schema
  fix if switching to permanent sharding, but irrelevant for F2a since
  the merge produces a single DB).
- ID conflict resolution: pre-assign non-overlapping rowid ranges per
  shard (shard N starts at N × 10_000_000). Zero post-merge fixup.

**G: Structural reductions (current status)**
- `scan_source` delta cache keyed by `(mtime, size, import schema version)`,
  plus parallel header extraction and memoized `cwd -> ResolvedProject`.
  Startup only, but stronger than "cache directory walk" alone.
- Inline rollup materialization during classification/write. **Tried and
  reverted in G3.** Distinct from D3: the goal was to eliminate the reread in
  `rollup.rs`, not defer it.
- Finer-grained invalidation than whole `project x day` chunks. This is
  mostly a warm-start product/architecture lever rather than a cold-import
  optimization.

See design doc Section 14 for full details.

### How to resume
1. `cd /home/ketan/project/gnomon`
2. Read this log's Phase Log and the RESUME HERE block.
3. Read design doc Section 14 for G1–G4 details.
4. Read the 2026-04-16 G4 entry above before changing code.
5. Only continue if the user explicitly wants behavior-changing follow-up work
   such as C3/background import or deferring heavy derived tables.

### Next-session prompt template

After reporting the result of any experiment, end the update with a
ready-to-paste prompt for the next fresh-session agent. Use this shape:

```text
Continue import-perf work in the linked worktree
/home/ketan/project/gnomon/.worktrees/import-perf on branch `import-perf`.

Before changing code, read:
1. docs/specs/2026-04-10-import-perf-log.md
2. docs/specs/2026-04-10-import-perf-design.md

The last completed experiment was <candidate name>. Result: <KEPT/REVERTED>
with <key metric/result>.

The next experiment to run is <candidate name> because <reason>.

After you run it, update the running log with hypothesis, measurements,
decision, rationale, and implied next candidate, then end your report with
an updated next-session prompt for the following phase of work.
```

## 2026-04-17 — candidate H1: defer startup action+path_ref tables until after open

**Branch:** `import-perf`
**Hypothesis:** If startup imports publish the changed startup chunk
immediately after normalize + turn persistence, then defer `build_actions`,
`message_path_ref`, and the action/path rollup rebuilds to a second background
pass, one-file `delta-startup` should improve materially without repeating
C3's "open on empty or stale snapshot before publish" behavior. Warm no-delta
startup should stay flat because it does no chunk work.

**Implementation:** Added a startup-only experimental path in
`crates/gnomon-core/src/import/chunk.rs`, gated by
`GNOMON_DEFER_STARTUP_DERIVED_TABLES=1`. Startup chunks now skip
`build_actions_in_tx_with_messages` during the initial import pass, publish as
`complete` after normalize/turns/count recomputation, record a
`pending_chunk_rebuild` row instead of rebuilding action/path rollups inline,
then run a second background pass labeled `finalizing startup derived data`
that rebuilds actions from persisted messages, rebuilds both rollup tables,
clears the pending row, and republishes the chunk with a new `publish_seq`.

A focused regression test,
`startup_import_can_defer_startup_derived_tables_until_after_open`, verifies
the observable behavior: the startup-open snapshot has zero action/path-derived
rows for the changed chunk, and the background pass restores them afterward.

**Measurements:**

Full corpus, `delta-startup`, 3 repeats, `GNOMON_DEFER_STARTUP_DERIVED_TABLES=1`:

| run | wall |
| --- | ---: |
| 1 | 0.484s |
| 2 | 0.428s |
| 3 | 0.393s |
| **median** | **0.428s** |

Comparison point: current kept product-preserving delta-startup result is G4 at
**0.541s median** on the full corpus. H1 improved initial open by **~113ms
(-21%)**.

Subset, `delta-startup`, 3 repeats:

| run | wall |
| --- | ---: |
| 1 | 0.117s |
| 2 | 0.138s |
| 3 | 0.142s |
| **median** | **0.138s** |

Comparison point: current kept subset delta-startup result is G4 at **0.244s
median**. H1 improved the subset by **~106ms (-43%)**.

Full corpus, `warm-startup`, 3 repeats:

| run | wall |
| --- | ---: |
| 1 | 0.221s |
| 2 | 0.236s |
| 3 | 0.231s |
| **median** | **0.231s** |

Warm no-delta startup remained flat vs. G1/G4's existing **0.230s median**.

Perf-log follow-up (`delta-startup`, full corpus, 1 repeat):
- wall: **0.400s**
- `import.scan_source` total: **553.713ms** across prefill + measured scan
- measured `import.rebuild_deferred_chunk_derived_tables`: **2.195s**
- measured `import.build_actions` inside the deferred pass: **4.317s** total
  across the prefill import plus the one changed-chunk rebuild

Row parity after the background pass: **PASS**
- full-corpus measured DB row counts still converged to
  `project=31`, `source_file=4548`, `import_chunk=163`, `conversation=4547`,
  `stream=4547`, `record=0`, `message=294995`, `message_part=411842`,
  `turn=13363`, `action=120922`
- targeted regression test confirms `pending_chunk_rebuild` is empty after the
  second pass

**Decision:** REVERTED for product semantics. The startup-open metric improved
meaningfully, but the opened snapshot is only authoritative for raw transcript
tables. Action-derived and path-derived views for the changed startup chunk are
temporarily empty/stale until the second pass republishes the chunk.

**Rationale:** H1 is a strictly narrower trade than C3:
- unlike C3, the UI opens only after a fresh startup chunk publish, so the user
  does not land on an entirely empty or globally stale snapshot
- unlike G4, the initial opened snapshot is still internally inconsistent:
  message/turn tables are fresh, but action/path-derived queries lag behind

That mismatch is material enough that the experiment needs explicit product
approval before it could be kept. The measured win is real, but it is not a
free optimization.

**Key finding #29:** Deferring startup-only `build_actions` +
`message_path_ref` + rollup rebuilds can cut authoritative full-corpus
one-file `delta-startup` from **0.541s → 0.428s median** without regressing the
warm no-delta case.

**Key finding #30:** That H1 win comes from publishing a partially-derived
startup snapshot: raw transcript state is fresh at open, but action/path views
for the changed chunk are only repaired in a second background publish roughly
**2.2s** later on the benchmark host.

**Next implied:** If startup-perf exploration continues, the remaining choices
are all explicit trade-offs:
- **Hybrid warm-only streaming gate** if the user wants to keep pushing startup
  open time with controlled snapshot staleness rules
- **simd-json** only if the goal is to exhaust the last low-confidence
  product-preserving micro-optimization, with the expectation of a sub-100ms
  win at best

## 2026-04-17 — candidate H2: hybrid warm-only streaming gate

**Branch:** `import-perf`
**Hypothesis:** If startup keeps the existing published snapshot visible and
returns immediately only when that snapshot already has imported coverage,
one-file `delta-startup` should fall toward the C3 ceiling without repeating
the empty-DB failure mode. Warm no-delta startup should remain effectively
flat, and bootstrap startup should still wait for the first startup-window
publish because there is no visible data to stream from.

**Implementation:** Added a second startup-only experimental gate in
`crates/gnomon-core/src/import/chunk.rs`, exposed via
`GNOMON_WARM_ONLY_STREAMING_GATE=1`. `start_startup_import_with_options()` now
captures the pre-worker snapshot before spawning the background importer. In
`RecentFirst` mode it returns immediately with
`StartupOpenReason::WarmSnapshotReady` only when that pre-worker snapshot has
visible imported coverage (`session_count > 0 || turn_count > 0`) and there is
startup work pending. Bootstrap startup still waits on the normal readiness
signal. Added focused regression tests:
- `warm_only_streaming_gate_opens_on_existing_snapshot_while_startup_import_continues`
- `warm_only_streaming_gate_does_not_open_bootstrap_startup_early`
- `cargo test -p gnomon-tui warm_snapshot_streaming`

**Measurements:**

Full corpus, `warm-startup`, 3 repeats, `GNOMON_WARM_ONLY_STREAMING_GATE=1`:

| run | wall |
| --- | ---: |
| 1 | 0.231s |
| 2 | 0.243s |
| 3 | 0.221s |
| **median** | **0.231s** |

Comparison point: current kept product-preserving full-corpus
`warm-startup` remains G1/G4 at **0.230s median**. H2 is effectively flat in
the no-delta case because there is no startup work to hide after the prefill.

Full corpus, `delta-startup`, 3 repeats:

| run | wall |
| --- | ---: |
| 1 | 0.212s |
| 2 | 0.227s |
| 3 | 0.236s |
| **median** | **0.227s** |

Comparison point: current kept product-preserving full-corpus
`delta-startup` result is G4 at **0.541s median**. H2 improved initial open by
**~314ms (-58%)**. It also beat H1's partially-derived snapshot result
(**0.428s median**) because it no longer waits for any changed startup chunk to
publish before opening.

Subset, `warm-startup`, 3 repeats:

| run | wall |
| --- | ---: |
| 1 | 0.155s |
| 2 | 0.096s |
| 3 | 0.095s |
| **median** | **0.096s** |

Comparison point: current kept subset `warm-startup` is G1 at **0.083s
median**. H2 stayed in the same range, with the small regression consistent
with run-to-run noise on the subset harness.

Subset, `delta-startup`, 3 repeats:

| run | wall |
| --- | ---: |
| 1 | 0.099s |
| 2 | 0.083s |
| 3 | 0.087s |
| **median** | **0.087s** |

Comparison point: current kept subset `delta-startup` result is G4 at
**0.244s median**. H2 improved initial open by **~157ms (-64%)**.

Observed snapshot shape at benchmark exit:
- full-corpus `delta-startup` still showed the prefill-visible data shape at
  open time: `project=31`, `source_file=4548`, `import_chunk=163`,
  `conversation=4547`, `stream=4547`, `record=0`, `message=294995`,
  `message_part=411842`, `turn=13363`, `action=120922`
- subset `delta-startup` likewise stayed on the prefill-visible snapshot:
  `project=1`, `source_file=1649`, `import_chunk=36`, `conversation=1648`,
  `stream=1648`, `record=0`, `message=130478`, `message_part=179412`,
  `turn=4915`, `action=50463`
- on 2026-04-17, empty-DB `startup` against the frozen corpus no longer
  exercised a meaningful cold-start window because the corpus had aged out of
  the 24-hour startup range. The bootstrap guard is covered instead by the
  focused regression test above.

**Decision:** REVERTED for product semantics. H2 is the fastest startup-open
trade explored so far on a warm corpus, but it intentionally opens on a stale
previous snapshot and leaves the just-changed startup chunk invisible until the
background import finishes and the user refreshes.

**Rationale:** H2 is the broadest "open sooner" trade that still avoids C3's
empty bootstrap failure:
- unlike C3, bootstrap startup does not open on an empty view; it still waits
  for the first startup-window publish when there is no visible data yet
- unlike H1, H2 does not open on a partially-derived changed chunk; it opens on
  the fully authoritative previous snapshot
- unlike G4, the opened snapshot is knowingly stale for the changed startup
  chunk, so the improvement is purchased entirely by relaxing freshness at open

That is still a product decision, not a free optimization. The measured win is
real, but it comes from serving the previous snapshot longer.

**Key finding #31:** A warm-only streaming gate can cut authoritative
full-corpus one-file `delta-startup` from **0.541s → 0.227s median** by
opening on the last published snapshot while the changed startup chunk imports
in the background.

**Key finding #32:** The hybrid gate avoids C3's empty-bootstrap failure, but
the price is explicit staleness: the opened snapshot remains fully authoritative
for the previous publish, not for the just-changed startup window, until the
user refreshes after background import completes.

**Next implied:** Startup-perf exploration is functionally exhausted unless the
user wants either:
- **simd-json** as the last product-preserving micro-experiment, with an
  expected ceiling below **100ms**
- **stop here**, because G1/G4 already beat the warm-start targets without
  changing startup semantics

### Next-session prompt for the current state

```text
Continue import-perf work in the linked worktree
/home/ketan/project/gnomon/.worktrees/import-perf on branch `import-perf`.

Before changing code, read:
1. docs/specs/2026-04-10-import-perf-log.md
2. docs/specs/2026-04-10-import-perf-design.md

The last completed experiment was H2 hybrid warm-only streaming gate.
Result: REVERTED. It improved authoritative full-corpus one-file
`delta-startup` wall time to `0.227s` median from G4's `0.541s`, and
left warm no-delta startup effectively flat at `0.231s` median, but
only by opening on the last published snapshot while the changed
startup chunk remained stale until background import completed and the
user refreshed.

The next experiment to run, if startup-perf exploration continues and
must stay product-preserving, is `simd-json`; expected upside is below
`100ms`. Otherwise stop the startup exploration here and treat G1/G4 as
the kept endpoint.

After you run it, update the running log with hypothesis, measurements,
decision, rationale, and implied next candidate, then end your report with
an updated next-session prompt for the following phase of work.
```

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
| E | Pragma hardening (`foreign_keys=OFF`, `locking_mode=EXCLUSIVE`, `wal_autocheckpoint=0`) | **REVERTED** | 27.1s median on full corpus; no repeatable win |
| T | Timing coverage hardening | **KEPT** | attributed checkpoint/savepoint/parse costs; no product behavior change |
| F | Parallel import -> sequential merge | **REVERTED** | 24.563s full vs 20.599s serial; subset win did not scale |
| G2 | Path-node memoization | **REVERTED** | 24.553s full vs 20.599s serial; subset win did not scale |
| G3 | Inline rollup materialization | **REVERTED** | 26.573s full vs 20.599s serial; removing the reread cost more than it saved |
| G1 | Scan-source delta cache + parallel header extraction | **KEPT** | warm startup 0.230s median full / 0.083s median subset |
| G4 | File-granular invalidation within day chunks | **KEPT** | delta-startup 0.541s median full / 0.244s median subset |
| C3 | Background streaming import | **REVERTED** | startup-open 0.613s median full / delta-startup 0.242s median full, but visible snapshot can be empty/stale |
| H1 | Defer startup action+path_ref tables until after open | **REVERTED** | delta-startup 0.428s median full / 0.138s median subset, but action/path views lag the opened snapshot |
| H2 | Hybrid warm-only streaming gate | **REVERTED** | delta-startup 0.227s median full / 0.087s median subset, but the opened snapshot stays on the previous publish until refresh |

### Current best metrics (post G4)
| metric | value | vs original baseline | vs target |
| --- | ---: | ---: | ---: |
| Cold full import (session-local best) | **20.599s** | ~84% from 126.1s | ~2.1× to 10s target |
| Warm startup (authoritative full corpus median) | **0.230s** | ~95% from ~5.1s empty-DB baseline | **beats 300ms stretch** |
| Warm startup, one-file delta (authoritative full corpus median) | **0.541s** | ~89% from ~5.1s empty-DB baseline | **beats <1s target** |

### Current phase distribution (full corpus, ~23s wall)

| phase | time | % of wall | note |
| --- | ---: | ---: | --- |
| normalize_jsonl | ~8.0s | ~34% | messages + parts INSERTs (295K + 412K) |
| build_actions | ~4.6s | ~20% | ~300ms classify CPU + ~4.3s DB persist |
| parse (rayon CPU) | ~2.9s | ~12% | parallel JSON parse, multi-core |
| scan_source | ~3.0s | ~13% | directory walk + VCS resolution |
| finalize_chunk + rollups | ~3.6s | ~15% | path rollup dominant |
| build_turns | ~1.1s | ~5% | in-memory + DB persist |
| uninstrumented | ~0.4s | ~2% | overhead |

### Key findings (cumulative)
1. **Btree pages are cached.** With 64MB page cache + WAL, secondary index maintenance during inserts is essentially free.
2. **The remaining SQL cost is core btree insert work** — CPU-bound, not I/O-bound.
3. **In-memory data passing saves ~2s in build_actions + build_turns** but the Vec assembly overhead partially offsets gains. Primary value: **architectural prerequisite for parallelism**.
4. **`RETURNING id` is expensive on high-frequency INSERTs.** Replacing with `execute` + `last_insert_rowid()` saved ~3.5s (−15%) across 416K+ inserts.
5. **Session-to-session variance is high (~30%)** on WSL2. Within-session relative comparisons are reliable.
6. **`classify_message()` CPU is ~300ms total.** Parallelizing classification alone yields no measurable improvement.
7. **Multi-row VALUES batching loses to `prepare_cached` single-row inserts** when batch sizes are small (~1.4 parts/message) and dynamic SQL prevents statement caching.
8. **In-memory staging does not help** when the DB is on tmpfs and the bottleneck is CPU-bound btree work. VACUUM INTO adds 1.5s overhead.
9. **Subset locality can still lie.** Both F2a and G2 improved the single-
project subset while regressing the authoritative full-corpus run.
10. **Inline rollup materialization also loses.** The hot-cache rollup reread is cheaper than carrying per-message and per-path aggregation inside the write path.
11. **Warm startup is solved for the no-delta case.** G1 reduced authoritative
full-corpus warm startup to ~230ms with 4548/4548 scan-cache hits.
12. **Changed-file warm startup is also solved at the structural level.** G4 reduced authoritative full-corpus one-file delta startup to ~541ms median without changing row parity.
13. **Deferring startup-only derived tables works, but it changes what "ready"
means.** H1 improved one-file delta startup to ~428ms median, but only by
letting the opened snapshot lag on action/path-derived views until a second
publish finishes ~2.2s later.
14. **Warm-only streaming is the fastest startup-open trade tried.** H2 reduced
full-corpus one-file delta startup to ~227ms median, but only by keeping the
previous publish visible until the user refreshes.

### 10s target assessment

The 10s target is **not achievable with SQLite** as the storage engine for this
workload. The analysis:

- DB write phases total ~17.2s, all CPU-bound btree work
- Non-DB phases (parse + scan_source + overhead) total ~6.3s, setting the floor
- Even zero-cost writes would give ~6.3s — barely under 10s
- Iterations 10-13 plus G2/G3 exhausted the remaining product-preserving cold-import candidates
- The `prepare_cached` single-row insert pattern is near-optimal for SQLite

Reaching 10s would require a fundamentally different storage approach (DuckDB,
columnar store) or deferring heavy tables (action, path_ref, rollups) to
background processing after TUI opens.

### Remaining unexplored candidates

| rank | candidate | est. win | confidence | note |
| --- | --- | --- | --- | --- |
| 1 | **Cold-import architecture beyond the current SQLite write path** | multi-second cold only | medium | required if the 10s cold-import target remains active |
| 2 | **Explicit startup-semantic tradeoffs** | sub-0.25s startup-open | medium | already demonstrated by C3/H1/H2, but changes what "ready" means |
| 3 | **Stop here** | — | high | G1/G4 already beat the warm-start targets without changing startup semantics |

### Startup close-out

Product-preserving startup exploration is complete.

- **Kept endpoint:** G1 + G4
- **Authoritative full-corpus medians:** `warm-startup = 0.230s`,
  `delta-startup = 0.541s`
- **Stretch/acceptable targets:** both satisfied without changing startup
  freshness semantics
- **Terminating experiment:** S1 `simd-json` line parsing, **REVERTED**

The branch now has a clear boundary:

- Do not run more startup-preserving parser or manifest micro-experiments
  unless new evidence materially changes the parse ceiling.
- Treat C3/H1/H2 as explored product trades, not as hidden optimizations still
  waiting to be productized.
- Treat the remaining open question as broader import-perf strategy, not
  startup tuning.

### Bench harness
```bash
cargo build -p gnomon-core --example import_bench --release

# Subset cold full import (~9s)
cargo run -p gnomon-core --example import_bench --release -- \
  --corpus subset --mode full --repeats 3

# Full corpus cold full import (~20-23s)
cargo run -p gnomon-core --example import_bench --release -- \
  --corpus full --mode full --repeats 3

# Full corpus warm startup (~0.23s measured pass after a one-time prefill)
cargo run -p gnomon-core --example import_bench --release -- \
  --corpus full --mode warm-startup --repeats 3

# Full corpus one-file delta startup (~0.45-0.55s median after one measured mutation)
cargo run -p gnomon-core --example import_bench --release -- \
  --corpus full --mode delta-startup --repeats 3
```

### Session-resumption sanity check
```bash
cd /home/ketan/project/gnomon
git log --oneline -5
cargo run -p gnomon-core --example import_bench --release -- \
  --corpus subset --mode warm-startup --repeats 1
# Measured pass should complete in ~0.08-0.09s after the prefill

cargo run -p gnomon-core --example import_bench --release -- \
  --corpus subset --mode delta-startup --repeats 1
# Measured pass should complete in ~0.16-0.39s after the prefill + one-file mtime bump
```

### 2026-04-17 — Experiment S1: `simd-json` line parsing

**Hypothesis:** Replace the hot JSONL `serde_json::from_str()` boundaries with
`simd-json` while preserving the same importer semantics and row shape. The
expected ceiling was small: below **100ms** on startup paths and below **1s**
on cold full import, because parse already sits below the dominant SQLite write
costs.

**Implementation tried:** Added `simd-json 0.17.0` and changed the source-scan
header extraction plus transcript/history JSONL normalization loops to read
mutable byte lines via `BufRead::read_until(b'\n', ...)`, then deserialize via
`simd_json::serde::from_slice()` into the existing `serde_json::Value` and
`SourceRecordHeader` shapes.

**Measurements:** full corpus, release build, 3 repeats each.

Warm `warm-startup`:

| run | wall |
| --- | ---: |
| 1 | 0.262s |
| 2 | 0.280s |
| 3 | 0.286s |
| **median** | **0.280s** |

Comparison point: kept G1/G4 warm startup is **0.230s median**. `simd-json`
was **~22% slower**.

Warm `delta-startup`:

| run | wall |
| --- | ---: |
| 1 | 0.602s |
| 2 | 0.665s |
| 3 | 1.974s |
| **median** | **0.665s** |

Comparison point: kept G4 one-file delta startup is **0.541s median**.
`simd-json` was **~23% slower**, with one severe outlier run.

Cold full import:

| run | wall |
| --- | ---: |
| 1 | 35.143s |
| 2 | 27.229s |
| 3 | 24.880s |
| **median** | **27.229s** |

Comparison point: current kept cold full-import result is **20.599s**.
`simd-json` was **~32% slower**.

Row counts remained unchanged on the measured runs:
- warm startup: `project=31`, `source_file=4548`, `import_chunk=162`,
  `conversation=4547`, `stream=4547`, `record=0`, `message=294995`,
  `message_part=411842`, `turn=13363`, `action=120922`
- delta startup: `project=31`, `source_file=4548`, `import_chunk=163`,
  `conversation=4547`, `stream=4547`, `record=0`, `message=294995`,
  `message_part=411842`, `turn=13363`, `action=120922`
- cold full import: `project=31`, `source_file=4548`, `import_chunk=162`,
  `conversation=4547`, `stream=4547`, `record=0`, `message=294995`,
  `message_part=411842`, `turn=13363`, `action=120922`

**Decision:** REVERTED.

**Rationale:** This workload does not benefit from the `simd-json` swap in its
current form. Deserializing through `simd_json::serde` into the existing
`serde_json::Value` contract plus the byte-oriented line handling was slower
than the baseline `serde_json::from_str()` path, and the parse phase is too
small a share of the remaining wall time to justify further product-preserving
parser micro-tuning.

**Implied next candidate:** Stop the startup exploration here and treat
**G1/G4** as the kept endpoint. If import-perf work continues, the next phase
should shift to a broader decision surface than startup-preserving parser work:
either close out the startup findings and move to cold-import architecture
choices, or explicitly revisit product-semantic tradeoffs such as deferred
derived tables / stale-at-open behavior.

**Validation after revert:**
- `cargo fmt --all`
- `cargo test -p gnomon-core`

### Next-session prompt for the following phase

```text
Continue import-perf work in the linked worktree
/home/ketan/project/gnomon/.worktrees/import-perf on branch `import-perf`.

Before changing code, read:
1. docs/specs/2026-04-10-import-perf-log.md
2. docs/specs/2026-04-10-import-perf-design.md

The last completed experiment was S1 `simd-json` line parsing.
Result: REVERTED. On the authoritative full-corpus harness it regressed all
measured modes versus the kept G1/G4 state:
- `warm-startup`: `0.280s` median vs `0.230s`
- `delta-startup`: `0.665s` median vs `0.541s`
- cold full import: `27.229s` median vs `20.599s`

Startup exploration should stop here. Treat G1/G4 as the kept endpoint for
product-preserving startup work.

The next phase is to close out startup exploration and decide what broader
import-perf work is worth doing next. Focus on one of:
1. documenting the final startup findings and tightening the design/log summary
   around G1/G4 as the endpoint, or
2. evaluating broader post-startup directions such as cold-import architecture
   changes beyond SQLite, or explicitly product-semantic tradeoffs that were
   previously rejected for startup freshness reasons.

Do not run more startup-preserving parser micro-experiments unless new evidence
changes the parse ceiling materially.
```
