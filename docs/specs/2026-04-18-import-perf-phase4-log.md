# Import Performance — Phase 4 Running Log

**Plan:** `docs/specs/2026-04-18-import-perf-phase4-plan.md`

---

## Frozen Header

| field | value |
| --- | --- |
| Corpus capture date | 2026-04-11T03:37:18Z |
| full.tar.zst SHA | 5784682a90da345adc90beb5ce13fdb0d10a030de412b63aa3152c0c01c37b30 |
| subset.tar.zst SHA | 053ea32815905ee3936e9e596a7e759713defb198257740ad9e45c44ec35ed89 |
| Host | ketan WSL2 (Linux pontoon 6.6.87.2-microsoft-standard-WSL2) |
| CPU | AMD Ryzen 5 5600X 6-Core Processor, 6 cores × 2 threads = 12 CPUs |
| Rust toolchain | rustc 1.93.1 (01f6ddf75 2026-02-11) |
| SQLite version | 3.46.0 (bundled via libsqlite3-sys 0.30.1) |
| Baseline full corpus | 18.969s median (runs: 17.585, 18.969, 21.963) |
| Baseline subset | 8.487s median (runs: 8.105, 8.412, 8.487, 8.722, 8.957) |
| Target | ≤10s full corpus |
| Phase 3 kept changes | G1 (scan_source delta cache), G4 (file-granular invalidation), D1b (preset turn_id) |

---

## Phase Log

*(Append-only. One entry per candidate.)*

---

## 2026-04-18 — candidate A3: `PRAGMA foreign_keys = OFF` on import connection

Branch: import-perf-p4-a3
Worktree: .worktrees/import-perf-p4-a3
Hypothesis: FK verification adds ~2.5M btree lookups per full import. The import pipeline
inserts in guaranteed parent-before-child order, so FK enforcement is redundant overhead.
Disabling it on the import connection alone (not the read-write connection) should eliminate
those lookups with zero correctness risk.
Implementation: Added `Database::open_for_import` in `crates/gnomon-core/src/db/mod.rs` (calls
new `configure_import_connection` which uses `PRAGMA foreign_keys = OFF` instead of `= ON`).
Changed both import entrypoints in `import/chunk.rs` (lines 336, 964) to use `open_for_import`.
Also added `gnomon db check` subcommand (PRAGMA integrity_check + foreign_key_check).
Fixed 3 pre-existing clippy warnings (unused import, dead code, clone-to-from_ref).
Measurements:
  Subset:       8.487s → 6.543s (−22.9%); runs: 6.543, 6.644, 6.723, 6.501, 6.263
  Full:         18.969s → 17.982s (−5.2%); runs: 16.344, 17.982, 18.063
  Row parity:   PASS (project:31, source_file:4548, import_chunk:162, conversation:4547,
                stream:4547, message:294995, message_part:411842, turn:13363, action:120922)
  Profile shift: subset wall dropped ~1.9s; full corpus ~1.0s
Decision: KEPT
Commit: 0a0a6ce
Key finding: FK enforcement was significant per-row overhead — disabling it on the import
connection (guaranteed parent-before-child order) gives a clean win with no correctness risk.
Subset improvement (~23%) is larger than full corpus (~5%) likely because the subset's single
large project has denser FK relationships per chunk than the 31-project full corpus.
Also discovered and documented: integration tests have 6 pre-existing failures
(imported_record_count_sum = 0) because recompute_chunk_counts uses COUNT(*) FROM history_event,
which is 0 for transcript imports — separate pre-existing bug, not caused by this change.
Next implied: A4 (PRAGMA locking_mode = EXCLUSIVE alone) — further decompose E-bundle.

---

## 2026-04-18 — candidate A4: `PRAGMA locking_mode = EXCLUSIVE` on import connection

Branch: import-perf-p4-a4
Worktree: .worktrees/import-perf-p4-a4
Hypothesis: The import connection is the sole writer during bulk import. EXCLUSIVE mode avoids
re-acquiring and releasing shared locks on every read/write operation. Tested previously only as
part of the E-bundle (which regressed overall); this isolates the pragma to measure its individual
contribution.
Implementation: Add `PRAGMA locking_mode = EXCLUSIVE;` to `configure_import_connection` in
`crates/gnomon-core/src/db/mod.rs`, alongside the existing `foreign_keys = OFF`.
Measurements:
  Subset:       6.543s → ~175s (catastrophic regression — 26× slower); 3 of 3 observed runs
                showed 0 rows for conversation/message/turn/action (all 35 chunks failed);
                benchmark terminated after 3 iterations
  Full:         not measured — subset result conclusive
  Row parity:   FAIL (all deferred chunks produced 0 rows; import_chunk:35 written but empty)
  Profile shift: N/A — pipeline deadlocked, not just slow
Decision: REVERTED
Commit:
Key finding: EXCLUSIVE locking mode is incompatible with the current import architecture.
The pipeline opens multiple SQLite connections with overlapping lifetimes per chunk (import
connection + at least one other reader/writer). EXCLUSIVE mode holds file locks indefinitely
after first write, so subsequent connection attempts time out. busy_timeout × 35 chunks ≈ 175s
exactly matches the observed wall time. To use EXCLUSIVE mode, the architecture would need to
ensure a single connection owns the DB for the entire import duration — a more invasive change
than a one-line pragma addition.
Next implied: A5 (PRAGMA wal_autocheckpoint = 0 + manual checkpoint) — next E-bundle decompose.

---

## 2026-04-19 — candidate A5: `PRAGMA wal_autocheckpoint = 0` + manual checkpoint

Branch: import-perf-p4-a5
Worktree: .worktrees/import-perf-p4-a5
Hypothesis: Automatic WAL checkpointing fires mid-import (after every 1000 pages by default),
causing checkpoint stalls while 35 concurrent import connections are active. Disabling autocheckpoint
(setting it to 0) defers all WAL consolidation until after all chunks complete, then a single
`PRAGMA wal_checkpoint(TRUNCATE)` consolidates the WAL. This eliminates mid-import checkpoint
contention and avoids WAL reader-writer conflicts. Tested previously only as part of the E-bundle
(which regressed overall because of EXCLUSIVE mode, not this pragma); this isolates A5 individually.
Implementation: Added `PRAGMA wal_autocheckpoint = 0;` to `configure_import_connection` in
`crates/gnomon-core/src/db/mod.rs`. Added `PRAGMA wal_checkpoint(TRUNCATE);` call via
`database.connection().execute_batch(...)` in `import_all_with_perf_logger` after all deferred
chunks complete, instrumented with a `PerfScope`. Used `options.perf_logger.clone()` since
`perf_logger` was already moved into `ImportWorkerOptions`.
Measurements:
  Subset:       6.543s → 10.967s median (+67.6%); runs: 11.922, 10.967, 12.372, 9.656, 8.276
  Full:         not measured — subset regression conclusive
  Row parity:   not checked — reverted before parity run
  Profile shift: N/A — regression, not improvement
Decision: REVERTED
Commit:
Key finding: WAL autocheckpointing during import is not the bottleneck — it helps by keeping the
WAL small and distributing checkpoint I/O across 35 chunks. Disabling it causes the WAL to grow
to ~full-DB size (~150MB) across all 35 chunks, then a single TRUNCATE checkpoint must write the
entire WAL back to the database file in one shot. This serialized I/O at the end is significantly
more expensive than the distributed autocheckpoints. The E-bundle regression was driven by EXCLUSIVE
mode (A4), not this pragma. This pragma alone makes things worse.
Next implied: A1 (SQLite page size 8K/16K) — never measured, low risk, moderate expected gain.

---

## 2026-04-19 — candidate A1: SQLite page size 8K

Branch: import-perf-p4-a1
Worktree: .worktrees/import-perf-p4-a1
Hypothesis: Default SQLite page size is 4096 bytes. Larger pages (8192) reduce btree depth
(fewer levels per lookup) and amortize page-header overhead across more rows per page. The
import workload has large sequential inserts — larger pages reduce page splits and internal
node updates. The bench harness creates a fresh DB per run so PRAGMA page_size takes effect.
Implementation: Add `PRAGMA page_size = 8192;` as the first pragma in `configure_import_connection`
(and `configure_read_write_connection` for consistency) in `crates/gnomon-core/src/db/mod.rs`,
before `PRAGMA journal_mode = WAL` so it precedes any writes.
Measurements:
  Subset:       6.543s → 8.312s median (+27.1%); runs: 8.603, 8.461, 8.258, 7.069, 8.312
  Full:         not measured — subset regression conclusive
  Row parity:   not checked — reverted before parity run
  Profile shift: db size with 8K pages: 155.74 MB (baseline not recorded; likely similar)
Decision: REVERTED
Commit:
Key finding: Larger page size (8192) regresses import throughput by ~27% on subset. Larger
pages mean each WAL page write doubles in size (8K vs 4K); the workload's dense sequential
inserts do not produce enough btree depth savings to overcome the increased per-page I/O cost.
The btree depth reduction benefit requires wide, sparse lookups — the import path's sequential
inserts with FK=OFF don't traverse the tree deeply enough to benefit. This is consistent with
the CPU-floor diagnostic (D0): the bottleneck is raw write I/O, not lookup depth.
Next implied: B1 (`:memory:` staging DB → `VACUUM INTO`) — biggest single-candidate potential,
eliminates WAL overhead entirely for the write phase.

---

## 2026-04-19 — candidate B1: `:memory:` staging DB → `VACUUM INTO`

Branch: import-perf-p4-b1
Worktree: .worktrees/import-perf-p4-b1
Hypothesis: All chunk writes go to an in-process `:memory:` SQLite to eliminate WAL overhead.
Measurements:
  Subset:       6.543s → ~24.6s single run (+276% catastrophic regression)
  Full:         not measured
  Row parity:   not checked — reverted
Decision: REVERTED
Key finding: SQLite `:memory:` DB is 4× SLOWER than WAL-backed disk with mmap for this workload.
  Root causes: (1) `:memory:` uses MEMORY journal mode (WAL rejected) — copies old page before
  each modification vs WAL's append-only writes. (2) Pages allocated as separate malloc() buffers:
  cache-hostile random access vs mmap'd contiguous memory. (3) Baseline's mmap_size=256MB gives
  "in-memory" read performance via OS page cache — same benefit without the write regression.
  B2 (parallel memory DBs) also discarded — same root cause.
Next implied: A6 (struct-based serde) — parse phase 5s/run, 2–4× speedup possible.

---

## 2026-04-19 — candidate A6: struct-based serde deserialization (replace `serde_json::Value`)

Branch: import-perf-p4-a6
Worktree: .worktrees/import-perf-p4-a6
Hypothesis: `parse_jsonl_file_inner` deserializes each transcript JSONL line into a fully general
`serde_json::Value` tree, allocating a HashMap per JSON object and a heap `String` per string field,
including for unknown fields (`cwd`, `userType`, `version`, `gitBranch`, `parentUuid`) that are
never read. Replacing the top-level record and the `message` sub-object with typed Rust structs
(`RawSourceRecord`, `RawMessage`) will skip unknown-field allocations entirely and eliminate two
HashMap allocations per message line (~295k messages × 2 = ~590k HashMaps). The `content` field
stays as `Value` since content parts have variable schema.
Implementation: Add `RawSourceRecord` + `RawMessage` + `RawSnapshot` in `normalize.rs`. Update
`parse_jsonl_file_inner` to deserialize via `RawSourceRecord`. Update `extract_message` and helpers
to take `&RawSourceRecord`. Update `ParsedRecord` (in `mod.rs`) to carry pre-extracted scalar fields
instead of `serde_json::Value`. Update write phase to use pre-extracted fields.
Measurements:
  Subset:       <pending>
  Full:         <pending>
  Row parity:   <pending>
  Profile shift: <pending>
Decision: PENDING
Commit:
Key finding: <pending>
Next implied: <pending>

---

## RESUME HERE

Phase: Phase 4
Long-lived branch: `import-perf-p4`
Long-lived worktree: `.worktrees/import-perf-p4`
Last completed: B1 — REVERTED (+276% regression on subset; `:memory:` DB 4× slower than WAL+mmap)
Next action: Run candidate A6: struct-based serde (replace `serde_json::Value` in normalize.rs).
  Parse phase is ~5s/run (~19% of total wall). A typed RawSourceRecord struct should cut it 2–4×.
  Create branch import-perf-p4-a6 and worktree .worktrees/import-perf-p4-a6 from import-perf-p4.
Current best (subset): 6.543s median (−22.9% from 8.487s baseline)
Current best (full): 17.982s median (−5.2% from 18.969s baseline)
Target: 10s full corpus
In-flight uncommitted state: none

Candidate ranking (live — re-rank after each result):
1. A6 — Struct-based serde (replace `serde_json::Value`) — parse phase 5s/run, 2–4× speedup possible
2. A2 — LTO + PGO — free binary-level gain (5–15% total wall)
3. A7 — `jwalk` parallel directory walk — scan_source reduction (0.2–0.5s)
4. A8 — path_node chunk-level cache (across files in chunk) — classify phase reduction (0.3–1.0s)
5. C1 — Per-project sharding + global metadata DB (F2c) — architectural ceiling-breaker
NOTE: B1 (`:memory:` staging), B2 (parallel memory DBs) — DISCARD. In-memory SQLite is slower
  than WAL+mmap due to MEMORY journal mode + malloc page fragmentation. A1 (page_size 8K),
  A4 (EXCLUSIVE locking), A5 (wal_autocheckpoint=0) also removed — all counterproductive.
