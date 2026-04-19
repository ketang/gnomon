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

## RESUME HERE

Phase: Phase 4
Long-lived branch: `import-perf-p4`
Long-lived worktree: `.worktrees/import-perf-p4`
Last completed: A3 — KEPT (0a0a6ce)
Next action: Run candidate A4: PRAGMA locking_mode = EXCLUSIVE on import connection alone.
Current best (subset): 6.543s median (−22.9% from 8.487s baseline)
Current best (full): 17.982s median (−5.2% from 18.969s baseline)
Target: 10s full corpus
In-flight uncommitted state: none

Candidate ranking (live — re-rank after each result):
1. A4 — `PRAGMA locking_mode = EXCLUSIVE` alone — decompose E-bundle, untested solo
2. A5 — `PRAGMA wal_autocheckpoint = 0` + manual checkpoint — decompose E-bundle
3. A1 — SQLite page size 8K/16K — never measured, low risk
4. B1 — `:memory:` staging DB → `VACUUM INTO` — biggest potential single-candidate win
5. A6 — Struct-based serde (replace `serde_json::Value`) — parse phase reduction
6. A2 — LTO + PGO — free binary-level gain
7. A7 — `jwalk` parallel directory walk — scan_source reduction
8. A8 — path_node chunk-level cache (across files in chunk) — classify phase reduction
9. B2 — Parallel per-chunk `:memory:` DBs + merge (requires B1 KEPT first)
10. C1 — Per-project sharding + global metadata DB (F2c) — architectural ceiling-breaker
