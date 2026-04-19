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

Branch: import-perf-p4/a3-fk-off
Worktree: .worktrees/import-perf-p4-a3
Hypothesis: FK verification adds ~2.5M btree lookups per full import. The import pipeline
inserts in guaranteed parent-before-child order, so FK enforcement is redundant overhead.
Disabling it on the import connection alone (not the read-write connection) should eliminate
those lookups with zero correctness risk.
Implementation: Split `configure_read_write_connection` in `crates/gnomon-core/src/db/mod.rs`
into two functions: the existing one (no change to FK behavior) and a new
`configure_import_connection` that additionally executes `PRAGMA foreign_keys = OFF;`.
Measurements:
  Subset:       *(pending)*
  Full:         *(pending)*
  Row parity:   *(pending)*
  Profile shift: *(pending)*
Decision: PENDING
Commit: *(pending)*
Key finding: *(pending)*
Next implied: *(pending)*

---

## RESUME HERE

Phase: Phase 4
Long-lived branch: `import-perf-p4`
Long-lived worktree: `.worktrees/import-perf-p4`
Last completed: *(none — A3 in flight)*
Next action: Implement A3 in `.worktrees/import-perf-p4-a3`, measure, verify parity, decide.
Current best (subset): 8.487s median (Phase 4 baseline)
Current best (full): 18.969s median (Phase 4 baseline)
Target: 10s full corpus
In-flight uncommitted state: A3 skeleton committed to import-perf-p4; candidate branch + worktree not yet created

Candidate ranking (live — re-rank after each result):
1. A3 — `PRAGMA foreign_keys = OFF` (import connection only) — in flight
2. B1 — `:memory:` staging DB → `VACUUM INTO` — biggest potential single-candidate win
3. A1 — SQLite page size 8K/16K — never measured, low risk
4. A4 — `PRAGMA locking_mode = EXCLUSIVE` alone — decompose E-bundle
5. A5 — `PRAGMA wal_autocheckpoint = 0` + manual checkpoint — decompose E-bundle
6. A6 — Struct-based serde (replace `serde_json::Value`) — parse phase reduction
7. A2 — LTO + PGO — free binary-level gain
8. A7 — `jwalk` parallel directory walk — scan_source reduction
9. A8 — path_node chunk-level cache (across files in chunk) — classify phase reduction
10. B2 — Parallel per-chunk `:memory:` DBs + merge (requires B1 KEPT first)
11. C1 — Per-project sharding + global metadata DB (F2c) — architectural ceiling-breaker
