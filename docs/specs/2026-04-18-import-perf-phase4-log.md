# Import Performance — Phase 4 Running Log

**Plan:** `docs/specs/2026-04-18-import-perf-phase4-plan.md`

---

## Frozen Header

*(Populate after first bench run on the candidate branch.)*

| field | value |
| --- | --- |
| Corpus capture date | *(from MANIFEST.md)* |
| full.tar.zst SHA | *(from MANIFEST.md)* |
| subset.tar.zst SHA | *(from MANIFEST.md)* |
| Host | ketan WSL2 |
| CPU | *(uname -a + lscpu)* |
| Rust toolchain | *(rustc --version)* |
| SQLite version | *(bundled, from rusqlite)* |
| Baseline full corpus | ~20.6s median |
| Baseline subset | ~8.3s median |
| Target | ≤10s full corpus |
| Phase 3 kept changes | G1 (scan_source delta cache), G4 (file-granular invalidation), D1b (preset turn_id) |

---

## Phase Log

*(Append-only. One entry per candidate.)*

---

## RESUME HERE

Phase: Phase 4
Long-lived branch: `import-perf-p4`
Long-lived worktree: `.worktrees/import-perf-p4`
Last completed: *(none — Phase 4 not started)*
Next action: Set up long-lived branch + worktree per Section 3 of plan, then run candidate A3.
Current best (subset): ~8.3s median (Phase 3 baseline)
Current best (full): ~20.6s median (Phase 3 baseline)
Target: 10s full corpus
In-flight uncommitted state: none

Candidate ranking (live — re-rank after each result):
1. A3 — `PRAGMA foreign_keys = OFF` (import connection only) — highest expected value, lowest effort
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
