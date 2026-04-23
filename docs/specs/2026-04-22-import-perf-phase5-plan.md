# Import Performance — Phase 5 Plan

> **For agentic workers:** Read the **RESUME HERE** block at the bottom of
> this document first. Phase 4 shipped the sharded architecture and closed
> its target; phase 5 starts from that branch state and lays out the
> backlog of correctness + perf work surfaced by expanding the test corpus.

**Goal:** Close known correctness gaps and pull down the remaining query-side
hotspots now that cold full-corpus import is under target. No single new
headline metric — the backlog is a set of scoped follow-ups.

**Architecture:** Continue the expedition model from phase 4. One long-lived
experiment branch (`import-perf-p5`) with a linked worktree. Each candidate
gets a throwaway branch + worktree. Kept candidates merge into the
long-lived branch; reverts are preserved as branches-only.

**Tech stack:** Rust, rusqlite/SQLite, rayon, serde_json, walkdir, jiff.

---

## 0. Context

Phase 4 converted the import pipeline to a 9-fixed-shard model and met the
10s target. The `KEPT` endpoint logged in the phase-4 log was the original
C1 implementation (per-project shards + rollup-copy). That approach
subsequently got redesigned on the same branch (commit `cb0c5d2`) to the
architecture described below, which supersedes the per-project-shards
design. A second round of correctness fixes landed after the 10-project
integration corpus was introduced.

Current phase-4 tip: `import-perf-p4` contains the merged redesign plus
four follow-up fixes. See **Section 1** for the architecture and
**Section 3** for the verified baselines.

---

## 1. Current architecture

The main SQLite DB holds **scheduling and manifest state**:

- `project`, `source_file`, `scan_source_cache`, `pending_chunk_rebuild`
- `import_chunk` (including `state`, `publish_seq`, and the `imported_*`
  aggregate counters)

Nine fixed shard DBs (`shard{0..8}.sqlite3`) siblings the main DB path
(`<main>/../shards/shard{i}.sqlite3`), holding **all bulk and rollup data**:

- `conversation`, `stream`, `record`, `message`, `message_part`, `turn`,
  `action`, `action_message`, `action_skill_attribution`, `path_node`,
  `message_path_ref`, `history_event`, `skill_invocation`, `import_warning`,
  `chunk_action_rollup`, `chunk_path_rollup`

Routing rule: every row that ultimately references a project lives in
`shard{project_id % SHARD_COUNT}`. `SHARD_COUNT = 9`. Each shard seeds its
`sqlite_sequence` at `shard_idx * SHARD_ID_STRIDE` (`1_000_000_000`) so
auto-increment ids don't collide across shards.

Read model: `Database::open` (and `open_read_only`) ATTACHes all nine
shards and creates a TEMP VIEW for every shard-data table that is
`UNION ALL` across `shard0.<table>..shard8.<table>`. Unqualified reads
from query code resolve to those views and see all shards at once.

Write model: import workers pick the right shard per project and open a
per-shard writer via `Database::open_shard_for_import`. Metadata writes
(`import_chunk` state transitions, `source_file` updates, project
insertions) go to the main DB serialized through SQLite's single writer.

Cross-cutting invariants that matter for future work:

- **Shards hold project rows too.** Each shard has the project it serves
  copied at init. This is what lets JOIN paths that need `project.root_path`
  stay shard-local (`rollup::LOAD_CHUNK_PATH_FACTS_SQL` relies on this).
- **FK=OFF on the import connection.** A3's optimization; any code that
  relies on `ON DELETE CASCADE` during import must inline the cascade
  explicitly. See `purge_conversation_subtree` in `normalize.rs`.
- **Shard-data VIEWs shadow main tables on read connections.** `DELETE FROM
  <shard_data_table>` on the main connection fails ("cannot modify view").
  Scan-side cleanups must route to `shard{idx}.<table>` directly.

---

## 2. Fixtures and verification harness

- `tests/fixtures/import-corpus/subset.tar.zst` — 10 projects, 1126 files,
  97 chunks. Mtimes pre-shifted 30 days so the "last 24h" startup window
  is stable across test runs.
- `tests/fixtures/import-corpus/full.tar.zst` — 22 projects, 4343 files,
  197 chunks, also time-shifted.
- Tarballs live in `~/tmp/gnomon-fixtures/` and are symlinked into the
  primary checkout + every worktree.
- `cargo run --release -p gnomon-core --example import_bench` drives the
  bench harness.
- `cargo test -p gnomon-core --test import_corpus_integration --
  --include-ignored` is the end-to-end gate. Seven scenarios, all green
  on `import-perf-p4`.

---

## 3. Baselines at phase-5 start

**Full-corpus import** (cold, 5 iterations, release build, 2026-04-22):

| iter | 1 | 2 | 3 | 4 | 5 | median |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| wall (s) | 8.592 | 8.251 | 9.141 | 8.222 | 8.465 | **8.465** |

Beats the phase-4 logged median of 8.966s. Under the 10s target.

**Query-side benchmark** (`gnomon benchmark --iterations 3` on full corpus):

| scenario | median |
| --- | ---: |
| refresh_snapshot_status | 0 ms |
| project_root_browse | 62 ms |
| category_root_browse | 61 ms |
| path_drill_browse | 0 ms |
| project_root_refresh | 420 ms |
| project_root_filter_change | 413 ms |
| project_category_model_filter_browse | 625 ms |
| **jump_target_build** | **21 067 ms** |
| non_path_prefetch_individual | 174 ms |
| non_path_prefetch_batched | 122 ms |
| path_prefetch_individual | 0 ms |
| path_prefetch_batched | 0 ms |

`jump_target_build` triggers when the user presses `g` in the TUI to open
the fuzzy jump-to navigator; 21 seconds to show a prompt is the single
scariest number in this phase.

**Correctness gates** on full corpus + 10-project subset:

- `cargo fmt --all --check`: clean
- `cargo clippy --workspace --all-targets -- -D warnings`: clean
- `cargo test --workspace`: 370 passed
- `cargo test -p gnomon-core --test import_corpus_integration --
  --include-ignored`: 7 passed
- `gnomon report` (all drill-down paths exercised in smoke): non-empty
  rollups via UNION-ALL views.
- `gnomon snapshot` (TUI render): project hierarchy + project drill-down
  + sunburst + statistics panel all populate.

---

## 4. Known outstanding correctness gaps

Carry these into phase 5 explicitly; every future plan decision should
account for them.

- **`imported_record_count_sum` is always 0 for transcript imports.**
  Pre-existing since well before phase 4. Transcripts don't populate the
  `record` table; the `recompute_chunk_counts` aggregate reads from
  `history_event + codex_rollout_record + codex_session_index_entry`, and
  transcripts add nothing to any of those. Any downstream that wanted
  "records imported across all chunks" is wrong. Integration tests assert
  `imported_record_count_sum: 0` to accommodate rather than fix.
- **`publish_seq` is non-deterministic** across runs under parallel
  sharded import (it's assigned in shard-writer completion order).
  Integration tests now sort `chunk_day_sequence` before comparison.
  Any external consumer that assumes `publish_seq` is stable across
  rebuilds will drift.
- **Corrupt-transcript identity flip.** When a file's content lacks a
  `cwd`, project identity falls back to path-based, which creates a
  transient new project while the corruption is live. Final counts are
  now clean (phase-4 follow-up fix routes cleanup through the shard),
  but a concurrent reader mid-corruption would see the transient
  project in the manifest.
- **Shard size imbalance.** Projects hash by `id % 9`. The current `.claude`
  corpus has "kapow" ≈3× the next-largest project, so one shard does 3×
  the work on bulk imports. Current id seeding makes rebalancing a
  migration-level change.

---

## 5. Optimization backlog

Ranked roughly by impact × difficulty. Each entry is a self-contained
candidate; pick one, spin a branch per the expedition protocol, measure,
decide KEPT or REVERTED.

### 5.1 Big (each warrants its own branch)

**J1 — Cache or background-materialize `jump_target_build`.**
> 21 s to start typing after `g`. The worker enumerates every jumpable
> target (projects, categories, actions, paths) across the snapshot and
> rebuilds on every `g` press. Options: keep a jump-target cache keyed
> by `snapshot.max_publish_seq` and invalidate on snapshot change; or
> build the index as a background task immediately after a snapshot
> publishes, so the first `g` press after import is instant. Worst-case
> acceptable latency target: <500 ms.

**R1 — Fix `imported_record_count_sum = 0`.**
> Transcripts need a source of truth for "records imported". Either wire
> transcript normalize to populate `record`, or change the aggregate
> definition in `recompute_chunk_counts` to something transcript imports
> actually update. Small code, visible bug.

**S1 revisit — retry `simd-json` under sharding.**
> Reverted pre-sharding because parse was <5% of wall time. Sharding
> parallelized the DB write path, so parse is now a larger relative share
> of several shards' wall time. Fresh measurement is cheap.

**C1 revisit — retry parallel-classify under sharding.**
> Reverted in phase 2 when build_actions was 95% serial DB writes. Writes
> are now parallel across 9 shards, so classification CPU cost is back in
> the budget. Fresh measurement is cheap.

### 5.2 Medium

**F1 — Switch import connection to FK=ON with deferred constraints.**
> Current FK=OFF forces user code to duplicate cascade-delete logic
> (`purge_conversation_subtree`, `purge_shard_source_file_rows`). Measure
> the import-cost of FK=ON-deferred; if within budget, remove the manual
> cascades and close an entire class of correctness bugs.

**P1 — Shard-scope `path_node` intern cache.**
> A8's chunk-level cache showed no improvement. A shard-level cache that
> stays warm across every chunk routed to that shard within a single
> import could cut inserts on a table with thousands of rows per import.

**W1 — Parallel filesystem scan.**
> `collect_candidate_source_files` in `import/source.rs` walks serially
> with `walkdir`. Rayon parallel walk or a channel-fed worker pool
> should cut scan wall time on 4000-file corpora.

**Q1 — Cache `filter_options` across snapshot boundaries.**
> TUI calls it on every render. ~60 ms per call at full-corpus size;
> zero when cached until `snapshot.max_publish_seq` advances.

### 5.3 Small (leftover housekeeping)

**I1 — Project-identity cache for corrupt files.**
> Remember the last successful (non-path) project identity per
> `source_file` row; prefer it when a re-scan's content no longer
> provides a `cwd`. Eliminates the transient-new-project flip that
> required the shard-purge fix.

**I2 — Deterministic `publish_seq`.**
> Serial "publish" step at chunk finalize time, outside the per-shard
> write loop. Keeps import parallel, makes publish ordering stable.

**D1 — Phase-5 doc updates at end-of-phase.**
> Don't let the log drift the way phase 4's did. The phase-4 log stops
> at pre-redesign C1; four commits of follow-up never made it in.

**G1 — Delete the fully-reverted experiment branches.**
> `import-perf-p4-{a1,a2,a4,a5,a7,a8,b1}` are preserved per expedition
> protocol but have no outstanding leads. Safe to delete after phase-5
> close.

### 5.4 Architectural (design work before touching)

**A1 — Content-aware sharding.**
> Size-balanced partitioning instead of `id % 9`. Requires a migration
> scheme for the `AUTOINCREMENT * shard_idx * 1_000_000_000` id-range
> convention. Non-trivial design; defer unless the kapow-style imbalance
> becomes a real bottleneck.

**A2 — Drop or wire up `record` table.**
> Currently unused for transcript imports but occupies schema +
> AUTOINCREMENT seeding in every shard. Either populate it (intersects
> R1 above) or remove it and simplify the schema.

---

## 6. Protocol

Unchanged from phase 4. Each candidate:

1. Create branch `import-perf-p5-<slug>` and linked worktree.
2. Symlink the fixture tarballs into the worktree.
3. Implement. Run `cargo fmt`, `cargo clippy -D warnings`, `cargo test
   --workspace`, `cargo test -p gnomon-core --test
   import_corpus_integration -- --include-ignored`.
4. Bench: `cargo run --release -p gnomon-core --example import_bench --
   --corpus full --mode full --repeats 5` (for import candidates) or
   `gnomon benchmark --iterations 5` (for query-side candidates).
5. Decide KEPT or REVERTED. Log the measurement.
6. Merge if KEPT; leave the branch alone if REVERTED.

A candidate that changes where data lives in the schema MUST also do the
`gnomon report` + `gnomon snapshot` spot-check before KEPT can be called —
the phase-4 process lesson that caught C1's empty `"rows": []`.

---

## 7. Phase-5 candidate log

### J1 — single-query `jump_target_build` — **KEPT**

Commit: `555b8b9` on `import-perf-p5-j1`, merged via `ff3c114`.

Problem: the TUI jump-to navigator (`g` key) was walking the hierarchy
via cascading browses — one project-root browse, one browse per project,
one per `(project, category)`, and the symmetric category-hierarchy walk.
On the full 22-project / 197-chunk corpus that's ~550 round-trips
through the UNION-ALL shard views; bench median was 21 067 ms.

Fix: every target is a pure function of the distinct
`(project_id, category, action)` tuple set, which is already what
`FILTER_OPTIONS_PROJECT_CATEGORY_ACTION_SQL` returns. New
`QueryEngine::action_rollup_tuples` exposes the raw tuples; the TUI
rebuilds the target list in memory when filters are default. Filtered
jumps keep the legacy per-browse walk.

Measurement (full corpus, 5 iters):

| scenario | before | after |
| --- | ---: | ---: |
| `jump_target_build` | 21 067 ms | **4 ms** |

Other scenarios within noise. Gates ✓. TUI render smoke ✓.

---

## 8. Post-J1 profile notes

Cold full-corpus import wall time is 9258 ms (perf-log snapshot after
J1). Share (cum across calls, then divided by observed parallelism):

| phase | cum ms | approx wall share |
| --- | ---: | ---: |
| `import.normalize_jsonl` (parallel × shards) | 7530 | ~2.7 s |
| `import.build_actions` (parallel × shards) | 7096 | ~2.5 s |
| `import.chunk_shard_commit` | 6236 | ~2.2 s |
| `import.rebuild_path_rollups` | 2601 | ~0.9 s |
| `import.parse_phase` | 1442 | ~0.5 s |
| `import.scan_source` | 386 | 0.4 s |

No single outlier. The import path is now roughly balanced; further
gains are incremental unless we attack shard imbalance (A1) or the
per-shard serial commit (architectural).

The remaining single-digit-percentage optimizations from §5 are still
all valid, but none should be expected to beat J1's headline. Priorities
from here are debatable — the user should steer which direction(s) to
push: UX (more query-side polishing), cold-import wall time (mostly
architectural), or correctness (R1, I1, I2).

---

## RESUME HERE

**Phase**: Phase 5 — J1 landed (KEPT). No candidate currently in
flight.

**Long-lived branch (from phase 4)**: `import-perf-p4`
**Long-lived worktree**: `.worktrees/import-perf-p4`

**Tip of `import-perf-p4`** (post J1 merge):

- `ff3c114` Merge branch 'import-perf-p5-j1' into import-perf-p4
- `555b8b9` J1: single-query jump_target_build, 21 s → 4 ms
- `17f804e` Add phase-5 plan capturing current architecture and backlog
- `ec00593` Merge branch 'import-perf-p4-malformed-restore-fix'
- `78fc57e` Purge shard rows when a stale source_file is removed during scan
- `d8f59cf` Merge branch 'import-perf-p4-c1'
- `8af6967` Fix sharded-import correctness regressions surfaced by a diverse corpus

**Pushed to remote** (`origin/import-perf-p4`): yes, up to `ff3c114`.
`p4 → main` merge still open.

**Kept preserved experiment branches**: `import-perf-p4-a7` (jwalk,
relevant to W1), `import-perf-p4-a8` (chunk-level `path_node` cache,
relevant to P1). All other phase-4 experiment branches and their
worktrees were cleaned up.

**Next action**: pick a phase-5 candidate from §5. With J1 landed the
remaining headroom is in the single-digit-percent range (see §8);
consider whether that's worth more cycles vs. shifting to correctness
(R1, I1, I2) or the architectural questions (A1, A2).

**Do not** re-derive state from `git status` or `git log --oneline`.
Use `git log import-perf-p4 ^main` for the p4 delta and
`git log import-perf-p4 ^import-perf-p5-<slug>` for per-candidate
deltas.
