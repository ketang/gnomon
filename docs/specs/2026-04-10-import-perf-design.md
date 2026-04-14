# Import Performance Optimization — Design

**Date:** 2026-04-10
**Status:** Phase 2 checkpoint — Tier A exhausted, Tier B next. Merged to main 2026-04-13.
**Running log:** [`2026-04-10-import-perf-log.md`](./2026-04-10-import-perf-log.md)

## 1. Goals & Non-Goals

### Primary goal
Reduce startup TUI latency: the wall time from `gnomon` invocation to an interactive TUI. Today the code imports a 24-hour window or hits a 10-second timeout (`crates/gnomon-core/src/import/chunk.rs:232`), whichever comes first, before opening the UI.

### Secondary goal
Reduce cold full-import wall time: the total time to ingest the entire `~/.claude` corpus into an empty database.

### Non-goals
- Warm re-scan change-detection performance. Only touched if profiling shows it contaminating primary or secondary goals.
- Query / read-path performance. `benchmark.rs` already covers reads.
- Schema stability and migration ergonomics. Per `docs/v1-design.md`, re-import on schema bump is the existing contract; we inherit it.
- Cross-platform performance. Single-machine (the user's WSL2 dev box) only.
- TUI rendering performance.

### Success criterion
A concrete target number for startup TUI latency, agreed with the user **at the end of Phase 1** after baseline data is captured. No target is committed before measurement. A secondary target for cold full import may be set later depending on what profiling reveals.

### Inherited constraints
- Branch + worktree required for all implementation work, under `.worktrees/import-perf-<slug>/`.
- Rust quality gates must pass on every commit: `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, `cargo build --workspace`.
- GitHub Issues is the tracker; no issues created unless the user asks.
- Merges to `main` via `git merge --no-ff`; no pull requests.
- New dependencies are allowed.
- Schema changes are allowed. Database is treated as disposable per the reimport-on-schema-bump model.
- Loss of durability during bulk loading is acceptable if it improves performance.

## 2. Current State Summary

Established via exploration of the codebase on 2026-04-10. File:line references are to the state of `main` at that time.

- **Storage.** Single SQLite database via `rusqlite`, WAL mode, roughly 20 tables and 25 indexes. Default path `~/.local/share/gnomon/usage.sqlite3`. Foreign keys enforced, 5-second busy timeout (`crates/gnomon-core/src/db/mod.rs:134-138`).
- **Parsing.** `BufReader::lines()` iterating `serde_json::from_str()` per line (`crates/gnomon-core/src/import/normalize.rs:89-102`).
- **Inserts.** One `conn.execute()` per row. No prepared-statement reuse, no batching. Wrapped in a per-chunk transaction (`crates/gnomon-core/src/import/normalize.rs:82`, `process_record` at `:683`, `upsert_message` at `:747`).
- **Concurrency.** Single background importer thread (`crates/gnomon-core/src/import/chunk.rs:327`). Chunks are `project × day`, scheduled round-robin, strictly serial. No rayon or tokio in the hot path.
- **Post-processing.** `build_turns` (`crates/gnomon-core/src/import/normalize.rs:903`) and `build_actions` (`crates/gnomon-core/src/classify/mod.rs:21`) run per-conversation serially inside the finalize transaction. `rebuild_chunk_action_rollups` and `rebuild_chunk_path_rollups` full-rebuild per chunk inside the same transaction (`crates/gnomon-core/src/import/chunk.rs:1023`). This directly affects startup latency because finalize blocks the TUI gate.
- **Instrumentation.** `PerfLogger` exists (`crates/gnomon-core/src/perf.rs`) but is **not wired into the import path**. No criterion benches for writes. No end-to-end import integration test.
- **Fixtures.** Only inline string fixtures in unit tests. No reproducible corpus snapshot.

## 3. Phase Structure

### Phase 1 — Measure (hard gate)

No optimization work begins until all deliverables below are complete and the user has approved a target.

**Deliverables:**
1. **Frozen corpus snapshot.** Full `~/.claude/projects` plus `~/.claude/history.jsonl` captured as `full.tar.zst`; a deterministic subset captured as `subset.tar.zst`. Stored under a gitignored `tests/fixtures/import-corpus/` directory. See Section 5.
2. **Benchmark harness.** Extracts a snapshot into a tmpdir, runs import against a fresh SQLite file, captures metrics, tears down. Two modes: `--full` and `--subset`. Deterministic and reproducible.
3. **Perf instrumentation wired in.** `PerfLogger` spans around `scan_source_manifest`, per-chunk `import_chunk`, the `process_record` loop (as a phase, not per row), `build_turns`, `build_actions`, `rebuild_chunk_*_rollups`. Additional split within `process_record` separating JSON parse time from SQL execute time. Opt-in via environment variable, off by default.
4. **Baseline metrics captured.** On full corpus and subset, with hardware fingerprint, WSL filesystem type for both repo path and DB path, SQLite page size and pragma settings. Metrics listed in Section 6.
5. **Profile captured.** At least one CPU profile (samply or perf) on full cold import, plus one on startup-mode import. Flamegraphs committed into `docs/specs/profiles/`.
6. **Target agreed.** User and assistant look at numbers together and write a target into the running log header.

**Gate behavior.** If Phase 1 reveals a trivially fixable problem (e.g., DB path is on a 9p Windows mount and fsyncs dominate), the finding is logged, the fix is applied, baselines are re-captured, and only then does Phase 2 begin. The gate is not skipped.

### Phase 2 — Iterate (open-ended loop)

Enters only after the Phase 1 gate opens. One iteration:

1. Pick the highest-expected-value unexplored candidate from the **live** ranked list. The list is re-ranked after every profile run, not frozen at the start.
2. Create a per-candidate branch and worktree under `.worktrees/import-perf-<slug>/`.
3. Implement the candidate.
4. Run the harness: subset first (fast feedback), then full (truth). Capture deltas vs. the current best.
5. Verify parity: row counts per table match the baseline database; spot-check representative queries for identical results.
6. Write a log entry covering hypothesis, measurements, kept/reverted decision, rationale, and implied next candidate.
7. **If kept:** summarize for the user, request commit approval, commit on approval, merge the candidate branch into the long-lived `import-perf` branch, re-profile, re-rank candidates.
8. **If reverted:** log the result, leave the worktree alone, pick the next candidate.

### Soft checkpoints

Fire when any of:
- Three iterations have completed since the last checkpoint.
- A candidate produced a surprising result (positive or negative).
- The profile shape changed materially.
- Remaining candidates all require structural approval.

A checkpoint produces a short summary, an updated ranking, and a "continue?" prompt defaulting to yes.

## 4. Candidate Strategy Ranking (initial)

This is the **starting** ranking. It will be re-ranked after every profile and should be treated as a live working hypothesis, not a frozen plan. Lower-tier candidates may jump tiers as profiling reveals where the hot path actually lives.

### Tier A — low risk, likely wins (try first)

**A1. Prepared-statement reuse across rows.** Cache `rusqlite::Statement` handles for the record, message, message_part, and turn inserts and reuse them across the loop. Eliminates per-row SQL re-parse. Expected: 2-5× on SQL-bound phase. Risk: very low.

**A2a. Safe SQLite pragma tuning.** On the importer connection: `synchronous=NORMAL`, `temp_store=MEMORY`, `cache_size=-262144` (256 MB), `mmap_size=268435456` (256 MB), plus page size review. Crash-safe under WAL (loses at most the last transaction on power loss). Risk: low.

**A2b. Aggressive durability relaxation.** On top of A2a: `synchronous=OFF`, `locking_mode=EXCLUSIVE` on the importer connection, and experimentation with `journal_mode=MEMORY` or `wal_autocheckpoint=0` plus a manual checkpoint at the end. Acceptable per user direction; corruption risk is bounded because the database is reimportable from source. Risk: low in this project's context.

**A3. Deferred secondary indexes during bulk load.** Drop non-unique indexes before chunk import, recreate after. Directly affects cold full import; less relevant to startup (which touches a small slice). Expected: 1.5-3× on insert phase of cold import. Risk: low, but needs careful transaction-boundary handling.

**A4. Move rollup rebuilds out of the per-chunk finalize transaction.** `rebuild_chunk_action_rollups` and `rebuild_chunk_path_rollups` currently run inside the finalize transaction (`crates/gnomon-core/src/import/chunk.rs:1023`). Move them to a post-import pass, or make them incremental. Directly affects startup latency because finalize blocks the TUI gate. Risk: medium (need to verify no reader depends on rollups being live mid-import).

**A5. Multi-row INSERT batching.** On top of A1, for the highest-fanout tables (`record`, `message_part`), batch inserts via multi-value `VALUES (...), (...), ...` statements. The marginal win over A1 alone is modest in SQLite compared to Postgres, but non-zero: fewer VM step cycles, fewer binding round-trips, better btree-descent amortization. Measure *after* A1 lands so the marginal value is visible. Risk: low.

### Tier B — medium risk, structural (consider after Tier A)

**B1. Parallel JSON parsing into a single SQLite writer.** Rayon worker pool parses JSONL lines into owned structs; a single writer thread drains an mpsc channel and does SQL. Only helps if parsing is a real fraction of wall time, which Phase 1's parse-vs-SQL split will confirm or refute. Risk: medium (ordering, error handling, backpressure).

**B2. Per-project parallel importer threads.** Each importer thread owns its own connection and its own set of projects. WAL allows one writer per DB file, so this helps writes only in combination with B3. Included here because the scheduling restructure is reusable.

**B3. Sharded database: one SQLite file per project (or per project group).** Each shard is a separate file; importer threads write to their own shard in parallel. Read side unions across shards. Potentially very large gain for cold full import; breaks the single-DB assumption throughout the read path. Risk: high.

**B4. `simd-json` or `sonic-rs` in place of `serde_json`.** SIMD-accelerated JSON parsing. Only pursue if profiling shows parsing is >~30% of wall time. Risk: low-medium.

**B5. Classification regex compilation cache.** `classify_message` patterns should compile once into `OnceLock<Regex>`. Likely small impact unless classify is hot. Risk: near zero.

### Tier C — structural, last resort (requires explicit approval)

**C1. Separate raw-ingest DB from analytical DB.** Raw JSONL lands in a minimal-schema ingest DB with no indexes; a background job derives the full schema into the analytical DB. Decouples startup latency from full-fidelity import. Impact: best possible for startup. Risk: very high.

**C2. DuckDB or columnar store for analytical side.** Only relevant if C1 happens and reads need to scale. Mentioned for completeness; probably not this project.

**C3. Background-stream-into-live-TUI model.** TUI opens instantly with whatever's currently in the database; new rows stream in behind it. Completely decouples startup latency from import. Adjacent to C1 but simpler — a UX change rather than a schema change. Impact: best possible for startup if behaviorally acceptable. Risk: medium.

### Explicitly not on the list
- Switching away from SQLite entirely.
- Async/tokio rewrite. SQLite is sync-native; async gains nothing for this workload.
- Writing a custom JSONL parser.
- Schema denormalization before evidence that joins are the problem.
- Caching parsed JSON to disk.

## 5. Corpus Snapshot

**Location.** `tests/fixtures/import-corpus/`, gitignored except for the manifest and capture script.

**Contents.**
- `full.tar.zst` — zstd-compressed tarball of `~/.claude/projects` plus `~/.claude/history.jsonl`, captured as-is. Local to the user's machine; never pushed.
- `subset.tar.zst` — deterministic sampled subset.
- `MANIFEST.md` — committed. Records capture date, host fingerprint, filesystem, byte counts, file counts, project counts, and SHA256 of each tarball.
- `capture.sh` — committed. Reproducible capture script that tars, hashes, runs subset selection, and writes the manifest.

**Subset sampling strategy.** Sort projects by total JSONL bytes descending. Include whole projects from the head of the list until the total reaches `min(5% of full corpus bytes, 100 MB)`. Never slice individual JSONL files — partial files can produce malformed record streams and would invalidate the benchmark. Including whole projects preserves the `project × day` chunking assumptions of the import pipeline. "Largest first" rather than uniform because the hot path is dominated by large projects and the subset should reflect that skew.

**Regeneration policy.** Capture once at the start of Phase 1. Treat as frozen. Re-capture only if the corpus changes materially and a re-baseline is needed, or at the user's explicit request. Each re-capture updates the manifest; the SHA delta makes staleness visible.

**Privacy.** Tarballs stay local (gitignored, never pushed). If the user later prefers them outside the repo worktree, they can move to `~/.cache/gnomon-bench/` with the manifest updated to reference that path.

## 6. Baseline Metrics

Captured during Phase 1, recorded in the running log, referenced by every Phase 2 candidate entry.

**Primary metrics:**
- Startup-mode import: wall time from invocation to TUI gate.
- Cold full import: total wall time, empty DB to complete.

**Breakdowns:**
- Per-phase wall time split: scan, parse, SQL execute, `build_turns`, `build_actions`, `rebuild_*_rollups`.
- Rows per second per table.
- JSONL megabytes per second parsed.
- Per-chunk wall-time variance (p50, p95, p99).

**Resource metrics:**
- Peak RSS.
- DB file size after full import.
- SQLite page cache hit rate (if cheaply measurable).
- fsync count per second (from strace or equivalent if needed).

**Environment fingerprint:**
- `uname -a`, CPU model, core count, RAM total.
- WSL filesystem for repo path (ext4 native vs. 9p Windows mount).
- WSL filesystem for DB path.
- Rust toolchain version.
- Bundled SQLite version.

## 7. Running Log

**File.** `docs/specs/2026-04-10-import-perf-log.md`, committed to the repo.

**Why in the repo.** Survives worktree switches (any worktree under `.worktrees/import-perf-*` can read it). Survives context resets (a fresh session reads it from `main` or whichever branch last committed). Version-controlled so the evolution of the candidate ranking and target is inspectable.

**Structure.**
1. **Frozen Header** — overwritten only when underlying facts change. Environment fingerprint, corpus SHAs, baseline metrics, target.
2. **Phase Log** — append-only dated entries. Types: baseline, profile, candidate, checkpoint, note.
3. **Resume Block** — always overwritten, at the bottom. Current phase, current branch, current worktree, last completed step, next action, in-flight uncommitted state, current candidate ranking.

**Candidate entry template:**
```
## <date> <time> — candidate <id>: <short description>
Branch: <branch>
Hypothesis: <why this should help>
Implementation: <1-2 sentences, file:line refs>
Measurements:
  Subset:          <before> → <after> (<delta>)
  Full:            <before> → <after> (<delta>)
  Startup-mode:    <before> → <after> (<delta>)
  Row parity:      PASS | FAIL (<details>)
  Profile shift:   <phase percentages before/after>
Decision: KEPT | REVERTED | PENDING USER
Commit: <sha or blank>
Next implied: <next candidate and why>
```

**Session-reset protocol.** First action of any fresh session: read the log, read the Resume Block, read the last 2-3 phase log entries for context, continue from the "Next action" line. Do not re-run baselines, re-derive state from `git status`, or ask the user to reconstruct context. The log is authoritative.

**Update cadence.**
- Immediately after baseline capture (header populated).
- Immediately after target agreement (header target populated).
- At the **start** of each candidate — entry skeleton and hypothesis written before implementation begins, so a mid-implementation session death is recoverable.
- After each measurement.
- After each decision.
- Resume Block updated before and after any non-trivial step.

**Log commits.** Separate from code commits. Message format `log: <what>`. Example: `log: A1 kept, +28% startup, committed abc123`. Committed directly without user approval — blocking on approval per log entry would kill the iteration loop's momentum. Code-change commits remain gated on explicit approval.

## 8. Commit & Merge Workflow

**Branch layout.**
- One long-lived feature branch: `import-perf`. All kept changes accumulate here across the project.
- Per-candidate throwaway branches: `import-perf/a1-prepared-stmts`, `import-perf/a2b-pragmas`, etc., each with its own worktree under `.worktrees/import-perf-<slug>/`.
- Kept candidates merge into `import-perf`. Reverted candidates are abandoned, not destructively deleted — per the user's CLAUDE.md, cleanup only happens on explicit request.
- `import-perf` merges into `main` only when the user says so, via `git merge --no-ff`. No pull requests.

**Commit-on-approval protocol.**
1. Candidate complete, measurements captured, row parity verified, log entry written.
2. Surface a one-paragraph summary: what changed, deltas, row parity, profile shift, recommendation.
3. **Ask for commit approval explicitly.** No batching. One approval per kept candidate.
4. On approval:
   - Commit the code change on the candidate branch, referencing the log entry.
   - Merge the candidate branch into `import-perf` with `--no-ff`.
   - Commit the log update separately.
   - Update the Resume Block.
5. On "let me think": changes stay uncommitted in the worktree, log entry marked `PENDING USER`, Resume Block reflects the pending state.
6. On rejection: log entry marked `REVERTED`, worktree left alone, next candidate picked.

**Triggers for commit prompts:**
- Baseline harness, corpus snapshot script, perf instrumentation wiring (Phase 1).
- Every kept candidate in Phase 2.
- Any out-of-band refactor, which is flagged explicitly and rare.

**Not triggered:**
- Reverted candidates (nothing to commit).
- Log-only updates (committed directly).
- Profile artifacts and flamegraphs (committed as part of their log entry).

**Quality gates before every commit prompt.** `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, `cargo build --workspace` all pass. If any fail, the assistant fixes first, surfaces the fix, and only then re-prompts. Hooks are never skipped.

**Merge to main.** Never automatic. At stopping time, or on explicit request, the assistant summarizes the cumulative delta and proposes the merge. The user approves or defers.

## 9. Stopping Criteria & Session Exit

Any one of the following ends the Phase 2 loop:

1. **Target met with margin.** Startup gate measurement reaches target × 0.9 or better on two consecutive runs. The margin protects against measurement noise.
2. **Diminishing returns triggers a checkpoint, not a stop.** Three consecutive candidates each yielding <5% improvement on the primary metric cause the assistant to pause and surface options:
   - (a) Re-profile fresh and re-rank from scratch — the ranking may be stale.
   - (b) Jump to a lower-tier or previously-skipped candidate the user wants to try.
   - (c) Explore a Tier C candidate (requires explicit approval).
   - (d) Actually stop.
   Default on silence is (a). Flat results never auto-stop the loop.
3. **Structural gate.** Remaining unexplored candidates are all Tier C or equivalent, and no approval has been granted. Stop and ask.
4. **Explicit stop.** User says stop. Highest priority.
5. **Unresolvable quality gate failure.** Clippy or test failure that the assistant cannot resolve and that is not caused by the current change. Stop and surface.
6. **Unexplained parity failure.** A candidate produces different row counts or query results vs. baseline, and the assistant cannot identify why within one iteration. Revert, log, stop to discuss. Continuing risks a class of silent-corruption bugs across other candidates.

**Stopping actions.**
1. Write a final log entry with reason and final metrics vs. baseline vs. target.
2. Update the Resume Block to show clearly: branch holding kept work, unmerged state, unexplored candidates.
3. Propose a merge path — either "ready to merge `import-perf` into `main`" or "leave branch alive for next session."
4. Wait for the user's decision.

**Cross-session continuity.** The running log plus Resume Block is the entire handoff. A fresh session reads it and resumes. The Phase 1 gate is passed once per project and stays passed across sessions unless:
- The corpus snapshot is regenerated (manifest SHA changed) → re-baseline.
- Hardware or filesystem changed → re-baseline.
- A major upstream change lands on `main` and is merged into `import-perf` → re-baseline.

In any of those cases the assistant surfaces the situation rather than silently re-running Phase 1.

## 10. Risks & Open Questions

- **WSL filesystem placement.** If the DB path lives on a 9p Windows mount, fsync cost could dominate every measurement and invalidate conclusions. Phase 1 captures this in the fingerprint and, if problematic, surfaces it immediately as a gating finding.
- **Ranking staleness.** The initial Tier A ranking is based on code reading, not profiling. Phase 1's profile may completely reshuffle priorities. This is expected and handled by the "re-rank after every profile" rule.
- **Parity verification depth.** Row counts per table is a weak check; query-result spot-checks are better but slower. The design uses both. If a candidate passes row-count parity but fails a spot check, treat it as criterion 6 (unexplained parity failure).
- **Rollup semantics under A4.** Moving rollup rebuilds out of the finalize transaction may expose in-progress state to concurrent readers. A4 must include a check that no reader relies on rollups being live at chunk-finalize time.
- **Subset representativeness.** A 5%/100 MB subset of largest projects may not reflect optimizations that matter for small-project workloads. Full-corpus runs remain the authoritative measurement for every kept candidate.

## 11. Checkpoint: Tier A Exhausted (2026-04-13)

Merged to `main` at `87c14ff`. Three Tier A candidates KEPT, one REVERTED. The
import-perf branch no longer exists separately — all work is on main.

### What landed

| Candidate | Code location | Mechanism |
| --- | --- | --- |
| Commit batching | `chunk.rs` — chunk-level tx + per-file savepoints | Eliminated per-file commit overhead |
| Prepared-statement caching | `normalize.rs`, `classify/mod.rs` — `prepare_cached()` | Eliminated per-row SQL compilation |
| Pragma tuning | `db/mod.rs` — `configure_read_write_connection()` | synchronous=NORMAL, 64MB cache, 256MB mmap, temp_store=MEMORY |

### What we learned (invalidated assumptions)

1. **Deferred secondary indexes do nothing** with a 64MB page cache. Btree pages are cached; index maintenance per insert is ~0. The dominant cost is the primary btree insert itself.
2. **Rollup queries are cache-locality sensitive.** Moving rollups out of the per-chunk transaction causes 10× regression because the data pages are evicted by index-recreation I/O.
3. **Session-to-session wall time varies ~30%** on WSL2. Only within-session interleaved comparisons are reliable.
4. **The remaining bottleneck is CPU-bound.** Not I/O, not fsync, not index overhead. Pure btree traversal and page manipulation for ~1.2M inserts across 7 tables.

### What must happen next

The remaining ~32s breaks down as:
- **~18s SQL inserts** (btree work, cannot be reduced without fewer rows or parallelism)
- **~7s build_actions** (2–3s is a redundant load_messages re-read from DB; rest is classification + inserts)
- **~3.3s JSON parsing** (serde_json, per-line)
- **~3.6s rollups + finalize** (path rollup dominant)
- **~2.5s scan_source** (directory walk)

To reach 10s, the recommended path is:

**Step 1 — In-memory data passing (candidate #2 in ranking): DONE**
- ~~Refactor `build_turns` to use in-memory message data instead of re-reading from DB~~
- ~~Refactor `build_actions` to accept parsed message + part data instead of the `load_messages` JOIN~~
- Landed at `2eca9fa`. Saves ~2s in build_actions + build_turns. Prerequisite for Step 2.

**Step 2 — Parallel parse + classify (candidate #1 in ranking): NEXT**
- Use rayon to parse all JSONL files in parallel → `Vec<NormalizedMessage>` per file
- Classify actions in parallel on in-memory data
- Single SQLite writer thread for all inserts
- Expected: ~30–40% wall reduction (CPU portion parallelizes across available cores)
- See Section 12 for architectural details and implementation guidance.

**Step 3 (optional) — Structural changes requiring product review:**
- Skip `record` table inserts (~490K rows, 33% of insert time) if raw-record access isn't needed
- In-memory staging DB → `VACUUM INTO` for zero mid-import I/O
- Alternative storage engine (DuckDB) for analytical queries

## 12. Checkpoint: In-Memory Data Passing Complete (2026-04-13)

Merged to `main` at `2eca9fa`. In-memory data passing (iteration 5) KEPT.

### What landed

| Candidate | Code location | Mechanism |
| --- | --- | --- |
| In-memory data passing | `import/mod.rs`, `normalize.rs`, `classify/mod.rs`, `chunk.rs` | Normalize produces `Vec<NormalizedMessage>` → build_turns annotates turn fields in-place → build_actions consumes directly, skipping `load_messages` 4-way JOIN |

### Current pipeline data flow

```
import_chunk (chunk.rs:933)
  └─ for each source_file:
       ├─ normalize_jsonl_file_in_tx(conn, params)
       │    → Returns (NormalizeJsonlFileOutcome, Vec<NormalizedMessage>)
       │    │  normalize_transcript_jsonl_file_core:
       │    │    1. Parse JSONL → INSERT records, messages, message_parts
       │    │    2. Build Vec<NormalizedMessage> from ImportState.message_states
       │    │    3. build_turns(&mut normalized_messages) — in-memory, back-annotates turn_id
       │    │    4. finish_import() — update counters
       │    └─ Returns outcome + normalized_messages
       └─ build_actions_in_tx_with_messages(conn, params, normalized_messages)
            └─ classify_and_persist_actions() — uses in-memory messages directly
```

### Key types (import/mod.rs)

- `NormalizedMessage` — id, stream_id, sequence_no, message_kind, timestamps, usage, parts, turn_id/turn_sequence_no/ordinal_in_turn
- `NormalizedPart` — id, part_kind, tool_name, tool_call_id, metadata_json
- `Usage` — shared token counter type (replaces duplicate definitions in normalize + classify)

### Current phase distribution (full corpus, ~31.5s wall)

| phase | time | % of wall | parallelizable? |
| --- | ---: | ---: | --- |
| normalize sql_ms | 9.7s | 31% | No (single SQLite writer) |
| build_actions | 5.6s | 18% | Classification: yes. Persist: no. |
| parse_ms | 3.0s | 10% | **Yes** (pure CPU, per-file independent) |
| scan_source | 2.7s | 9% | Partially (directory walk) |
| finalize + rollups | 3.2s | 10% | No (DB-bound) |
| build_turns | 1.1s | 3% | Turn grouping: yes. Persist: no. |
| other/overhead | 6.2s | 20% | Unknown |

### What parallelism can target

The CPU-bound work that can run in parallel across files:
1. **JSON parsing** (3.0s) — `serde_json::from_str` per line, per-file independent
2. **Message extraction** — `extract_message()` + `ExtractedMessage` construction
3. **Classification** — `classify_message()` + `build_tool_use_lookup()` + path ref extraction
4. **Turn grouping** — grouping messages into turns (just the logic, not the DB persist)

The DB-bound work that must remain serial:
1. **All INSERTs** — record, message, message_part, turn, turn_message, action, action_message, message_path_ref
2. **Rollup rebuilds** — rebuild_chunk_action_rollups, rebuild_chunk_path_rollups
3. **Finalize** — update counters

### Architecture for parallel import

**Recommended approach: parse-ahead pipeline**

```
Phase 1: Parallel parse (rayon thread pool)
  - For each source file in chunk, spawn a rayon task:
    - Read JSONL file
    - Parse all lines → Vec<serde_json::Value>
    - Extract messages → Vec<ExtractedMessage> (with parts)
    - No DB access in this phase
  - Result: Vec<ParsedFile> where ParsedFile holds the extracted data

Phase 2: Serial write (single thread, current connection)
  - For each ParsedFile:
    - INSERT records, messages, message_parts (using existing ImportState logic)
    - Build Vec<NormalizedMessage> from ImportState
    - build_turns (in-memory, persist to DB)
    - build_actions_in_tx_with_messages (classify in-memory, persist to DB)
  - This phase is identical to today's flow but fed from pre-parsed data

Why not parallel writes?
  - SQLite WAL allows only one writer at a time
  - Sharding (one DB per project) would give parallel writes but breaks the
    single-DB assumption throughout the read path — that's a Tier C change
```

**Alternative: deeper parallelism (classify in parallel too)**

```
Phase 1: Parallel parse + classify (rayon)
  - Parse JSONL → extract messages → group into turns → classify actions
  - All in-memory, no DB access
  - Result: Vec<ClassifiedFile> with messages, turns, actions, path_refs

Phase 2: Serial write
  - Bulk INSERT all pre-computed data
  - Possibly with multi-row INSERT batching for additional speed

This is more complex but saves the build_actions time (~5.6s) from serial execution.
The classification logic (classify/mod.rs) would need to be refactored to work
on ExtractedMessage/ExtractedMessagePart rather than NormalizedMessage (which has
DB-assigned IDs). Turn grouping can happen without DB IDs. Action classification
can happen without DB IDs. Only the persist step needs IDs.
```

### Constraints for the parallel implementation

1. **Record sequence numbers** must be globally ordered per conversation. Currently assigned by `ImportState.next_record_sequence_no` during serial processing. With parallel parsing, assign after parsing completes (sort by source_line_no).
2. **Message sequence numbers** similarly must be ordered. Assign post-parse.
3. **Part deduplication** (`seen_part_keys` in MessageState) happens per-message. This is already per-file scoped and safe in parallel.
4. **Conversation/stream creation** (INSERT into conversation, stream tables) must happen before message INSERTs due to FK constraints. This is per-file and can be batched.
5. **`import_chunk` processes files serially within a chunk transaction.** The parallelism should be at the file level within a chunk, or across chunks.
6. **Error handling:** a parse failure in one file should not abort other files. Use `Result` collection and handle failures per-file as today (savepoint rollback).
