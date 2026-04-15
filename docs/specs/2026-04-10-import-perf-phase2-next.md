# Next Session: Import Perf — Iteration 9+

**Date:** 2026-04-15
**For:** Fresh Claude session continuing the import performance optimization.

## What to do

Resume the import performance optimization project. Eight iterations complete
(6 KEPT, 2 REVERTED). Current best is ~24s, target is 10s.

**Iteration 8 (parallel classify) was REVERTED** — the classify_message() CPU
cost is only ~300ms total (~6% of build_actions). The build_actions phase is
~95% DB persist. No wall improvement from parallelizing classification.

## How to start

1. Read the session-reset protocol in `docs/specs/2026-04-10-import-perf-log.md`
   — start at the **RESUME HERE** block at the bottom. It has the full iteration
   summary, current metrics, phase distribution, candidate ranking, and bench
   harness commands.

2. Read `docs/specs/2026-04-10-import-perf-design.md` for:
   - **Section 3** — Phase 2 iteration loop (how candidates are evaluated)
   - **Section 4** — Candidate ranking
   - **Section 7** — Running log protocol (how to write log entries)
   - **Section 8** — Commit & merge workflow

3. Create a feature branch + worktree per repo policy:
   ```bash
   cd /home/ketan/project/gnomon
   git branch import-perf-<slug> main
   git worktree add .worktrees/import-perf-<slug> import-perf-<slug>
   ```

4. Corpus tarballs are gitignored. Symlink from an existing worktree:
   ```bash
   cd .worktrees/import-perf-<slug>/tests/fixtures/import-corpus
   ln -s /home/ketan/project/gnomon/tests/fixtures/import-corpus/full.tar.zst .
   ln -s /home/ketan/project/gnomon/tests/fixtures/import-corpus/subset.tar.zst .
   ```

## Current state

- **Branch:** `main` at latest (post iteration 8 log commit)
- **Current best:** ~24.4s cold full import (from 126.1s baseline, −81%)
- **Target:** 10s
- **8 iterations complete:** 6 KEPT, 2 REVERTED

### What just happened (iteration 8)

**Iteration 8 — Parallel classify (REVERTED):**
Pre-classified messages during rayon Phase 1 (parse). Properly handled message
upserts via external_id-keyed HashMap, part dedup, and ID resolution. Correctness
verified (all row counts identical). But classify_message() CPU is only ~300ms
total across 4547 conversations — the build_actions phase is 95% DB persist.
No measurable wall improvement.

### Key files

| File | Role |
|------|------|
| `crates/gnomon-core/src/import/mod.rs` | Shared types: `ParsedFile`, `ParsedRecord`, `NormalizedMessage` |
| `crates/gnomon-core/src/import/normalize.rs` | `parse_jsonl_file` (parallel CPU) + `write_parsed_file_in_tx` (serial DB) |
| `crates/gnomon-core/src/import/chunk.rs` | `import_chunk` — rayon `par_iter` parse, then serial write loop |
| `crates/gnomon-core/src/classify/mod.rs` | `build_actions_in_tx_with_messages` — classification + persist |
| `docs/specs/2026-04-10-import-perf-log.md` | Running log with RESUME HERE block |
| `docs/specs/2026-04-10-import-perf-design.md` | Design doc with architecture |

### Phase distribution (~24.4s wall, from perf-log)

| phase | time | parallelizable? |
| --- | ---: | --- |
| normalize_jsonl (messages + parts INSERTs) | ~9.0s | No (single SQLite writer) |
| build_actions (~5.1s persist + ~0.3s classify) | ~5.5s | Persist: no. Classify: yes but negligible. |
| scan_source | ~2.8s | Partially |
| finalize + rollups | ~3.3s | No |
| build_turns | ~1.1s | Partially |
| other/overhead | ~3.3s | Unknown |

## What to consider next

### 1. Fresh perf-log profile run
The uninstrumented overhead is now ~3.3s (down from ~9s in earlier sessions).
This difference is likely session-to-session variance. A fresh profile would
clarify where time is actually spent and whether the candidate ranking is stale.

### 2. Channel-based pipeline (candidate #6)
Replace the `par_iter().collect()` barrier with a producer-consumer channel
(crossbeam) to overlap parsing file N+1 with writing file N. Expected: modest
gain if parse and write phases are similarly sized. Simpler than structural changes.

### 3. Reduce action/path_ref persist cost (candidate #7)
build_actions is ~5.5s, almost all DB persist. Options:
- Batch INSERT for actions (multi-row VALUES)
- Skip path_ref inserts for non-file-tool actions
- Reduce number of action rows (coarser grouping)

### 4. Structural changes (Tier C, requires approval)
- In-memory staging DB → `VACUUM INTO` for zero mid-import I/O
- DuckDB or columnar store for analytical queries
- Skip/defer action+path_ref tables entirely

## Measurement protocol

Per the design doc Section 7:
- Write a log entry skeleton BEFORE implementation starts
- Run subset first (fast feedback, ~9s), then full corpus (truth, ~24s)
- Use interleaved multi-repeat runs for reliable comparison
- Verify row parity (9 non-record tables must match baseline)
- Quality gates: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`

## Dependencies

- `rayon` already in workspace
- `crossbeam-channel` may be needed for channel-based pipeline (already a
  transitive dep via rayon)
