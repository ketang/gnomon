# Next Session: Import Perf — Iteration 8+

**Date:** 2026-04-14
**For:** Fresh Claude session continuing the import performance optimization.

## What to do

Resume the import performance optimization project. Seven iterations complete
(6 KEPT, 1 REVERTED). Current best is ~27s, target is 10s.

The next candidate is **parallel classify** (candidate #1b) — extending the
existing rayon parallelism to cover the classification phase (`build_actions`),
which is ~5.6s and mostly CPU-bound.

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
   - **Section 12** — Pipeline data flow, phase distribution, and the
     **"deeper parallelism" architecture** that parallelizes classification.

3. Create a feature branch + worktree per repo policy:
   ```bash
   cd /home/ketan/project/gnomon
   git branch import-perf-classify main
   git worktree add .worktrees/import-perf-classify import-perf-classify
   ```

4. Corpus tarballs are gitignored. Symlink from an existing worktree:
   ```bash
   cd .worktrees/import-perf-classify/tests/fixtures/import-corpus
   ln -s /home/ketan/project/gnomon/.worktrees/import-perf/tests/fixtures/import-corpus/full.tar.zst .
   ln -s /home/ketan/project/gnomon/.worktrees/import-perf/tests/fixtures/import-corpus/subset.tar.zst .
   ```

## Current state

- **Branch:** `main` at `c524747`
- **Current best:** ~27.2s cold full import (from 126.1s baseline, −78%)
- **Target:** 10s
- **7 iterations complete:** 6 KEPT, 1 REVERTED

### What just landed (iterations 6 + 7)

**Iteration 6 — Parallel JSONL parsing (rayon):**
Split `normalize_jsonl_file_in_tx` into `parse_jsonl_file` (pure CPU, rayon
`par_iter`) + `write_parsed_file_in_tx` (serial DB writes). New types
`ParsedFile`, `ParsedRecord`, `ParseResult` carry pre-parsed data. ~29s (−10%).

**Iteration 7 — Skip record table inserts:**
Removed all `INSERT INTO record` (490K rows). The `record` table was never
read outside the import pipeline. `IMPORT_SCHEMA_VERSION` bumped to 5.
~27s (−7%), DB size −13%.

### Key files

| File | Role |
|------|------|
| `crates/gnomon-core/src/import/mod.rs` | Shared types: `ParsedFile`, `ParsedRecord`, `ParseResult`, `NormalizedMessage` |
| `crates/gnomon-core/src/import/normalize.rs` | `parse_jsonl_file` (parallel CPU) + `write_parsed_file_in_tx` (serial DB) |
| `crates/gnomon-core/src/import/chunk.rs` | `import_chunk` — rayon `par_iter` parse, then serial write loop |
| `crates/gnomon-core/src/classify/mod.rs` | `build_actions_in_tx_with_messages` — classification + persist |
| `docs/specs/2026-04-10-import-perf-log.md` | Running log with RESUME HERE block |
| `docs/specs/2026-04-10-import-perf-design.md` | Design doc with Section 12 architecture |

### Phase distribution (~27s wall)

| phase | time | parallelizable? |
| --- | ---: | --- |
| normalize sql_ms (messages + parts) | ~5s | No (single SQLite writer) |
| build_actions (classify + persist) | ~5.6s | Classification: yes. Persist: no. |
| parse_ms (rayon parallel) | ~0s | Already parallelized |
| scan_source | ~2.7s | Partially |
| finalize + rollups | ~3.2s | No |
| build_turns | ~1.1s | Grouping: yes. Persist: no. |
| other/overhead | ~9s | Unknown |

## What to implement next

### Candidate #1b — Parallel classify

The current pipeline parses files in parallel (rayon), then writes serially.
Classification (`build_actions_in_tx_with_messages`) happens inside the serial
write phase. The CPU-bound classification work (~3-4s of the 5.6s) could be
moved to the parallel parse phase.

**Challenge:** Classification currently works on `NormalizedMessage` which has
DB-assigned IDs (`id`, `stream_id`). To classify in parallel (before DB
writes), the classification logic needs to work on `ExtractedMessage` data
without IDs. Only the persist step needs IDs.

**Approach from design doc Section 12:**
1. Extend the parallel parse phase to also classify: parse JSONL → extract
   messages → group into turns → classify actions (all in-memory)
2. Result: `ClassifiedFile` with messages, turns, actions, path_refs
3. Serial write phase: bulk INSERT all pre-computed data, assigning IDs at
   persist time

### Alternative: Channel-based pipeline

Replace the `par_iter().collect()` barrier with a producer-consumer channel
(crossbeam) to overlap parsing file N+1 with writing file N. Simpler than
parallel classify but lower expected gain.

### Alternative: Fresh perf-log run to update phase distribution

The phase distribution is stale (pre-parallel-parse, pre-skip-records). A
perf-logged run would reveal the actual current bottleneck breakdown, which
might change the priority of candidates.

## Measurement protocol

Per the design doc Section 7:
- Write a log entry skeleton BEFORE implementation starts
- Run subset first (fast feedback, ~12s), then full corpus (truth, ~27s)
- Use interleaved multi-repeat runs for reliable comparison
- Verify row parity (9 non-record tables must match baseline)
- Quality gates: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`

## Dependencies

- `rayon` already in workspace
- `crossbeam-channel` may be needed for channel-based pipeline (already a
  transitive dep via rayon)
