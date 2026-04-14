# Next Session: Parallel Parse + Classify (Import Perf Candidate #1)

**Date:** 2026-04-13
**For:** Fresh Claude session continuing the import performance optimization.

## What to do

Resume the import performance optimization project. The next candidate is
**parallel parse + classify via rayon** (candidate #1 in the ranking, Tier B).

## How to start

1. Read the session-reset protocol in `docs/specs/2026-04-10-import-perf-log.md`
   — start at the **RESUME HERE** block at the bottom. It has the full iteration
   summary, current metrics, phase distribution, candidate ranking, and bench
   harness commands.

2. Read `docs/specs/2026-04-10-import-perf-design.md` for:
   - **Section 3** — Phase 2 iteration loop (how candidates are evaluated)
   - **Section 4** — Candidate ranking (B1 is parallel processing)
   - **Section 7** — Running log protocol (how to write log entries)
   - **Section 8** — Commit & merge workflow
   - **Section 12** — Current pipeline data flow, phase distribution, and
     **detailed architecture for the parallel implementation** including the
     recommended approach, alternative approach, and constraints.

3. Create a feature branch + worktree per repo policy:
   ```bash
   cd /home/ketan/project/gnomon
   git branch import-perf-parallel main
   git worktree add .worktrees/import-perf-parallel import-perf-parallel
   ```

4. Corpus tarballs are gitignored. Symlink from an existing worktree:
   ```bash
   cd .worktrees/import-perf-parallel/tests/fixtures/import-corpus
   ln -s /home/ketan/project/gnomon/.worktrees/import-perf/tests/fixtures/import-corpus/full.tar.zst .
   ln -s /home/ketan/project/gnomon/.worktrees/import-perf/tests/fixtures/import-corpus/subset.tar.zst .
   ```

## Current state

- **Branch:** `main` at `6243b47`
- **Current best:** ~31.8s cold full import (from 126.1s baseline, -75%)
- **Target:** 10s
- **5 iterations complete:** 4 KEPT, 1 REVERTED

### What just landed (iteration 5)

In-memory data passing: `normalize_jsonl_file_in_tx` now returns
`(NormalizeJsonlFileOutcome, Vec<NormalizedMessage>)`. The `build_turns`
function iterates in-memory messages instead of querying DB. The
`build_actions_in_tx_with_messages` accepts in-memory messages, skipping the
4-way `load_messages` JOIN. This is the **prerequisite** for parallelism.

### Key files

| File | Role |
|------|------|
| `crates/gnomon-core/src/import/mod.rs` | Shared types: `Usage`, `NormalizedMessage`, `NormalizedPart` |
| `crates/gnomon-core/src/import/normalize.rs` | Parse + normalize + build_turns (in-memory) |
| `crates/gnomon-core/src/import/chunk.rs:933` | `import_chunk` — orchestrates per-chunk import |
| `crates/gnomon-core/src/classify/mod.rs` | `build_actions_in_tx_with_messages` — in-memory classify path |
| `docs/specs/2026-04-10-import-perf-log.md` | Running log with RESUME HERE block |
| `docs/specs/2026-04-10-import-perf-design.md` | Design doc with Section 12 architecture |

### Phase distribution (~31.5s wall)

| phase | time | parallelizable? |
| --- | ---: | --- |
| normalize sql_ms (INSERTs) | 9.7s | No (single SQLite writer) |
| build_actions (classify + persist) | 5.6s | Classification: yes. Persist: no. |
| parse_ms (serde_json) | 3.0s | **Yes** |
| scan_source | 2.7s | Partially |
| finalize + rollups | 3.2s | No |
| build_turns | 1.1s | Grouping: yes. Persist: no. |
| other/overhead | 6.2s | Unknown |

## What to implement

The recommended approach (from design doc Section 12):

**Phase 1 — Parallel parse (rayon thread pool):**
For each source file in a chunk, spawn a rayon task that reads the JSONL file,
parses all lines, and extracts messages + parts. No DB access. Returns a
`ParsedFile` struct with all extracted data.

**Phase 2 — Serial write (single thread):**
For each `ParsedFile`, run the existing INSERT + build_turns + build_actions
flow, fed from the pre-parsed data instead of parsing inline.

This separates the CPU-bound parsing (~3s) from the DB-bound writes (~10s) and
lets parsing happen in parallel while the writer is busy with the previous file.

A deeper approach (also in Section 12) parallelizes classification too, which
could save more of the build_actions time (~5.6s), but is more complex.

## Measurement protocol

Per the design doc Section 7:
- Write a log entry skeleton BEFORE implementation starts
- Run subset first (fast feedback, ~14s), then full corpus (truth, ~32s)
- Use interleaved multi-repeat runs for reliable comparison
- Verify row parity (all 10 table counts must match baseline)
- Quality gates: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`

## Dependencies

- `rayon` needs to be added to `Cargo.toml` for `gnomon-core`
- No other new dependencies expected
