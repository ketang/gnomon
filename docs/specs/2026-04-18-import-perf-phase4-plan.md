# Import Performance — Phase 4 Plan

> **For agentic workers:** Read the **RESUME HERE** block at the bottom of
> `2026-04-18-import-perf-phase4-log.md` first. It has the current branch,
> worktree, last completed experiment, and the next candidate to run. Do not
> re-derive state from `git status`; the log is authoritative.

**Goal:** Drive cold full-corpus import from ~20.6s to ≤10s, and ensure
first-launch UX (scan + startup window) is as fast as possible.

**Architecture:** One long-lived experiment branch (`import-perf-p4`) with a
linked worktree at `.worktrees/import-perf-p4/`. Individual candidates each
get their own throwaway branch (`import-perf-p4/<slug>`) and worktree
(`.worktrees/import-perf-p4-<slug>/`). Kept candidates merge into
`import-perf-p4`; reverted candidates are abandoned (not deleted). When
the full candidate list is exhausted or the target is met, `import-perf-p4`
merges into `main` via `git merge --no-ff`.

**Tech stack:** Rust, rusqlite/SQLite, rayon, serde_json, walkdir, jiff.

---

## 0. Context

Phase 3 closed out startup work (G1/G4 kept, S1 reverted). The kept endpoint:

| metric | value |
| --- | ---: |
| Warm startup (no-delta) | **0.230s** median |
| Delta startup (1-file change) | **0.541s** median |
| Cold full import | **~20.6s** median |
| Target (cold full) | **10s** |

CPU floor (D0 zero-write diagnostic): **2.82s subset ≈ 5.6s full corpus**.
SQL writes are **67% of wall time**. The single-threaded SQLite writer is
near its ceiling without architectural change.

**Previously tried and reverted:** deferred indexes, deferred rollups,
multi-row INSERT batching, channel pipeline, parallel classify, FK+EXCLUSIVE+WAL
pragma bundle (tested together), parallel-import-then-merge (F2a), simd-json,
path_node per-file memoization (full-corpus regression), inline rollup
materialization, background streaming, deferred startup tables, D1/D2/D2b/D3
schema experiments.

---

## 1. Candidate Ranking

Candidates are ordered by expected value × effort. Re-rank after every kept
result or fresh profile. The ranking is a live hypothesis, not a frozen plan.

### Tier 1 — Highest expected value, low–medium effort

**A3. `PRAGMA foreign_keys = OFF` on the import connection (alone)**

The E-bundle (FK=OFF + EXCLUSIVE + wal_autocheckpoint=0) was tested as a
unit and regressed (+13% full corpus). Individual isolation was never done.
FK verification adds ~2.5M btree lookups per full import (estimated 1.3–5.0s
at 0.5–2µs each). Disabling it on the import connection (which inserts in
guaranteed parent-before-child order by construction) should eliminate those
lookups with zero correctness risk. A post-import `PRAGMA foreign_key_check`
can verify integrity if desired.

Code location: `crates/gnomon-core/src/db/mod.rs:configure_read_write_connection`.
Add `PRAGMA foreign_keys = OFF;` to the `execute_batch` call, guarded by a
new `configure_import_connection` function called only by the importer.

Expected: **1–3s** on full corpus.
Risk: low. Import data comes from a trusted, parser-validated source.

---

**B1. `:memory:` staging database → `VACUUM INTO` final path**

All chunk writes go to an in-process `:memory:` SQLite. At chunk end, instead
of committing to the WAL-backed disk file, call `VACUUM INTO <final_path>`.
In-memory btree operations avoid WAL overhead, fsync, shared-memory lock
acquisition, and page cache thrashing from concurrent WAL reads. The merge is
a sequential page copy, not a row-by-row INSERT.

This is distinct from Iteration 13 ("in-memory staging"), which cached Rust
structs in memory but still wrote to the disk-backed SQLite. This experiment
uses SQLite itself as the in-memory staging layer.

Key constraint: the `:memory:` DB needs the same schema as the production DB.
Apply migrations against it at chunk start. The `VACUUM INTO` call requires
SQLite ≥ 3.27 (bundled version is current, so this is satisfied).

Expected: **4–8s** on full corpus (memory writes are 10–100× faster than
WAL-backed disk writes for this workload size).
Risk: medium. Requires careful handling of per-chunk scope: the `:memory:` DB
must be seeded with the project/source_file/import_chunk rows from the
production DB before writing, then the written rows merged back.
Parity check is essential.

---

**A1. SQLite page size: 8192 or 16384 bytes**

Default page size is 4096. Larger pages reduce btree depth (fewer levels to
traverse per lookup) and amortize page-header overhead across more rows per
page. The import workload has large sequential inserts — large pages reduce
the number of page splits and internal node updates.

Must be set before any data is written (`PRAGMA page_size = N` has no effect
on an existing DB). The import bench harness creates a fresh DB per run, so
this is testable directly. Production use requires `db reset --force` before
the first import on a new page size (documented in README).

Code location: add to `configure_read_write_connection` or set as the first
pragma before migrations.

Expected: **1–3s** on full corpus (fewer btree traversals per insert).
Risk: low. Fully reversible; DB is disposable per the reimport-on-schema-bump
contract.

---

**A4. `PRAGMA locking_mode = EXCLUSIVE` (alone)**

The import connection is the sole writer during bulk import. `EXCLUSIVE` mode
avoids acquiring and releasing shared locks on every read/write operation.
Tested only as part of the E bundle (which regressed overall); individual
isolation never measured.

Code location: same as A3 — `configure_import_connection`.
Expected: **0.3–0.8s**. Risk: near zero.

---

**A5. `PRAGMA wal_autocheckpoint = 0` + manual checkpoint at end**

Disables automatic WAL consolidation during import. The WAL grows until a
manual `PRAGMA wal_checkpoint(TRUNCATE)` at import end. Prevents mid-import
WAL checkpoint stalls. Tested only as part of the E bundle.

Code location: `configure_import_connection`; add `wal_checkpoint(TRUNCATE)`
call in `finalize_chunk_import_core` or after all chunks complete.
Expected: **0.5–1.5s** (eliminates mid-import checkpoint stalls).
Risk: low. WAL file grows to ~DB size temporarily; on tmpfs (bench) this is free.

---

**A6. Struct-based serde deserialization (not `serde_json::Value`)**

`parse_jsonl_file` deserializes each line into `serde_json::Value` — a fully
general tree that allocates for every JSON node. A concrete Rust struct with
`#[derive(Deserialize)]` is 2–4× faster for the same data because serde skips
unknown fields and avoids heap allocation for known scalar fields.

The JSONL source has a known schema: `type`, `uuid`, `sessionId`, `cwd`,
`message`, `parentUuid`, `isSidechain`, `userType`, `version`, `gitBranch`,
`timestamp`. A `RawSourceRecord` struct with `#[serde(borrow)]` slices into
the original line buffer for string fields.

Code location: `crates/gnomon-core/src/import/normalize.rs` — replace the
`serde_json::from_str::<Value>` call in `parse_jsonl_file_inner` and downstream
field accesses.

Expected: **0.5–1.5s** on full corpus (reduces allocations in the 3.0s parse phase).
Risk: low-medium. Requires touching field-extraction logic throughout normalize.rs.

---

### Tier 2 — Moderate expected value, lower effort

**A2. LTO + PGO on release binary**

LTO: enables cross-crate inlining at link time (rusqlite, serde_json, rayon
hot paths inline into gnomon). PGO: recompile using a real execution profile
so the compiler can optimize for actual hot branches.

Workflow:
1. Build with instrumentation: `RUSTFLAGS="-Cprofile-generate=/tmp/pgo-data" cargo build --release`
2. Run the bench harness against the corpus to collect profiles.
3. Merge: `llvm-profdata merge -output=/tmp/pgo-merged.profdata /tmp/pgo-data/*.profraw`
4. Rebuild: `RUSTFLAGS="-Cprofile-use=/tmp/pgo-merged.profdata -Clinker-plugin-lto" cargo build --release`

LTO flag for Cargo.toml: `[profile.release] lto = "thin"` or `lto = true`.

Expected: **5–15% total wall reduction** (applies to the ~33% non-SQL phases).
Risk: low. Only affects release build; debug/test builds unchanged.

---

**A7. `jwalk` for parallel directory walk in `scan_source`**

`discover_source_files` uses `walkdir::WalkDir` (serial). `jwalk` parallelizes
directory traversal using rayon. On a cold filesystem cache this can halve the
walk time. On WSL2 with ext4 this is meaningful because directory reads are
not cached between runs.

Code location: `crates/gnomon-core/src/import/source.rs` — replace `WalkDir`
in `discover_source_files_inner`. Add `jwalk = "0.8"` to
`crates/gnomon-core/Cargo.toml`.

Expected: **0.2–0.5s** (scan_source is ~2.8s; walk is a fraction of that).
Risk: low.

---

**A8. path_node chunk-level cache (HashMap across all files in a chunk)**

`persist_path_refs` → `ensure_path_node_chain` does `SELECT`/`INSERT` per
path component. G2 tested per-file memoization and the full corpus regressed
(repeated path-node lookups were not the dominant cost at per-file scope).
This experiment is different: a `HashMap<(project_id, full_path), path_node_id>`
that persists across ALL files within a single chunk. Paths like `/home/ketan/project/gnomon/crates/gnomon-core/src/` appear in many files of the same project-day chunk; caching them across files avoids repeated SELECT+INSERT for shared path prefixes.

Code location: `crates/gnomon-core/src/classify/mod.rs` — thread a
`&mut HashMap<String, i64>` through `classify_and_persist_actions` →
`persist_path_refs` → `ensure_path_node_chain`. Initialize in `import_chunk`
before the file loop, pass per-file.

Expected: **0.3–1.0s**. Risk: low. Cache is scoped to one chunk; no
cross-chunk state.

---

### Tier 3 — Higher effort, higher ceiling

**B2. Parallel per-chunk `:memory:` DBs + merge**

Extension of B1: if B1 shows that `:memory:` staging is fast, run multiple
chunks in parallel (rayon), each with its own `:memory:` DB, then merge all
into the production DB sequentially. Addresses the single-writer bottleneck.

Only attempt if B1 is KEPT. The merge step is the critical constraint: row IDs
must not conflict. Use pre-assigned non-overlapping ID ranges per chunk
(chunk_N starts IDs at `N * 10_000_000`).

Expected: additional **4–8s** reduction on top of B1 (parallelizes the
currently-serial write phase across available cores).
Risk: medium-high. ID range pre-assignment, FK integrity, correct ordering.

---

**C1. Permanent per-project SQLite shards + global metadata DB (F2c)**

Each project gets its own `.sqlite3` file. A small global DB stores
cross-project aggregates: project list, filter options, snapshot bounds,
global rollups. Import writes to project shards in parallel (rayon). The TUI
root view reads the global metadata DB; drill-down ATTACHes the relevant shard.

This is the highest-ceiling option and requires a read-path rewrite. Only
attempt if the Tier 1–2 candidates do not reach 10s.

Expected: potentially **10–15s** reduction (full write parallelism across 31
projects). Risk: high. Requires auditing and rewriting most of `query/mod.rs`.

---

## 2. Session Protocol

### Starting a new experiment

```bash
# From the long-lived branch worktree
cd /path/to/repo/.worktrees/import-perf-p4

# Create per-candidate branch + worktree
git branch import-perf-p4/<slug> import-perf-p4
git worktree add ../.worktrees/import-perf-p4-<slug> import-perf-p4/<slug>
cd ../.worktrees/import-perf-p4-<slug>

# Symlink corpus fixtures
cd tests/fixtures/import-corpus
ln -s /home/ketan/project/gnomon/tests/fixtures/import-corpus/full.tar.zst .
ln -s /home/ketan/project/gnomon/tests/fixtures/import-corpus/subset.tar.zst .
```

### Measurement protocol

1. Write a log entry skeleton in `2026-04-18-import-perf-phase4-log.md`
   **before** implementation starts (hypothesis + branch name).
2. Build release: `cargo build --release -p gnomon`
3. Run subset first (fast feedback, ~8s baseline):
   ```bash
   cargo run --release --example import_bench -- --subset --repeat 5
   ```
4. Run full corpus (authoritative, ~20s baseline):
   ```bash
   cargo run --release --example import_bench -- --full --repeat 3
   ```
5. Interleave baseline and candidate runs for within-session comparison.
6. Verify row parity: 9 non-record tables must match baseline row counts.
7. Run quality gates:
   ```bash
   cargo fmt --all
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   cargo build --workspace
   ```
8. Run integration tests if import pipeline changed:
   ```bash
   cargo test -p gnomon-core --test import_corpus_integration -- --include-ignored
   ```

### Decision criteria

- **KEPT:** subset improves AND full corpus confirms; row parity PASS; quality gates pass;
  **query layer PASS**; **no divergent write paths**.
- **REVERTED:** any regression on full corpus; parity failure; or marginal gain (<2%) not
  worth the complexity.

**Query layer PASS** requires at least one of:
- An automated test that runs `QueryEngine::filter_options` / `browse` against the kept-db
  and asserts the top-level project rows are non-empty and sum to the expected action count.
- A manual spot-check of `cargo run -p gnomon -- --db <kept-db> report` showing a non-empty
  `rows` array whose per-project `item_count` values sum to the bench's action row count.

The automated test is preferred. The manual spot-check is acceptable only when the candidate
doesn't move data between schemas or tables — once a candidate changes *where data lives*
(sharding, view-based reads, etc.), automated query-layer coverage is mandatory.

**No divergent write paths** means: if the candidate changes how imports write data (new
tables, new shards, new ordering), every production entry point (`import_all`, `run_import_worker`,
anything called from the TUI or the CLI) must use the new path. Keeping one path on the old
architecture and another on the new one creates a correctness hazard (data from the TUI path
may become invisible through the CLI path or vice versa). Audit by: `grep -rn "Database::open_for_import\|fn import_chunk"` and verify there is a single write path.

Decision rationale for the first attempt at candidate C1 (committed as 8aa0e9a + d36b289,
later superseded): declared KEPT with the above criteria absent. `gnomon report` returned
`"rows": []` because the query layer read rollups from the main DB, but rollups lived in
shards only. The failing integration tests masked this by panicking on a pre-existing
`imported_record_count_sum = 0` bug *before* reaching any query-layer assertion. The
revised criteria above are the direct response.

### After a KEPT result

1. Write full log entry (hypothesis, measurements, parity, profile shift, decision).
2. Surface summary to user; request commit approval.
3. On approval:
   - Commit code on candidate branch.
   - Merge candidate branch into `import-perf-p4` with `--no-ff`.
   - Commit log update separately (`log: <slug> kept — <delta>`).
   - Update Resume Block.
4. Re-rank candidates if profile changed.

### After a REVERTED result

1. Write log entry (result, key finding).
2. Leave candidate worktree alone (do not delete).
3. Update Resume Block with next candidate.
4. Produce next-session prompt.

### Quality gates (every commit)

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace
```

---

## 3. Branch and Worktree Setup

```bash
# One-time setup of the long-lived branch and worktree
cd /home/ketan/project/gnomon
git branch import-perf-p4 main
git worktree add .worktrees/import-perf-p4 import-perf-p4

cd .worktrees/import-perf-p4
mkdir -p tests/fixtures/import-corpus
cd tests/fixtures/import-corpus
ln -s /home/ketan/project/gnomon/tests/fixtures/import-corpus/full.tar.zst .
ln -s /home/ketan/project/gnomon/tests/fixtures/import-corpus/subset.tar.zst .
```

---

## 4. Log File

Running log: `docs/specs/2026-04-18-import-perf-phase4-log.md`

Same format as `2026-04-10-import-perf-log.md`:
- **Frozen Header**: environment fingerprint, corpus SHAs, baseline metrics, target.
- **Phase Log**: append-only candidate entries.
- **Resume Block**: always overwritten, at the bottom.

### Candidate entry template

```
## <date> <time> — candidate <id>: <short description>
Branch: import-perf-p4/<slug>
Worktree: .worktrees/import-perf-p4-<slug>
Hypothesis: <why this should help>
Implementation: <1-2 sentences, file:line refs>
Measurements:
  Subset:       <before> → <after> (<delta>)
  Full:         <before> → <after> (<delta>)
  Row parity:   PASS | FAIL (<details>)
  Profile shift: <phase percentages before/after if measured>
Decision: KEPT | REVERTED | PENDING USER
Commit: <sha or blank>
Key finding: <what this result teaches us>
Next implied: <next candidate and why>
```

### Resume Block template (always at bottom of log)

```
## RESUME HERE

Phase: Phase 4
Long-lived branch: import-perf-p4
Long-lived worktree: .worktrees/import-perf-p4
Last completed: <candidate id> — <KEPT|REVERTED>
Next action: <exact next step>
Current best (subset): <time>s
Current best (full): <time>s
Target: 10s
In-flight uncommitted state: <none | description>

Candidate ranking (live):
1. <id> — <description> — <reason still top>
2. ...
```

---

## 5. Experiment Steps (per candidate)

Each agent session executes ONE candidate, then hands off.

### Step 0: Session start

- [ ] Read `2026-04-18-import-perf-phase4-log.md` — start at **RESUME HERE** block.
- [ ] If this is the very first session and the Frozen Header is unpopulated,
  run a baseline bench immediately after worktree setup and populate it:
  ```bash
  rustc --version
  cargo run --release --example import_bench -- --full --repeat 3
  cargo run --release --example import_bench -- --subset --repeat 5
  # Also capture: uname -a, lscpu, cat tests/fixtures/import-corpus/MANIFEST.md
  ```
- [ ] Confirm the branch and worktree from the Resume Block exist:
  ```bash
  git worktree list
  ```
- [ ] If the long-lived worktree does not exist, create it per Section 3 above.
- [ ] Create the candidate branch + worktree per Section 2 above.

---

### Step 1: Write log entry skeleton

Before touching code, open `docs/specs/2026-04-18-import-perf-phase4-log.md`
and append a candidate entry with hypothesis filled in, measurements blank.
Commit the skeleton to the long-lived branch:
```bash
git add docs/specs/2026-04-18-import-perf-phase4-log.md
git commit -m "log: <slug> skeleton — hypothesis written"
```

---

### Step 2: Implement

Implement the candidate in the candidate worktree. See Section 1 for
file:line references per candidate.

---

### Step 3: Measure

Run the bench harness per Section 2. Record raw numbers.

---

### Step 4: Verify parity

```bash
# In candidate worktree after a full-corpus bench run:
cargo test -p gnomon-core --test import_corpus_integration -- --include-ignored
```

Also spot-check representative queries:
```bash
cargo run -p gnomon -- --db /tmp/gnomon-bench-candidate/usage.sqlite3 report
```

---

### Step 5: Quality gates

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace
```

---

### Step 6: Fill in log entry + decide

Update the candidate entry in the log with measurements and decision.
Update the Resume Block with current best and next candidate.

---

### Step 7: If KEPT — request commit approval

Surface to user:
> "Candidate `<id>` KEPT. Subset: Xs → Ys (−Z%). Full: Xs → Ys (−Z%). Row parity PASS.
> Quality gates pass. Recommend committing and merging into `import-perf-p4`. Approve?"

On approval:
```bash
# In candidate worktree
cargo fmt --all
git add -p   # stage code changes
git commit -m "perf(import): <slug> — <one-line description>"

# In long-lived worktree
cd /home/ketan/project/gnomon/.worktrees/import-perf-p4
git merge --no-ff import-perf-p4/<slug> -m "Merge import-perf-p4/<slug>"

# Commit the log update
git add docs/specs/2026-04-18-import-perf-phase4-log.md
git commit -m "log: <slug> kept — <delta summary>"
```

---

### Step 8: Produce next-session prompt

Append to the log and print to user:

```
Continue Phase 4 import-perf work.
Branch: import-perf-p4
Long-lived worktree: /home/ketan/project/gnomon/.worktrees/import-perf-p4

Before changing code, read:
1. docs/specs/2026-04-18-import-perf-phase4-log.md  (start at RESUME HERE)
2. docs/specs/2026-04-18-import-perf-phase4-plan.md  (candidate descriptions)

Last completed: <id> — <KEPT|REVERTED>
Result: <key measurement or finding>

Next candidate: <id> — <description>
<one sentence on what to implement>

Follow the session protocol in Section 2 of the plan.
```

---

## 6. Merge to Main

When the target is met (full corpus ≤10s on two consecutive runs) or all
candidates are exhausted:

1. Write final log entry with cumulative delta and reason for stopping.
2. Summarize to user: total reduction, candidates kept, candidates reverted.
3. Request merge approval:
   ```bash
   cd /home/ketan/project/gnomon
   git merge --no-ff .worktrees/import-perf-p4 -m "Merge import-perf-p4: Phase 4 perf work"
   git push origin main
   ```

---

## 7. Initial Candidate Order (starting ranking)

| # | ID | Description | Effort | Expected gain |
|---|----|----|----|----|
| 1 | A3 | `PRAGMA foreign_keys = OFF` (import connection only) | 30 min | 1–3s |
| 2 | B1 | `:memory:` staging DB → `VACUUM INTO` | 4–8h | 4–8s |
| 3 | A1 | SQLite page size 8K or 16K | 1h | 1–3s |
| 4 | A4 | `PRAGMA locking_mode = EXCLUSIVE` (alone) | 30 min | 0.3–0.8s |
| 5 | A5 | `PRAGMA wal_autocheckpoint = 0` + manual checkpoint | 1h | 0.5–1.5s |
| 6 | A6 | Struct-based serde (replace `serde_json::Value`) | 4–8h | 0.5–1.5s |
| 7 | A2 | LTO + PGO | 2h | 5–15% total wall |
| 8 | A7 | `jwalk` parallel directory walk | 1h | 0.2–0.5s |
| 9 | A8 | path_node chunk-level cache (across files in chunk) | 2h | 0.3–1.0s |
| 10 | B2 | Parallel per-chunk `:memory:` DBs + merge (needs B1 first) | 1–2 days | 4–8s addl |
| 11 | C1 | Per-project sharding + global metadata DB (F2c) | 1–2 weeks | 10–15s |

Re-rank after every KEPT result or fresh CPU profile. Do not treat this as
frozen.
