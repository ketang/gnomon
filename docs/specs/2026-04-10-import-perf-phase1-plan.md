# Import Perf — Phase 1 (Measure) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Capture a reproducible baseline of gnomon's session-import performance — startup-mode wall time and cold full-import wall time — with per-phase attribution, against a frozen corpus snapshot. Produces the data needed to set a concrete performance target before any optimization work begins.

**Architecture:** Wire the existing `PerfLogger` into the import hot path via spans added to `import_chunk`, `normalize_transcript_jsonl_file`, `build_turns`, `build_actions`, and the rollup rebuild functions, plus a parse-vs-SQL split inside the per-record loop. Create a deterministic corpus snapshot (full plus sampled subset) under `tests/fixtures/import-corpus/`. Add a `gnomon-core` example binary (`import_bench`) that extracts a snapshot into a tmpdir, runs either full cold import or startup-mode import against a fresh SQLite file, and emits metrics. Capture baseline runs and one CPU profile each for the two primary scenarios. End with a user checkpoint to agree a target number.

**Tech Stack:** Rust 2024, rusqlite (bundled), existing `PerfLogger` (`crates/gnomon-core/src/perf.rs`), `tempfile`, `walkdir`, `jiff`, `clap`. External tools for snapshot capture: `tar`, `zstd`, `sha256sum`. External tool for profiling: `samply`.

**Design spec:** `docs/specs/2026-04-10-import-perf-design.md`. Running log: `docs/specs/2026-04-10-import-perf-log.md` (created in Task 1).

**Branch & worktree:** all work happens on `import-perf` in `.worktrees/import-perf/`. Per-task branches are not required inside Phase 1 — the measurement scaffolding lands as a single coherent feature on `import-perf`. (Per-candidate branches start in Phase 2.)

---

## File Structure

### Created in this plan
- `docs/specs/2026-04-10-import-perf-log.md` — running log, committed (Task 1).
- `.gitignore` entries for corpus tarballs (Task 2).
- `tests/fixtures/import-corpus/.gitkeep` — committed (Task 2).
- `tests/fixtures/import-corpus/capture.sh` — committed snapshot script (Task 3).
- `tests/fixtures/import-corpus/MANIFEST.md` — committed corpus manifest (Task 4).
- `tests/fixtures/import-corpus/full.tar.zst` — gitignored tarball (Task 4, user-local).
- `tests/fixtures/import-corpus/subset.tar.zst` — gitignored tarball (Task 4, user-local).
- `crates/gnomon-core/examples/import_bench.rs` — benchmark harness binary (Task 9).
- `docs/specs/profiles/` directory for flamegraphs (Task 12).

### Modified in this plan
- `crates/gnomon-core/src/import/chunk.rs` — add `perf_logger: Option<PerfLogger>` to `ImportWorkerOptions`; thread through `import_chunk`; add spans (Tasks 5, 7).
- `crates/gnomon-core/src/import/normalize.rs` — accept optional logger, add spans in `normalize_transcript_jsonl_file` and per-record loop (Tasks 6, 8).
- `crates/gnomon-core/src/import/mod.rs` — export helper to construct `ImportWorkerOptions` with a logger, re-export as needed (Task 5).
- `crates/gnomon-core/src/classify/mod.rs` — add span around `build_actions` (Task 7).
- `crates/gnomon-core/src/rollup.rs` — add spans around `rebuild_chunk_*_rollups` (Task 7).
- `crates/gnomon-core/Cargo.toml` — no new workspace deps expected; may add `clap` if the example needs it as a dev-dependency.

---

## Preflight Checks

- [x] **PF1: Confirm on the correct branch and worktree**

Run:
```bash
git -C /home/ketan/project/gnomon/.worktrees/import-perf rev-parse --abbrev-ref HEAD
pwd
```
Expected: branch is `import-perf`, pwd is `/home/ketan/project/gnomon/.worktrees/import-perf` (or switch to it).

- [x] **PF2: Confirm design doc exists on this branch**

Run: `ls docs/specs/2026-04-10-import-perf-design.md`
Expected: file exists.

- [x] **PF3: Confirm external tools available**

Run: `which tar zstd sha256sum samply`
Expected: first three present; `samply` may be absent — if so, install with `cargo install samply` before Task 12.

---

## Task 1: Initialize the running log

**Files:**
- Create: `docs/specs/2026-04-10-import-perf-log.md`

The log is initialized with an empty Frozen Header, an empty Phase Log section, and a placeholder Resume Block. It gets populated as Phase 1 progresses.

- [x] **Step 1: Create the log file**

Write `docs/specs/2026-04-10-import-perf-log.md` with exactly this content:

```markdown
# Import Perf — Running Log

> Companion to `docs/specs/2026-04-10-import-perf-design.md`. Append-only phase log plus overwritten Resume Block. First action of any fresh session: read the Resume Block, then the last 2–3 phase entries, then continue.

## Environment
- Host: _(to be captured in Phase 1)_
- CPU: _(to be captured)_
- RAM: _(to be captured)_
- WSL filesystem for repo: _(to be captured)_
- WSL filesystem for DB path (`~/.local/share/gnomon/`): _(to be captured)_
- Rust: _(to be captured)_
- SQLite (bundled): _(to be captured)_

## Corpus Snapshot
- Manifest: `tests/fixtures/import-corpus/MANIFEST.md`
- Full SHA256: _(to be captured)_
- Subset SHA256: _(to be captured)_

## Baseline
_(to be captured in Phase 1 — Task 13)_

## Target
_(to be agreed with user at end of Phase 1 — Task 14)_

---

## Phase Log

### 2026-04-10 — Phase 1 started
Kicked off Phase 1 (measure). Design doc committed on `import-perf`. Running log initialized.

---

## RESUME HERE (if session was reset, read this first)

Last updated: 2026-04-10
Current phase: Phase 1 — measure (pre-baseline)
Current branch: import-perf (worktree: .worktrees/import-perf)
Last completed: Running log initialized.
Next action: Task 2 — add .gitignore entries and create the fixture directory.
Uncommitted state: none
Target status: not set (pending Task 14)
Candidate ranking: see design doc Section 4; live re-ranking begins in Phase 2.
```

- [x] **Step 2: Commit the log**

Run:
```bash
git add docs/specs/2026-04-10-import-perf-log.md
git commit -m "log: initialize import perf running log

Companion to the import-perf design doc. Populated as Phase 1
progresses.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```
Expected: one commit on `import-perf` adding one file.

- [x] **Step 3: Update Resume Block to reflect commit**

Edit the Resume Block in the file you just committed so `Last completed` reads `Running log committed (sha <new-sha>).` and `Next action` stays on Task 2. Commit with message `log: update resume block after Task 1`.

---

## Task 2: Gitignore corpus tarballs and create fixture directory

**Files:**
- Modify: `.gitignore`
- Create: `tests/fixtures/import-corpus/.gitkeep`

- [x] **Step 1: Append to `.gitignore`**

Append these lines to the end of `.gitignore` at the repo root:

```
# Import perf corpus snapshot tarballs — user-local, never pushed.
tests/fixtures/import-corpus/*.tar.zst
```

- [x] **Step 2: Create the fixture directory and keepfile**

Run:
```bash
mkdir -p tests/fixtures/import-corpus
: > tests/fixtures/import-corpus/.gitkeep
```

- [x] **Step 3: Verify gitignore works**

Create a dummy tarball, confirm git ignores it, remove it:
```bash
: > tests/fixtures/import-corpus/sanity.tar.zst
git status --porcelain tests/fixtures/import-corpus/
rm tests/fixtures/import-corpus/sanity.tar.zst
```
Expected: the `.gitkeep` shows as untracked but `sanity.tar.zst` does not appear.

- [x] **Step 4: Commit**

Run:
```bash
git add .gitignore tests/fixtures/import-corpus/.gitkeep
git commit -m "chore: reserve tests/fixtures/import-corpus for perf snapshots

Committed directory placeholder and gitignore for local tarballs
used by the import-perf benchmark harness. Tarballs themselves
stay local per the design doc privacy note.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

- [x] **Step 5: Update Resume Block**

Edit the log: `Last completed` → `Task 2: corpus fixture dir reserved and gitignored.` `Next action` → `Task 3: write capture script.` Commit as `log: update resume block after Task 2`.

---

## Task 3: Write the corpus capture script

**Files:**
- Create: `tests/fixtures/import-corpus/capture.sh`

This script captures `~/.claude/projects` and `~/.claude/history.jsonl` into `full.tar.zst`, selects a subset by largest projects, captures it into `subset.tar.zst`, computes SHAs, and writes `MANIFEST.md`. Running it is Task 4.

- [x] **Step 1: Write the script**

Create `tests/fixtures/import-corpus/capture.sh` with exactly this content:

```bash
#!/usr/bin/env bash
# Capture a frozen snapshot of ~/.claude session data for import-perf benchmarking.
# Output: full.tar.zst, subset.tar.zst, MANIFEST.md in this directory.
# Idempotent: overwrites existing tarballs and manifest.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

src_root="${HOME}/.claude"
projects_dir="${src_root}/projects"
history_file="${src_root}/history.jsonl"

if [[ ! -d "${projects_dir}" ]]; then
  echo "ERROR: ${projects_dir} does not exist" >&2
  exit 1
fi

staging="$(mktemp -d)"
trap 'rm -rf "${staging}"' EXIT

echo "Staging full corpus at ${staging}/full ..."
mkdir -p "${staging}/full"
cp -a "${projects_dir}" "${staging}/full/projects"
if [[ -f "${history_file}" ]]; then
  cp -a "${history_file}" "${staging}/full/history.jsonl"
fi

full_bytes="$(du -sb "${staging}/full" | awk '{print $1}')"
full_files="$(find "${staging}/full" -type f -name '*.jsonl' | wc -l)"
full_projects="$(find "${staging}/full/projects" -mindepth 1 -maxdepth 1 -type d | wc -l)"

echo "Creating full.tar.zst ..."
tar -C "${staging}/full" -cf - . | zstd -19 -T0 -o full.tar.zst --force
full_sha="$(sha256sum full.tar.zst | awk '{print $1}')"

echo "Selecting subset (largest projects, target min(5% of full, 100MB)) ..."
target_bytes=$(( full_bytes / 20 ))
max_bytes=$(( 100 * 1024 * 1024 ))
if (( target_bytes > max_bytes )); then
  target_bytes=${max_bytes}
fi

mkdir -p "${staging}/subset/projects"
if [[ -f "${staging}/full/history.jsonl" ]]; then
  cp -a "${staging}/full/history.jsonl" "${staging}/subset/history.jsonl"
fi

# List projects by size desc; add whole projects until we cross target_bytes.
mapfile -t sorted_projects < <(
  find "${staging}/full/projects" -mindepth 1 -maxdepth 1 -type d -print0 \
    | xargs -0 -I{} du -sb "{}" \
    | sort -rn \
    | awk '{print $2}'
)

accum=0
selected=0
for proj in "${sorted_projects[@]}"; do
  if (( accum >= target_bytes )); then
    break
  fi
  cp -a "${proj}" "${staging}/subset/projects/"
  proj_bytes="$(du -sb "${proj}" | awk '{print $1}')"
  accum=$(( accum + proj_bytes ))
  selected=$(( selected + 1 ))
done

subset_bytes="$(du -sb "${staging}/subset" | awk '{print $1}')"
subset_files="$(find "${staging}/subset" -type f -name '*.jsonl' | wc -l)"
subset_projects="${selected}"

echo "Creating subset.tar.zst ..."
tar -C "${staging}/subset" -cf - . | zstd -19 -T0 -o subset.tar.zst --force
subset_sha="$(sha256sum subset.tar.zst | awk '{print $1}')"

echo "Writing MANIFEST.md ..."
{
  echo "# Import Perf Corpus Snapshot Manifest"
  echo ""
  echo "Captured: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "Host: $(uname -a)"
  echo "Filesystem (this directory): $(stat -f -c '%T' . 2>/dev/null || stat --file-system --format='%T' . 2>/dev/null || echo unknown)"
  echo ""
  echo "## Full corpus"
  echo "- path: full.tar.zst"
  echo "- uncompressed_bytes: ${full_bytes}"
  echo "- jsonl_file_count: ${full_files}"
  echo "- project_count: ${full_projects}"
  echo "- sha256: ${full_sha}"
  echo ""
  echo "## Subset"
  echo "- path: subset.tar.zst"
  echo "- selection: largest projects by bytes until >= min(5% full, 100MB)"
  echo "- uncompressed_bytes: ${subset_bytes}"
  echo "- jsonl_file_count: ${subset_files}"
  echo "- project_count: ${subset_projects}"
  echo "- sha256: ${subset_sha}"
} > MANIFEST.md

echo "Done."
echo "  full.tar.zst    $(du -h full.tar.zst | awk '{print $1}')   sha256=${full_sha}"
echo "  subset.tar.zst  $(du -h subset.tar.zst | awk '{print $1}') sha256=${subset_sha}"
```

- [x] **Step 2: Make executable**

Run: `chmod +x tests/fixtures/import-corpus/capture.sh`

- [x] **Step 3: Commit**

Run:
```bash
git add tests/fixtures/import-corpus/capture.sh
git commit -m "feat: add corpus capture script for import-perf benchmarks

Captures ~/.claude/projects and history.jsonl into reproducible
full and subset tarballs, computes SHA256s, writes a manifest.
Largest-first project subsetting preserves chunking skew.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

- [x] **Step 4: Update Resume Block**

`Last completed` → `Task 3: capture script written.` `Next action` → `Task 4: run capture.sh and write MANIFEST.md.` Commit.

---

## Task 4: Run capture, commit MANIFEST.md

**Files:**
- Create: `tests/fixtures/import-corpus/MANIFEST.md` (via script)

**User checkpoint:** This task requires running against the user's real `~/.claude` data, which may take several minutes and consume gigabytes of disk. Pause and confirm before running.

- [x] **Step 1: Confirm with user**

Surface: "About to run `tests/fixtures/import-corpus/capture.sh` — this reads your real `~/.claude` data, writes two zstd-compressed tarballs under `tests/fixtures/import-corpus/` (gitignored), and may take several minutes. OK to proceed?"
Wait for approval.

- [x] **Step 2: Run capture**

Run: `./tests/fixtures/import-corpus/capture.sh`
Expected: script completes, prints SHAs, creates `full.tar.zst`, `subset.tar.zst`, `MANIFEST.md`.

- [x] **Step 3: Verify gitignore behavior one more time**

Run: `git status --porcelain tests/fixtures/import-corpus/`
Expected: only `MANIFEST.md` appears as untracked; tarballs do not.

- [x] **Step 4: Commit MANIFEST**

Run:
```bash
git add tests/fixtures/import-corpus/MANIFEST.md
git commit -m "chore: commit import-corpus manifest for initial snapshot

Records file counts, byte counts, and SHA256s for full and subset
tarballs captured on $(date -u +%Y-%m-%d). Tarballs themselves
stay local.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

- [x] **Step 5: Update log Corpus Snapshot section and Resume Block**

Copy the SHAs from MANIFEST.md into the Frozen Header's Corpus Snapshot section. Update Resume Block: `Last completed` → `Task 4: corpus captured, manifest committed.` `Next action` → `Task 5: wire PerfLogger into ImportWorkerOptions.` Commit as `log: record corpus SHAs`.

---

## Task 5: Add `perf_logger` to `ImportWorkerOptions` and thread through `import_chunk`

**Files:**
- Modify: `crates/gnomon-core/src/import/chunk.rs:66-69`, `:828-901`

The existing `ImportWorkerOptions` is private with one field (`per_chunk_delay`). We add an `Option<PerfLogger>` field, construct it from env in the public entry points (`import_all`, `start_startup_import_with_progress`), and use it inside `import_chunk` to emit a span covering the whole chunk.

- [x] **Step 1: Add the field and import**

At the top of `crates/gnomon-core/src/import/chunk.rs`, add to existing imports:

```rust
use crate::perf::{PerfLogger, PerfScope};
```

Replace the `ImportWorkerOptions` struct (currently at lines 66-69):

```rust
#[derive(Debug, Clone, Default)]
struct ImportWorkerOptions {
    per_chunk_delay: Duration,
    perf_logger: Option<PerfLogger>,
}
```

- [x] **Step 2: Update `import_all` to build a logger**

Replace the body of `import_all` (lines 239-290). Build a logger from env using the db_path's parent as state_dir, then pass it into both loops:

```rust
pub fn import_all(
    conn: &Connection,
    db_path: &Path,
    source_root: &Path,
) -> Result<ImportExecutionReport> {
    let now = Timestamp::now();
    let time_zone = TimeZone::system();
    let plan = build_import_plan(conn, now, &time_zone)?;
    let prepared = prepare_import_plan(conn, &plan)?;
    let mut database =
        Database::open(db_path).with_context(|| format!("unable to open {}", db_path.display()))?;

    let state_dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    let perf_logger = PerfLogger::from_env(state_dir).ok().flatten();
    let options = ImportWorkerOptions {
        perf_logger: perf_logger.clone(),
        ..ImportWorkerOptions::default()
    };

    for chunk in &prepared.startup_chunks {
        import_chunk(&mut database, source_root, chunk, &options).with_context(|| {
            format!(
                "unable to import startup chunk {}:{}",
                chunk.project_key, chunk.chunk_day_local
            )
        })?;
    }

    let mut deferred_failures = Vec::new();
    for chunk in &prepared.deferred_chunks {
        if let Err(err) =
            import_chunk(&mut database, source_root, chunk, &options).with_context(|| {
                format!(
                    "unable to import deferred chunk {}:{}",
                    chunk.project_key, chunk.chunk_day_local
                )
            })
        {
            deferred_failures.push(compact_status_text(format!("{err:#}")));
        }
    }

    Ok(ImportExecutionReport {
        startup_chunk_count: prepared.startup_chunks.len(),
        deferred_chunk_count: prepared.deferred_chunks.len(),
        deferred_failure_count: deferred_failures.len(),
        deferred_failure_summary: summarize_deferred_failures(&deferred_failures),
    })
}
```

- [x] **Step 3: Update `start_startup_import_with_mode_and_progress` to build a logger**

At the top of its body (currently creating `ImportWorkerOptions::default()` at line 234), construct the logger from `db_path.parent()` and plumb it through the same way. Replace the call site:

```rust
    let state_dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    let perf_logger = PerfLogger::from_env(state_dir).ok().flatten();
    let options = ImportWorkerOptions {
        perf_logger,
        ..ImportWorkerOptions::default()
    };
    start_startup_import_with_options(
        conn,
        db_path,
        source_root,
        Duration::from_secs(STARTUP_OPEN_DEADLINE_SECS),
        import_mode,
        options,
        Some(&mut on_progress),
    )
```

- [x] **Step 4: Emit a span in `import_chunk`**

In `import_chunk` (line 828), immediately after `begin_chunk_import` and the optional delay, wrap the inner `(|| { ... })()` closure in a perf scope. Replace the existing function body with:

```rust
fn import_chunk(
    database: &mut Database,
    source_root: &Path,
    chunk: &PreparedChunk,
    options: &ImportWorkerOptions,
) -> Result<()> {
    begin_chunk_import(database.connection_mut(), chunk)?;

    if options.per_chunk_delay > Duration::ZERO {
        thread::sleep(options.per_chunk_delay);
    }

    let mut scope = PerfScope::new(options.perf_logger.clone(), "import.chunk");
    scope.field("project_key", chunk.project_key.as_str());
    scope.field("chunk_day_local", chunk.chunk_day_local.as_str());
    scope.field("source_file_count", chunk.source_files.len());

    let import_result = (|| {
        for source_file in &chunk.source_files {
            let path = source_root.join(&source_file.relative_path);
            let outcome = normalize_jsonl_file(
                database.connection_mut(),
                &NormalizeJsonlFileParams {
                    project_id: chunk.project_id,
                    source_file_id: source_file.source_file_id,
                    import_chunk_id: chunk.import_chunk_id,
                    path,
                    perf_logger: options.perf_logger.clone(),
                },
            )
            .with_context(|| {
                format!(
                    "unable to normalize source file {}",
                    source_root.join(&source_file.relative_path).display()
                )
            })?;

            match outcome {
                NormalizeJsonlFileOutcome::Imported(result) => {
                    if let Some(conversation_id) = result.conversation_id {
                        let _ = build_actions(
                            database.connection_mut(),
                            &BuildActionsParams {
                                conversation_id,
                                perf_logger: options.perf_logger.clone(),
                            },
                        )
                        .with_context(|| {
                            format!(
                                "unable to build actions for source file {}",
                                source_root.join(&source_file.relative_path).display()
                            )
                        })?;
                    }
                }
                NormalizeJsonlFileOutcome::Skipped => {}
                NormalizeJsonlFileOutcome::Warning(warning) => {
                    insert_import_warning(
                        database.connection_mut(),
                        chunk.import_chunk_id,
                        source_file.source_file_id,
                        &warning,
                    )?;
                }
            }
        }

        finalize_chunk_import(database.connection_mut(), chunk)?;
        Ok(())
    })();

    if let Err(err) = import_result {
        let error_message = compact_status_text(format!("{err:#}"));
        let _ = mark_chunk_failed(
            database.connection_mut(),
            chunk.import_chunk_id,
            &error_message,
        );
        scope.finish_error(&err);
        return Err(err);
    }

    scope.finish_ok();
    Ok(())
}
```

Note: this step introduces `perf_logger` fields on `NormalizeJsonlFileParams` (Task 6) and `BuildActionsParams` (Task 7). The code will not compile until those tasks are done. This is intentional — the three tasks form one atomic change. Do not commit or run cargo between Tasks 5, 6, and 7.

- [x] **Step 5: No commit yet**

Compilation is broken until Tasks 6 and 7 land. Leave the working tree dirty; move to Task 6.

---

## Task 6: Add logger + spans to `normalize.rs`

**Files:**
- Modify: `crates/gnomon-core/src/import/mod.rs` — `NormalizeJsonlFileParams` definition.
- Modify: `crates/gnomon-core/src/import/normalize.rs` — thread logger in, emit spans.

- [x] **Step 1: Add `perf_logger` to `NormalizeJsonlFileParams`**

Find the definition of `NormalizeJsonlFileParams` in `crates/gnomon-core/src/import/mod.rs`. Add the new field:

```rust
pub struct NormalizeJsonlFileParams {
    pub project_id: i64,
    pub source_file_id: i64,
    pub import_chunk_id: i64,
    pub path: std::path::PathBuf,
    pub perf_logger: Option<crate::perf::PerfLogger>,
}
```

Keep other fields as they already are. Update the `Clone` derive status to match — `PerfLogger` is already `Clone`.

If tests in the same file construct `NormalizeJsonlFileParams` by name, add `perf_logger: None` to each. Run `cargo check -p gnomon-core 2>&1 | grep -i 'perf_logger'` after this step to find any missed sites — expected a few compile errors to fix.

- [x] **Step 2: Add phase spans in `normalize_transcript_jsonl_file`**

In `crates/gnomon-core/src/import/normalize.rs`, add at the top:

```rust
use crate::perf::PerfScope;
```

In `normalize_transcript_jsonl_file` (currently starts at line 78), wrap the whole function body in a span that captures timings. Immediately after `let mut state = ImportState::new(params.clone());` add:

```rust
    let mut scope = PerfScope::new(params.perf_logger.clone(), "import.normalize_jsonl");
    scope.field("path", params.path.display().to_string());
```

On the two early-return paths (`Ok(NormalizeJsonlFileOutcome::Warning(...))` at the malformed-json site, and `Ok(NormalizeJsonlFileOutcome::Skipped)` at the metadata-only site) call `scope.field("outcome", "warning")` / `"skipped"` immediately before the return, and call `scope.finish_ok()` on those paths. On the `bail!` path (no sessionId) call `scope.finish_error(...)` before bailing — use a clone of the error message. On the happy return, emit:

```rust
    scope.field("record_count", state.record_count);
    scope.field("message_count", state.message_states.len());
    scope.field("turn_count", turn_count);
    scope.finish_ok();
```

placed after the successful `tx.commit()`.

- [x] **Step 3: Also wire the history-file path if it exists**

If `normalize_history_jsonl_file` (or whatever the sibling function for the `history.jsonl` path is) exists in this file, mirror the same span treatment. If it does not, skip this step.

- [x] **Step 4: No commit yet**

`build_turns` span still to add in Task 7. Move on.

---

## Task 7: Add spans to `build_turns`, `build_actions`, and rollup rebuilds

**Files:**
- Modify: `crates/gnomon-core/src/import/normalize.rs` — span around `build_turns`.
- Modify: `crates/gnomon-core/src/classify/mod.rs` — add `perf_logger` to `BuildActionsParams`, span around `build_actions`.
- Modify: `crates/gnomon-core/src/rollup.rs` — spans around `rebuild_chunk_action_rollups` and `rebuild_chunk_path_rollups`.
- Modify: `crates/gnomon-core/src/import/chunk.rs` — span around `finalize_chunk_import` and any rollup call sites.

- [x] **Step 1: Wrap `build_turns`**

Inside `normalize_transcript_jsonl_file` (normalize.rs), replace:

```rust
    let turn_count = state.build_turns(&mut tx)?;
```

with:

```rust
    let mut turns_scope =
        PerfScope::new(params.perf_logger.clone(), "import.build_turns");
    let turn_count = match state.build_turns(&mut tx) {
        Ok(n) => {
            turns_scope.field("turn_count", n);
            turns_scope.finish_ok();
            n
        }
        Err(err) => {
            turns_scope.finish_error(&err);
            return Err(err);
        }
    };
```

- [x] **Step 2: Add `perf_logger` to `BuildActionsParams`**

Open `crates/gnomon-core/src/classify/mod.rs`. Add:

```rust
use crate::perf::{PerfLogger, PerfScope};
```

Modify the `BuildActionsParams` struct:

```rust
pub struct BuildActionsParams {
    pub conversation_id: i64,
    pub perf_logger: Option<PerfLogger>,
}
```

- [x] **Step 3: Wrap `build_actions` in a span**

In the `build_actions` function, at the very top of the body create:

```rust
    let mut scope = PerfScope::new(params.perf_logger.clone(), "import.build_actions");
    scope.field("conversation_id", params.conversation_id);
```

At the successful return (just before the final `Ok(...)`), set any counters you can cheaply get (e.g. number of actions inserted) and call `scope.finish_ok()`. On error paths, call `scope.finish_error(&err)`. If `build_actions` is written in `?`-heavy style, wrap the body in an inner closure and match on its result at the outer level, like the `import_chunk` pattern in Task 5.

- [x] **Step 4: Wrap rollup rebuilds**

Open `crates/gnomon-core/src/rollup.rs`. Add:

```rust
use crate::perf::{PerfLogger, PerfScope};
```

For `rebuild_chunk_action_rollups` and `rebuild_chunk_path_rollups`, change the signature to accept an `Option<PerfLogger>` as a final optional argument. Each function gets:

```rust
    let mut scope = PerfScope::new(perf_logger.clone(), "import.rebuild_action_rollups"); // or path
    scope.field("import_chunk_id", import_chunk_id);
```

at the top, and `scope.finish_ok()` / `scope.finish_error(&err)` at return paths.

- [x] **Step 5: Update call sites in `chunk.rs`**

Find `rebuild_chunk_action_rollups` and `rebuild_chunk_path_rollups` call sites in `crates/gnomon-core/src/import/chunk.rs` (around line 1023) and pass `options.perf_logger.clone()` through. Also pass `clear_chunk_action_rollups` / `clear_chunk_path_rollups` an `Option<PerfLogger>` if you want their timings too — skip if the change would be invasive and the rollup clear is trivial.

Wrap `finalize_chunk_import` in its own span inside `import_chunk`:

```rust
    let mut finalize_scope =
        PerfScope::new(options.perf_logger.clone(), "import.finalize_chunk");
    let finalize_result = finalize_chunk_import(database.connection_mut(), chunk);
    match &finalize_result {
        Ok(()) => finalize_scope.finish_ok(),
        Err(err) => finalize_scope.finish_error(err),
    }
    finalize_result?;
```

Replace the existing `finalize_chunk_import(...)?;` call inside the closure with this pattern.

- [x] **Step 6: Fix compile errors and run quality gates**

Run:
```bash
cargo fmt --all
cargo build -p gnomon-core
```
Expected: clean build. Fix any errors surfaced.

Run:
```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
Expected: all pass. Fix any failures before moving on.

- [x] **Step 7: Commit Tasks 5-7 together**

Run:
```bash
git add -A
git commit -m "feat(import): wire PerfLogger into import hot path

Adds an Option<PerfLogger> to ImportWorkerOptions, threads it
through import_chunk → normalize_jsonl_file → build_turns →
build_actions → finalize_chunk_import → rebuild_chunk_*_rollups
via new perf_logger fields on NormalizeJsonlFileParams and
BuildActionsParams. Emits PerfScope spans per phase with
row/message/turn counters where available.

Opt-in via GNOMON_PERF_LOG env var; off by default.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

- [x] **Step 8: Update Resume Block**

`Last completed` → `Tasks 5-7: perf spans wired into import path.` `Next action` → `Task 8: parse-vs-SQL split in per-record loop.` Commit as `log: update resume block after Task 7`.

---

## Task 8: Parse-vs-SQL split inside the per-record loop

**Files:**
- Modify: `crates/gnomon-core/src/import/normalize.rs` — add a verbose span per record inside `normalize_transcript_jsonl_file`'s main loop, *batched* so we do not emit one event per row.

The point of this split is to answer "how much of the per-record-loop wall time is `serde_json::from_str` vs the `process_record` SQL work." We do **not** want a span per row — that would flood the log. Instead we maintain two local `Duration` accumulators and emit one summary event at the end of the loop.

- [x] **Step 1: Add accumulators and emit the summary span**

In `normalize_transcript_jsonl_file`, before the `for (zero_based_line_no, line_result) in reader.lines().enumerate() {` loop, add:

```rust
    use std::time::{Duration, Instant};
    let mut parse_total: Duration = Duration::ZERO;
    let mut sql_total: Duration = Duration::ZERO;
```

(Place the `use` at the top of the file instead if preferred — just avoid duplication.)

Inside the loop body, wrap the `serde_json::from_str` call to accumulate:

```rust
        let parse_start = Instant::now();
        let record: Value = match serde_json::from_str(&line) {
            Ok(record) => record,
            Err(_) => {
                // ... existing warning-return branch unchanged
            }
        };
        parse_total += parse_start.elapsed();
```

And wrap `state.process_record(&mut tx, record, line_no as i64)?;` (plus the `initialize_context` / `flush_buffered_records` calls in the "no conversation yet" branch):

```rust
        let sql_start = Instant::now();
        // existing conversation-init / process_record logic
        sql_total += sql_start.elapsed();
```

After the loop and before `scope.finish_ok()` on the happy path, add:

```rust
    scope.field("parse_ms", parse_total.as_secs_f64() * 1000.0);
    scope.field("sql_ms", sql_total.as_secs_f64() * 1000.0);
```

- [x] **Step 2: Quality gates and commit**

Run `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo build --workspace`.

Commit:
```bash
git add -A
git commit -m "feat(import): split parse vs SQL time in per-record loop

Accumulates serde_json::from_str time and process_record time
separately inside normalize_transcript_jsonl_file and emits them
as parse_ms / sql_ms fields on the import.normalize_jsonl span.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

- [x] **Step 3: Update Resume Block**

`Last completed` → `Task 8: parse-vs-SQL split added.` `Next action` → `Task 9: write bench harness example.` Commit.

---

## Task 9: Benchmark harness as a `gnomon-core` example

**Files:**
- Create: `crates/gnomon-core/examples/import_bench.rs`

The harness is a single `main()` that:
1. Parses CLI args: `--corpus <subset|full>`, `--mode <full|startup>`, `--repeats N` (default 1), `--perf-log <path>` (optional), `--keep-db` (optional).
2. Extracts the chosen tarball (`tests/fixtures/import-corpus/{subset,full}.tar.zst`) into a `tempfile::TempDir`.
3. Creates a fresh SQLite file in another tempdir.
4. Sets `GNOMON_PERF_LOG` (and `GNOMON_PERF_LOG_FORMAT=jsonl`) in-process before calling import, so the existing `PerfLogger::from_env` plumbing picks it up.
5. For `--mode full`: calls `import::chunk::import_all`. For `--mode startup`: calls `start_startup_import_with_progress` and waits for it to return (this is time-to-TUI-gate).
6. Measures wall time with `std::time::Instant`.
7. Queries row counts from the resulting DB.
8. Prints a one-shot human-readable report: wall time, rows per table, DB file size, JSONL MB parsed, MB/s, rows/s.
9. Tears down unless `--keep-db`.

- [x] **Step 1: Check workspace dev-deps**

Run: `grep -n clap crates/gnomon-core/Cargo.toml`
If `clap` is not already a dep of `gnomon-core`, add it under `[dev-dependencies]` in `crates/gnomon-core/Cargo.toml` using the workspace version:

```toml
[dev-dependencies]
clap = { workspace = true }
tempfile = { workspace = true }
```

(Check existing dev-deps first; only add what's missing.)

- [x] **Step 2: Write the example**

Create `crates/gnomon-core/examples/import_bench.rs` with this content:

```rust
// Import benchmark harness for the import-perf project.
// Run with:
//   cargo run -p gnomon-core --example import_bench --release -- \
//     --corpus subset --mode startup --perf-log /tmp/gnomon-perf.jsonl

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use clap::{Parser, ValueEnum};
use gnomon_core::db::Database;
use gnomon_core::import::chunk::{
    import_all, start_startup_import_with_progress,
};
use rusqlite::Connection;
use tempfile::TempDir;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CorpusChoice {
    Subset,
    Full,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ModeChoice {
    Full,
    Startup,
}

#[derive(Parser, Debug)]
#[command(about = "Gnomon import benchmark harness")]
struct Args {
    #[arg(long, value_enum, default_value_t = CorpusChoice::Subset)]
    corpus: CorpusChoice,

    #[arg(long, value_enum, default_value_t = ModeChoice::Full)]
    mode: ModeChoice,

    #[arg(long, default_value_t = 1)]
    repeats: u32,

    #[arg(long)]
    perf_log: Option<PathBuf>,

    #[arg(long)]
    keep_db: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let repo_root = find_repo_root()?;
    let corpus_path = match args.corpus {
        CorpusChoice::Subset => repo_root.join("tests/fixtures/import-corpus/subset.tar.zst"),
        CorpusChoice::Full => repo_root.join("tests/fixtures/import-corpus/full.tar.zst"),
    };
    if !corpus_path.exists() {
        return Err(anyhow!(
            "corpus tarball not found at {}; run tests/fixtures/import-corpus/capture.sh first",
            corpus_path.display()
        ));
    }

    for iteration in 1..=args.repeats {
        println!(
            "=== iteration {iteration}/{} ({:?} / {:?}) ===",
            args.repeats, args.corpus, args.mode
        );
        run_once(&corpus_path, args.mode, args.perf_log.as_deref(), args.keep_db)?;
    }
    Ok(())
}

fn run_once(
    corpus_path: &Path,
    mode: ModeChoice,
    perf_log: Option<&Path>,
    keep_db: bool,
) -> Result<()> {
    let source_dir = TempDir::new().context("unable to create source tmpdir")?;
    let db_dir = TempDir::new().context("unable to create db tmpdir")?;

    // SAFETY: the PerfLogger reads env vars at call time via PerfLogger::from_env,
    // so set them before invoking the import entry points.
    if let Some(log_path) = perf_log {
        unsafe {
            env::set_var("GNOMON_PERF_LOG", log_path);
            env::set_var("GNOMON_PERF_LOG_FORMAT", "jsonl");
            env::set_var("GNOMON_PERF_LOG_GRANULARITY", "verbose");
        }
    }

    println!("extracting {} ...", corpus_path.display());
    let extract_start = Instant::now();
    let status = Command::new("tar")
        .arg("-C")
        .arg(source_dir.path())
        .arg("-I")
        .arg("zstd")
        .arg("-xf")
        .arg(corpus_path)
        .status()
        .context("unable to spawn tar to extract corpus")?;
    if !status.success() {
        return Err(anyhow!("tar -xf failed for {}", corpus_path.display()));
    }
    let extract_elapsed = extract_start.elapsed();

    let source_root = source_dir.path().join("projects");
    let source_root = if source_root.exists() {
        source_root
    } else {
        source_dir.path().to_path_buf()
    };

    let db_path = db_dir.path().join("usage.sqlite3");

    let bytes = total_jsonl_bytes(&source_root)?;
    println!(
        "jsonl bytes: {:.2} MB   extract: {:.2}s",
        bytes as f64 / (1024.0 * 1024.0),
        extract_elapsed.as_secs_f64()
    );

    let import_start = Instant::now();

    match mode {
        ModeChoice::Full => {
            let mut database = Database::open(&db_path)
                .with_context(|| format!("unable to open db at {}", db_path.display()))?;
            // Use the existing public API; import_all needs an &Connection for plan building.
            let plan_conn = Connection::open(&db_path)
                .with_context(|| format!("unable to open plan conn at {}", db_path.display()))?;
            // Run source scan first so chunks exist; reuse gnomon-core's public scan entry.
            gnomon_core::import::source::scan_source_manifest(&plan_conn, &source_root)
                .context("source scan failed")?;
            drop(plan_conn);
            let plan_conn = Connection::open(&db_path)?;
            let report = import_all(&plan_conn, &db_path, &source_root)
                .context("import_all failed")?;
            println!(
                "startup chunks: {}, deferred chunks: {}, deferred failures: {}",
                report.startup_chunk_count,
                report.deferred_chunk_count,
                report.deferred_failure_count,
            );
            drop(database);
        }
        ModeChoice::Startup => {
            let conn = Connection::open(&db_path)?;
            gnomon_core::import::source::scan_source_manifest(&conn, &source_root)
                .context("source scan failed")?;
            let startup =
                start_startup_import_with_progress(&conn, &db_path, &source_root, |_| {})
                    .context("start_startup_import_with_progress failed")?;
            drop(startup);
        }
    }

    let import_elapsed = import_start.elapsed();

    let row_counts = count_rows(&db_path)?;
    let db_bytes = fs::metadata(&db_path)
        .map(|m| m.len())
        .unwrap_or(0);

    println!("--- results ---");
    println!("wall: {:.3}s", import_elapsed.as_secs_f64());
    println!(
        "throughput: {:.2} MB/s parsed",
        (bytes as f64 / (1024.0 * 1024.0)) / import_elapsed.as_secs_f64().max(1e-6)
    );
    for (table, count) in &row_counts {
        println!("  {table:<20} {count}");
    }
    println!("db size: {:.2} MB", db_bytes as f64 / (1024.0 * 1024.0));

    if keep_db {
        let kept = db_path.clone();
        println!("db kept at: {}", kept.display());
        std::mem::forget(db_dir);
    }

    Ok(())
}

fn find_repo_root() -> Result<PathBuf> {
    let mut cur = env::current_dir()?;
    loop {
        if cur.join("Cargo.toml").exists() && cur.join("crates/gnomon-core").exists() {
            return Ok(cur);
        }
        if !cur.pop() {
            return Err(anyhow!("unable to find repo root with crates/gnomon-core"));
        }
    }
}

fn total_jsonl_bytes(root: &Path) -> Result<u64> {
    let mut total: u64 = 0;
    for entry in walkdir::WalkDir::new(root) {
        let entry = entry?;
        if entry.file_type().is_file()
            && entry.path().extension().and_then(|s| s.to_str()) == Some("jsonl")
        {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

fn count_rows(db_path: &Path) -> Result<Vec<(String, i64)>> {
    let conn = Connection::open(db_path)?;
    let tables = [
        "project",
        "source_file",
        "import_chunk",
        "conversation",
        "stream",
        "record",
        "message",
        "message_part",
        "turn",
        "action",
    ];
    let mut out = Vec::new();
    for table in tables {
        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0))
            .unwrap_or(0);
        out.push((table.to_string(), count));
    }
    Ok(out)
}
```

**Important compile-time concerns:**
- `gnomon_core::import::chunk::import_all` and `start_startup_import_with_progress` may not be `pub` at the `gnomon_core::import::chunk` path today. Check `crates/gnomon-core/src/import/mod.rs` and `crates/gnomon-core/src/lib.rs` for the actual re-export path. Adjust the `use` lines in the example to match. If either function is crate-private, add a `pub use` in `crates/gnomon-core/src/lib.rs` to surface it.
- `gnomon_core::import::source::scan_source_manifest` — verify it's `pub`. If not, add a `pub use`.
- `Database` may or may not be `pub` via `gnomon_core::db::Database`. Verify; add a re-export if needed.
- `walkdir` needs to be accessible from the example. If it isn't a direct dep of `gnomon-core`, add it as a dev-dependency (workspace version) alongside `clap` and `tempfile`.
- Avoid `unwrap()` — the workspace denies `clippy::unwrap_used`. Use `?` or explicit match arms everywhere.

- [x] **Step 3: Build and run smoke-test on subset, mode=full**

Run:
```bash
cargo build -p gnomon-core --example import_bench --release
GNOMON_PERF_LOG=/tmp/gnomon-perf-smoke.jsonl GNOMON_PERF_LOG_FORMAT=jsonl \
  cargo run -p gnomon-core --example import_bench --release -- \
  --corpus subset --mode full
```
Expected: prints `wall: <n>s`, row counts are nonzero for at least `project`, `record`, `message`. Check `/tmp/gnomon-perf-smoke.jsonl` contains `import.chunk`, `import.normalize_jsonl`, `import.build_turns`, `import.build_actions`, `import.finalize_chunk` events.

If spans are missing, something is wrong with Task 5-7 plumbing — fix before proceeding.

- [x] **Step 4: Smoke-test on subset, mode=startup**

Run:
```bash
cargo run -p gnomon-core --example import_bench --release -- \
  --corpus subset --mode startup
```
Expected: prints a wall time (the time-to-TUI-gate); row counts reflect only the 24h window (may be zero if your real corpus' most-recent 24h has no data for the subset's projects — that is a real data property to note).

- [x] **Step 5: Quality gates and commit**

Run `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo build --workspace`.

Commit:
```bash
git add -A
git commit -m "feat: add import_bench example for perf measurement

A reproducible harness: extracts a frozen corpus snapshot into a
tmpdir, runs full cold import or startup-mode import against a
fresh SQLite file, emits wall time, row counts, and throughput,
and enables GNOMON_PERF_LOG so per-phase spans land in a JSONL
file. Used by the import-perf Phase 1 baseline capture.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

- [x] **Step 6: Update Resume Block**

`Last completed` → `Task 9: bench harness built and smoke-tested.` `Next action` → `Task 10: capture subset baselines.` Commit.

---

## Task 10: Capture baseline — subset

- [x] **Step 1: Record environment fingerprint**

Run these and paste the outputs into the log's Environment section of the Frozen Header:
```bash
uname -a
lscpu | grep -E 'Model name|Socket|Core|Thread'
grep MemTotal /proc/meminfo
stat --file-system --format='%T' .
stat --file-system --format='%T' "${HOME}/.local/share/gnomon" 2>/dev/null || echo "db path does not exist yet"
rustc --version
```

Also capture the bundled SQLite version — in the bench tempdir, run `.version` via rusqlite, or grep the rusqlite crate.

Commit as `log: record environment fingerprint`.

- [x] **Step 2: Capture subset, mode=full, 3 runs**

Run:
```bash
for i in 1 2 3; do
  GNOMON_PERF_LOG=/tmp/gnomon-perf-subset-full-$i.jsonl \
  GNOMON_PERF_LOG_FORMAT=jsonl \
  GNOMON_PERF_LOG_GRANULARITY=verbose \
  cargo run -p gnomon-core --example import_bench --release -- \
    --corpus subset --mode full 2>&1 | tee /tmp/gnomon-subset-full-$i.log
done
```

- [x] **Step 3: Capture subset, mode=startup, 3 runs**

```bash
for i in 1 2 3; do
  GNOMON_PERF_LOG=/tmp/gnomon-perf-subset-startup-$i.jsonl \
  GNOMON_PERF_LOG_FORMAT=jsonl \
  GNOMON_PERF_LOG_GRANULARITY=verbose \
  cargo run -p gnomon-core --example import_bench --release -- \
    --corpus subset --mode startup 2>&1 | tee /tmp/gnomon-subset-startup-$i.log
done
```

- [x] **Step 4: Summarize into log Phase Log**

Add a Phase Log entry dated today:

```
## <date> — baseline: subset
Full mode wall (3 runs): <t1>s / <t2>s / <t3>s (median <m>s)
Startup mode wall (3 runs): <t1>s / <t2>s / <t3>s (median <m>s)
Row counts (full mode): project=<n> record=<n> message=<n> turn=<n> action=<n>
Per-phase split (from perf log, median run):
  import.normalize_jsonl: <total ms> (parse_ms=<n> sql_ms=<n>)
  import.build_turns:     <total ms>
  import.build_actions:   <total ms>
  import.finalize_chunk:  <total ms>
  import.rebuild_*_rollups: <total ms>
Notes: <anything surprising>
```

Commit as `log: subset baseline captured`.

---

## Task 11: Capture baseline — full corpus

- [x] **Step 1: Capture full, mode=full, 3 runs**

Same recipe as Task 10 but with `--corpus full`. Expect significantly longer wall time — do not cancel runs.

- [x] **Step 2: Capture full, mode=startup, 3 runs**

Same. Startup mode on the full corpus is the **primary metric** we care about. Capture carefully.

- [x] **Step 3: Summarize into log**

New Phase Log entry `<date> — baseline: full corpus`. Same structure as Task 10. Commit as `log: full baseline captured`.

- [x] **Step 4: Populate the log's Baseline section in the Frozen Header**

With the median numbers from Tasks 10 and 11, fill in the `## Baseline` placeholder in the Frozen Header. Commit as `log: populate baseline header`.

---

## Task 12: Capture CPU profiles

**Files:**
- Create: `docs/specs/profiles/` directory.
- Create: `docs/specs/profiles/baseline-full.svg` (or `.html` — whichever samply produces).
- Create: `docs/specs/profiles/baseline-startup.svg`.

- [ ] **Step 1: Install samply if missing**

Run: `which samply || cargo install samply`

- [ ] **Step 2: Profile mode=full on full corpus**

Run:
```bash
mkdir -p docs/specs/profiles
samply record --save-only --output docs/specs/profiles/baseline-full.json.gz -- \
  cargo run -p gnomon-core --example import_bench --release -- \
    --corpus full --mode full
```

Then generate a flamegraph SVG:
```bash
samply load docs/specs/profiles/baseline-full.json.gz --save-svg docs/specs/profiles/baseline-full.svg
```
(If `--save-svg` is not supported by your samply version, open the profile in the browser via `samply load ...` and export manually.)

- [ ] **Step 3: Profile mode=startup on full corpus**

Same recipe with `--mode startup` and output names `baseline-startup.*`.

- [ ] **Step 4: Commit the profiles**

Run:
```bash
git add docs/specs/profiles/
git commit -m "perf: add baseline CPU profiles for full and startup modes

Flamegraphs captured with samply against the full corpus snapshot
documented in tests/fixtures/import-corpus/MANIFEST.md. Used as
the Phase 1 reference for ranking import-perf candidates.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 5: Log entry**

Phase Log entry `<date> — baseline profiles captured` referencing the file paths and summarizing the top 5 hottest functions by self-time from each flamegraph. Commit as `log: baseline profiles referenced`.

---

## Task 13: User checkpoint — review baseline data

**Not a code task.** Surface the baseline to the user. Produce a one-page summary message with:
- Environment fingerprint.
- Median wall times (subset/full × full/startup) and their run-to-run variance.
- Per-phase breakdown as percentages of total.
- The two flamegraph file paths with a one-line summary each ("parse 34%, SQL 52%, rollups 8%, other 6%" — whatever the actual numbers are).
- Any surprising findings (e.g. "DB path is on a 9p mount, fsync dominates" — if true).

- [ ] **Step 1: Compose and surface the summary**

Wait for user response before proceeding to Task 14. The user may request re-baselining (e.g. "the subset looks weird, regenerate"), additional profiling, or direct continuation.

---

## Task 14: User checkpoint — agree on target

**Not a code task.** Collaboratively agree on a concrete target number for startup-mode wall time on the full corpus, and optionally a secondary target for cold full import.

- [ ] **Step 1: Propose a target based on baseline**

Propose something like "startup mode < 1.5s on full corpus (median of 5 runs), stretch 0.8s" — concrete, measurable. Justification anchored in which phases are realistically compressible per the flamegraph.

- [ ] **Step 2: Write the target into the log Frozen Header**

On user agreement, edit the `## Target` section in the Frozen Header with the agreed numbers and the date of agreement.

- [ ] **Step 3: Commit and update Resume Block**

Commit as `log: record agreed Phase 1 target`.

Resume Block: `Last completed` → `Phase 1 complete. Target agreed.` `Next action` → `Phase 2: iterate loop begins. First candidate per re-ranking informed by baseline profile.` Commit.

---

## Task 15: Phase 1 exit checkpoint

- [ ] **Step 1: Verify gate conditions**

All of the following must be true:
1. `docs/specs/2026-04-10-import-perf-log.md` Frozen Header is fully populated (environment, corpus SHAs, baseline, target).
2. `tests/fixtures/import-corpus/MANIFEST.md` committed; tarballs exist locally.
3. `crates/gnomon-core/examples/import_bench.rs` runs cleanly in both modes on both corpora.
4. `docs/specs/profiles/` contains at least `baseline-full.*` and `baseline-startup.*`.
5. `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo build --workspace` all pass on `import-perf`.
6. Resume Block reflects Phase 1 complete.

- [ ] **Step 2: Announce gate open**

Surface to user: "Phase 1 complete. Gate open. Ready to start Phase 2 iterate loop. First candidate will be chosen from the re-ranked list after reviewing the baseline profiles together — even though the design doc lists A1 (prepared statements) as the initial top pick, the profile may reshuffle. Shall I propose a candidate now?"

Wait for user direction.

---

## Self-Review Checklist

- [x] **Spec coverage:**
  - Spec §1 goals/non-goals → reflected in plan header and task selection.
  - Spec §3 Phase 1 deliverables 1-6 → Tasks 2-4 (snapshot), 9 (harness), 5-8 (instrumentation), 10-12 (baseline+profile), 14 (target).
  - Spec §4 candidate ranking → out of scope for Phase 1 (Phase 2 work).
  - Spec §5 corpus snapshot structure → Tasks 2-4 match.
  - Spec §6 baseline metrics → Tasks 10-12 capture all listed metrics.
  - Spec §7 log format → Task 1 initializes; every task ends with a Resume Block update.
  - Spec §8 commit workflow → each task has an explicit commit step.
  - Spec §9 stopping criteria → out of scope for Phase 1.
- [x] **Placeholder scan:** No TBD / "add error handling" / "tests for the above" patterns. Code blocks present where code is changed.
- [x] **Type consistency:** `NormalizeJsonlFileParams.perf_logger`, `BuildActionsParams.perf_logger`, `ImportWorkerOptions.perf_logger` all use `Option<PerfLogger>`. `PerfScope::new(Option<PerfLogger>, impl Into<String>)` matches the API in `crates/gnomon-core/src/perf.rs:204`.
- [x] **Known unknowns flagged inline:** Task 9 Step 2 explicitly warns that `import_all`, `start_startup_import_with_progress`, `scan_source_manifest`, and `Database` may need `pub use` re-exports — the implementer must verify and fix.

## Out of Scope for This Plan
- Any performance optimization candidate (A1-A5, B1-B5, C1-C3). Those are Phase 2 and get their own smaller plans.
- CI integration of the benchmark harness.
- Synthetic corpus generation.
- Cross-platform fingerprinting.
- Merging `import-perf` into `main`.
