# gnomon v1 Design

## Goal

Build a single-binary terminal application that analyzes Claude session history and surfaces the highest-usage patterns so the user can optimize token consumption.

## Product Shape

- Primary interface: interactive terminal UI
- Future mode: optional embedded web server, not part of `v1`
- Default root views:
  - `All projects -> project -> action category -> action -> directory/file`
  - `All projects -> action category -> action -> project -> directory/file`
- Late drill-down only:
  - `session -> turn -> action -> event`

## Metrics

The primary metric is configurable, but the default ranking lens is `uncached input`.

Definitions:

- `uncached input = input_tokens + cache_creation_input_tokens`
- `cached input = cache_read_input_tokens`
- `gross input = uncached input + cached input`
- `output = output_tokens`

## Opportunity Annotations

Opportunity is a cross-cutting annotation layer over the existing rollup
hierarchy, not a new hierarchy root or a replacement navigation model.

- Keep the current browse hierarchy as the primary structure:
  - `project -> action category -> action -> directory/file`
  - `action category -> action -> project -> directory/file`
- Attach opportunity metadata to existing rollup rows.
- Keep the map + statistics explorer as the primary interaction surface.
- Treat opportunity as sparse row-level data:
  - a row may have zero, one, or several opportunity annotations
  - absence of annotations means "no confident signal", not "confirmed healthy"

Each rollup row may carry:

- per-category opportunity scores
- a top opportunity summary
- confidence for each fired category
- compact evidence suitable for table/report output
- recommendation text derived from, but not identical to, the taxonomy label

The opportunity taxonomy for `v1` is:

- `session setup`
- `task setup`
- `history drag`
- `delegation`
- `model mismatch`
- `prompt yield`
- `search churn`
- `tool-result bloat`

These taxonomy labels are stable mechanism categories. They describe the kind of
observed overhead, not the exact advice shown to the user at a given point in
time. Recommendations remain a separate layer so guidance can evolve without
renaming the category itself.

### Confidence And Suppression

Opportunity annotations should be conservative.

- `high`: strong direct evidence and low ambiguity
- `medium`: useful signal with some ambiguity
- `low`: weak or incomplete evidence

Suppression rules:

- Do not surface a category when evidence is too weak to clear the `medium`
  confidence threshold.
- Suppress categories when the observed pattern is explainable by multiple
  equally plausible causes and the system cannot distinguish between them.
- Suppress categories when the sample is too small for the score to be
  meaningful.
- Prefer no annotation over a noisy annotation.

## Classification

- Deterministic local classifier for `v1`
- Conservative behavior:
  - explicit `mixed`
  - explicit `unclassified`
  - UI renders those special states as bracketed labels: `[mixed]` and `[unclassified]`
  - no user correction loop in `v1`
- Grouped-run actions:
  - consecutive messages in the same turn with the same classification collapse into one action

Working top-level action taxonomy:

- `user input`
- `project discovery`
- `local search/navigation`
- `external/web research`
- `editing`
- `test/build/run`
- `debugging/investigation`
- `data traffic`
- `planning/reasoning`
- `team communication/coordination`
- `documentation writing`

## Project Identity

- Authoritative project identity is discovered from Git
- `v1` supports `git` only
- If a path cannot be resolved to a Git root, it becomes its own path-based project with a warning/reason
- A user config file can apply include/exclude policy to discovered projects
  before they enter the derived cache
- The identity policy surface should own narrow recovery heuristics such as
  stale Claude worktree re-attribution
- Missing Claude worktree paths under a recognized `.../.claude/worktrees/...`
  layout are a narrow exception:
  `v1` recovers the candidate repo root above that worktree segment and uses it
  only when Git can still resolve a canonical root from that recovered path
- `v1` does not perform broad longest-prefix or cross-session project inference
  for missing paths beyond that stale-worktree recovery

## Path Attribution

Path and file drill-down should only use explicit file-oriented tools:

- `Read`
- `Write`
- `Edit`
- `MultiEdit`

Shell commands do not create path attribution.

## Import Model

### Storage

- Derived SQLite cache only
- No raw JSONL blob storage
- Reimport from source as needed

### Chunking

- Import unit: `project x day`
- Day boundary: local machine timezone
- Timestamps stored in UTC

### Scheduling

- Newest to oldest
- Round-robin by project
- Example:
  - `project A: today`
  - `project B: today`
  - `project C: today`
  - `project A: yesterday`
  - `project B: yesterday`
  - `project C: yesterday`

### Visibility and Consistency

- A chunk becomes visible only when fully imported
- Startup imports the last 24 hours first
- Open the TUI when:
  - last 24 hours are ready, or
  - 10 seconds have elapsed
- Failed startup chunks do not block the TUI; mark them failed and print the
  first actionable error on stderr outside the TUI lifecycle
- Continue importing in one background worker after the UI opens
- Do not auto-apply new data to the visible UI
- Instead, show `new data available`
- The active UI queries against a pinned set of completed chunks until manual refresh

## TUI

- Keyboard-first
- Wide layout: map view + statistics view
- Narrow layout: one pane at a time with toggle
- Visual encoding:
  - area/width = selected input lens
  - color = classification bucket
  - cached vs uncached proportions = secondary indicators, not color
- Include:
  - basic current-view filtering
  - global jump
  - persistent UI state across launches
  - fresh launches start at the top level unless explicit startup drill-down flags are provided
- Opportunity presentation:
  - optional table columns and filters augment existing rows
  - inspect details explain fired opportunities for the selected row
  - no separate "opportunity tree" is introduced for `v1`

## Default Column Priority

1. `name`
2. `visual bar`
3. `input`
4. `uncached input`
5. `output`
6. `cached input`
7. `% of parent`
8. `% of total`
9. `item count`

## Initial Workspace Shape

```text
crates/
  gnomon/
    src/main.rs
  gnomon-core/
    src/
      config.rs
      dirs.rs
      db/
      import/
      query/
      vcs/
  gnomon-tui/
    src/
      app.rs
      lib.rs
```

## Milestones

1. Workspace bootstrap, docs, and placeholder TUI
2. SQLite schema and migration framework
3. Source scanning, Git root discovery, and import chunk scheduler
4. JSONL normalization into messages, turns, and actions
5. Deterministic classification and grouped-run construction
6. Stable aggregate queries with pinned chunk visibility
7. Statistics-first TUI with filtering and global jump
8. Map view synchronized with statistics selection
9. Performance and scale validation against roughly `10x` the current corpus
