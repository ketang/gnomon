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

## Classification

- Deterministic local classifier for `v1`
- Conservative behavior:
  - explicit `mixed`
  - explicit `unclassified`
  - no user correction loop in `v1`
- Grouped-run actions:
  - consecutive messages in the same turn with the same classification collapse into one action

Working top-level action taxonomy:

- `user input`
- `project discovery`
- `local search/navigation`
- `external/web research`
- `Editing`
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
- Failed startup chunks do not block the TUI; mark them failed and surface the
  first actionable error in the UI status area
- Continue importing in one background worker after the UI opens
- Do not auto-apply new data to the visible UI
- Instead, show `new data available`
- The active UI queries against a pinned set of completed chunks until manual refresh

## TUI

- Keyboard-first
- Wide layout: radial view + table view
- Narrow layout: one pane at a time with toggle
- Visual encoding:
  - area/width = selected input lens
  - color = classification bucket
  - cached vs uncached proportions = secondary indicators, not color
- Include:
  - basic current-view filtering
  - global jump
  - persistent UI state across launches

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
7. Table-first TUI with filtering and global jump
8. Radial view synchronized with table selection
9. Performance and scale validation against roughly `10x` the current corpus
