# Plan: Sunburst Snapshot Testing

## Context

The radial (sunburst) view had an overlapping-slice bug caused by duplicate
layers. The bug was only caught via manual tmux `capture-pane` screenshots.
There are no automated visual regression tests — the existing raster tests
check individual properties (dominant segment, quantization) but never capture
the full rendered output. We need snapshot tests that would catch visual
regressions like this.

## Replacing tmux capture-pane

This approach **fully replaces** the manual tmux-based screenshotting workflow.
The ratatui `Buffer` is the last layer we own — it contains the exact
characters and colors that get sent to the terminal. Everything after that
(crossterm ANSI emission, terminal rendering) is third-party code. Testing at
the Buffer level is deterministic, requires no database or running instance,
and runs in `cargo test`.

## Approach: `insta` snapshot tests on rendered Buffer output

### Why `insta`

- Mature Rust snapshot testing crate, well-integrated with cargo
- `cargo insta review` provides interactive accept/reject workflow
- Snapshots stored as plain text files in `snapshots/` directories next to tests
- Supports both inline and file-based snapshots — file-based is right for
  multi-line rendered output

### What to snapshot

Render `SunburstPane` directly into a `ratatui::buffer::Buffer` (same pattern
as the benchmark), then serialize the buffer to a string that captures **both
geometry and color**. This is critical — the overlap bug manifested as
same-colored regions bleeding together, which plain-text (symbols only) would
miss.

**Serialization format:** For each cell, emit the foreground color index
alongside the character. A compact format like `[67]▐[73]█[15]▛` makes
snapshots readable while capturing which bucket owns each pixel. The exact
format:

```
<fg_color_tag><symbol>
```

Where `fg_color_tag` is emitted when the color changes from the previous cell
(to keep snapshots compact). Empty/default cells get no tag.

### Test fixtures

Build `SunburstModel` directly in tests (same approach as `benchmark.rs`
`synthetic_model` and the existing `make_layer` test helpers in `raster.rs`).
No database or App needed.

**Fixture set (each rendered at a fixed small size like 40x20):**

1. **Single layer, no selection** — baseline donut, no overlap possible
2. **Single layer, one segment selected** — selection highlight only
3. **Two layers, selection in layer 0** — the exact pattern that triggered the
   overlap bug (ancestor + descendant, no duplicate current layer)
4. **Two layers, no selection** — ancestor + current, no descendant
5. **Three layers, nested selection** — deeper drill-down

### File layout

```
crates/gnomon-tui/src/sunburst/
├── mod.rs
├── geometry.rs
├── model.rs
├── raster.rs
├── render.rs
├── snapshot_tests.rs          ← NEW: snapshot test module
└── snapshots/                 ← NEW: insta snapshot files (auto-generated)
    └── gnomon_tui__sunburst__snapshot_tests__*.snap
```

### Implementation steps

#### Step 0: Remove `tools/tui-shot/`

The Playwright+xterm.js PTY screenshot harness is replaced by the in-process
`insta` snapshots. Remove the entire directory and any references to it.

- Delete `tools/tui-shot/` (package.json, src/, scenarios/, README.md)
- Remove any mentions from the root README.md or docs

#### Step 1: Add `insta` dev-dependency

Add to `crates/gnomon-tui/Cargo.toml`.

#### Step 2: Add `snapshot_tests.rs` module

In `crates/gnomon-tui/src/sunburst/`:
- Add `#[cfg(test)] mod snapshot_tests;` to `sunburst/mod.rs`
- Write a `buffer_to_color_string(buf, area) -> String` helper that
  serializes a rendered Buffer to a readable string encoding both symbols
  and foreground color changes
- Write a `render_fixture(model, width, height, mode) -> String` helper
  that creates a Buffer, renders `SunburstPane` into it, and returns the
  serialized string
- Write fixture builder functions that construct `SunburstModel` values
  for each scenario above
- Write one `#[test]` per fixture calling `insta::assert_snapshot!(...)`

#### Step 3: Generate initial snapshots

`cargo insta test` then `cargo insta accept` to establish the baseline.

#### Step 4: Verify the overlap scenario

Write a test with the OLD buggy model (3 layers with duplicate current layer)
and the FIXED model (2 layers). Confirm they produce different snapshots,
proving the test would have caught the original bug.

### Files to modify

- `tools/tui-shot/` — DELETE entirely
- `crates/gnomon-tui/Cargo.toml` — add `insta` dev-dependency
- `crates/gnomon-tui/src/sunburst/mod.rs` — add `#[cfg(test)] mod snapshot_tests;`
- `crates/gnomon-tui/src/sunburst/snapshot_tests.rs` — NEW, all test code

### Verification

```bash
cargo test -p gnomon-tui snapshot_tests
cargo insta test -p gnomon-tui    # should show all snapshots matching
cargo clippy --workspace --all-targets -- -D warnings
```

After initial acceptance, deliberately break a fixture (e.g. add a duplicate
layer) and verify the snapshot diff catches it.
