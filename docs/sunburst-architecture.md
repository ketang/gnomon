# Sunburst Architecture

`gnomon`'s map pane is now split into an internal focused-sunburst renderer and
a thin app-specific adapter. The goal is to keep rendering mechanics reusable
without pretending that the current API is ready for standalone publication.

## Terminology

- `sunburst`: the generic renderer boundary under `crates/gnomon-tui/src/sunburst/`
- `focused sunburst`: the current rendering mode used by `gnomon`, where the
  selected branch can receive conservative visual distortion so tiny but active
  drill-down paths stay inspectable
- `gnomon adapter`: the code that maps `RollupRow`, browse-path labels, and
  current selection state into the generic sunburst model

## Module Layout

- `crates/gnomon-tui/src/sunburst/model.rs`
  Defines generic renderer-facing types such as the sunburst model, layers,
  segments, center labels, render mode, and distortion policy.
- `crates/gnomon-tui/src/sunburst/geometry.rs`
  Owns span lookup, selected-child span propagation, and the distortion policy
  used for minimum visible sweep and focused-branch zoom.
- `crates/gnomon-tui/src/sunburst/raster.rs`
  Owns the raster pipeline. Coarse and Braille rendering both flow through this
  layer.
- `crates/gnomon-tui/src/sunburst/render.rs`
  Owns the pane widget and center-label rendering. It delegates pixel work to
  the raster pipeline.
- `crates/gnomon-tui/src/gnomon_sunburst.rs`
  Owns the app-specific mapping from `gnomon` browse rows and labels into the
  generic sunburst model.
- `crates/gnomon-tui/src/app.rs`
  Owns query orchestration, browse state, and TUI controller behavior. It
  should not grow new renderer internals unless they are truly app-specific.

## Boundary Rules

Reusable renderer code lives under `sunburst/` and should stay free of direct
`gnomon_core::query::*` dependencies.

`gnomon_sunburst.rs` is intentionally allowed to know about `RollupRow`,
selection labels, and browse-specific terminology. That is the seam where
future extraction should cut:

1. Keep `sunburst/` generic and renderer-focused.
2. Keep app-specific row mapping in `gnomon_sunburst.rs`.
3. When the renderer is ready for extraction, move `sunburst/` into a new crate
   first.
4. Replace `gnomon_sunburst.rs` with an adapter crate or local glue module that
   depends on both the extracted renderer crate and `gnomon`'s query model.

## Distortion Policy

The current focused sunburst intentionally allows mild visual distortion:

- tiny selected segments can receive a minimum visible sweep
- selected branches can be zoomed when their raw share would otherwise be too
  small to inspect
- table metrics remain authoritative; the map is an inspection and navigation
  surface, not the canonical numeric view

That policy is encoded in `SunburstDistortionPolicy`. It should remain
deterministic, conservative, and explicit. If the distortion becomes hard to
explain in prose, it is too aggressive.

## High-Resolution Direction

Braille rendering is the current terminal-native high-resolution mode. The
direction was inspired by `tui-piechart`, but `gnomon` does not depend on that
project and does not reuse its code directly.

## Benchmark Path

Renderer-only benchmarks live in `crates/gnomon-tui/src/benchmark.rs` and can
be run with:

```bash
cargo run -p gnomon-tui --bin sunburst-benchmark -- --iterations 10
```

The benchmark uses deterministic synthetic fixtures so coarse and Braille modes
can be compared across the same layer counts, segment densities, and terminal
sizes.
