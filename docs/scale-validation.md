# Scale Validation And Release Builds

Issue `#10` establishes a repeatable way to validate importer/query behavior at
larger corpus sizes and documents the first practical release build workflow.

## Current Boundary

The repository does **not** yet ship the table-first TUI from issue `#7`.
That means this milestone can measure:

- discovery and startup import timing
- time to the first usable UI gate
- time to the last-24h-ready slice
- continued backfill behavior
- query-layer responsiveness against published snapshots

It cannot yet measure interactive table filtering or jump performance because
that UI does not exist on `main`.

## Validation Tool

Use the synthetic corpus validator:

```bash
cargo run -p gnomon --bin validate-scale -- --profile quick
```

For the larger issue-10 run, keep the generated corpus so you can inspect the
artifacts afterward:

```bash
cargo run -p gnomon --release --bin validate-scale -- --profile ten-x --root /tmp/gnomon-scale
```

Profiles:

- `quick`: `2` projects x `4` days x `3` sessions/day (`24` JSONL files)
- `ten-x`: `6` projects x `14` days x `10` sessions/day (`840` JSONL files)

Both profiles generate real JSONL source files, run them through source
discovery, startup import scheduling, normalization, classification, and the
published-snapshot query layer.

The validator reports:

- fixture generation time
- source scan and manifest shaping time
- time to first usable UI
- time to last-24h ready
- time to full backfill
- query timings for filter options, project-root browse, category-root browse,
  and one project drill

## Query Benchmark Workflow

For focused browse/filter/jump benchmarking and query-plan capture, run the main
CLI benchmark command against either a real cache or a synthetic cache:

```bash
cargo run -p gnomon -- benchmark
cargo run -p gnomon -- --db /path/to/usage.sqlite3 benchmark --iterations 10
```

The benchmark output is JSON and includes:

- timing samples for `refresh_snapshot_status`
- timing samples for project-root and category-root browse
- timing samples for a representative path drill
- timing samples for refresh, filter-change, and jump-target-build query flows
- `EXPLAIN QUERY PLAN` output for the latest-snapshot, action-fact, and
  path-fact statements

The command is read-only against the SQLite cache. Use `--iterations` to gather
more stable before/after samples when comparing query work.

To pair the benchmark with synthetic fixtures, generate the cache first and then
point `gnomon benchmark` at the resulting SQLite file:

```bash
cargo run -p gnomon --bin validate-scale -- --profile quick --root /tmp/gnomon-bench
cargo run -p gnomon -- --db /tmp/gnomon-bench/validation.sqlite3 benchmark --iterations 10
```

For before/after comparisons, capture the JSON output from both runs and diff
the scenario timing samples and `query_plans` sections.

## Measured Baseline

On March 27, 2026, this repository was validated on Linux with:

```bash
cargo run -p gnomon --release --bin validate-scale -- --profile ten-x --root /tmp/gnomon-scale-24h
```

Observed results for the `ten-x` profile:

- `6` discovered projects
- `840` discovered source files
- `84` published `project x day` chunks
- `1697 ms` to first usable UI
- `1697 ms` to last-24h ready
- `7216 ms` to full backfill
- `13 ms` for filter options
- `13 ms` for project-root browse
- `14 ms` for category-root browse
- `13 ms` for one project drill browse

These timings are machine-dependent. Treat them as the current Linux baseline
for this codebase and rerun the validator after material importer, query, or
TUI changes.

## Product Contract Checks

The validator is meant to confirm the current `v1` startup contract:

- the importer prioritizes the last 24 hours first
- the UI opens after that slice is ready or after the 10-second gate
- older chunks continue importing in the background
- published query snapshots remain bounded by completed chunks only

The automated smoke test lives in
`crates/gnomon-core/src/validation.rs` and uses the `quick`-scale profile. The
`ten-x` profile is intended for explicit validation runs, not the default test
suite.

## Release Builds

Build the release artifacts on the target platform:

```bash
cargo build --release --workspace
```

The distributable CLI binary is:

```text
target/release/gnomon
```

Current packaging assumptions:

- Linux: build the Linux release on Linux
- macOS: build the macOS release on macOS
- Cross-compilation is not documented or validated in this repository yet
- The application is currently a single terminal binary with no sidecar web
  assets

## Linux Packaging Check

On March 27, 2026, `cargo build --release --workspace` produced:

- `target/release/gnomon` at `8.2M`
- `target/release/validate-scale` at `7.2M`

Linux dependency inspection with:

```bash
ldd target/release/gnomon
```

showed only the normal glibc/libm/libgcc loader dependencies and did **not**
show a dynamic `libsqlite3.so` dependency.

## SQLite Bundling Strategy

`gnomon` uses:

```toml
rusqlite = { version = "0.39.0", features = ["bundled"] }
```

That means SQLite is compiled into the binary build rather than relying on a
system-provided SQLite runtime. For the current release workflow, that keeps
distribution simpler and avoids a separate SQLite installation prerequisite on
Linux or macOS.

For macOS validation, run:

```bash
otool -L target/release/gnomon
```

and confirm that no `libsqlite3.dylib` dependency appears in the release
binary.

## Suggested Release Verification

Run this sequence on each target platform before handing off a binary:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo build --release --workspace
cargo run -p gnomon --release -- --help
cargo run -p gnomon --bin validate-scale --release -- --profile ten-x --root /tmp/gnomon-scale
```

Optional local inspection:

```bash
ls -lh target/release/gnomon
ldd target/release/gnomon
```

If the validator exposes obvious bottlenecks, record the measured corpus shape,
timings, and the limiting query or import phase before optimizing.
