# Selection Lag Measurement

This note captures a reproducible selection-lag trace using the 10 ms perf
logs from issue #86.

## Reproduction

Run the focused measurement test:

```bash
cargo test -p gnomon-tui selection_move_measurement_trace_is_reproducible -- --nocapture
```

The harness uses a synthetic scale fixture with `12` projects, `8` days, and
`4` sessions per day, then drives a fixed five-step `Down/Up` navigation trace
in the statistics pane and prints a JSON summary.

## Observed Trace

Latest run results:

- `tui.selection_change`: `2` slow events, max `36.134 ms`, cache hits `0`
- `tui.selection_context_load`: `2` slow events, max `35.630 ms`
- `tui.prefetch_batch`: `1` slow event, max `44.416 ms`
- Prefetch queue wait: max `0.057 ms`, total `0.057 ms`
- Selection-path fanout: `8` browse requests total, `6` distinct, `2` duplicate
- Browse cache sources during selection-path work: `4` memory hits, `0` persisted hits, `2` live queries

## Findings

- The slow path is dominated by live SQL and selection-context construction.
- Queue delay is negligible in this trace.
- The selected row's cached context was not warmed before the revisits in this
  trace, so the cache-hit rate was `0/2` for the observed selection changes.
