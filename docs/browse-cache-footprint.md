# Browse Cache Footprint

`gnomon benchmark` now emits a `browse_footprint` block that estimates the
serialized payload size of warmed browse row sets. The measurement is intended
to size future persisted browse caches and to tune recursive prefetch defaults
using observed payloads rather than guesswork.

## What The Report Covers

The footprint report includes:

- shallow and deep non-path prefetch scenarios
- shallow and deep path-prefetch scenarios
- total payload bytes per scenario
- request count and row count per scenario
- payload breakdown by browse level, such as `project-category`,
  `category-action`, and recursive path levels

The payload estimate is based on serializing each warmed `Vec<RollupRow>` to
JSON, which matches the shape a persisted browse cache would need to store.

## Reading The Output

Run:

```bash
cargo run -p gnomon -- benchmark
```

Look for:

```json
"browse_footprint": {
  "snapshot_max_publish_seq": 123,
  "scenarios": [
    {
      "name": "non_path_deep_prefetch",
      "request_count": 4,
      "row_count": 27,
      "payload_bytes": 18542,
      "by_level": [
        {
          "level": "project-category",
          "request_count": 4,
          "row_count": 27,
          "payload_bytes": 18542
        }
      ]
    }
  ],
  "recommendations": {
    "estimated_budget_bytes": 4194304,
    "snapshot_retention_count": 2,
    "recursion_depth_limit": 3,
    "recursion_breadth_limit": 4
  }
}
```

## Current Defaults

The current prefetch implementation uses these defaults:

- recursion depth limit: `3`
- recursion breadth limit: `4`
- estimated persisted-cache budget target: `4 MiB` minimum, or `4x` the largest
  measured scenario from the benchmark report
- estimated snapshot retention count: `2`

These values are intentionally conservative. Path-prefetch payloads can expand
quickly because recursive path drill carries many repeated labels and
`full_path` strings, so the benchmark output should be reviewed on
representative corpora before raising depth or breadth.
