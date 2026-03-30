# Browse Cache

`gnomon` now persists warmed browse results in a dedicated SQLite sidecar under
the normal state directory:

```text
<state_dir>/browse-cache.sqlite3
```

This cache is separate from the main derived usage database. It exists only to
speed up repeated TUI browse requests across launches.

## Stored Data

Each persisted entry records:

- `snapshot_max_publish_seq`: the published snapshot generation the entry was
  computed from
- `request_key`: a stable key derived from the normal browse request identity
  (`root`, `lens`, `filters`, and `path`)
- `request_json`: the serialized request identity for inspection/debugging
- `row_count`: the number of cached rollup rows
- `payload_json`: the serialized `Vec<RollupRow>` payload
- `payload_bytes`: the serialized payload size used for retention accounting
- `created_at_utc`
- `last_accessed_at_utc`

## Invalidation And Retention

The browse cache is keyed primarily by `snapshot_max_publish_seq`.

Behavior:

- A persisted hit is valid only for the same published snapshot generation.
- When a newer snapshot generation is stored or loaded, older snapshot entries
  are deleted automatically.
- The default retained payload budget is `64 MiB`.
- If the current snapshot generation would exceed that budget, the least
  recently accessed entries for that generation are evicted first.
- If a single payload is larger than the total budget by itself, it is not
  persisted.

## Runtime Interaction

The TUI still keeps the existing in-memory browse cache for the active session.

The lookup order is:

1. in-memory browse cache
2. persisted browse-cache sidecar
3. live query execution against the main usage database

When a persisted entry is used, it is hydrated back into the in-memory cache so
subsequent requests in the same session reuse the normal fast path.
