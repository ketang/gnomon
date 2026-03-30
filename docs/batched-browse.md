# Batched Browse Query and Cache Architecture

This document defines the target architecture for batched browse prefetch in the
gnomon TUI. It covers batched query types, prefetch queue semantics, cache
layering, snapshot-scoped invalidation, and eviction policy. Follow-on issues
(#76, #77, #78, #80) are scoped against this design.

## Status Quo

The current browse path is single-request:

```
expand node → load_rows_for_path(path) → cached_browse(BrowseRequest)
  → in-memory cache hit?  → return
  → SQLite sidecar hit?   → hydrate to memory, return
  → live query execution   → store in both caches, return
```

Each `BrowseRequest` fetches children for exactly one parent `BrowsePath`. There
is no batching, no prefetch, and no queue management. Expanding N siblings
produces N sequential queries.

## Design Goals

1. Fetch child row sets for many parent nodes in one batch.
2. Prioritize work around the current selection.
3. Reorder pending work when the view changes.
4. Use fewer, larger grouped queries where beneficial.
5. Persist warmed results across runs via the existing SQLite sidecar.
6. Remain fully compatible with the existing single-request browse path.

---

## Batch Browse Types

### BatchBrowseRequest

A batch request shares immutable context across all requested parents:

```rust
pub struct BatchBrowseRequest {
    pub snapshot: SnapshotBounds,
    pub root: RootView,
    pub lens: MetricLens,
    pub filters: BrowseFilters,
    pub paths: Vec<BrowsePath>,
}
```

`paths` contains one or more parent paths whose children should be fetched.
A single-element `paths` vec is equivalent to the existing `BrowseRequest`.

### BatchBrowseResponse

Results are keyed per parent path so callers can store them under the same cache
keys used by single-request browse:

```rust
pub struct BatchBrowseResponse {
    pub results: Vec<BatchBrowseResult>,
}

pub struct BatchBrowseResult {
    pub path: BrowsePath,
    pub rows: Vec<RollupRow>,
}
```

### Compatibility

The existing `BrowseRequest` remains the fundamental cache key unit. Batch
execution is an optimization over issuing N individual requests — the cache
layer is unaware of batching. `QueryEngine::browse()` continues to accept
single `BrowseRequest` values. A new `QueryEngine::browse_batch()` method
accepts `BatchBrowseRequest` and returns `BatchBrowseResponse`.

The TUI's `cached_browse()` function continues to operate on individual
`BrowseRequest` values. The prefetch system is the only caller of
`browse_batch()`, and it decomposes batch results into per-parent entries
before writing them into the cache.

---

## Grouped vs Path Browse Batching

The query engine already distinguishes two execution strategies based on
`BrowsePath` shape:

- **Grouped browse**: non-path levels (project, category, action). Uses
  precomputed `chunk_action_rollup` aggregates.
- **Path browse**: file/directory drill-down under a specific action. Uses
  `load_path_facts()` + `aggregate_path_request()`.

Batching works differently for each.

### Grouped Browse Batching

Grouped browse queries share the same base aggregation pipeline. A batch of N
sibling grouped paths (e.g., children of N different projects) can be served by
a single SQL query with an `IN`-clause on the parent discriminator column. This
is where batching provides the largest performance benefit:

- One SQL round-trip instead of N.
- One filter compilation and window computation shared across all parents.
- Results are partitioned by parent key in the application layer and written
  back as individual cache entries.

The batch grouped query adds a `parent_keys: Vec<ParentKey>` parameter to the
existing rollup aggregation path. `ParentKey` is an enum matching the
`BrowsePath` discriminator for the level being queried:

```rust
enum ParentKey {
    ProjectId(i64),
    Category(String),
    Action(ActionKey),
    CategoryAction { category: String, action: ActionKey },
    // ... one variant per grouped BrowsePath level
}
```

When `parent_keys` has one element, behavior is identical to the current
single-parent path. When it has multiple elements, the SQL uses
`IN (?1, ?2, ...)` on the discriminator column.

### Path Browse Batching

Path browse queries are inherently per-parent: each parent action defines an
independent `path_node` subtree. There is no SQL-level grouping benefit because
each subtree query touches disjoint rows.

Batching path browse requests still provides value by:

- Amortizing the overhead of filter compilation and snapshot window setup.
- Allowing the prefetch queue to submit multiple path queries as a unit.
- Enabling the query worker thread to execute them in a tight loop without
  returning to the event loop between each.

Path browse batching is implemented as a loop over individual
`load_path_facts()` calls within a single `browse_batch()` invocation, not as
a combined SQL query.

---

## Prefetch Queue

The prefetch system predicts which `BrowsePath` values the user is likely to
navigate to and fetches them before interaction. It operates as a priority
queue managed by a dedicated prefetch coordinator, separate from the existing
query worker.

### Queue Structure

```rust
struct PrefetchQueue {
    pending: VecDeque<PrefetchEntry>,
    in_flight: HashSet<BrowsePath>,
    completed: HashSet<BrowsePath>,
}

struct PrefetchEntry {
    path: BrowsePath,
    priority: PrefetchPriority,
}

enum PrefetchPriority {
    /// Children of the currently selected node.
    SelectedChildren,
    /// Siblings of the selected node (same parent, different key).
    SelectedSiblings,
    /// Children of visible but unselected expanded nodes.
    VisibleExpanded,
    /// Recursive depth: children of prefetched nodes, up to a depth limit.
    RecursiveDepth { depth: u8 },
}
```

Priority ordering (highest to lowest):

1. `SelectedChildren` — the user is most likely to expand the selected node.
2. `SelectedSiblings` — the user may arrow up/down to a sibling and expand.
3. `VisibleExpanded` — already-visible expanded nodes may need child refresh.
4. `RecursiveDepth { depth: 1 }` — one level beyond selected children.
5. `RecursiveDepth { depth: 2 }` — two levels beyond, and so on.

A configurable `max_recursive_depth` (default: 2) caps how deep prefetch goes.

### Enqueueing

Prefetch entries are enqueued when:

- The view is first rendered (root children + selected node children).
- A node is expanded (children of the newly expanded node).
- Selection changes (re-enqueue with updated priorities).
- A snapshot refresh occurs (all visible paths re-enqueued).

Before enqueueing, the coordinator checks:

1. Is this path already in the in-memory cache or row cache? Skip.
2. Is this path already in `in_flight` or `pending`? Update priority, skip
   duplicate.

### Reprioritization

When the user moves selection (arrow keys, jump), the coordinator:

1. Re-scores every entry in `pending` based on the new selection context.
2. Re-sorts `pending` by the new priority ordering.
3. Does **not** cancel in-flight queries — they will complete and populate the
   cache regardless. Cancellation adds complexity for minimal benefit since
   queries are fast.

This is O(n) in the pending queue size, which is bounded by the number of
visible + prefetchable paths (typically < 200).

### Batch Submission

The coordinator drains entries from the front of `pending` and groups them
into `BatchBrowseRequest` values:

- Group consecutive entries that share the same `BrowsePath` level type
  (all project-level, all category-level, etc.).
- Cap each batch at a configurable `max_batch_size` (default: 20 paths).
- Submit each batch to the query worker as a single unit.

When results arrive, the coordinator:

1. Decomposes `BatchBrowseResponse` into per-parent `(BrowseRequest, Vec<RollupRow>)` pairs.
2. Writes each pair into the in-memory `QueryResultCache`.
3. Writes each pair into the `BrowseCacheStore` (SQLite sidecar).
4. Moves paths from `in_flight` to `completed`.
5. Optionally enqueues recursive children of completed paths (up to depth limit).

### Interaction with User-Initiated Queries

User-initiated expansion always takes precedence:

- If the user expands a node whose children are already cached (from prefetch),
  `cached_browse()` returns the hit immediately — zero latency.
- If the user expands a node whose children are in-flight via prefetch, the
  existing `cached_browse()` path executes its own query. The prefetch result
  arrives later and is a no-op (cache already populated).
- Prefetch never blocks user interaction. The prefetch coordinator runs on the
  query worker thread, yielding to user-initiated `LoadView` requests.

---

## Cache Layering

The cache hierarchy remains three layers, unchanged in structure:

```
Layer 1: In-memory QueryResultCache  (fastest, session-scoped)
Layer 2: SQLite BrowseCacheStore     (persistent, cross-session)
Layer 3: Live query execution         (authoritative, slowest)
```

### What Changes

- **Layer 1** gains entries from prefetch in addition to user-initiated queries.
  No structural change — prefetch writes use the same `QueryResultCache` API.

- **Layer 2** gains entries from prefetch. No structural change — prefetch
  writes use the same `BrowseCacheStore::store()` API. The sidecar file remains
  `browse-cache.sqlite3` under the gnomon state directory.

- **No new database**. The existing `BrowseCacheStore` schema is sufficient.
  Batched results are decomposed into individual entries before storage, so the
  `(snapshot_max_publish_seq, request_key)` primary key is preserved.

### Cache Key Identity

The cache key is the JSON serialization of `PersistedBrowseRequest` (root,
lens, filters, path). This is unchanged. Batch queries produce N results, each
stored under its own `PersistedBrowseRequest` key. The cache cannot distinguish
whether an entry was populated by a user-initiated query or by prefetch — this
is intentional.

---

## Snapshot-Scoped Invalidation

Invalidation uses the existing `import_chunk.publish_seq` mechanism, unchanged:

1. Every `BrowseRequest` carries `SnapshotBounds { max_publish_seq, ... }`.
2. `BrowseCacheStore::load()` and `::store()` call
   `prune_superseded_snapshots(current_max_publish_seq)`, which deletes all
   entries with a lower `snapshot_max_publish_seq`.
3. When new data is imported and the user refreshes, `max_publish_seq` advances.
   The next cache access prunes all stale entries in one `DELETE` statement.

### Batch interaction with invalidation

Batch requests carry the same `SnapshotBounds` as single requests (the snapshot
is view-global). Pruning happens once per `store()` call, so a batch of 20
results triggers 20 prune checks — but after the first prune deletes stale
rows, the remaining 19 are no-ops (no rows match the `WHERE` clause). This is
harmless and does not require optimization.

### Prefetch queue invalidation

When a snapshot refresh occurs:

1. The prefetch coordinator clears `completed` and `in_flight` sets.
2. All entries in `pending` are discarded.
3. The coordinator re-enqueues paths based on the current view state and the
   new snapshot bounds.
4. The `QueryResultCache` (Layer 1) is cleared as it already is today on
   snapshot change.
5. The `BrowseCacheStore` (Layer 2) self-prunes on next access.

---

## Eviction and Cleanup

### In-Memory Cache (Layer 1)

- Scoped to the current `SnapshotBounds` generation. Cleared entirely on
  snapshot refresh.
- No size limit — bounded by the number of distinct `BrowsePath` values
  explored in one session, which is small relative to available memory.
- Prefetch adds more entries than today but the total remains manageable
  (hundreds, not thousands, at typical recursive depth).

### SQLite Sidecar Cache (Layer 2)

The existing eviction machinery in `BrowseCacheStore` is sufficient:

- **Superseded snapshot pruning**: `prune_superseded_snapshots()` deletes all
  entries from older snapshots on every `load()` and `store()` call. Only the
  current snapshot's entries survive.

- **Size budget enforcement**: `enforce_budget()` evicts the least-recently-
  accessed entries (by `last_accessed_at_utc`) when total `payload_bytes`
  exceeds `max_bytes` (default: 64 MiB).

- **Single-entry overflow protection**: entries larger than `max_bytes` are
  silently dropped rather than evicting the entire cache.

### Metadata Tracked

The existing `browse_entry` table already tracks:

| Column                       | Purpose                          |
|------------------------------|----------------------------------|
| `snapshot_max_publish_seq`   | Snapshot generation for pruning  |
| `request_key`                | Canonical cache key (JSON)       |
| `row_count`                  | Number of `RollupRow` values     |
| `payload_bytes`              | Size accounting for budget       |
| `created_at_utc`             | Entry creation time              |
| `last_accessed_at_utc`       | LRU eviction ordering            |

No schema changes are needed. Prefetch-generated entries are
indistinguishable from user-initiated entries in the persistence layer.

### Tuning Knobs

| Parameter                        | Default  | Location         |
|----------------------------------|----------|------------------|
| `browse_cache_max_bytes`         | 64 MiB   | `BrowseCacheStore` |
| `max_batch_size`                 | 20       | Prefetch coordinator |
| `max_recursive_depth`            | 2        | Prefetch coordinator |

Issue #80 will measure actual cache footprint under prefetch workloads and
adjust `browse_cache_max_bytes` if the 64 MiB default is insufficient.

---

## Component Interaction Summary

```
┌─────────────────────────────────────────────────────────┐
│                     TUI Event Loop                       │
│                                                          │
│  selection change ──→ PrefetchCoordinator.reprioritize() │
│  node expand ──────→ cached_browse() [existing path]     │
│                      PrefetchCoordinator.enqueue()       │
│  snapshot refresh ─→ PrefetchCoordinator.reset()         │
└──────────────────────────┬──────────────────────────────┘
                           │
              ┌────────────▼────────────┐
              │   PrefetchCoordinator   │
              │                         │
              │  pending: VecDeque      │
              │  in_flight: HashSet     │
              │  completed: HashSet     │
              │                         │
              │  drain + batch ─────────┼──→ BatchBrowseRequest
              └────────────┬────────────┘
                           │
              ┌────────────▼────────────┐
              │      Query Worker       │
              │                         │
              │  browse_batch() ────────┼──→ QueryEngine
              │                         │       │
              │  ◄── BatchBrowseResponse│◄──────┘
              └────────────┬────────────┘
                           │
              decompose into per-parent results
                           │
         ┌─────────────────┼─────────────────┐
         ▼                                   ▼
  QueryResultCache                   BrowseCacheStore
  (in-memory, L1)                    (SQLite sidecar, L2)
```

---

## Follow-On Issue Mapping

| Issue | Scope |
|-------|-------|
| #76   | Implement `browse_batch()` for grouped (non-path) queries with `IN`-clause batching |
| #77   | Implement `browse_batch()` for path-drill queries with loop-based batching |
| #78   | Add `PrefetchCoordinator` with priority queue, reprioritization, and recursive enqueue |
| #80   | Measure cache footprint under prefetch, tune `max_bytes` and `max_batch_size` |
