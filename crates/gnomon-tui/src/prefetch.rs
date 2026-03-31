use std::collections::{BTreeMap, BTreeSet};

use gnomon_core::query::{BrowseFilters, BrowsePath, MetricLens, RootView, SnapshotBounds};

/// Cap for a single batch submission to the worker.
const MAX_BATCH_SIZE: usize = 20;

/// Maximum recursive depth for prefetch expansion (0-indexed: depth 0 spawns
/// depth 1, depth 1 spawns depth 2, depth 2 does not spawn further).
/// Maximum recursive depth for prefetch expansion. Reduced from 2 to 1 based
/// on footprint analysis (docs/browse-cache-footprint.md): depth 2 causes
/// geometric entry growth that exceeds the 64 MiB cache budget at 20+ projects.
const MAX_RECURSIVE_DEPTH: u8 = 1;

/// Number of visible rows above/below the selection to prefetch.
const NEARBY_VISIBLE_HALF_WINDOW: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PrefetchPriority {
    /// The selected row's selection context.
    SelectedRowContext,
    /// Nearby visible rows around the current selection.
    NearbyVisibleRow,
    /// The rest of the visible window.
    VisibleExpanded,
    /// Recursive depth: children of prefetched nodes.
    RecursiveDepth(u8),
}

#[derive(Debug, Clone)]
pub(crate) struct PrefetchEntry {
    pub path: BrowsePath,
    pub priority: PrefetchPriority,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PrefetchBatchProfile {
    pub selected_row_count: usize,
    pub nearby_visible_row_count: usize,
    pub visible_row_count: usize,
    pub recursive_depth_count: usize,
}

/// Immutable context shared across all entries in one prefetch generation.
#[derive(Debug, Clone)]
pub(crate) struct PrefetchContext {
    pub snapshot: SnapshotBounds,
    pub root: RootView,
    pub lens: MetricLens,
    pub filters: BrowseFilters,
}

/// Minimal view of a TreeRow needed for priority classification.
pub(crate) struct VisibleRowInfo {
    pub node_path: Option<BrowsePath>,
}

pub(crate) struct PrefetchCoordinator {
    context: Option<PrefetchContext>,
    pending: Vec<PrefetchEntry>,
    in_flight: BTreeSet<String>,
    /// Tracks the priority each in-flight path was enqueued at, so
    /// `complete_batch` can determine the correct recursive child depth.
    in_flight_priorities: BTreeMap<String, PrefetchPriority>,
    completed: BTreeSet<String>,
    pending_keys: BTreeSet<String>,
}

impl PrefetchCoordinator {
    pub fn new() -> Self {
        Self {
            context: None,
            pending: Vec::new(),
            in_flight: BTreeSet::new(),
            in_flight_priorities: BTreeMap::new(),
            completed: BTreeSet::new(),
            pending_keys: BTreeSet::new(),
        }
    }

    /// Clear all state and repopulate from current view.
    ///
    /// Called after `apply_loaded_view()` and on snapshot refresh.
    pub fn populate(
        &mut self,
        context: PrefetchContext,
        selected_path: Option<&BrowsePath>,
        selected_parent: Option<&BrowsePath>,
        visible_rows: &[VisibleRowInfo],
        selected_index: Option<usize>,
    ) {
        self.reset();
        self.context = Some(context);
        self.enqueue_from_view(selected_path, selected_parent, visible_rows, selected_index);
        self.sort_pending();
    }

    /// Re-score and re-sort pending entries for a new selection.
    ///
    /// Does not touch in_flight or completed. Does not add new entries from
    /// visible_rows — only reclassifies existing pending entries and adds
    /// newly-relevant paths from the view.
    pub fn reprioritize(
        &mut self,
        selected_path: Option<&BrowsePath>,
        selected_parent: Option<&BrowsePath>,
        visible_rows: &[VisibleRowInfo],
        selected_index: Option<usize>,
    ) {
        // Re-classify existing pending entries.
        for entry in &mut self.pending {
            entry.priority = Self::classify_priority(
                &entry.path,
                selected_path,
                selected_parent,
                selected_index,
                visible_rows,
            );
        }

        // Add any new paths from the view that aren't already tracked.
        self.enqueue_from_view(selected_path, selected_parent, visible_rows, selected_index);

        self.sort_pending();
    }

    /// Full reset: clear everything.
    pub fn reset(&mut self) {
        self.context = None;
        self.pending.clear();
        self.in_flight.clear();
        self.in_flight_priorities.clear();
        self.completed.clear();
        self.pending_keys.clear();
    }

    /// Drain up to `MAX_BATCH_SIZE` entries from the front of pending,
    /// skipping any that are already completed or in flight.
    ///
    /// Returns `None` if nothing to submit. Moves drained paths into
    /// `in_flight`.
    pub fn drain_batch(
        &mut self,
    ) -> Option<(PrefetchContext, Vec<BrowsePath>, PrefetchBatchProfile)> {
        let context = self.context.as_ref()?.clone();
        let mut batch = Vec::new();
        let mut profile = PrefetchBatchProfile::default();

        while batch.len() < MAX_BATCH_SIZE && !self.pending.is_empty() {
            let entry = self.pending.remove(0);
            let key = Self::path_key(&entry.path);
            self.pending_keys.remove(&key);

            if self.completed.contains(&key) || self.in_flight.contains(&key) {
                continue;
            }

            self.in_flight.insert(key.clone());
            profile.record(entry.priority.clone());
            self.in_flight_priorities.insert(key, entry.priority);
            batch.push(entry.path);
        }

        if batch.is_empty() {
            return None;
        }

        Some((context, batch, profile))
    }

    /// Record that results have arrived for these paths.
    ///
    /// Moves them from in_flight to completed. Enqueues recursive children
    /// as `RecursiveDepth` entries if depth < `MAX_RECURSIVE_DEPTH`.
    ///
    /// `child_paths_per_parent` maps each completed path to the child
    /// `BrowsePath` values derived from its result rows.
    pub fn complete_batch(&mut self, child_paths_per_parent: &[(BrowsePath, Vec<BrowsePath>)]) {
        for (parent_path, children) in child_paths_per_parent {
            let key = Self::path_key(parent_path);
            self.in_flight.remove(&key);
            let parent_priority = self
                .in_flight_priorities
                .remove(&key)
                .unwrap_or(PrefetchPriority::VisibleExpanded);
            self.completed.insert(key);

            if let Some(depth) = Self::child_depth_for_priority(&parent_priority) {
                for child in children {
                    self.enqueue_if_new(child.clone(), PrefetchPriority::RecursiveDepth(depth));
                }
            }
        }

        self.sort_pending();
    }

    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub fn has_in_flight(&self) -> bool {
        !self.in_flight.is_empty()
    }

    // --- internal helpers ---

    fn path_key(path: &BrowsePath) -> String {
        serde_json::to_string(path).expect("BrowsePath serialization should not fail")
    }

    fn enqueue_if_new(&mut self, path: BrowsePath, priority: PrefetchPriority) {
        let key = Self::path_key(&path);
        if self.completed.contains(&key)
            || self.in_flight.contains(&key)
            || self.pending_keys.contains(&key)
        {
            return;
        }
        self.pending_keys.insert(key);
        self.pending.push(PrefetchEntry { path, priority });
    }

    fn enqueue_from_view(
        &mut self,
        selected_path: Option<&BrowsePath>,
        _selected_parent: Option<&BrowsePath>,
        visible_rows: &[VisibleRowInfo],
        selected_index: Option<usize>,
    ) {
        // 1. The selected row's selection context.
        if let Some(sel_path) = selected_path {
            self.enqueue_if_new(sel_path.clone(), PrefetchPriority::SelectedRowContext);
        }

        // 2. Nearby visible rows (window around selection).
        if let Some(sel_idx) = selected_index {
            let start = sel_idx.saturating_sub(NEARBY_VISIBLE_HALF_WINDOW);
            let end = (sel_idx + NEARBY_VISIBLE_HALF_WINDOW + 1).min(visible_rows.len());
            for row in &visible_rows[start..end] {
                if let Some(ref node_path) = row.node_path {
                    self.enqueue_if_new(node_path.clone(), PrefetchPriority::NearbyVisibleRow);
                }
            }
        }

        // 3. All remaining visible rows.
        for row in visible_rows {
            if let Some(ref node_path) = row.node_path {
                self.enqueue_if_new(node_path.clone(), PrefetchPriority::VisibleExpanded);
            }
        }
    }

    fn classify_priority(
        path: &BrowsePath,
        selected_path: Option<&BrowsePath>,
        _selected_parent: Option<&BrowsePath>,
        selected_index: Option<usize>,
        visible_rows: &[VisibleRowInfo],
    ) -> PrefetchPriority {
        // If this is the selected node's path, it is the selected row's context.
        if let Some(sel_path) = selected_path
            && path == sel_path
        {
            return PrefetchPriority::SelectedRowContext;
        }

        // If this path is inside the nearby selection window, keep it ahead of the rest.
        if let Some(sel_idx) = selected_index {
            let start = sel_idx.saturating_sub(NEARBY_VISIBLE_HALF_WINDOW);
            let end = (sel_idx + NEARBY_VISIBLE_HALF_WINDOW + 1).min(visible_rows.len());
            for row in &visible_rows[start..end] {
                if let Some(ref node_path) = row.node_path
                    && node_path == path
                {
                    return PrefetchPriority::NearbyVisibleRow;
                }
            }
        }

        // Otherwise it's a visible expandable node.
        PrefetchPriority::VisibleExpanded
    }

    fn sort_pending(&mut self) {
        self.pending.sort_by(|a, b| a.priority.cmp(&b.priority));
    }

    /// Determine the recursive depth for children of a completed parent.
    ///
    /// Returns `None` if children should not be enqueued (depth exceeded).
    /// We track depth via the entry's `PrefetchPriority::RecursiveDepth(d)`
    /// value, but once an entry is completed we no longer have its priority.
    /// Top-level entries (selected row / nearby / visible) spawn children at
    /// depth 0. The depth is approximate but bounded by `MAX_RECURSIVE_DEPTH`.
    fn child_depth_for_priority(priority: &PrefetchPriority) -> Option<u8> {
        match priority {
            PrefetchPriority::RecursiveDepth(d) => {
                let next = d + 1;
                if next >= MAX_RECURSIVE_DEPTH {
                    None
                } else {
                    Some(next)
                }
            }
            _ => Some(0),
        }
    }
}

impl PrefetchBatchProfile {
    fn record(&mut self, priority: PrefetchPriority) {
        match priority {
            PrefetchPriority::SelectedRowContext => self.selected_row_count += 1,
            PrefetchPriority::NearbyVisibleRow => self.nearby_visible_row_count += 1,
            PrefetchPriority::VisibleExpanded => self.visible_row_count += 1,
            PrefetchPriority::RecursiveDepth(_) => self.recursive_depth_count += 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gnomon_core::query::{BrowseFilters, BrowsePath, MetricLens, RootView, SnapshotBounds};

    fn test_context() -> PrefetchContext {
        PrefetchContext {
            snapshot: SnapshotBounds::bootstrap(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::Total,
            filters: BrowseFilters::default(),
        }
    }

    fn project_path(id: i64) -> BrowsePath {
        BrowsePath::Project { project_id: id }
    }

    fn category_path(name: &str) -> BrowsePath {
        BrowsePath::Category {
            category: name.to_string(),
        }
    }

    fn make_row(_parent: BrowsePath, node: Option<BrowsePath>) -> VisibleRowInfo {
        VisibleRowInfo { node_path: node }
    }

    #[test]
    fn populate_enqueues_selected_context_then_nearby_visible_rows() {
        let mut coord = PrefetchCoordinator::new();
        let rows: Vec<VisibleRowInfo> = (0..12)
            .map(|i| make_row(BrowsePath::Root, Some(project_path(i))))
            .collect();
        let selected = project_path(5);

        coord.populate(
            test_context(),
            Some(&selected),
            Some(&BrowsePath::Root),
            &rows,
            Some(5),
        );

        assert!(!coord.pending.is_empty());
        assert_eq!(coord.pending[0].path, selected);
        assert_eq!(
            coord.pending[0].priority,
            PrefetchPriority::SelectedRowContext
        );

        let (_, batch, profile) = coord.drain_batch().expect("should have entries");
        assert_eq!(batch.first(), Some(&selected));
        assert_eq!(profile.selected_row_count, 1);
        assert_eq!(profile.nearby_visible_row_count, 8);
        assert_eq!(profile.visible_row_count, 3);
        assert_eq!(profile.recursive_depth_count, 0);
    }

    #[test]
    fn populate_deduplicates_paths() {
        let mut coord = PrefetchCoordinator::new();
        let selected = project_path(1);
        // The selected path appears both as the selected row and in the visible window.
        let rows = vec![
            make_row(BrowsePath::Root, Some(project_path(1))),
            make_row(BrowsePath::Root, Some(project_path(2))),
        ];

        coord.populate(
            test_context(),
            Some(&selected),
            Some(&BrowsePath::Root),
            &rows,
            Some(0),
        );

        let path_count = coord.pending.iter().filter(|e| e.path == selected).count();
        assert_eq!(path_count, 1, "selected path should appear exactly once");
    }

    #[test]
    fn drain_batch_respects_max_size() {
        let mut coord = PrefetchCoordinator::new();
        let rows: Vec<VisibleRowInfo> = (0..30)
            .map(|i| make_row(BrowsePath::Root, Some(project_path(i))))
            .collect();

        coord.populate(test_context(), None, None, &rows, None);

        let (_, batch, _) = coord.drain_batch().expect("should have entries");
        assert!(batch.len() <= MAX_BATCH_SIZE);
        assert_eq!(batch.len(), MAX_BATCH_SIZE);
    }

    #[test]
    fn drain_batch_skips_completed() {
        let mut coord = PrefetchCoordinator::new();
        let rows = vec![
            make_row(BrowsePath::Root, Some(project_path(1))),
            make_row(BrowsePath::Root, Some(project_path(2))),
        ];

        coord.populate(test_context(), None, None, &rows, None);

        // Mark project 1 as completed.
        coord
            .completed
            .insert(PrefetchCoordinator::path_key(&project_path(1)));

        let (_, batch, _) = coord.drain_batch().expect("should have entries");
        assert!(!batch.contains(&project_path(1)));
        assert!(batch.contains(&project_path(2)));
    }

    #[test]
    fn drain_batch_skips_in_flight() {
        let mut coord = PrefetchCoordinator::new();
        let rows = vec![
            make_row(BrowsePath::Root, Some(project_path(1))),
            make_row(BrowsePath::Root, Some(project_path(2))),
        ];

        coord.populate(test_context(), None, None, &rows, None);

        // Mark project 1 as in flight.
        coord
            .in_flight
            .insert(PrefetchCoordinator::path_key(&project_path(1)));

        let (_, batch, _) = coord.drain_batch().expect("should have entries");
        assert!(!batch.contains(&project_path(1)));
        assert!(batch.contains(&project_path(2)));
    }

    #[test]
    fn complete_batch_enqueues_recursive_children() {
        let mut coord = PrefetchCoordinator::new();
        coord.context = Some(test_context());
        let parent = project_path(1);
        let child_a = category_path("coding");
        let child_b = category_path("review");

        coord
            .in_flight
            .insert(PrefetchCoordinator::path_key(&parent));

        coord.complete_batch(&[(parent.clone(), vec![child_a.clone(), child_b.clone()])]);

        assert!(
            coord
                .completed
                .contains(&PrefetchCoordinator::path_key(&parent))
        );
        assert!(
            !coord
                .in_flight
                .contains(&PrefetchCoordinator::path_key(&parent))
        );
        assert!(coord.pending.iter().any(|e| e.path == child_a));
        assert!(coord.pending.iter().any(|e| e.path == child_b));
        assert!(
            coord
                .pending
                .iter()
                .all(|e| matches!(e.priority, PrefetchPriority::RecursiveDepth(0)))
        );
    }

    #[test]
    fn complete_batch_respects_max_depth() {
        let mut coord = PrefetchCoordinator::new();
        coord.context = Some(test_context());

        // Enqueue a recursive depth entry at MAX_RECURSIVE_DEPTH - 1.
        // When completed, its children should get depth MAX_RECURSIVE_DEPTH,
        // which is at the limit, so no further children should be enqueued.
        let deep_path = project_path(99);
        coord.enqueue_if_new(
            deep_path.clone(),
            PrefetchPriority::RecursiveDepth(MAX_RECURSIVE_DEPTH - 1),
        );

        // Drain and simulate completion.
        let (_, batch, _) = coord.drain_batch().expect("should have entries");
        assert!(batch.contains(&deep_path));

        let child = category_path("deep-child");
        coord.complete_batch(&[(deep_path.clone(), vec![child.clone()])]);

        // Children of a MAX_RECURSIVE_DEPTH-1 parent should NOT be enqueued
        // because their depth would be MAX_RECURSIVE_DEPTH (at the limit).
        assert!(
            !coord.pending.iter().any(|e| e.path == child),
            "children at max depth should not be enqueued"
        );
    }

    #[test]
    fn reprioritize_reorders_pending() {
        let mut coord = PrefetchCoordinator::new();
        let rows = vec![
            make_row(BrowsePath::Root, Some(project_path(1))),
            make_row(BrowsePath::Root, Some(project_path(2))),
            make_row(BrowsePath::Root, Some(project_path(3))),
        ];

        // Populate with project 1 selected.
        coord.populate(
            test_context(),
            Some(&project_path(1)),
            Some(&BrowsePath::Root),
            &rows,
            Some(0),
        );

        let first_before = coord.pending[0].path.clone();
        assert_eq!(first_before, project_path(1));

        // Reprioritize with project 3 selected.
        coord.reprioritize(
            Some(&project_path(3)),
            Some(&BrowsePath::Root),
            &rows,
            Some(2),
        );

        assert_eq!(coord.pending[0].path, project_path(3));
        assert_eq!(
            coord.pending[0].priority,
            PrefetchPriority::SelectedRowContext
        );
    }

    #[test]
    fn reprioritize_preserves_in_flight_and_completed() {
        let mut coord = PrefetchCoordinator::new();
        let rows = vec![make_row(BrowsePath::Root, Some(project_path(1)))];

        coord.populate(
            test_context(),
            Some(&project_path(1)),
            Some(&BrowsePath::Root),
            &rows,
            Some(0),
        );

        coord
            .in_flight
            .insert(PrefetchCoordinator::path_key(&project_path(10)));
        coord
            .completed
            .insert(PrefetchCoordinator::path_key(&project_path(20)));

        coord.reprioritize(
            Some(&project_path(1)),
            Some(&BrowsePath::Root),
            &rows,
            Some(0),
        );

        assert!(
            coord
                .in_flight
                .contains(&PrefetchCoordinator::path_key(&project_path(10)))
        );
        assert!(
            coord
                .completed
                .contains(&PrefetchCoordinator::path_key(&project_path(20)))
        );
    }

    #[test]
    fn reset_clears_everything() {
        let mut coord = PrefetchCoordinator::new();
        let rows = vec![make_row(BrowsePath::Root, Some(project_path(1)))];

        coord.populate(
            test_context(),
            Some(&project_path(1)),
            Some(&BrowsePath::Root),
            &rows,
            Some(0),
        );
        coord
            .in_flight
            .insert(PrefetchCoordinator::path_key(&project_path(10)));
        coord
            .completed
            .insert(PrefetchCoordinator::path_key(&project_path(20)));

        coord.reset();

        assert!(coord.pending.is_empty());
        assert!(coord.in_flight.is_empty());
        assert!(coord.completed.is_empty());
        assert!(coord.pending_keys.is_empty());
        assert!(coord.context.is_none());
    }

    #[test]
    fn reset_clears_in_flight() {
        let mut coord = PrefetchCoordinator::new();
        coord.context = Some(test_context());
        let path = project_path(1);
        coord.in_flight.insert(PrefetchCoordinator::path_key(&path));

        coord.reset();

        assert!(
            !coord
                .in_flight
                .contains(&PrefetchCoordinator::path_key(&path))
        );
        assert!(
            !coord
                .completed
                .contains(&PrefetchCoordinator::path_key(&path))
        );
        assert!(coord.context.is_none());
    }

    #[test]
    fn drain_returns_none_when_empty() {
        let mut coord = PrefetchCoordinator::new();
        coord.context = Some(test_context());
        assert!(coord.drain_batch().is_none());
    }

    #[test]
    fn drain_returns_none_without_context() {
        let mut coord = PrefetchCoordinator::new();
        assert!(coord.drain_batch().is_none());
    }

    #[test]
    fn populate_after_reset_starts_fresh() {
        let mut coord = PrefetchCoordinator::new();
        let rows = vec![make_row(BrowsePath::Root, Some(project_path(1)))];

        coord.populate(
            test_context(),
            Some(&project_path(1)),
            Some(&BrowsePath::Root),
            &rows,
            Some(0),
        );

        coord.reset();

        let rows2 = vec![make_row(BrowsePath::Root, Some(project_path(99)))];
        coord.populate(
            test_context(),
            Some(&project_path(99)),
            Some(&BrowsePath::Root),
            &rows2,
            Some(0),
        );

        assert!(coord.pending.iter().any(|e| e.path == project_path(99)));
        assert!(!coord.pending.iter().any(|e| e.path == project_path(1)));
    }

    #[test]
    fn drip_feed_loop_processes_all_pending() {
        let mut coord = PrefetchCoordinator::new();
        let rows: Vec<VisibleRowInfo> = (0..5)
            .map(|i| make_row(BrowsePath::Root, Some(project_path(i))))
            .collect();

        coord.populate(test_context(), None, None, &rows, None);

        let mut all_batched = Vec::new();
        while let Some((_, batch, _)) = coord.drain_batch() {
            // Simulate completion with no children.
            let completions: Vec<(BrowsePath, Vec<BrowsePath>)> =
                batch.iter().map(|p| (p.clone(), vec![])).collect();
            coord.complete_batch(&completions);
            all_batched.extend(batch);
        }

        assert_eq!(all_batched.len(), 5);
        assert!(!coord.has_pending());
        assert!(!coord.has_in_flight());
    }
}
