use std::any::Any;
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Stdout};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::gnomon_sunburst::{
    build_sunburst_layer, build_sunburst_model, build_sunburst_scope_label,
};
use crate::prefetch::{PrefetchBatchProfile, PrefetchContext, PrefetchCoordinator, VisibleRowInfo};
use crate::sunburst::{
    SunburstDistortionPolicy, SunburstLayer, SunburstModel, SunburstPane, SunburstRenderConfig,
    SunburstSpan, sunburst_selected_child_span,
};
use crate::{StartupBrowseState, StartupLoadProgressUpdate};
use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use gnomon_core::browse_cache::{
    BrowseCacheStore, default_browse_cache_path, reset_derived_cache_artifacts,
};
use gnomon_core::config::RuntimeConfig;
use gnomon_core::db::{Database, reset_sqlite_database, sqlite_artifact_size_bytes};
use gnomon_core::import::{
    StartupOpenReason, StartupProgressUpdate, StartupWorkerEvent, import_all,
    scan_source_manifest_with_policy,
};
use gnomon_core::opportunity::{OpportunityCategory, OpportunityConfidence};
use gnomon_core::perf::{PerfLogger, PerfScope};
use gnomon_core::query::{
    ActionKey, BrowseFilters, BrowsePath, BrowseRequest, ClassificationState, FilterOptions,
    MetricLens, QueryEngine, RollupRow, RollupRowKind, RootView, SkillAttributionConfidence,
    SnapshotBounds, SnapshotCoverageSummary, TimeWindowFilter,
};
use gnomon_core::vcs::ProjectIdentityKind;
use jiff::ToSpan;
use nucleo_matcher::pattern::{CaseMatching, Normalization, Pattern};
use nucleo_matcher::{Config as MatcherConfig, Matcher};
use ratatui::backend::{CrosstermBackend, TestBackend};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Table, TableState, Wrap,
};
use ratatui::{Frame, Terminal};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use std::f64::consts::TAU;

#[cfg(test)]
#[path = "selection_lag_measurement.rs"]
mod selection_lag_measurement;

#[cfg(test)]
use crate::sunburst::{
    SunburstBucket, SunburstCenter, SunburstRenderMode, SunburstSegment,
    sunburst_center_label_area, sunburst_center_label_style, sunburst_segment_at_angle,
};

const UI_STATE_FILENAME: &str = "tui-state.json";
const REFRESH_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const WIDE_LAYOUT_WIDTH: u16 = 120;
const MAP_PANE_INNER_ASPECT_NUMERATOR: u16 = 9;
const MAP_PANE_INNER_ASPECT_DENOMINATOR: u16 = 4;
const MAP_PANE_MIN_WIDTH: u16 = 48;
const STATISTICS_PANE_MIN_WIDTH: u16 = 56;
const JUMP_MATCH_LIMIT: usize = 8;
const ACTIVITY_SPINNER_FRAMES: [&str; 4] = ["|", "/", "-", "\\"];
const VIEW_LOAD_PHASE_TOTAL: usize = 8;
const JUMP_TARGET_PHASE_TOTAL: usize = 4;
const PREFETCH_RECURSION_BREADTH_LIMIT: usize = 4;
const SELECTION_SLOW_LOG_THRESHOLD: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum QueryCacheDomain {
    Browse,
    FilterOptions,
    SnapshotCoverage,
}

#[derive(Debug, Clone, Serialize)]
struct BrowseFanoutRepeatedRequest {
    signature: String,
    count: usize,
}

#[derive(Debug, Default)]
struct BrowseFanoutStats {
    total_requests: usize,
    requests: BTreeMap<String, usize>,
}

impl BrowseFanoutStats {
    fn record(&mut self, request: &BrowseRequest) {
        self.total_requests += 1;
        let signature = browse_request_signature(request);
        *self.requests.entry(signature).or_default() += 1;
    }

    fn total_requests(&self) -> usize {
        self.total_requests
    }

    fn distinct_request_count(&self) -> usize {
        self.requests.len()
    }

    fn duplicate_request_count(&self) -> usize {
        self.total_requests.saturating_sub(self.requests.len())
    }

    fn repeated_requests(&self, limit: usize) -> Vec<BrowseFanoutRepeatedRequest> {
        let mut repeated = self
            .requests
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(signature, count)| BrowseFanoutRepeatedRequest {
                signature: signature.clone(),
                count: *count,
            })
            .collect::<Vec<_>>();

        repeated.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.signature.cmp(&right.signature))
        });

        repeated.truncate(limit);
        repeated
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct QueryCacheStats {
    hits: usize,
    misses: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct BrowseCacheSourceStats {
    memory_hits: usize,
    persisted_hits: usize,
    live_queries: usize,
}

impl BrowseCacheSourceStats {
    fn record(&mut self, source: BrowseCacheSource) {
        match source {
            BrowseCacheSource::Memory => self.memory_hits += 1,
            BrowseCacheSource::Persisted => self.persisted_hits += 1,
            BrowseCacheSource::Live => self.live_queries += 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrowseCacheSource {
    Memory,
    Persisted,
    Live,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct QueryCacheKey {
    domain: QueryCacheDomain,
    input: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SelectionContextKey {
    snapshot: SnapshotBounds,
    root: RootView,
    lens: MetricLens,
    filters: BrowseFilters,
    selected_row_key: Option<TreeRowKey>,
    active_path: BrowsePath,
}

#[derive(Debug, Clone)]
struct SelectionContextValue {
    active_path: BrowsePath,
    current_project_root: Option<String>,
    breadcrumb_targets: Vec<BreadcrumbTarget>,
    radial_context: RadialContext,
    selected_project_identity: Option<SelectedProjectIdentity>,
}

#[derive(Debug, Clone)]
struct SelectionContextEntry {
    key: SelectionContextKey,
    value: SelectionContextValue,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SelectionContextCacheStats {
    hits: usize,
    misses: usize,
}

#[derive(Default)]
struct SelectionContextCache {
    entries: BTreeMap<String, SelectionContextEntry>,
    stats: SelectionContextCacheStats,
}

struct SnapshotQueryCache {
    snapshot: SnapshotBounds,
    entries: BTreeMap<QueryCacheKey, Box<dyn Any>>,
}

#[derive(Default)]
struct QueryResultCache {
    snapshots: Vec<SnapshotQueryCache>,
    stats: BTreeMap<QueryCacheDomain, QueryCacheStats>,
}

impl QueryResultCache {
    fn memoize<K, V, F>(
        &mut self,
        domain: QueryCacheDomain,
        snapshot: &SnapshotBounds,
        input: &K,
        build: F,
    ) -> Result<V>
    where
        K: Serialize,
        V: Clone + Any + 'static,
        F: FnOnce() -> Result<V>,
    {
        let key = QueryCacheKey {
            domain,
            input: serde_json::to_string(input).context("unable to serialize query cache key")?,
        };

        if let Some(cached) = self.cached::<V>(snapshot, &key) {
            self.stats.entry(domain).or_default().hits += 1;
            return Ok(cached);
        }

        self.stats.entry(domain).or_default().misses += 1;
        let value = build()?;
        self.snapshot_bucket_mut(snapshot)
            .entries
            .insert(key, Box::new(value.clone()));
        Ok(value)
    }

    fn retain_snapshot(&mut self, snapshot: &SnapshotBounds) {
        self.snapshots.retain(|bucket| &bucket.snapshot == snapshot);
    }

    fn clear_domain(&mut self, domain: QueryCacheDomain) {
        for bucket in &mut self.snapshots {
            bucket.entries.retain(|key, _| key.domain != domain);
        }
        self.stats.remove(&domain);
    }

    fn cached<V>(&self, snapshot: &SnapshotBounds, key: &QueryCacheKey) -> Option<V>
    where
        V: Clone + Any + 'static,
    {
        let bucket = self
            .snapshots
            .iter()
            .find(|bucket| bucket.snapshot == *snapshot)?;
        let value = bucket.entries.get(key)?;
        let typed = value.downcast_ref::<V>()?;
        Some(typed.clone())
    }

    fn snapshot_bucket_mut(&mut self, snapshot: &SnapshotBounds) -> &mut SnapshotQueryCache {
        if let Some(index) = self
            .snapshots
            .iter()
            .position(|bucket| bucket.snapshot == *snapshot)
        {
            return &mut self.snapshots[index];
        }

        self.snapshots.push(SnapshotQueryCache {
            snapshot: snapshot.clone(),
            entries: BTreeMap::new(),
        });
        self.snapshots
            .last_mut()
            .expect("snapshot cache bucket inserted")
    }

    fn insert_browse_rows(
        &mut self,
        snapshot: &SnapshotBounds,
        request: &BrowseRequest,
        rows: Vec<RollupRow>,
    ) -> Result<()> {
        let key = QueryCacheKey {
            domain: QueryCacheDomain::Browse,
            input: serde_json::to_string(request).context("unable to serialize query cache key")?,
        };
        self.snapshot_bucket_mut(snapshot)
            .entries
            .insert(key, Box::new(rows));
        Ok(())
    }

    #[cfg(test)]
    fn stats_for(&self, domain: QueryCacheDomain) -> QueryCacheStats {
        self.stats.get(&domain).copied().unwrap_or_default()
    }

    #[cfg(test)]
    fn snapshot_count(&self) -> usize {
        self.snapshots.len()
    }
}

impl SelectionContextCache {
    #[cfg(test)]
    fn memoize<F>(&mut self, key: SelectionContextKey, build: F) -> Result<SelectionContextValue>
    where
        F: FnOnce() -> Result<SelectionContextValue>,
    {
        let serialized_key = serde_json::to_string(&key)
            .context("unable to serialize selection context cache key")?;

        if let Some(entry) = self.entries.get(&serialized_key) {
            self.stats.hits += 1;
            return Ok(entry.value.clone());
        }

        self.stats.misses += 1;
        let value = build()?;
        self.entries.insert(
            serialized_key,
            SelectionContextEntry {
                key,
                value: value.clone(),
            },
        );
        Ok(value)
    }

    fn lookup(&mut self, key: &SelectionContextKey) -> Result<Option<SelectionContextValue>> {
        let serialized_key = serde_json::to_string(key)
            .context("unable to serialize selection context cache key")?;
        Ok(self
            .entries
            .get(&serialized_key)
            .map(|entry| entry.value.clone()))
    }

    fn store(&mut self, key: SelectionContextKey, value: SelectionContextValue) -> Result<()> {
        let serialized_key = serde_json::to_string(&key)
            .context("unable to serialize selection context cache key")?;
        self.entries
            .insert(serialized_key, SelectionContextEntry { key, value });
        Ok(())
    }

    fn retain_snapshot(&mut self, snapshot: &SnapshotBounds) {
        self.entries
            .retain(|_, entry| &entry.key.snapshot == snapshot);
    }

    #[cfg(test)]
    fn stats(&self) -> SelectionContextCacheStats {
        self.stats
    }
}

struct QueryWorker {
    requests: Sender<QueryWorkerRequest>,
    results: Receiver<QueryWorkerResult>,
    handle: Option<JoinHandle<()>>,
}

impl QueryWorker {
    fn spawn(
        db_path: PathBuf,
        browse_cache_path: PathBuf,
        perf_logger: Option<PerfLogger>,
    ) -> Self {
        let (request_tx, request_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            run_query_worker(
                db_path,
                browse_cache_path,
                perf_logger,
                request_rx,
                result_tx,
            );
        });

        Self {
            requests: request_tx,
            results: result_rx,
            handle: Some(handle),
        }
    }

    fn send(&self, request: QueryWorkerRequest) -> Result<()> {
        self.requests
            .send(request)
            .context("interactive query worker is unavailable")
    }
}

impl Drop for QueryWorker {
    fn drop(&mut self) {
        let (placeholder_tx, _) = mpsc::channel();
        let sender = std::mem::replace(&mut self.requests, placeholder_tx);
        drop(sender);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Debug)]
enum QueryWorkerRequest {
    LoadView(ViewLoadRequest),
    RefreshLatest(RefreshViewRequest),
    BuildJumpTargets(JumpTargetRequest),
    Prefetch(PrefetchRequest),
}

#[derive(Debug)]
struct ViewLoadRequest {
    sequence: u64,
    label: &'static str,
    snapshot: SnapshotBounds,
    ui_state: PersistedUiState,
    selected_key: Option<TreeRowKey>,
}

#[derive(Debug)]
struct RefreshViewRequest {
    sequence: u64,
    label: &'static str,
    current_snapshot: SnapshotBounds,
    ui_state: PersistedUiState,
    selected_key: Option<TreeRowKey>,
}

#[derive(Debug)]
struct JumpTargetRequest {
    sequence: u64,
    label: &'static str,
    snapshot: SnapshotBounds,
    ui_state: PersistedUiState,
}

#[derive(Debug)]
struct PrefetchRequest {
    sequence: u64,
    label: &'static str,
    queued_at: Instant,
    requests: Vec<BrowseRequest>,
    profile: PrefetchBatchProfile,
}

#[derive(Debug)]
enum QueryWorkerResult {
    Progress(QueryProgressUpdate),
    ViewLoaded(ViewLoadResult),
    RefreshCompleted(RefreshViewResult),
    JumpTargetsBuilt(JumpTargetsResult),
    PrefetchCompleted(PrefetchResult),
    Failed(QueryWorkerFailure),
}

#[derive(Debug)]
struct QueryProgressUpdate {
    sequence: u64,
    task: PendingTaskKind,
    phase: String,
    progress: Option<PhaseProgress>,
}

#[derive(Debug)]
struct ViewLoadResult {
    sequence: u64,
    loaded_view: LoadedView,
}

#[derive(Debug)]
struct RefreshViewResult {
    sequence: u64,
    latest_snapshot: SnapshotBounds,
    loaded_view: Option<LoadedView>,
}

#[derive(Debug)]
struct JumpTargetsResult {
    sequence: u64,
    targets: Vec<JumpTarget>,
}

#[derive(Debug)]
struct PrefetchResult {
    sequence: u64,
    requests: Vec<BrowseRequest>,
    row_sets: Vec<Vec<RollupRow>>,
}

#[derive(Debug)]
struct QueryWorkerFailure {
    sequence: u64,
    task: PendingTaskKind,
    message: String,
}

#[derive(Debug, Clone, Copy)]
struct PhaseProgress {
    current: usize,
    total: usize,
}

trait ProgressSink {
    fn update(&mut self, phase: String, progress: Option<PhaseProgress>);

    fn phase(&mut self, phase: String) {
        self.update(phase, None);
    }

    fn step(&mut self, current: usize, total: usize, phase: String) {
        self.update(phase, Some(PhaseProgress { current, total }));
    }
}

#[derive(Debug)]
struct LoadedView {
    snapshot: SnapshotBounds,
    snapshot_coverage: SnapshotCoverageSummary,
    ui_state: PersistedUiState,
    filter_options: FilterOptions,
    raw_rows: Vec<RollupRow>,
    breadcrumb_targets: Vec<BreadcrumbTarget>,
    radial_context: RadialContext,
    current_project_root: Option<String>,
    selected_key: Option<TreeRowKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingTaskKind {
    View,
    Jump,
    Prefetch,
}

#[derive(Debug, Clone, Copy)]
struct PendingRequest {
    sequence: u64,
    label: &'static str,
    state: PendingTaskState,
    phase: Option<&'static str>,
    progress: Option<PhaseProgress>,
    supersedes_previous: bool,
    updated_at: Instant,
}

impl PendingRequest {
    fn queued(sequence: u64, label: &'static str, supersedes_previous: bool) -> Self {
        Self {
            sequence,
            label,
            state: PendingTaskState::Queued,
            phase: None,
            progress: None,
            supersedes_previous,
            updated_at: Instant::now(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingTaskState {
    Queued,
    Running,
}

#[derive(Debug, Default)]
struct PendingAsyncWork {
    view: PendingTaskTracker,
    jump: PendingTaskTracker,
    prefetch: PendingTaskTracker,
}

#[derive(Debug, Clone)]
struct StartupActivity {
    label: String,
    detail: String,
    progress: PhaseProgress,
    updated_at: Instant,
}

#[derive(Debug, Default)]
struct PendingTaskTracker {
    current: Option<PendingRequest>,
    next: Option<PendingRequest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingFinish {
    Apply,
    IgnoredBySupersedingRequest,
    NotFound,
}

impl PendingTaskTracker {
    fn begin(&mut self, sequence: u64, label: &'static str) {
        if self.current.is_some() {
            self.next = Some(PendingRequest::queued(sequence, label, true));
        } else {
            self.current = Some(PendingRequest::queued(sequence, label, false));
        }
    }

    fn apply_progress(&mut self, progress: &QueryProgressUpdate) -> bool {
        let Some(request) = self.current.as_mut() else {
            return false;
        };
        if request.sequence != progress.sequence {
            return false;
        }

        request.state = PendingTaskState::Running;
        request.phase = phase_label_for(progress.task, &progress.phase);
        request.progress = progress.progress;
        request.supersedes_previous = false;
        request.updated_at = Instant::now();
        true
    }

    fn finish(&mut self, sequence: u64) -> PendingFinish {
        match self.current {
            Some(request) if request.sequence == sequence => {
                if let Some(next) = self.next.take() {
                    self.current = Some(next);
                    PendingFinish::IgnoredBySupersedingRequest
                } else {
                    self.current = None;
                    PendingFinish::Apply
                }
            }
            _ => PendingFinish::NotFound,
        }
    }

    fn summary_parts(&self) -> Vec<String> {
        let mut parts = Vec::new();
        if let Some(request) = self.current {
            parts.push(render_pending_request(request));
        }
        if let Some(request) = self.next {
            parts.push(render_pending_request(request));
        }
        parts
    }
}

impl PendingAsyncWork {
    fn begin_view(&mut self, sequence: u64, label: &'static str) {
        self.view.begin(sequence, label);
    }

    fn begin_jump(&mut self, sequence: u64, label: &'static str) {
        self.jump.begin(sequence, label);
    }

    fn begin_prefetch(&mut self, sequence: u64, label: &'static str) {
        self.prefetch.begin(sequence, label);
    }

    fn apply_progress(&mut self, progress: QueryProgressUpdate) -> bool {
        match progress.task {
            PendingTaskKind::View => self.view.apply_progress(&progress),
            PendingTaskKind::Jump => self.jump.apply_progress(&progress),
            PendingTaskKind::Prefetch => self.prefetch.apply_progress(&progress),
        }
    }

    fn finish(&mut self, task: PendingTaskKind, sequence: u64) -> PendingFinish {
        match task {
            PendingTaskKind::View => self.view.finish(sequence),
            PendingTaskKind::Jump => self.jump.finish(sequence),
            PendingTaskKind::Prefetch => self.prefetch.finish(sequence),
        }
    }

    fn summary(&self) -> Option<String> {
        let mut parts = Vec::new();
        parts.extend(self.view.summary_parts());
        parts.extend(self.jump.summary_parts());
        parts.extend(self.prefetch.summary_parts());

        if parts.is_empty() {
            None
        } else {
            Some(parts.join("  |  "))
        }
    }
}

impl StartupActivity {
    fn from_progress(update: StartupProgressUpdate) -> Self {
        Self {
            label: update.label.to_string(),
            detail: update.detail,
            progress: PhaseProgress {
                current: update.current,
                total: update.total,
            },
            updated_at: Instant::now(),
        }
    }

    fn summary(&self) -> String {
        format!(
            "{} {} [{}/{}]: {}",
            spinner_frame_for(self.updated_at),
            self.label,
            self.progress.current,
            self.progress.total,
            self.detail
        )
    }
}

fn render_pending_request(request: PendingRequest) -> String {
    match request.state {
        PendingTaskState::Queued => {
            let mut text = format!("queued {}", request.label);
            if request.supersedes_previous {
                text.push_str(": superseding older request");
            }
            text
        }
        PendingTaskState::Running => {
            let spinner = spinner_frame_for(request.updated_at);
            let mut text = format!("{spinner} {}", request.label);
            if let Some(progress) = request.progress {
                text.push_str(&format!(" [{}/{}]", progress.current, progress.total));
            }
            if let Some(phase) = request.phase {
                text.push_str(": ");
                text.push_str(phase);
            }
            text
        }
    }
}

fn badge(label: impl Into<String>, tone: BadgeTone) -> Span<'static> {
    Span::styled(format!("[{}]", label.into()), badge_style(tone))
}

fn separator_span() -> Span<'static> {
    Span::styled(
        " │ ",
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::DIM),
    )
}

#[derive(Debug, Clone, Copy)]
enum BadgeTone {
    Accent,
    Info,
    Success,
    Warning,
    Muted,
}

fn badge_style(tone: BadgeTone) -> Style {
    match tone {
        BadgeTone::Accent => Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
        BadgeTone::Info => Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
        BadgeTone::Success => Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD),
        BadgeTone::Warning => Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
        BadgeTone::Muted => Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::DIM),
    }
}

fn opportunity_category_label(category: OpportunityCategory) -> &'static str {
    match category {
        OpportunityCategory::SessionSetup => "session setup",
        OpportunityCategory::TaskSetup => "task setup",
        OpportunityCategory::HistoryDrag => "history drag",
        OpportunityCategory::Delegation => "delegation",
        OpportunityCategory::ModelMismatch => "model mismatch",
        OpportunityCategory::PromptYield => "prompt yield",
        OpportunityCategory::SearchChurn => "search churn",
        OpportunityCategory::ToolResultBloat => "tool-result bloat",
    }
}

fn opportunity_confidence_label(confidence: OpportunityConfidence) -> &'static str {
    match confidence {
        OpportunityConfidence::Low => "low",
        OpportunityConfidence::Medium => "medium",
        OpportunityConfidence::High => "high",
    }
}

fn confidence_color(confidence: OpportunityConfidence) -> Color {
    match confidence {
        OpportunityConfidence::Low => Color::DarkGray,
        OpportunityConfidence::Medium => Color::Yellow,
        OpportunityConfidence::High => Color::Red,
    }
}

fn skill_attribution_confidence_label(confidence: SkillAttributionConfidence) -> &'static str {
    match confidence {
        SkillAttributionConfidence::High => "high",
    }
}

fn snapshot_state_badge(snapshot: &SnapshotBounds, has_newer_snapshot: bool) -> Span<'static> {
    if snapshot.is_bootstrap() {
        if has_newer_snapshot {
            badge("waiting", BadgeTone::Warning)
        } else {
            badge("empty", BadgeTone::Muted)
        }
    } else if has_newer_snapshot {
        badge("new data", BadgeTone::Success)
    } else {
        badge("pinned", BadgeTone::Accent)
    }
}

fn spinner_frame_for(updated_at: Instant) -> &'static str {
    let frame = (updated_at.elapsed().as_millis() / 250) as usize % ACTIVITY_SPINNER_FRAMES.len();
    ACTIVITY_SPINNER_FRAMES[frame]
}

fn phase_label_for(task: PendingTaskKind, phase: &str) -> Option<&'static str> {
    match (task, phase) {
        (PendingTaskKind::View, "loading filter options") => Some("loading filter options"),
        (PendingTaskKind::View, "sanitizing UI state") => Some("sanitizing UI state"),
        (PendingTaskKind::View, "applying active filters") => Some("applying active filters"),
        (PendingTaskKind::View, "browsing current path") => Some("browsing current path"),
        (PendingTaskKind::View, "resolving project root") => Some("resolving project root"),
        (PendingTaskKind::View, "building breadcrumbs") => Some("building breadcrumbs"),
        (PendingTaskKind::View, "recomputing map context") => Some("recomputing map context"),
        (PendingTaskKind::View, "refreshing snapshot coverage") => {
            Some("refreshing snapshot coverage")
        }
        (PendingTaskKind::View, "checking for newer snapshot") => {
            Some("checking for newer snapshot")
        }
        (PendingTaskKind::Jump, "loading current filters") => Some("loading current filters"),
        (PendingTaskKind::Jump, "walking project hierarchy") => Some("walking project hierarchy"),
        (PendingTaskKind::Jump, "walking category hierarchy") => Some("walking category hierarchy"),
        (PendingTaskKind::Jump, "finalizing jump targets") => Some("finalizing jump targets"),
        _ => None,
    }
}

pub struct App {
    config: RuntimeConfig,
    database: Database,
    browse_cache: BrowseCacheStore,
    worker: QueryWorker,
    ui_state_path: PathBuf,
    ui_state: PersistedUiState,
    snapshot: SnapshotBounds,
    snapshot_coverage: SnapshotCoverageSummary,
    latest_snapshot: SnapshotBounds,
    startup_open_reason: StartupOpenReason,
    has_newer_snapshot: bool,
    filter_options: FilterOptions,
    raw_rows: Vec<RollupRow>,
    visible_rows: Vec<TreeRow>,
    table_state: TableState,
    input_mode: InputMode,
    focused_pane: PaneFocus,
    breadcrumb_targets: Vec<BreadcrumbTarget>,
    breadcrumb_picker: BreadcrumbPickerState,
    current_project_root: Option<String>,
    radial_context: RadialContext,
    radial_context_path: BrowsePath,
    radial_model: RadialModel,
    jump_state: JumpState,
    status_message: Option<StatusMessage>,
    startup_activity: Option<StartupActivity>,
    status_updates: Option<Receiver<StartupWorkerEvent>>,
    last_refresh_check: Instant,
    next_request_sequence: u64,
    pending_async_work: PendingAsyncWork,
    perf_logger: Option<PerfLogger>,
    query_cache: QueryResultCache,
    selection_context_cache: SelectionContextCache,
    row_cache: Vec<CachedRows>,
    remembered_selections: Vec<RememberedSelection>,
    expanded_paths: Vec<BrowsePath>,
    selected_project_identity: Option<SelectedProjectIdentity>,
    show_inspect_pane: bool,
    pending_confirmation: Option<ConfirmationAction>,
    prefetch: PrefetchCoordinator,
}

impl App {
    // Startup construction necessarily threads snapshot state, startup-import
    // channels, and optional prelaunch progress hooks through one boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: RuntimeConfig,
        snapshot: SnapshotBounds,
        startup_open_reason: StartupOpenReason,
        _startup_status_message: Option<String>,
        startup_progress_update: Option<StartupProgressUpdate>,
        startup_browse_state: Option<StartupBrowseState>,
        status_updates: Option<Receiver<StartupWorkerEvent>>,
        startup_load_progress: Option<&mut dyn FnMut(StartupLoadProgressUpdate)>,
        perf_logger: Option<PerfLogger>,
    ) -> Result<Self> {
        let ui_state_path = config.state_dir.join(UI_STATE_FILENAME);
        let (ui_state, status_message) = match PersistedUiState::load(&ui_state_path) {
            Ok(Some(state)) => (state, None),
            Ok(None) => (PersistedUiState::default(), None),
            Err(error) => (
                PersistedUiState::default(),
                Some(StatusMessage::error(compact_status_text(format!(
                    "Unable to load saved TUI state: {error:#}"
                )))),
            ),
        };
        let mut ui_state = ui_state;
        ui_state.apply_startup_browse_state(startup_browse_state);
        let focused_pane = PaneFocus::from_pane_mode(ui_state.pane_mode);

        let database = Database::open(&config.db_path)?;
        let browse_cache = BrowseCacheStore::open(default_browse_cache_path(&config.state_dir))?;
        let worker = QueryWorker::spawn(
            config.db_path.clone(),
            default_browse_cache_path(&config.state_dir),
            perf_logger.clone(),
        );
        let mut app = Self {
            config,
            database,
            browse_cache,
            worker,
            ui_state_path,
            ui_state,
            latest_snapshot: snapshot.clone(),
            snapshot: snapshot.clone(),
            snapshot_coverage: SnapshotCoverageSummary::default(),
            startup_open_reason,
            has_newer_snapshot: false,
            filter_options: FilterOptions {
                projects: Vec::new(),
                models: Vec::new(),
                categories: Vec::new(),
                actions: Vec::new(),
            },
            raw_rows: Vec::new(),
            visible_rows: Vec::new(),
            table_state: TableState::default(),
            input_mode: InputMode::Normal,
            focused_pane,
            breadcrumb_targets: Vec::new(),
            breadcrumb_picker: BreadcrumbPickerState::default(),
            current_project_root: None,
            radial_context: RadialContext::default(),
            radial_context_path: BrowsePath::Root,
            radial_model: RadialModel::default(),
            jump_state: JumpState::default(),
            status_message,
            startup_activity: startup_progress_update.map(StartupActivity::from_progress),
            status_updates,
            last_refresh_check: Instant::now(),
            next_request_sequence: 1,
            pending_async_work: PendingAsyncWork::default(),
            perf_logger,
            query_cache: QueryResultCache::default(),
            selection_context_cache: SelectionContextCache::default(),
            row_cache: Vec::new(),
            remembered_selections: Vec::new(),
            expanded_paths: Vec::new(),
            selected_project_identity: None,
            show_inspect_pane: false,
            pending_confirmation: None,
            prefetch: PrefetchCoordinator::new(),
        };

        let perf_logger = app.perf_logger.clone();
        let conn = app.database.connection();
        let query_engine = QueryEngine::with_perf(conn, perf_logger.clone());
        let mut load_progress_reporter = startup_load_progress.map(LoadProgressReporter::new);
        let initial_view = load_view_for_state(
            &query_engine,
            &mut app.query_cache,
            &mut app.browse_cache,
            perf_logger,
            snapshot,
            app.ui_state.clone(),
            None,
            load_progress_reporter
                .as_mut()
                .map(|reporter| reporter as &mut dyn ProgressSink),
        )?;
        app.apply_loaded_view(initial_view)?;
        app.refresh_snapshot_status()?;
        Ok(app)
    }

    pub fn run(mut self) -> Result<()> {
        let mut terminal = TerminalGuard::enter()?;

        loop {
            self.drain_status_updates();
            self.drain_query_results();
            if self.last_refresh_check.elapsed() >= REFRESH_CHECK_INTERVAL {
                self.refresh_snapshot_status()?;
                self.last_refresh_check = Instant::now();
            }

            terminal.terminal.draw(|frame| self.render(frame))?;

            if !event::poll(Duration::from_millis(250))? {
                continue;
            }

            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => {
                    if self.handle_key(key)? {
                        break;
                    }
                }
                Event::Resize(_, _) => {}
                _ => {}
            }
        }

        Ok(())
    }

    /// Render the TUI once into a fixed-size buffer and return the content as
    /// a newline-delimited string. Used for non-interactive snapshot testing.
    pub(crate) fn render_snapshot(&mut self, width: u16, height: u16) -> Result<String> {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend)?;
        terminal.draw(|frame| self.render(frame))?;

        let buffer = terminal.backend().buffer();
        let mut output = String::new();
        for y in 0..height {
            for x in 0..width {
                output.push_str(buffer.cell((x, y)).map_or(" ", |c| c.symbol()));
            }
            output.push('\n');
        }
        Ok(output)
    }

    fn drain_status_updates(&mut self) {
        loop {
            let mut disconnected = false;
            let update = {
                let Some(receiver) = self.status_updates.as_ref() else {
                    return;
                };

                match receiver.try_recv() {
                    Ok(update) => Some(update),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => {
                        disconnected = true;
                        None
                    }
                }
            };

            if disconnected {
                self.status_updates = None;
            }

            let Some(update) = update else {
                break;
            };

            self.apply_status_update(update);
        }
    }

    fn apply_status_update(&mut self, update: StartupWorkerEvent) {
        match update {
            StartupWorkerEvent::Progress { update } => {
                self.startup_activity = Some(StartupActivity::from_progress(update));
            }
            StartupWorkerEvent::StartupSettled {
                startup_status_message: _,
            } => {}
            StartupWorkerEvent::DeferredFailures {
                deferred_status_message: _,
            } => {}
            StartupWorkerEvent::Finished => {
                self.startup_activity = None;
            }
        }
    }

    fn render(&mut self, frame: &mut Frame<'_>) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(9),
                Constraint::Min(10),
                Constraint::Length(4),
            ])
            .split(frame.area());

        self.render_header(frame, layout[0]);
        self.render_body(frame, layout[1]);
        self.render_footer(frame, layout[2]);

        match self.input_mode {
            InputMode::JumpInput => self.render_jump_overlay(frame),
            InputMode::BreadcrumbPicker => self.render_breadcrumb_overlay(frame),
            InputMode::ColumnChooser => self.render_columns_overlay(frame),
            InputMode::BrowseCacheMenu => self.render_browse_cache_overlay(frame),
            InputMode::BrowseCacheConfirm | InputMode::DatabaseConfirm => {
                self.render_confirmation_overlay(frame)
            }
            InputMode::DatabaseMenu => self.render_database_overlay(frame),
            InputMode::Normal | InputMode::FilterInput => {}
        }
    }

    fn render_header(&self, frame: &mut Frame<'_>, area: Rect) {
        let mut lines = vec![
            Line::from(vec![
                Span::styled("▣ ", Style::default().fg(Color::Cyan)),
                Span::styled("gnomon", Style::default().add_modifier(Modifier::BOLD)),
            ]),
            view_line(&self.breadcrumb_targets),
            Line::from(vec![
                badge("snapshot", BadgeTone::Info),
                separator_span(),
                snapshot_state_badge(&self.snapshot, self.has_newer_snapshot),
                separator_span(),
                Span::styled(
                    snapshot_summary_text(&self.snapshot, self.has_newer_snapshot),
                    Style::default().fg(Color::White),
                ),
                separator_span(),
                badge("lens", BadgeTone::Accent),
                separator_span(),
                Span::styled(
                    metric_lens_label(self.ui_state.lens),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
            ]),
            Line::from(vec![
                badge(
                    "refresh",
                    if self.has_newer_snapshot {
                        BadgeTone::Warning
                    } else {
                        BadgeTone::Muted
                    },
                ),
                separator_span(),
                Span::styled(
                    snapshot_refresh_text(
                        &self.snapshot,
                        &self.latest_snapshot,
                        self.startup_open_reason,
                        self.has_newer_snapshot,
                    ),
                    if self.has_newer_snapshot {
                        Style::default().fg(Color::Yellow)
                    } else {
                        Style::default().fg(Color::Gray)
                    },
                ),
            ]),
        ];

        lines.push(self.header_detail_line());
        lines.push(self.browse_cache_header_line());
        lines.push(self.database_header_line());

        let header = Paragraph::new(Text::from(lines))
            .block(Block::default().borders(Borders::ALL).title("Status"))
            .wrap(Wrap { trim: true });
        frame.render_widget(header, area);
    }

    fn render_body(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let (main_area, inspect_area) = if self.show_inspect_pane {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(6), Constraint::Length(12)])
                .split(area);
            (chunks[0], Some(chunks[1]))
        } else {
            (area, None)
        };

        if main_area.width >= WIDE_LAYOUT_WIDTH {
            let panes = wide_layout_panes(main_area);
            self.render_radial(frame, panes[0]);
            self.render_table(frame, panes[1]);
        } else {
            match self.ui_state.pane_mode {
                PaneMode::Table => self.render_table(frame, main_area),
                PaneMode::Radial => self.render_radial(frame, main_area),
            }
        }

        if let Some(inspect_area) = inspect_area {
            self.render_inspect_pane(frame, inspect_area);
        }
    }

    fn render_table(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let visible_columns = active_columns(
            area.width,
            self.ui_state.lens,
            &self.ui_state.enabled_columns,
        );

        let header = Row::new(
            visible_columns
                .iter()
                .map(|column| Cell::from(column.title.clone()))
                .collect::<Vec<_>>(),
        )
        .style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        );

        let rows = if self.visible_rows.is_empty() {
            vec![Row::new(vec![Cell::from(if self.raw_rows.is_empty() {
                "No rows in the current path and filter set."
            } else {
                "The current-view filter hides all rows."
            })])]
        } else {
            self.visible_rows
                .iter()
                .map(|row| {
                    Row::new(
                        visible_columns
                            .iter()
                            .map(|column| {
                                let value = if column.key == ColumnKey::Label {
                                    render_tree_label(row)
                                } else {
                                    render_column_value(column.key, &row.row, self.ui_state.lens)
                                };
                                Cell::from(value)
                            })
                            .collect::<Vec<_>>(),
                    )
                })
                .collect()
        };

        let widths = visible_columns
            .iter()
            .map(|column| column.constraint)
            .collect::<Vec<_>>();

        let table = Table::new(rows, widths)
            .header(header.style(table_header_style(self.focused_pane == PaneFocus::Table)))
            .block(pane_block(
                "Statistics",
                self.focused_pane == PaneFocus::Table,
            ))
            .row_highlight_style(table_row_highlight_style(
                self.focused_pane == PaneFocus::Table,
            ))
            .highlight_symbol(if self.focused_pane == PaneFocus::Table {
                "▶ "
            } else {
                "  "
            });
        frame.render_stateful_widget(table, area, &mut self.table_state);
    }

    fn render_radial(&self, frame: &mut Frame<'_>, area: Rect) {
        frame.render_widget(
            &SunburstPane {
                model: &self.radial_model,
                focused: self.focused_pane == PaneFocus::Radial,
                config: SunburstRenderConfig::default(),
            },
            area,
        );
    }

    fn render_inspect_pane(&self, frame: &mut Frame<'_>, area: Rect) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(Span::styled(
                " Opportunity Details ",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        let lines = match self.selected_row() {
            None => vec![Line::styled(
                "No row selected",
                Style::default().fg(Color::DarkGray),
            )],
            Some(row) => {
                let mut lines = Vec::new();
                if let Some(skill_attribution) = &row.skill_attribution {
                    lines.push(Line::from(vec![
                        Span::styled("skill ", Style::default().fg(Color::Green)),
                        Span::styled(
                            skill_attribution.skill_name.as_str(),
                            Style::default()
                                .fg(Color::Green)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::raw("  confidence "),
                        Span::styled(
                            skill_attribution_confidence_label(skill_attribution.confidence),
                            Style::default().fg(Color::Green),
                        ),
                    ]));
                }

                if row.opportunities.is_empty() {
                    if !lines.is_empty() {
                        lines.push(Line::raw(""));
                    }
                    lines.push(Line::styled(
                        "No opportunities detected for this row",
                        Style::default().fg(Color::DarkGray),
                    ));
                } else {
                    for annotation in &row.opportunities.annotations {
                        if !lines.is_empty() {
                            lines.push(Line::raw(""));
                        }
                        lines.push(Line::from(vec![
                            Span::styled(
                                opportunity_category_label(annotation.category),
                                Style::default()
                                    .fg(Color::Cyan)
                                    .add_modifier(Modifier::BOLD),
                            ),
                            Span::raw("  score "),
                            Span::styled(
                                format!("{:.2}", annotation.score),
                                Style::default().fg(Color::Yellow),
                            ),
                            Span::raw("  confidence "),
                            Span::styled(
                                opportunity_confidence_label(annotation.confidence),
                                Style::default().fg(confidence_color(annotation.confidence)),
                            ),
                        ]));
                        for piece in &annotation.evidence {
                            lines.push(Line::from(vec![
                                Span::styled("  • ", Style::default().fg(Color::DarkGray)),
                                Span::raw(piece.as_str()),
                            ]));
                        }
                        if let Some(recommendation) = &annotation.recommendation {
                            lines.push(Line::from(vec![
                                Span::styled("  → ", Style::default().fg(Color::Green)),
                                Span::styled(
                                    recommendation.as_str(),
                                    Style::default().fg(Color::Green),
                                ),
                            ]));
                        }
                    }
                }
                lines
            }
        };

        let paragraph = Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false });
        frame.render_widget(paragraph, inner);
    }

    fn render_footer(&self, frame: &mut Frame<'_>, area: Rect) {
        let lines = match self.input_mode {
            InputMode::Normal => vec![
                Line::from(vec![
                    badge("keys", BadgeTone::Info),
                    separator_span(),
                    Span::raw("Enter drill"),
                    separator_span(),
                    Span::raw("Right expand"),
                    separator_span(),
                    Span::raw("Left collapse/parent"),
                    separator_span(),
                    Span::raw("Backspace up"),
                    separator_span(),
                    Span::raw("Space clear scope/root"),
                    separator_span(),
                    Span::raw("b breadcrumbs"),
                    separator_span(),
                    Span::raw("1/2 hierarchy"),
                    separator_span(),
                    Span::raw("l lens"),
                    separator_span(),
                    Span::raw("Tab focus/pane"),
                    separator_span(),
                    Span::raw("o columns"),
                    separator_span(),
                    Span::raw("i inspect"),
                    separator_span(),
                    Span::raw("q quit"),
                ]),
                Line::from(vec![
                    badge("statistics", BadgeTone::Accent),
                    separator_span(),
                    Span::raw("up/down rows"),
                    separator_span(),
                    Span::raw("t/m filters"),
                    separator_span(),
                    Span::raw("p/c/a cycle scope"),
                    separator_span(),
                    Span::raw("x opp-filter"),
                    separator_span(),
                    Span::raw("0 clear"),
                    separator_span(),
                    Span::raw("/ row filter"),
                    separator_span(),
                    Span::raw("g jump"),
                    separator_span(),
                    Span::raw("r refresh"),
                    separator_span(),
                    Span::raw("B cache"),
                    separator_span(),
                    Span::raw("D database"),
                ]),
                Line::from({
                    let mut spans = vec![
                        badge("coverage", BadgeTone::Success),
                        separator_span(),
                        Span::styled(
                            snapshot_coverage_footer_text(&self.snapshot_coverage),
                            Style::default().fg(Color::White),
                        ),
                        separator_span(),
                        badge("selection", BadgeTone::Accent),
                        separator_span(),
                        Span::styled(
                            self.selection_footer_text(),
                            Style::default().fg(Color::White),
                        ),
                    ];
                    if let Some(cat) = self.ui_state.opportunity_filter {
                        spans.push(separator_span());
                        spans.push(badge("opp-filter", BadgeTone::Warning));
                        spans.push(separator_span());
                        spans.push(Span::styled(
                            opportunity_category_label(cat).to_string(),
                            Style::default().fg(Color::Yellow),
                        ));
                    }
                    spans
                }),
            ],
            InputMode::FilterInput => vec![
                Line::from(vec![
                    badge("filter", BadgeTone::Warning),
                    separator_span(),
                    Span::raw("Type to filter rows immediately."),
                ]),
                Line::from(vec![
                    badge("keys", BadgeTone::Muted),
                    separator_span(),
                    Span::raw("Enter or Esc returns to navigation mode."),
                ]),
                Line::from(vec![
                    badge("query", BadgeTone::Accent),
                    separator_span(),
                    Span::raw(format!("filter> {}", self.ui_state.row_filter)),
                ]),
            ],
            InputMode::JumpInput => vec![
                Line::from(vec![
                    badge("jump", BadgeTone::Accent),
                    separator_span(),
                    Span::raw("Type to fuzzy-match major navigation nodes."),
                ]),
                Line::from(vec![
                    badge("keys", BadgeTone::Muted),
                    separator_span(),
                    Span::raw("Up/down to change selection. Enter jumps. Esc closes."),
                ]),
                Line::from(vec![
                    badge("query", BadgeTone::Info),
                    separator_span(),
                    Span::raw(format!("jump> {}", self.jump_state.query)),
                ]),
            ],
            InputMode::BreadcrumbPicker => vec![
                Line::from(vec![
                    badge("breadcrumbs", BadgeTone::Accent),
                    separator_span(),
                    Span::raw("Choose an ancestor scope to jump to."),
                ]),
                Line::from(vec![
                    badge("keys", BadgeTone::Muted),
                    separator_span(),
                    Span::raw("Up/down or j/k to change selection. Enter jumps. Esc closes."),
                ]),
                Line::from(vec![
                    badge("target", BadgeTone::Info),
                    separator_span(),
                    Span::raw(format!(
                        "breadcrumb> {}",
                        self.breadcrumb_targets
                            .get(self.breadcrumb_picker.selected)
                            .map(|target| target.display.as_str())
                            .unwrap_or("none")
                    )),
                ]),
            ],
            InputMode::ColumnChooser => vec![
                Line::from(vec![
                    badge("columns", BadgeTone::Accent),
                    separator_span(),
                    Span::raw(
                        "Toggle with k/g/o/t/f/w/u/i metrics, T/S/C opp summary, 1-8 opp categories.",
                    ),
                ]),
                Line::from(vec![
                    badge("keys", BadgeTone::Muted),
                    separator_span(),
                    Span::raw("Esc closes the chooser."),
                ]),
                Line::from(vec![
                    badge("enabled", BadgeTone::Info),
                    separator_span(),
                    Span::raw(format!(
                        "enabled columns: {}",
                        enabled_column_summary(&self.ui_state.enabled_columns)
                    )),
                ]),
            ],
            InputMode::BrowseCacheMenu => vec![
                Line::from(vec![
                    badge("browse cache", BadgeTone::Accent),
                    separator_span(),
                    Span::raw("c clear, r rebuild, Esc close"),
                ]),
                Line::from(vec![
                    badge("status", BadgeTone::Muted),
                    separator_span(),
                    Span::raw(
                        "Rebuild clears persisted entries, resets live browse cache state, and reloads the current view.",
                    ),
                ]),
            ],
            InputMode::BrowseCacheConfirm => vec![Line::from(vec![
                badge("confirm", BadgeTone::Warning),
                separator_span(),
                Span::raw("Enter confirms. Esc cancels."),
            ])],
            InputMode::DatabaseMenu => vec![
                Line::from(vec![
                    badge("database", BadgeTone::Accent),
                    separator_span(),
                    Span::raw("r refresh status, b rebuild database, Esc close"),
                ]),
                Line::from(vec![
                    badge("warning", BadgeTone::Warning),
                    separator_span(),
                    Span::raw(
                        "Database rebuild clears persisted browse-cache artifacts before reimporting the full derived cache.",
                    ),
                ]),
            ],
            InputMode::DatabaseConfirm => vec![Line::from(vec![
                badge("confirm", BadgeTone::Warning),
                separator_span(),
                Span::raw("Enter confirms. Esc cancels."),
            ])],
        };

        let footer = Paragraph::new(Text::from(lines))
            .block(Block::default().borders(Borders::ALL).title("Keys"))
            .wrap(Wrap { trim: true });
        frame.render_widget(footer, area);
    }

    fn render_jump_overlay(&self, frame: &mut Frame<'_>) {
        let area = centered_rect(frame.area(), 72, 18);
        frame.render_widget(Clear, area);

        let mut items = if self.jump_state.matches.is_empty() {
            vec![ListItem::new("No matches for the current jump query.")]
        } else {
            self.jump_state
                .matches
                .iter()
                .enumerate()
                .map(|(index, target)| {
                    let prefix = if index == self.jump_state.selected {
                        "▶ "
                    } else {
                        "  "
                    };
                    ListItem::new(format!("{prefix}{}  |  {}", target.label, target.detail))
                })
                .collect::<Vec<_>>()
        };

        items.insert(0, ListItem::new(format!("jump> {}", self.jump_state.query)));
        let list = List::new(items)
            .block(Block::default().borders(Borders::ALL).title("Global Jump"))
            .highlight_style(Style::default().add_modifier(Modifier::BOLD));
        frame.render_widget(list, area);
    }

    fn render_breadcrumb_overlay(&self, frame: &mut Frame<'_>) {
        let area = centered_rect(frame.area(), 80, 18);
        frame.render_widget(Clear, area);

        let mut items = if self.breadcrumb_targets.is_empty() {
            vec![ListItem::new("No breadcrumb targets available.")]
        } else {
            self.breadcrumb_targets
                .iter()
                .enumerate()
                .map(|(index, target)| {
                    let prefix = if index == self.breadcrumb_picker.selected {
                        "▶ "
                    } else {
                        "  "
                    };
                    ListItem::new(format!("{prefix}{}", target.display))
                })
                .collect::<Vec<_>>()
        };

        items.insert(
            0,
            ListItem::new("Breadcrumb target picker. Enter jumps. Esc closes."),
        );
        let list = List::new(items)
            .block(Block::default().borders(Borders::ALL).title("Breadcrumbs"))
            .highlight_style(Style::default().add_modifier(Modifier::BOLD));
        frame.render_widget(list, area);
    }

    fn render_columns_overlay(&self, frame: &mut Frame<'_>) {
        let area = centered_rect(frame.area(), 76, 18);
        frame.render_widget(Clear, area);

        let cols = &self.ui_state.enabled_columns;
        let lines = vec![
            Line::from("Toggle optional columns:"),
            Line::from(format!(
                "k kind [{}]   g all input [{}]   o output [{}]   t total [{}]",
                toggle_mark(cols, OptionalColumn::Kind),
                toggle_mark(cols, OptionalColumn::GrossInput),
                toggle_mark(cols, OptionalColumn::Output),
                toggle_mark(cols, OptionalColumn::Total),
            )),
            Line::from(format!(
                "f 5h [{}]     w 1w [{}]      u ref [{}]      i items [{}]",
                toggle_mark(cols, OptionalColumn::Last5Hours),
                toggle_mark(cols, OptionalColumn::LastWeek),
                toggle_mark(cols, OptionalColumn::UncachedReference),
                toggle_mark(cols, OptionalColumn::Items),
            )),
            Line::from(""),
            Line::from("Opportunity columns:"),
            Line::from(format!(
                "T top [{}]    S score [{}]   C conf [{}]",
                toggle_mark(cols, OptionalColumn::TopOpportunity),
                toggle_mark(cols, OptionalColumn::OppScore),
                toggle_mark(cols, OptionalColumn::Confidence),
            )),
            Line::from(format!(
                "1 sess [{}]   2 task [{}]    3 hist [{}]   4 deleg [{}]",
                toggle_mark(cols, OptionalColumn::OppSessionSetup),
                toggle_mark(cols, OptionalColumn::OppTaskSetup),
                toggle_mark(cols, OptionalColumn::OppHistoryDrag),
                toggle_mark(cols, OptionalColumn::OppDelegation),
            )),
            Line::from(format!(
                "5 model [{}]  6 yield [{}]   7 srch [{}]   8 bloat [{}]",
                toggle_mark(cols, OptionalColumn::OppModelMismatch),
                toggle_mark(cols, OptionalColumn::OppPromptYield),
                toggle_mark(cols, OptionalColumn::OppSearchChurn),
                toggle_mark(cols, OptionalColumn::OppToolResultBloat),
            )),
            Line::from(""),
            Line::from("The label and selected-lens columns are always visible."),
            Line::from("Narrow terminals automatically hide lower-priority enabled columns."),
            Line::from("Esc closes the chooser."),
        ];

        let popup = Paragraph::new(Text::from(lines))
            .block(Block::default().borders(Borders::ALL).title("Columns"))
            .wrap(Wrap { trim: true });
        frame.render_widget(popup, area);
    }

    fn render_browse_cache_overlay(&self, frame: &mut Frame<'_>) {
        let stats = self.browse_cache.stats().ok();
        let summary = stats
            .map(|stats| {
                format!(
                    "entries: {}  |  payload: {}",
                    stats.entry_count,
                    format_storage_bytes(stats.total_payload_bytes)
                )
            })
            .unwrap_or_else(|| "browse-cache stats unavailable".to_string());
        self.render_popup(
            frame,
            "Browse Cache",
            76,
            12,
            vec![
                Line::from("Persisted browse-cache management"),
                Line::from(""),
                Line::from(summary),
                Line::from(""),
                Line::from("c clear cache"),
                Line::from("r rebuild cache from the current view"),
                Line::from("Esc closes without changes"),
            ],
        );
    }

    fn render_database_overlay(&self, frame: &mut Frame<'_>) {
        let size_text = sqlite_artifact_size_bytes(&self.config.db_path)
            .map(format_storage_bytes)
            .unwrap_or_else(|_| "unavailable".to_string());
        self.render_popup(
            frame,
            "Database",
            88,
            13,
            vec![
                Line::from("Database status and maintenance"),
                Line::from(""),
                Line::from(format!("path: {}", self.config.db_path.display())),
                Line::from(format!("size: {size_text}")),
                Line::from(format!(
                    "current snapshot: publish_seq <= {} ({} chunks)",
                    self.snapshot.max_publish_seq, self.snapshot.published_chunk_count
                )),
                Line::from(""),
                Line::from("r refresh database status"),
                Line::from("b rebuild database and reload the current view"),
                Line::from("Esc closes without changes"),
            ],
        );
    }

    fn render_confirmation_overlay(&self, frame: &mut Frame<'_>) {
        let (title, lines) = match self.pending_confirmation {
            Some(ConfirmationAction::ClearBrowseCache) => (
                "Confirm Browse-Cache Clear",
                vec![
                    Line::from("Clear persisted browse-cache contents?"),
                    Line::from("The app remains usable immediately."),
                    Line::from(""),
                    Line::from("Enter confirms. Esc cancels."),
                ],
            ),
            Some(ConfirmationAction::RebuildBrowseCache) => (
                "Confirm Browse-Cache Rebuild",
                vec![
                    Line::from("Rebuild the browse cache from the current visible view?"),
                    Line::from(
                        "This clears persisted entries, resets live browse cache state, and reloads the current view.",
                    ),
                    Line::from(""),
                    Line::from("Enter confirms. Esc cancels."),
                ],
            ),
            Some(ConfirmationAction::RebuildDatabase) => (
                "Confirm Database Rebuild",
                vec![
                    Line::from("Rebuild the derived usage database?"),
                    Line::from(
                        "This clears persisted browse-cache artifacts, reruns source scan, and performs a full import.",
                    ),
                    Line::from(""),
                    Line::from("Enter confirms. Esc cancels."),
                ],
            ),
            None => (
                "Confirm",
                vec![Line::from("No pending action."), Line::from("Esc closes.")],
            ),
        };
        self.render_popup(frame, title, 84, 11, lines);
    }

    fn render_popup(
        &self,
        frame: &mut Frame<'_>,
        title: &'static str,
        width: u16,
        height: u16,
        lines: Vec<Line<'static>>,
    ) {
        let area = centered_rect(frame.area(), width, height);
        frame.render_widget(Clear, area);
        let popup = Paragraph::new(Text::from(lines))
            .block(Block::default().borders(Borders::ALL).title(title))
            .wrap(Wrap { trim: true });
        frame.render_widget(popup, area);
    }

    fn handle_key(&mut self, key: KeyEvent) -> Result<bool> {
        match self.input_mode {
            InputMode::Normal => self.handle_normal_key(key),
            InputMode::FilterInput => self.handle_filter_input(key),
            InputMode::JumpInput => self.handle_jump_input(key),
            InputMode::BreadcrumbPicker => self.handle_breadcrumb_picker(key),
            InputMode::ColumnChooser => self.handle_column_key(key),
            InputMode::BrowseCacheMenu => self.handle_browse_cache_menu(key),
            InputMode::BrowseCacheConfirm | InputMode::DatabaseConfirm => {
                self.handle_confirmation_key(key)
            }
            InputMode::DatabaseMenu => self.handle_database_menu(key),
        }
    }

    fn handle_normal_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => Ok(true),
            KeyCode::Enter => {
                self.descend_into_selection()?;
                Ok(false)
            }
            KeyCode::Backspace => {
                self.navigate_up()?;
                Ok(false)
            }
            KeyCode::Char(' ') => {
                self.clear_structural_scope_or_reset_to_root()?;
                Ok(false)
            }
            KeyCode::Char('1') => {
                self.switch_root(RootView::ProjectHierarchy)?;
                Ok(false)
            }
            KeyCode::Char('2') => {
                self.switch_root(RootView::CategoryHierarchy)?;
                Ok(false)
            }
            KeyCode::Char('l') => {
                self.ui_state.lens = next_metric_lens(self.ui_state.lens);
                self.enqueue_view_reload("loading view")?;
                Ok(false)
            }
            KeyCode::Tab => {
                self.focused_pane = self.focused_pane.toggle();
                self.ui_state.pane_mode = PaneMode::from_focus(self.focused_pane);
                self.save_state();
                Ok(false)
            }
            KeyCode::Char('t') => {
                self.ui_state.time_window = self.ui_state.time_window.next();
                self.enqueue_view_reload("loading view")?;
                Ok(false)
            }
            KeyCode::Char('m') => {
                self.ui_state.model =
                    cycle_option(self.ui_state.model.clone(), &self.filter_options.models);
                self.enqueue_view_reload("loading view")?;
                Ok(false)
            }
            KeyCode::Char('p') => {
                self.ui_state.project_id =
                    cycle_project(self.ui_state.project_id, &self.filter_options.projects);
                self.enqueue_view_reload("loading view")?;
                Ok(false)
            }
            KeyCode::Char('c') => {
                self.ui_state.action_category = cycle_option(
                    self.ui_state.action_category.clone(),
                    &self.filter_options.categories,
                );
                self.enqueue_view_reload("loading view")?;
                Ok(false)
            }
            KeyCode::Char('a') => {
                self.ui_state.action = cycle_action(
                    self.ui_state.action.clone(),
                    &self.filter_options.actions,
                    self.ui_state.action_category.as_deref(),
                );
                self.enqueue_view_reload("loading view")?;
                Ok(false)
            }
            KeyCode::Char('x') => {
                self.ui_state.opportunity_filter =
                    cycle_opportunity_filter(self.ui_state.opportunity_filter);
                self.apply_row_filter()?;
                self.save_state();
                Ok(false)
            }
            KeyCode::Char('0') => {
                self.ui_state.clear_filters();
                self.enqueue_view_reload("loading view")?;
                Ok(false)
            }
            KeyCode::Char('/') => {
                self.input_mode = InputMode::FilterInput;
                Ok(false)
            }
            KeyCode::Char('g') => {
                self.input_mode = InputMode::JumpInput;
                self.jump_state.query.clear();
                self.enqueue_jump_target_rebuild()?;
                Ok(false)
            }
            KeyCode::Char('b') => {
                self.open_breadcrumb_picker();
                Ok(false)
            }
            KeyCode::Char('r') => {
                self.refresh_snapshot()?;
                Ok(false)
            }
            KeyCode::Char('o') => {
                self.input_mode = InputMode::ColumnChooser;
                Ok(false)
            }
            KeyCode::Char('B') => {
                self.input_mode = InputMode::BrowseCacheMenu;
                Ok(false)
            }
            KeyCode::Char('D') => {
                self.input_mode = InputMode::DatabaseMenu;
                Ok(false)
            }
            KeyCode::Char('i') => {
                self.show_inspect_pane = !self.show_inspect_pane;
                Ok(false)
            }
            _ if self.focused_pane == PaneFocus::Table => self.handle_table_navigation_key(key),
            _ => self.handle_radial_navigation_key(key),
        }
    }

    fn handle_table_navigation_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::PageUp => self.move_selection(-10),
            KeyCode::PageDown => self.move_selection(10),
            KeyCode::Home => self.select_first(),
            KeyCode::End => self.select_last(),
            KeyCode::Right => self.expand_selected_or_descend()?,
            KeyCode::Left => self.collapse_selected_or_move_to_parent()?,
            _ => {}
        }

        Ok(false)
    }

    fn handle_radial_navigation_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Left | KeyCode::Up | KeyCode::Char('h') | KeyCode::Char('k') => {
                self.move_selection(-1)
            }
            KeyCode::Right | KeyCode::Down | KeyCode::Char('l') | KeyCode::Char('j') => {
                self.move_selection(1)
            }
            KeyCode::Home => self.select_first(),
            KeyCode::End => self.select_last(),
            _ => {}
        }

        Ok(false)
    }

    fn handle_filter_input(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => {
                self.input_mode = InputMode::Normal;
                self.save_state();
            }
            KeyCode::Backspace => {
                self.ui_state.row_filter.pop();
                self.apply_row_filter()?;
                self.save_state();
            }
            KeyCode::Char(ch) => {
                self.ui_state.row_filter.push(ch);
                self.apply_row_filter()?;
                self.save_state();
            }
            _ => {}
        }

        Ok(false)
    }

    fn handle_jump_input(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
            }
            KeyCode::Backspace => {
                self.jump_state.query.pop();
                self.enqueue_jump_target_rebuild()?;
            }
            KeyCode::Up => {
                if self.jump_state.selected > 0 {
                    self.jump_state.selected -= 1;
                }
            }
            KeyCode::Down => {
                if self.jump_state.selected + 1 < self.jump_state.matches.len() {
                    self.jump_state.selected += 1;
                }
            }
            KeyCode::Enter => {
                if let Some(target) = self
                    .jump_state
                    .matches
                    .get(self.jump_state.selected)
                    .cloned()
                {
                    self.ui_state.root = target.root;
                    self.ui_state.path = target.path;
                    self.input_mode = InputMode::Normal;
                    self.enqueue_view_reload("loading view")?;
                }
            }
            KeyCode::Char(ch) => {
                self.jump_state.query.push(ch);
                self.enqueue_jump_target_rebuild()?;
            }
            _ => {}
        }

        Ok(false)
    }

    fn handle_breadcrumb_picker(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.move_breadcrumb_selection(-1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.move_breadcrumb_selection(1);
            }
            KeyCode::Left | KeyCode::Char('h') => {
                self.move_breadcrumb_selection(-1);
            }
            KeyCode::Right | KeyCode::Char('l') => {
                self.move_breadcrumb_selection(1);
            }
            KeyCode::Home => {
                if !self.breadcrumb_targets.is_empty() {
                    self.breadcrumb_picker.selected = 0;
                }
            }
            KeyCode::End => {
                if !self.breadcrumb_targets.is_empty() {
                    self.breadcrumb_picker.selected =
                        self.breadcrumb_targets.len().saturating_sub(1);
                }
            }
            KeyCode::Enter => {
                if let Some(target) = self.breadcrumb_targets.get(self.breadcrumb_picker.selected) {
                    self.ui_state.root = target.root;
                    self.ui_state.path = target.path.clone();
                    self.input_mode = InputMode::Normal;
                    self.enqueue_view_reload("loading view")?;
                }
            }
            _ => {}
        }

        Ok(false)
    }

    fn handle_column_key(&mut self, key: KeyEvent) -> Result<bool> {
        let toggled = match key.code {
            KeyCode::Esc | KeyCode::Enter => {
                self.input_mode = InputMode::Normal;
                return Ok(false);
            }
            KeyCode::Char('k') => Some(OptionalColumn::Kind),
            KeyCode::Char('g') => Some(OptionalColumn::GrossInput),
            KeyCode::Char('o') => Some(OptionalColumn::Output),
            KeyCode::Char('t') => Some(OptionalColumn::Total),
            KeyCode::Char('f') => Some(OptionalColumn::Last5Hours),
            KeyCode::Char('w') => Some(OptionalColumn::LastWeek),
            KeyCode::Char('u') => Some(OptionalColumn::UncachedReference),
            KeyCode::Char('i') => Some(OptionalColumn::Items),
            KeyCode::Char('T') => Some(OptionalColumn::TopOpportunity),
            KeyCode::Char('S') => Some(OptionalColumn::OppScore),
            KeyCode::Char('C') => Some(OptionalColumn::Confidence),
            KeyCode::Char('1') => Some(OptionalColumn::OppSessionSetup),
            KeyCode::Char('2') => Some(OptionalColumn::OppTaskSetup),
            KeyCode::Char('3') => Some(OptionalColumn::OppHistoryDrag),
            KeyCode::Char('4') => Some(OptionalColumn::OppDelegation),
            KeyCode::Char('5') => Some(OptionalColumn::OppModelMismatch),
            KeyCode::Char('6') => Some(OptionalColumn::OppPromptYield),
            KeyCode::Char('7') => Some(OptionalColumn::OppSearchChurn),
            KeyCode::Char('8') => Some(OptionalColumn::OppToolResultBloat),
            _ => None,
        };

        if let Some(column) = toggled {
            toggle_column(&mut self.ui_state.enabled_columns, column);
            self.save_state();
        }

        Ok(false)
    }

    fn handle_browse_cache_menu(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => {
                self.pending_confirmation = None;
                self.input_mode = InputMode::Normal;
            }
            KeyCode::Char('c') => {
                self.pending_confirmation = Some(ConfirmationAction::ClearBrowseCache);
                self.input_mode = InputMode::BrowseCacheConfirm;
            }
            KeyCode::Char('r') => {
                self.pending_confirmation = Some(ConfirmationAction::RebuildBrowseCache);
                self.input_mode = InputMode::BrowseCacheConfirm;
            }
            _ => {}
        }
        Ok(false)
    }

    fn handle_database_menu(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => {
                self.pending_confirmation = None;
                self.input_mode = InputMode::Normal;
            }
            KeyCode::Char('r') => {
                self.refresh_snapshot_status()?;
                self.status_message = Some(StatusMessage::info("Database status refreshed."));
            }
            KeyCode::Char('b') => {
                self.pending_confirmation = Some(ConfirmationAction::RebuildDatabase);
                self.input_mode = InputMode::DatabaseConfirm;
            }
            _ => {}
        }
        Ok(false)
    }

    fn handle_confirmation_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc => {
                self.pending_confirmation = None;
                self.input_mode = InputMode::Normal;
            }
            KeyCode::Enter => {
                self.execute_confirmation_action()?;
            }
            _ => {}
        }
        Ok(false)
    }

    fn descend_into_selection(&mut self) -> Result<()> {
        let Some(row) = self.selected_tree_row().cloned() else {
            return Ok(());
        };

        if let Some(next_path) = row.node_path {
            self.ui_state.path = next_path;
            self.expanded_paths.clear();
            self.row_cache.clear();
            self.enqueue_view_reload("loading view")?;
        } else {
            self.status_message = Some(StatusMessage::info("Reached the current leaf row."));
        }

        Ok(())
    }

    fn expand_selected_or_descend(&mut self) -> Result<()> {
        let Some(row) = self.selected_tree_row() else {
            return Ok(());
        };

        if row.is_expandable() && !row.is_expanded {
            self.expand_selected()?;
            return Ok(());
        }

        self.descend_into_selection()
    }

    fn collapse_selected_or_move_to_parent(&mut self) -> Result<()> {
        let Some(index) = self.table_state.selected() else {
            return self.navigate_up();
        };
        let Some(row) = self.visible_rows.get(index) else {
            return self.navigate_up();
        };

        if row.is_expanded {
            self.collapse_selected()?;
            return Ok(());
        }

        if let Some(parent_index) = self.parent_visible_index(index) {
            self.table_state.select(Some(parent_index));
            self.refresh_active_context_for_selection();
            return Ok(());
        }

        self.navigate_up()
    }

    fn open_breadcrumb_picker(&mut self) {
        self.breadcrumb_picker.selected = self.breadcrumb_targets.len().saturating_sub(1);
        self.input_mode = InputMode::BreadcrumbPicker;
    }

    fn move_breadcrumb_selection(&mut self, delta: isize) {
        if self.breadcrumb_targets.is_empty() {
            self.breadcrumb_picker.selected = 0;
            return;
        }

        let current = self.breadcrumb_picker.selected as isize;
        let max_index = self.breadcrumb_targets.len().saturating_sub(1) as isize;
        self.breadcrumb_picker.selected = (current + delta).clamp(0, max_index) as usize;
    }

    fn navigate_up(&mut self) -> Result<()> {
        self.ui_state.path =
            parent_browse_path(&self.ui_state.path, self.current_project_root.as_deref());
        self.enqueue_view_reload("loading view")?;
        Ok(())
    }

    fn clear_structural_scope_or_reset_to_root(&mut self) -> Result<()> {
        if self.ui_state.action.is_some() {
            self.ui_state.action = None;
        } else if self.ui_state.action_category.is_some() {
            self.ui_state.action_category = None;
            self.ui_state.action = None;
        } else if self.ui_state.project_id.is_some() {
            self.ui_state.project_id = None;
        } else if !matches!(self.ui_state.path, BrowsePath::Root) {
            self.ui_state.path = BrowsePath::Root;
            self.expanded_paths.clear();
            self.row_cache.clear();
        } else {
            self.status_message = Some(StatusMessage::info("Already at the unscoped root view."));
            return Ok(());
        }

        self.enqueue_view_reload("loading view")?;
        Ok(())
    }

    fn switch_root(&mut self, root: RootView) -> Result<()> {
        self.ui_state.root = root;
        self.ui_state.path = BrowsePath::Root;
        self.enqueue_view_reload("loading view")?;
        Ok(())
    }

    fn refresh_snapshot(&mut self) -> Result<()> {
        let mut perf = self.perf_scope("tui.refresh_snapshot");
        if !self.has_newer_snapshot {
            self.status_message = Some(StatusMessage::info(
                "No newer published snapshot is available.",
            ));
            perf.field("has_newer_snapshot", false);
            perf.finish_ok();
            return Ok(());
        }

        let sequence = self.next_request_sequence();
        self.pending_async_work
            .begin_view(sequence, "refreshing snapshot");
        self.worker
            .send(QueryWorkerRequest::RefreshLatest(RefreshViewRequest {
                sequence,
                label: "refreshing snapshot",
                current_snapshot: self.snapshot.clone(),
                ui_state: self.ui_state.clone(),
                selected_key: self.selected_tree_row_key(),
            }))?;
        perf.field("queued_sequence", sequence);
        perf.finish_ok();
        Ok(())
    }

    fn refresh_snapshot_status(&mut self) -> Result<()> {
        let mut perf = self.perf_scope("tui.refresh_snapshot_status");
        self.latest_snapshot = self.query_engine().latest_snapshot_bounds()?;
        self.has_newer_snapshot =
            self.latest_snapshot.max_publish_seq > self.snapshot.max_publish_seq;
        perf.field("latest_snapshot", &self.latest_snapshot);
        perf.field("has_newer_snapshot", self.has_newer_snapshot);
        perf.finish_ok();
        Ok(())
    }

    fn execute_confirmation_action(&mut self) -> Result<()> {
        let action = self.pending_confirmation;
        self.pending_confirmation = None;
        self.input_mode = InputMode::Normal;

        match action {
            Some(ConfirmationAction::ClearBrowseCache) => self.clear_browse_cache(),
            Some(ConfirmationAction::RebuildBrowseCache) => self.rebuild_browse_cache(),
            Some(ConfirmationAction::RebuildDatabase) => self.rebuild_database(),
            None => Ok(()),
        }
    }

    fn clear_browse_cache(&mut self) -> Result<()> {
        self.browse_cache.clear()?;
        self.status_message = Some(StatusMessage::info("Cleared persisted browse cache."));
        Ok(())
    }

    fn rebuild_browse_cache(&mut self) -> Result<()> {
        self.browse_cache.clear()?;
        self.query_cache.clear_domain(QueryCacheDomain::Browse);
        self.row_cache.clear();
        self.expanded_paths.clear();
        self.restart_query_worker();
        self.status_message = Some(StatusMessage::info(
            "Rebuilding browse cache from the current view.",
        ));
        self.enqueue_view_reload("rebuilding browse cache")
    }

    fn rebuild_database(&mut self) -> Result<()> {
        let browse_cache_path = default_browse_cache_path(&self.config.state_dir);
        let placeholder_db_path = self
            .config
            .state_dir
            .join("db-maintenance-placeholder.sqlite3");
        let placeholder_browse_cache_path = self
            .config
            .state_dir
            .join("browse-cache-maintenance-placeholder.sqlite3");
        let placeholder_database = Database::open(&placeholder_db_path)?;
        let placeholder_browse_cache = BrowseCacheStore::open(&placeholder_browse_cache_path)?;
        let placeholder_worker = QueryWorker::spawn(
            placeholder_db_path.clone(),
            placeholder_browse_cache_path.clone(),
            self.perf_logger.clone(),
        );

        let old_database = std::mem::replace(&mut self.database, placeholder_database);
        let old_browse_cache = std::mem::replace(&mut self.browse_cache, placeholder_browse_cache);
        let old_worker = std::mem::replace(&mut self.worker, placeholder_worker);
        drop(old_worker);
        drop(old_browse_cache);
        drop(old_database);

        let reset_report =
            reset_derived_cache_artifacts(&self.config.db_path, &self.config.state_dir)?;
        let mut database = Database::open(&self.config.db_path)?;
        let scan_report = scan_source_manifest_with_policy(
            &mut database,
            &self.config.source_root,
            &self.config.project_identity,
            &self.config.project_filters,
        )?;
        let import_report = import_all(
            database.connection(),
            &self.config.db_path,
            &self.config.source_root,
        )?;
        let completed_chunks: i64 = database.connection().query_row(
            "SELECT COUNT(*) FROM import_chunk WHERE state = 'complete'",
            [],
            |row| row.get(0),
        )?;

        let browse_cache = BrowseCacheStore::open(&browse_cache_path)?;
        let worker = QueryWorker::spawn(
            self.config.db_path.clone(),
            browse_cache_path.clone(),
            self.perf_logger.clone(),
        );
        let placeholder_database = std::mem::replace(&mut self.database, database);
        let placeholder_browse_cache = std::mem::replace(&mut self.browse_cache, browse_cache);
        let placeholder_worker = std::mem::replace(&mut self.worker, worker);
        drop(placeholder_worker);
        drop(placeholder_browse_cache);
        drop(placeholder_database);
        let _ = reset_sqlite_database(&placeholder_db_path);
        let _ = reset_sqlite_database(&placeholder_browse_cache_path);

        self.query_cache = QueryResultCache::default();
        self.row_cache.clear();
        self.expanded_paths.clear();
        self.jump_state.matches.clear();
        self.jump_state.selected = 0;

        let selected_key = self.selected_tree_row_key();
        let perf_logger = self.perf_logger.clone();
        let conn = self.database.connection();
        let query_engine = QueryEngine::with_perf(conn, perf_logger.clone());
        let latest_snapshot = query_engine.latest_snapshot_bounds()?;
        let loaded_view = load_view_for_state(
            &query_engine,
            &mut self.query_cache,
            &mut self.browse_cache,
            perf_logger,
            latest_snapshot.clone(),
            self.ui_state.clone(),
            selected_key,
            None,
        )?;
        self.apply_loaded_view(loaded_view)?;
        self.latest_snapshot = latest_snapshot.clone();
        self.snapshot = latest_snapshot;
        self.has_newer_snapshot = false;
        self.status_message = Some(StatusMessage::info(format!(
            "Rebuilt database: removed {} artifact(s), discovered {} source files, imported {} completed chunks ({} startup, {} deferred).",
            reset_report.removed_path_count(),
            scan_report.discovered_source_files,
            completed_chunks,
            import_report.startup_chunk_count,
            import_report.deferred_chunk_count
        )));
        Ok(())
    }

    fn restart_query_worker(&mut self) {
        let replacement = QueryWorker::spawn(
            self.config.db_path.clone(),
            default_browse_cache_path(&self.config.state_dir),
            self.perf_logger.clone(),
        );
        let old_worker = std::mem::replace(&mut self.worker, replacement);
        drop(old_worker);
    }

    fn current_query_filters(&self) -> Result<BrowseFilters> {
        Ok(BrowseFilters {
            time_window: self.ui_state.time_window.to_filter(&self.snapshot)?,
            model: self.ui_state.model.clone(),
            project_id: self.ui_state.project_id,
            action_category: self.ui_state.action_category.clone(),
            action: self.ui_state.action.clone(),
        })
    }

    fn apply_row_filter(&mut self) -> Result<()> {
        let mut perf = self.perf_scope("tui.apply_row_filter");
        perf.field("raw_row_count", self.raw_rows.len());
        let root_path = self.ui_state.path.clone();
        self.visible_rows = self.build_visible_rows(&root_path, 0)?;
        perf.field("visible_row_count", self.visible_rows.len());
        perf.finish_ok();
        Ok(())
    }

    fn restore_selection(&mut self, preferred_key: Option<TreeRowKey>) -> Result<()> {
        if self.visible_rows.is_empty() {
            self.table_state.select(None);
            self.rebuild_radial_model();
            return Ok(());
        }

        if let Some(index) = preferred_key
            .as_ref()
            .filter(|preferred_key| preferred_key.parent_path == self.ui_state.path)
            .and_then(|preferred_key| {
                self.visible_rows
                    .iter()
                    .position(|row| row.key() == *preferred_key)
            })
        {
            self.table_state.select(Some(index));
            self.rebuild_radial_model();
            return Ok(());
        }

        if let Some(index) = self
            .remembered_selection_for_current_path()
            .and_then(|selected_key| {
                self.visible_rows
                    .iter()
                    .position(|row| row.key() == selected_key)
            })
        {
            self.table_state.select(Some(index));
            self.rebuild_radial_model();
            return Ok(());
        }

        self.table_state.select(Some(0));
        self.rebuild_radial_model();
        Ok(())
    }

    fn selected_row(&self) -> Option<&RollupRow> {
        self.selected_tree_row().map(|row| &row.row)
    }

    fn selected_tree_row(&self) -> Option<&TreeRow> {
        self.table_state
            .selected()
            .and_then(|index| self.visible_rows.get(index))
    }

    fn select_first(&mut self) {
        if self.visible_rows.is_empty() {
            self.table_state.select(None);
        } else {
            self.table_state.select(Some(0));
        }
        self.refresh_active_context_for_selection();
    }

    fn select_last(&mut self) {
        if self.visible_rows.is_empty() {
            self.table_state.select(None);
        } else {
            self.table_state
                .select(Some(self.visible_rows.len().saturating_sub(1)));
        }
        self.refresh_active_context_for_selection();
    }

    fn move_selection(&mut self, delta: isize) {
        if self.visible_rows.is_empty() {
            self.table_state.select(None);
            self.rebuild_radial_model();
            return;
        }

        let current = self.table_state.selected().unwrap_or(0) as isize;
        let max_index = self.visible_rows.len().saturating_sub(1) as isize;
        let next = (current + delta).clamp(0, max_index) as usize;
        self.table_state.select(Some(next));
        self.refresh_active_context_for_selection();
    }

    fn selected_tree_row_key(&self) -> Option<TreeRowKey> {
        self.selected_tree_row().map(TreeRow::key)
    }

    fn expand_selected(&mut self) -> Result<()> {
        let Some(path) = self
            .selected_tree_row()
            .and_then(|row| row.node_path.clone())
        else {
            return Ok(());
        };
        if !self.expanded_paths.contains(&path) {
            self.expanded_paths.push(path);
        }
        let preferred_key = self.selected_tree_row_key();
        self.apply_row_filter()?;
        self.restore_selection(preferred_key)?;
        self.refresh_active_context_for_selection();
        Ok(())
    }

    fn collapse_selected(&mut self) -> Result<()> {
        let Some(path) = self
            .selected_tree_row()
            .and_then(|row| row.node_path.clone())
        else {
            return Ok(());
        };
        self.expanded_paths
            .retain(|expanded| !path_is_within(expanded, &path) || *expanded == path);
        self.expanded_paths.retain(|expanded| *expanded != path);
        let preferred_key = self.selected_tree_row_key();
        self.apply_row_filter()?;
        self.restore_selection(preferred_key)?;
        self.refresh_active_context_for_selection();
        Ok(())
    }

    fn parent_visible_index(&self, index: usize) -> Option<usize> {
        let depth = self.visible_rows.get(index)?.depth;
        if depth == 0 {
            return None;
        }

        self.visible_rows[..index]
            .iter()
            .enumerate()
            .rev()
            .find(|(_, row)| row.depth + 1 == depth)
            .map(|(parent_index, _)| parent_index)
    }

    fn project_root_for(
        &mut self,
        project_id: i64,
        browse_stats: &mut BrowseFanoutStats,
    ) -> Result<Option<String>> {
        let filters = self.current_query_filters()?;
        let relaxed_filters = BrowseFilters {
            time_window: filters.time_window,
            model: filters.model,
            project_id: None,
            action_category: None,
            action: None,
        };
        let root_rows = self.cached_browse(
            BrowseRequest {
                snapshot: self.snapshot.clone(),
                root: RootView::ProjectHierarchy,
                lens: self.ui_state.lens,
                filters: relaxed_filters,
                path: BrowsePath::Root,
            },
            browse_stats,
        )?;

        Ok(root_rows
            .into_iter()
            .find(|row| row.project_id == Some(project_id))
            .and_then(|row| row.full_path))
    }

    fn project_identity_for(&self, project_id: i64) -> Result<Option<SelectedProjectIdentity>> {
        self.database
            .connection()
            .query_row(
                "
                SELECT identity_kind, root_path, identity_reason
                FROM project
                WHERE id = ?1
                ",
                [project_id],
                |row| {
                    let identity_kind: String = row.get(0)?;
                    let identity_kind = match identity_kind.as_str() {
                        "git" => ProjectIdentityKind::Git,
                        "path" => ProjectIdentityKind::Path,
                        other => {
                            return Err(rusqlite::Error::FromSqlConversionFailure(
                                0,
                                rusqlite::types::Type::Text,
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("unexpected project identity kind: {other}"),
                                )),
                            ));
                        }
                    };

                    Ok(SelectedProjectIdentity {
                        identity_kind,
                        root_path: row.get(1)?,
                        identity_reason: row.get(2)?,
                    })
                },
            )
            .optional()
            .context("unable to load selected project identity metadata")
    }

    fn rows_for_path(&self, path: &BrowsePath) -> Option<&Vec<RollupRow>> {
        self.row_cache
            .iter()
            .find(|cached| cached.path == *path)
            .map(|cached| &cached.rows)
    }

    fn cache_rows(&mut self, path: BrowsePath, rows: Vec<RollupRow>) {
        if let Some(cached) = self.row_cache.iter_mut().find(|cached| cached.path == path) {
            cached.rows = rows;
        } else {
            self.row_cache.push(CachedRows { path, rows });
        }
    }

    fn expanded_for_path(&self, path: &BrowsePath) -> bool {
        self.expanded_paths.contains(path)
    }

    fn build_visible_rows(
        &mut self,
        parent_path: &BrowsePath,
        depth: usize,
    ) -> Result<Vec<TreeRow>> {
        let rows = self.rows_for_path(parent_path).cloned().unwrap_or_default();
        let filter = self.ui_state.row_filter.trim().to_ascii_lowercase();
        let mut visible = Vec::new();

        for row in rows {
            let node_path = next_browse_path(&self.ui_state.root, parent_path, &row);
            let is_expanded = node_path
                .as_ref()
                .is_some_and(|path| self.expanded_for_path(path));
            let include = (filter.is_empty() || row_search_text(&row).contains(&filter))
                && self
                    .ui_state
                    .opportunity_filter
                    .is_none_or(|cat| row.opportunities.top_category == Some(cat));

            if include {
                visible.push(TreeRow {
                    row: row.clone(),
                    parent_path: parent_path.clone(),
                    node_path: node_path.clone(),
                    depth,
                    is_expanded,
                });
            }

            if let Some(path) = node_path
                && is_expanded
            {
                let children = self.load_rows_for_path(path.clone())?;
                if !children.is_empty() {
                    visible.extend(self.build_visible_rows(&path, depth + 1)?);
                }
            }
        }

        Ok(visible)
    }

    fn load_rows_for_path(&mut self, path: BrowsePath) -> Result<Vec<RollupRow>> {
        if let Some(rows) = self.rows_for_path(&path) {
            return Ok(rows.clone());
        }

        let filters = self.current_query_filters()?;
        let mut browse_stats = BrowseFanoutStats::default();
        let rows = self.cached_browse(
            BrowseRequest {
                snapshot: self.snapshot.clone(),
                root: self.ui_state.root,
                lens: self.ui_state.lens,
                filters,
                path: path.clone(),
            },
            &mut browse_stats,
        )?;
        self.cache_rows(path, rows.clone());
        Ok(rows)
    }
    fn filter_summary(&self) -> String {
        let mut parts = vec![format!("time {}", self.ui_state.time_window.label())];
        if let Some(model) = &self.ui_state.model {
            parts.push(format!("model {model}"));
        }
        if let Some(project_id) = self.ui_state.project_id {
            let name = self
                .filter_options
                .projects
                .iter()
                .find(|project| project.id == project_id)
                .map(|project| project.display_name.clone())
                .unwrap_or_else(|| format!("#{project_id}"));
            parts.push(format!("project: {name}"));
        }
        if let Some(category) = &self.ui_state.action_category {
            parts.push(format!("category: {category}"));
        }
        if let Some(action) = &self.ui_state.action {
            parts.push(format!("action: {}", action_label(action)));
        }
        if !self.ui_state.row_filter.is_empty() {
            parts.push(format!("row {}", self.ui_state.row_filter));
        }
        parts.join("  |  ")
    }

    fn selection_footer_text(&self) -> String {
        let filter = if self.ui_state.row_filter.is_empty() {
            "(none)".to_string()
        } else {
            self.ui_state.row_filter.clone()
        };
        let selected = self
            .selected_row()
            .map(|row| {
                format!(
                    "{} ({}, {} {})",
                    row.label,
                    row_kind_label(row.kind),
                    metric_lens_label(self.ui_state.lens),
                    format_metric(row.metrics.lens_value(self.ui_state.lens))
                )
            })
            .unwrap_or_else(|| "none".to_string());
        let node_metadata = self.selected_row().and_then(|row| {
            selected_node_metadata_text(row, self.selected_project_identity.as_ref())
        });

        let mut parts = vec![format!("selected: {selected}")];
        if let Some(node_metadata) = node_metadata {
            parts.push(node_metadata);
        }
        parts.push(format!("row filter: {filter}"));

        parts.join("  |  ")
    }

    fn header_detail_line(&self) -> Line<'static> {
        let activity = match (
            self.startup_activity.as_ref().map(StartupActivity::summary),
            self.pending_async_work.summary(),
        ) {
            (Some(startup), Some(async_work)) => Some(format!("{startup}  |  {async_work}")),
            (Some(startup), None) => Some(startup),
            (None, Some(async_work)) => Some(async_work),
            (None, None) => None,
        };

        match (activity, self.status_message.as_ref()) {
            (Some(activity), Some(message)) => Line::from(vec![
                badge("activity", BadgeTone::Accent),
                separator_span(),
                Span::styled(activity, Style::default().fg(Color::White)),
                separator_span(),
                badge(
                    "status",
                    match message.tone {
                        StatusTone::Info => BadgeTone::Info,
                        StatusTone::Error => BadgeTone::Warning,
                    },
                ),
                separator_span(),
                Span::styled(message.text.clone(), status_tone_style(message.tone)),
            ]),
            (Some(activity), None) => Line::from(vec![
                badge("activity", BadgeTone::Accent),
                separator_span(),
                Span::styled(activity, Style::default().fg(Color::White)),
            ]),
            (None, Some(message)) => Line::from(vec![
                badge(
                    "status",
                    match message.tone {
                        StatusTone::Info => BadgeTone::Info,
                        StatusTone::Error => BadgeTone::Warning,
                    },
                ),
                separator_span(),
                Span::styled(message.text.clone(), status_tone_style(message.tone)),
            ]),
            (None, None) => Line::from(vec![
                badge("filters", BadgeTone::Muted),
                separator_span(),
                Span::styled(self.filter_summary(), Style::default().fg(Color::Gray)),
            ]),
        }
    }

    fn browse_cache_header_line(&self) -> Line<'static> {
        match self.browse_cache.stats() {
            Ok(stats) => Line::from(vec![
                badge("browse-cache", BadgeTone::Accent),
                separator_span(),
                Span::styled(
                    format!(
                        "{} entries  |  payload {}  |  B manage",
                        stats.entry_count,
                        format_storage_bytes(stats.total_payload_bytes)
                    ),
                    Style::default().fg(Color::White),
                ),
            ]),
            Err(error) => Line::from(vec![
                badge("browse-cache", BadgeTone::Warning),
                separator_span(),
                Span::styled(
                    compact_status_text(format!("unavailable: {error:#}")),
                    Style::default().fg(Color::Yellow),
                ),
            ]),
        }
    }

    fn database_header_line(&self) -> Line<'static> {
        match sqlite_artifact_size_bytes(&self.config.db_path) {
            Ok(size_bytes) => Line::from(vec![
                badge("database", BadgeTone::Info),
                separator_span(),
                Span::styled(
                    format!(
                        "{}  |  size {}  |  publish_seq <= {} ({} chunks)  |  D manage",
                        self.config.db_path.display(),
                        format_storage_bytes(size_bytes),
                        self.snapshot.max_publish_seq,
                        self.snapshot.published_chunk_count
                    ),
                    Style::default().fg(Color::White),
                ),
            ]),
            Err(error) => Line::from(vec![
                badge("database", BadgeTone::Warning),
                separator_span(),
                Span::styled(
                    compact_status_text(format!("unavailable: {error:#}")),
                    Style::default().fg(Color::Yellow),
                ),
            ]),
        }
    }

    fn build_radial_context(
        &mut self,
        filters: &BrowseFilters,
        path: &BrowsePath,
        pre_resolved_project_root: Option<Option<String>>,
        browse_stats: &mut BrowseFanoutStats,
    ) -> Result<RadialContext> {
        let mut perf = self.slow_perf_scope_with_filters("tui.selection_context_load", filters);
        perf.field("active_path", path);
        perf.field("selected_tree_row_key", self.selected_tree_row_key());
        let perf_logger = self.perf_logger.clone();
        let conn = self.database.connection();
        let query_engine = QueryEngine::with_perf(conn, perf_logger.clone());
        let mut cache_sources = BrowseCacheSourceStats::default();
        let project_root = match pre_resolved_project_root {
            Some(resolved) => resolved,
            None => match project_id_from_path(path) {
                Some(project_id) => project_root_for_state_with_source(
                    &mut self.query_cache,
                    &mut self.browse_cache,
                    &query_engine,
                    &self.snapshot,
                    self.ui_state.lens,
                    filters,
                    project_id,
                    browse_stats,
                    &mut cache_sources,
                )?,
                None => None,
            },
        };
        let radial_context = build_radial_context_for_state_with_source(
            &mut self.query_cache,
            &mut self.browse_cache,
            &query_engine,
            perf_logger,
            RadialContextRequest {
                snapshot: &self.snapshot,
                ui_state: &self.ui_state,
                active_path: path,
                filters,
                project_root: project_root.as_deref(),
            },
            browse_stats,
            &mut cache_sources,
        )?;
        perf.field("browse_request_count", browse_stats.total_requests());
        perf.field(
            "distinct_browse_request_count",
            browse_stats.distinct_request_count(),
        );
        perf.field(
            "duplicate_browse_request_count",
            browse_stats.duplicate_request_count(),
        );
        perf.field(
            "repeated_browse_requests",
            browse_stats.repeated_requests(3),
        );
        perf.field("cache_memory_hit_count", cache_sources.memory_hits);
        perf.field("cache_persisted_hit_count", cache_sources.persisted_hits);
        perf.field("cache_live_query_count", cache_sources.live_queries);
        perf.finish_ok();

        Ok(radial_context)
    }

    fn build_selection_context(
        &mut self,
        filters: &BrowseFilters,
        active_path: BrowsePath,
        selected_row: Option<TreeRow>,
    ) -> Result<SelectionContextValue> {
        let mut browse_stats = BrowseFanoutStats::default();
        let selected_project_identity = selected_row.as_ref().and_then(|row| {
            matches!(row.row.kind, RollupRowKind::Project)
                .then_some(row.row.project_id)
                .flatten()
        });
        let current_project_root = match project_id_from_path(&active_path) {
            Some(project_id) => self.project_root_for(project_id, &mut browse_stats)?,
            None => None,
        };
        let breadcrumb_targets = build_breadcrumb_targets(
            &self.ui_state.root,
            &active_path,
            &self.filter_options,
            current_project_root.as_deref(),
        );
        let radial_context = self.build_radial_context(
            filters,
            &active_path,
            Some(current_project_root.clone()),
            &mut browse_stats,
        )?;

        let selected_project_identity = match selected_project_identity {
            Some(project_id) => self.project_identity_for(project_id).ok().flatten(),
            None => None,
        };

        Ok(SelectionContextValue {
            active_path,
            current_project_root,
            breadcrumb_targets,
            radial_context,
            selected_project_identity,
        })
    }

    fn next_request_sequence(&mut self) -> u64 {
        let sequence = self.next_request_sequence;
        self.next_request_sequence += 1;
        sequence
    }

    fn enqueue_view_reload(&mut self, label: &'static str) -> Result<()> {
        let sequence = self.next_request_sequence();
        self.pending_async_work.begin_view(sequence, label);
        self.worker
            .send(QueryWorkerRequest::LoadView(ViewLoadRequest {
                sequence,
                label,
                snapshot: self.snapshot.clone(),
                ui_state: self.ui_state.clone(),
                selected_key: self.selected_tree_row_key(),
            }))
    }

    fn enqueue_jump_target_rebuild(&mut self) -> Result<()> {
        let sequence = self.next_request_sequence();
        self.pending_async_work
            .begin_jump(sequence, "building jump targets");
        self.jump_state.matches.clear();
        self.jump_state.selected = 0;
        self.worker
            .send(QueryWorkerRequest::BuildJumpTargets(JumpTargetRequest {
                sequence,
                label: "building jump targets",
                snapshot: self.snapshot.clone(),
                ui_state: self.ui_state.clone(),
            }))
    }

    fn prefetch_context(&self) -> Result<PrefetchContext> {
        Ok(PrefetchContext {
            snapshot: self.snapshot.clone(),
            root: self.ui_state.root,
            lens: self.ui_state.lens,
            filters: self.current_query_filters()?,
        })
    }

    fn visible_row_infos(&self) -> Vec<VisibleRowInfo> {
        self.visible_rows
            .iter()
            .map(|row| VisibleRowInfo {
                node_path: row.node_path.clone(),
            })
            .collect()
    }

    fn populate_prefetch(&mut self) {
        let Ok(context) = self.prefetch_context() else {
            return;
        };
        let selected = self.selected_tree_row();
        let selected_path = selected.and_then(|r| r.node_path.clone());
        let selected_parent = selected.map(|r| r.parent_path.clone());
        let selected_index = self.table_state.selected();
        let row_infos = self.visible_row_infos();
        self.prefetch.populate(
            context,
            selected_path.as_ref(),
            selected_parent.as_ref(),
            &row_infos,
            selected_index,
        );
        let _ = self.submit_next_prefetch_batch();
    }

    fn reprioritize_prefetch(&mut self) {
        let selected = self.selected_tree_row();
        let selected_path = selected.and_then(|r| r.node_path.clone());
        let selected_parent = selected.map(|r| r.parent_path.clone());
        let selected_index = self.table_state.selected();
        let row_infos = self.visible_row_infos();
        self.prefetch.reprioritize(
            selected_path.as_ref(),
            selected_parent.as_ref(),
            &row_infos,
            selected_index,
        );
        let _ = self.submit_next_prefetch_batch();
    }

    fn submit_next_prefetch_batch(&mut self) -> Result<()> {
        if self.prefetch.has_in_flight() || !self.prefetch.has_pending() {
            return Ok(());
        }
        let Some((context, paths, profile)) = self.prefetch.drain_batch() else {
            return Ok(());
        };
        let requests: Vec<BrowseRequest> = paths
            .into_iter()
            .map(|path| BrowseRequest {
                snapshot: context.snapshot.clone(),
                root: context.root,
                lens: context.lens,
                filters: context.filters.clone(),
                path,
            })
            .collect();
        let sequence = self.next_request_sequence();
        self.pending_async_work
            .begin_prefetch(sequence, "warming visible selection contexts");
        self.worker
            .send(QueryWorkerRequest::Prefetch(PrefetchRequest {
                sequence,
                label: "warming visible selection contexts",
                queued_at: Instant::now(),
                requests,
                profile,
            }))
    }

    fn drain_query_results(&mut self) {
        while let Ok(result) = self.worker.results.try_recv() {
            if let Err(error) = self.apply_query_result(result) {
                self.status_message = Some(StatusMessage::error(format!(
                    "Unable to apply interactive query result: {error:#}"
                )));
            }
        }
    }

    fn apply_query_result(&mut self, result: QueryWorkerResult) -> Result<()> {
        match result {
            QueryWorkerResult::Progress(progress) => {
                self.pending_async_work.apply_progress(progress);
            }
            QueryWorkerResult::ViewLoaded(result) => {
                if self
                    .pending_async_work
                    .finish(PendingTaskKind::View, result.sequence)
                    == PendingFinish::Apply
                {
                    self.apply_loaded_view(result.loaded_view)?;
                }
            }
            QueryWorkerResult::RefreshCompleted(result) => {
                if self
                    .pending_async_work
                    .finish(PendingTaskKind::View, result.sequence)
                    == PendingFinish::Apply
                {
                    self.latest_snapshot = result.latest_snapshot.clone();
                    if let Some(loaded_view) = result.loaded_view {
                        self.apply_loaded_view(loaded_view)?;
                        self.has_newer_snapshot = false;
                        self.status_message = Some(StatusMessage::info(format!(
                            "Switched to the newest imported snapshot {}.",
                            snapshot_coverage_tail(&self.snapshot)
                        )));
                    } else {
                        self.has_newer_snapshot =
                            self.latest_snapshot.max_publish_seq > self.snapshot.max_publish_seq;
                        self.status_message = Some(StatusMessage::info(
                            "No newer published snapshot is available.",
                        ));
                    }
                }
            }
            QueryWorkerResult::JumpTargetsBuilt(result) => {
                if self
                    .pending_async_work
                    .finish(PendingTaskKind::Jump, result.sequence)
                    == PendingFinish::Apply
                {
                    self.jump_state.matches =
                        build_jump_matches(&self.jump_state.query, result.targets);
                    self.jump_state.selected = 0;
                }
            }
            QueryWorkerResult::PrefetchCompleted(result) => {
                if self
                    .pending_async_work
                    .finish(PendingTaskKind::Prefetch, result.sequence)
                    == PendingFinish::Apply
                {
                    let mut child_paths_per_parent = Vec::new();
                    for (request, rows) in
                        result.requests.into_iter().zip(result.row_sets.into_iter())
                    {
                        let children: Vec<BrowsePath> = prefetch_child_requests(&request, &rows)
                            .into_iter()
                            .map(|r| r.path)
                            .collect();
                        child_paths_per_parent.push((request.path.clone(), children));
                        self.query_cache.insert_browse_rows(
                            &request.snapshot,
                            &request,
                            rows.clone(),
                        )?;
                    }
                    self.prefetch.complete_batch(&child_paths_per_parent);
                    let _ = self.submit_next_prefetch_batch();
                }
            }
            QueryWorkerResult::Failed(failure) => {
                let is_prefetch = failure.task == PendingTaskKind::Prefetch;
                if self
                    .pending_async_work
                    .finish(failure.task, failure.sequence)
                    == PendingFinish::Apply
                {
                    self.status_message = Some(StatusMessage::error(failure.message));
                }
                if is_prefetch {
                    // Clear in-flight state so the next batch can be submitted.
                    // We don't know which paths failed, so we can't call
                    // fail_batch precisely. Reset in_flight and try next batch.
                    self.prefetch.reset();
                    let _ = self.submit_next_prefetch_batch();
                }
            }
        }

        Ok(())
    }

    fn apply_loaded_view(&mut self, loaded_view: LoadedView) -> Result<()> {
        self.snapshot = loaded_view.snapshot;
        self.query_cache.retain_snapshot(&self.snapshot);
        self.selection_context_cache.retain_snapshot(&self.snapshot);
        self.snapshot_coverage = loaded_view.snapshot_coverage;
        self.ui_state = loaded_view.ui_state;
        self.filter_options = loaded_view.filter_options;
        self.raw_rows = loaded_view.raw_rows;
        self.breadcrumb_targets = loaded_view.breadcrumb_targets;
        self.breadcrumb_picker.selected = self.breadcrumb_targets.len().saturating_sub(1);
        self.current_project_root = loaded_view.current_project_root;
        self.radial_context = loaded_view.radial_context;
        self.radial_context_path = self.ui_state.path.clone();
        self.row_cache.clear();
        self.cache_rows(self.ui_state.path.clone(), self.raw_rows.clone());
        self.expanded_paths
            .retain(|expanded| path_is_within(expanded, &self.ui_state.path));
        self.apply_row_filter()?;
        self.restore_selection(loaded_view.selected_key)?;
        self.refresh_active_context_for_selection();
        self.populate_prefetch();
        self.save_state();
        Ok(())
    }

    fn rebuild_radial_model(&mut self) {
        let (path, rows, selected_row) = self.active_radial_state();
        self.radial_model = build_radial_model(
            &self.radial_context,
            &rows,
            selected_row.as_ref(),
            &self.ui_state.root,
            &path,
            &self.filter_options,
            self.ui_state.lens,
        );
    }

    fn refresh_active_context_for_selection(&mut self) {
        self.remember_selection_for_current_path();
        let selected_row = self.selected_tree_row().cloned();
        let active_path = self.active_path();
        let selected_row_key = selected_row.as_ref().map(TreeRow::key);
        let mut perf = self.slow_perf_scope("tui.selection_change");
        perf.field("selected_tree_row_key", &selected_row_key);
        perf.field("active_path", &active_path);
        let mut selection_context_cache_hit = None;

        if let Ok(filters) = self.current_query_filters() {
            let key = SelectionContextKey {
                snapshot: self.snapshot.clone(),
                root: self.ui_state.root,
                lens: self.ui_state.lens,
                filters: filters.clone(),
                selected_row_key,
                active_path: active_path.clone(),
            };

            let context = match self.selection_context_cache.lookup(&key) {
                Ok(Some(context)) => {
                    self.selection_context_cache.stats.hits += 1;
                    selection_context_cache_hit = Some(true);
                    Some(context)
                }
                Ok(None) => {
                    selection_context_cache_hit = Some(false);
                    match self.build_selection_context(&filters, active_path.clone(), selected_row)
                    {
                        Ok(context) => {
                            self.selection_context_cache.stats.misses += 1;
                            let _ = self.selection_context_cache.store(key, context.clone());
                            Some(context)
                        }
                        Err(_) => None,
                    }
                }
                Err(_) => None,
            };

            if let Some(context) = context {
                self.apply_selection_context(context);
            }
        }

        self.rebuild_radial_model();
        self.reprioritize_prefetch();
        perf.field("selection_context_cache_hit", selection_context_cache_hit);
        perf.field("radial_context_path", &self.radial_context_path);
        perf.finish_ok();
    }

    fn active_path(&self) -> BrowsePath {
        self.selected_tree_row()
            .and_then(|row| row.node_path.clone())
            .unwrap_or_else(|| self.ui_state.path.clone())
    }

    fn remember_selection_for_current_path(&mut self) {
        let Some(selected_key) = self.selected_tree_row_key() else {
            return;
        };

        if let Some(existing) = self.remembered_selections.iter_mut().find(|existing| {
            existing.root == self.ui_state.root && existing.path == self.ui_state.path
        }) {
            existing.selected_key = selected_key;
            return;
        }

        self.remembered_selections.push(RememberedSelection {
            root: self.ui_state.root,
            path: self.ui_state.path.clone(),
            selected_key,
        });
    }

    fn remembered_selection_for_current_path(&self) -> Option<TreeRowKey> {
        self.remembered_selections
            .iter()
            .find(|existing| {
                existing.root == self.ui_state.root && existing.path == self.ui_state.path
            })
            .map(|existing| existing.selected_key.clone())
    }

    fn active_radial_state(&self) -> (BrowsePath, Vec<RollupRow>, Option<RollupRow>) {
        let Some(selected) = self.selected_tree_row() else {
            return (self.ui_state.path.clone(), self.raw_rows.clone(), None);
        };

        if let Some(parent_rows) = self.rows_for_path(&selected.parent_path).cloned() {
            return (
                selected.parent_path.clone(),
                parent_rows,
                Some(selected.row.clone()),
            );
        }

        (
            self.ui_state.path.clone(),
            self.raw_rows.clone(),
            Some(selected.row.clone()),
        )
    }

    fn apply_selection_context(&mut self, context: SelectionContextValue) {
        self.current_project_root = context.current_project_root;
        self.breadcrumb_targets = context.breadcrumb_targets;
        self.breadcrumb_picker.selected = self.breadcrumb_targets.len().saturating_sub(1);
        self.radial_context = context.radial_context;
        self.radial_context_path = context.active_path;
        self.selected_project_identity = context.selected_project_identity;
    }

    fn save_state(&mut self) {
        if let Err(error) = self.ui_state.save(&self.ui_state_path) {
            self.status_message = Some(StatusMessage::error(format!(
                "Unable to save TUI state: {error:#}"
            )));
        }
    }

    fn perf_scope(&self, operation: &str) -> PerfScope {
        let mut scope = PerfScope::new(self.perf_logger.clone(), operation);
        scope.field("snapshot", &self.snapshot);
        scope.field("root", self.ui_state.root);
        scope.field("path", &self.ui_state.path);
        scope.field("lens", self.ui_state.lens);
        scope.field("row_filter", &self.ui_state.row_filter);
        scope.field("selected_row_count", self.visible_rows.len());
        scope
    }

    fn slow_perf_scope(&self, operation: &str) -> PerfScope {
        self.perf_scope(operation)
            .with_min_duration(SELECTION_SLOW_LOG_THRESHOLD)
    }

    fn slow_perf_scope_with_filters(&self, operation: &str, filters: &BrowseFilters) -> PerfScope {
        let mut scope = self.slow_perf_scope(operation);
        scope.field("filters", filters);
        scope
    }

    fn query_engine(&self) -> QueryEngine<'_> {
        QueryEngine::with_perf(self.database.connection(), self.perf_logger.clone())
    }

    fn cached_browse(
        &mut self,
        request: BrowseRequest,
        browse_stats: &mut BrowseFanoutStats,
    ) -> Result<Vec<RollupRow>> {
        let perf_logger = self.perf_logger.clone();
        let conn = self.database.connection();
        let query_engine = QueryEngine::with_perf(conn, perf_logger);
        cached_browse(
            &mut self.query_cache,
            &mut self.browse_cache,
            &query_engine,
            request,
            browse_stats,
        )
    }
}

#[derive(Debug, Clone)]
struct CachedRows {
    path: BrowsePath,
    rows: Vec<RollupRow>,
}

#[derive(Debug, Clone)]
struct RememberedSelection {
    root: RootView,
    path: BrowsePath,
    selected_key: TreeRowKey,
}

#[derive(Debug, Clone)]
struct TreeRow {
    row: RollupRow,
    parent_path: BrowsePath,
    node_path: Option<BrowsePath>,
    depth: usize,
    is_expanded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SelectedProjectIdentity {
    identity_kind: ProjectIdentityKind,
    root_path: String,
    identity_reason: Option<String>,
}

impl TreeRow {
    fn key(&self) -> TreeRowKey {
        TreeRowKey {
            parent_path: self.parent_path.clone(),
            row_key: self.row.key.clone(),
        }
    }

    fn is_expandable(&self) -> bool {
        self.node_path.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct TreeRowKey {
    parent_path: BrowsePath,
    row_key: String,
}

struct QueryProgressReporter<'a> {
    results: &'a Sender<QueryWorkerResult>,
    sequence: u64,
    task: PendingTaskKind,
}

impl<'a> QueryProgressReporter<'a> {
    fn new(results: &'a Sender<QueryWorkerResult>, sequence: u64, task: PendingTaskKind) -> Self {
        Self {
            results,
            sequence,
            task,
        }
    }
}

impl ProgressSink for QueryProgressReporter<'_> {
    fn update(&mut self, phase: String, progress: Option<PhaseProgress>) {
        let _ = self
            .results
            .send(QueryWorkerResult::Progress(QueryProgressUpdate {
                sequence: self.sequence,
                task: self.task,
                phase,
                progress,
            }));
    }
}

struct LoadProgressReporter<'a> {
    callback: &'a mut dyn FnMut(StartupLoadProgressUpdate),
}

impl<'a> LoadProgressReporter<'a> {
    fn new(callback: &'a mut dyn FnMut(StartupLoadProgressUpdate)) -> Self {
        Self { callback }
    }
}

impl ProgressSink for LoadProgressReporter<'_> {
    fn update(&mut self, phase: String, progress: Option<PhaseProgress>) {
        (self.callback)(StartupLoadProgressUpdate {
            phase,
            current: progress.map(|progress| progress.current),
            total: progress.map(|progress| progress.total),
        });
    }
}

fn run_query_worker(
    db_path: PathBuf,
    browse_cache_path: PathBuf,
    perf_logger: Option<PerfLogger>,
    requests: Receiver<QueryWorkerRequest>,
    results: Sender<QueryWorkerResult>,
) {
    let database = match Database::open_read_only(&db_path) {
        Ok(database) => database,
        Err(error) => {
            let _ = results.send(QueryWorkerResult::Failed(QueryWorkerFailure {
                sequence: 0,
                task: PendingTaskKind::View,
                message: format!("Unable to start interactive query worker: {error:#}"),
            }));
            return;
        }
    };
    let query_engine = QueryEngine::with_perf(database.connection(), perf_logger.clone());
    let mut query_cache = QueryResultCache::default();
    let mut browse_cache = match BrowseCacheStore::open(browse_cache_path) {
        Ok(browse_cache) => browse_cache,
        Err(error) => {
            let _ = results.send(QueryWorkerResult::Failed(QueryWorkerFailure {
                sequence: 0,
                task: PendingTaskKind::View,
                message: format!("Unable to start browse-cache store: {error:#}"),
            }));
            return;
        }
    };

    while let Ok(request) = requests.recv() {
        let result = match request {
            QueryWorkerRequest::LoadView(request) => {
                let mut progress =
                    QueryProgressReporter::new(&results, request.sequence, PendingTaskKind::View);
                load_view_for_state(
                    &query_engine,
                    &mut query_cache,
                    &mut browse_cache,
                    perf_logger.clone(),
                    request.snapshot,
                    request.ui_state,
                    request.selected_key,
                    Some(&mut progress),
                )
                .map(|loaded_view| {
                    QueryWorkerResult::ViewLoaded(ViewLoadResult {
                        sequence: request.sequence,
                        loaded_view,
                    })
                })
                .unwrap_or_else(|error| {
                    QueryWorkerResult::Failed(QueryWorkerFailure {
                        sequence: request.sequence,
                        task: PendingTaskKind::View,
                        message: format!("Unable to {}: {error:#}", request.label),
                    })
                })
            }
            QueryWorkerRequest::RefreshLatest(request) => {
                let mut progress =
                    QueryProgressReporter::new(&results, request.sequence, PendingTaskKind::View);
                progress.phase("checking for newer snapshot".to_string());
                let latest_snapshot = query_engine.latest_snapshot_bounds();
                match latest_snapshot {
                    Ok(latest_snapshot)
                        if latest_snapshot.max_publish_seq
                            > request.current_snapshot.max_publish_seq =>
                    {
                        progress.phase("loading filter options".to_string());
                        load_view_for_state(
                            &query_engine,
                            &mut query_cache,
                            &mut browse_cache,
                            perf_logger.clone(),
                            latest_snapshot.clone(),
                            request.ui_state,
                            request.selected_key,
                            Some(&mut progress),
                        )
                        .map(|loaded_view| {
                            QueryWorkerResult::RefreshCompleted(RefreshViewResult {
                                sequence: request.sequence,
                                latest_snapshot,
                                loaded_view: Some(loaded_view),
                            })
                        })
                        .unwrap_or_else(|error| {
                            QueryWorkerResult::Failed(QueryWorkerFailure {
                                sequence: request.sequence,
                                task: PendingTaskKind::View,
                                message: format!("Unable to {}: {error:#}", request.label),
                            })
                        })
                    }
                    Ok(latest_snapshot) => QueryWorkerResult::RefreshCompleted(RefreshViewResult {
                        sequence: request.sequence,
                        latest_snapshot,
                        loaded_view: None,
                    }),
                    Err(error) => QueryWorkerResult::Failed(QueryWorkerFailure {
                        sequence: request.sequence,
                        task: PendingTaskKind::View,
                        message: format!("Unable to {}: {error:#}", request.label),
                    }),
                }
            }
            QueryWorkerRequest::BuildJumpTargets(request) => {
                let mut progress =
                    QueryProgressReporter::new(&results, request.sequence, PendingTaskKind::Jump);
                build_jump_targets_for_state(
                    &query_engine,
                    &mut query_cache,
                    &mut browse_cache,
                    perf_logger.clone(),
                    request.snapshot,
                    request.ui_state,
                    Some(&mut progress),
                )
                .map(|targets| {
                    QueryWorkerResult::JumpTargetsBuilt(JumpTargetsResult {
                        sequence: request.sequence,
                        targets,
                    })
                })
                .unwrap_or_else(|error| {
                    QueryWorkerResult::Failed(QueryWorkerFailure {
                        sequence: request.sequence,
                        task: PendingTaskKind::Jump,
                        message: format!("Unable to {}: {error:#}", request.label),
                    })
                })
            }
            QueryWorkerRequest::Prefetch(request) => {
                let sequence = request.sequence;
                let label = request.label;
                run_prefetch_requests(
                    &mut query_cache,
                    &mut browse_cache,
                    &query_engine,
                    perf_logger.clone(),
                    request,
                )
                .map(|(requests, row_sets)| {
                    QueryWorkerResult::PrefetchCompleted(PrefetchResult {
                        sequence,
                        requests,
                        row_sets,
                    })
                })
                .unwrap_or_else(|error| {
                    QueryWorkerResult::Failed(QueryWorkerFailure {
                        sequence,
                        task: PendingTaskKind::Prefetch,
                        message: format!("Unable to {}: {error:#}", label),
                    })
                })
            }
        };

        if results.send(result).is_err() {
            break;
        }
    }
}

fn run_prefetch_requests(
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    query_engine: &QueryEngine<'_>,
    perf_logger: Option<PerfLogger>,
    request: PrefetchRequest,
) -> Result<(Vec<BrowseRequest>, Vec<Vec<RollupRow>>)> {
    let mut perf = prefetch_batch_perf_scope(perf_logger);
    perf.field(
        "queue_wait_ms",
        request.queued_at.elapsed().as_secs_f64() * 1000.0,
    );
    perf.field("request_count", request.requests.len());
    perf.field(
        "selected_row_request_count",
        request.profile.selected_row_count,
    );
    perf.field(
        "nearby_visible_row_request_count",
        request.profile.nearby_visible_row_count,
    );
    perf.field(
        "visible_row_request_count",
        request.profile.visible_row_count,
    );
    perf.field(
        "recursive_depth_request_count",
        request.profile.recursive_depth_count,
    );

    let mut browse_stats = BrowseFanoutStats::default();
    let mut cache_sources = BrowseCacheSourceStats::default();
    let row_sets = cached_browse_many_with_source(
        query_cache,
        browse_cache,
        query_engine,
        request.requests.clone(),
        &mut browse_stats,
        &mut cache_sources,
    )?;
    perf.field("browse_request_count", browse_stats.total_requests());
    perf.field(
        "distinct_browse_request_count",
        browse_stats.distinct_request_count(),
    );
    perf.field(
        "duplicate_browse_request_count",
        browse_stats.duplicate_request_count(),
    );
    perf.field("cache_memory_hit_count", cache_sources.memory_hits);
    perf.field("cache_persisted_hit_count", cache_sources.persisted_hits);
    perf.field("cache_live_query_count", cache_sources.live_queries);
    perf.finish_ok();
    Ok((request.requests, row_sets))
}

fn prefetch_batch_perf_scope(perf_logger: Option<PerfLogger>) -> PerfScope {
    PerfScope::new(perf_logger, "tui.prefetch_batch")
        .with_min_duration(SELECTION_SLOW_LOG_THRESHOLD)
}

fn cached_browse(
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    query_engine: &QueryEngine<'_>,
    request: BrowseRequest,
    browse_stats: &mut BrowseFanoutStats,
) -> Result<Vec<RollupRow>> {
    let (rows, _) = cached_browse_with_source(
        query_cache,
        browse_cache,
        query_engine,
        request,
        browse_stats,
    )?;
    Ok(rows)
}

fn cached_browse_with_source(
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    query_engine: &QueryEngine<'_>,
    request: BrowseRequest,
    browse_stats: &mut BrowseFanoutStats,
) -> Result<(Vec<RollupRow>, BrowseCacheSource)> {
    browse_stats.record(&request);
    let snapshot = request.snapshot.clone();
    let key = QueryCacheKey {
        domain: QueryCacheDomain::Browse,
        input: serde_json::to_string(&request).context("unable to serialize query cache key")?,
    };

    if let Some(cached) = query_cache.cached::<Vec<RollupRow>>(&snapshot, &key) {
        query_cache
            .stats
            .entry(QueryCacheDomain::Browse)
            .or_default()
            .hits += 1;
        return Ok((cached, BrowseCacheSource::Memory));
    }

    query_cache
        .stats
        .entry(QueryCacheDomain::Browse)
        .or_default()
        .misses += 1;

    if let Some(rows) = browse_cache.load(&request)? {
        query_cache
            .snapshot_bucket_mut(&snapshot)
            .entries
            .insert(key, Box::new(rows.clone()));
        return Ok((rows, BrowseCacheSource::Persisted));
    }

    let rows = query_engine.browse(&request)?;
    query_cache
        .snapshot_bucket_mut(&snapshot)
        .entries
        .insert(key, Box::new(rows.clone()));
    browse_cache.store(&request, &rows)?;
    Ok((rows, BrowseCacheSource::Live))
}

fn cached_browse_many(
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    query_engine: &QueryEngine<'_>,
    requests: Vec<BrowseRequest>,
    browse_stats: &mut BrowseFanoutStats,
) -> Result<Vec<Vec<RollupRow>>> {
    let rows = cached_browse_many_with_source(
        query_cache,
        browse_cache,
        query_engine,
        requests,
        browse_stats,
        &mut BrowseCacheSourceStats::default(),
    )?;
    Ok(rows)
}

fn cached_browse_many_with_source(
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    query_engine: &QueryEngine<'_>,
    requests: Vec<BrowseRequest>,
    browse_stats: &mut BrowseFanoutStats,
    cache_sources: &mut BrowseCacheSourceStats,
) -> Result<Vec<Vec<RollupRow>>> {
    let mut outputs = vec![Vec::new(); requests.len()];
    let mut missed_indexes = Vec::new();
    let mut missed_requests = Vec::new();

    for (index, request) in requests.iter().enumerate() {
        browse_stats.record(request);
        let key = QueryCacheKey {
            domain: QueryCacheDomain::Browse,
            input: serde_json::to_string(request).context("unable to serialize query cache key")?,
        };
        if let Some(cached) = query_cache.cached::<Vec<RollupRow>>(&request.snapshot, &key) {
            query_cache
                .stats
                .entry(QueryCacheDomain::Browse)
                .or_default()
                .hits += 1;
            outputs[index] = cached;
            cache_sources.record(BrowseCacheSource::Memory);
        } else if let Some(rows) = browse_cache.load(request)? {
            query_cache.insert_browse_rows(&request.snapshot, request, rows.clone())?;
            outputs[index] = rows;
            cache_sources.record(BrowseCacheSource::Persisted);
        } else {
            query_cache
                .stats
                .entry(QueryCacheDomain::Browse)
                .or_default()
                .misses += 1;
            missed_indexes.push(index);
            missed_requests.push(request.clone());
            cache_sources.record(BrowseCacheSource::Live);
        }
    }

    if !missed_requests.is_empty() {
        let missed_rows = query_engine.browse_many(&missed_requests)?;
        for ((request_index, request), rows) in missed_indexes
            .into_iter()
            .zip(missed_requests.into_iter())
            .zip(missed_rows.into_iter())
        {
            query_cache.insert_browse_rows(&request.snapshot, &request, rows.clone())?;
            browse_cache.store(&request, &rows)?;
            outputs[request_index] = rows;
        }
    }

    Ok(outputs)
}

fn browse_request_signature(request: &BrowseRequest) -> String {
    serde_json::to_string(request).unwrap_or_else(|error| format!("serialization error: {error:#}"))
}

fn cached_filter_options(
    query_cache: &mut QueryResultCache,
    query_engine: &QueryEngine<'_>,
    snapshot: &SnapshotBounds,
) -> Result<FilterOptions> {
    query_cache.memoize(QueryCacheDomain::FilterOptions, snapshot, &(), || {
        query_engine.filter_options(snapshot)
    })
}

fn cached_snapshot_coverage_summary(
    query_cache: &mut QueryResultCache,
    query_engine: &QueryEngine<'_>,
    snapshot: &SnapshotBounds,
) -> Result<SnapshotCoverageSummary> {
    query_cache.memoize(QueryCacheDomain::SnapshotCoverage, snapshot, &(), || {
        query_engine.snapshot_coverage_summary(snapshot)
    })
}

// The view loader coordinates the snapshot, UI state, async progress, and both
// cache layers in one place; splitting it further would just move the same
// coupled parameters through more wrapper structs.
#[allow(clippy::too_many_arguments)]
fn load_view_for_state(
    query_engine: &QueryEngine<'_>,
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    perf_logger: Option<PerfLogger>,
    snapshot: SnapshotBounds,
    mut ui_state: PersistedUiState,
    selected_key: Option<TreeRowKey>,
    mut progress: Option<&mut dyn ProgressSink>,
) -> Result<LoadedView> {
    let mut perf = PerfScope::new(perf_logger.clone(), "tui.reload_view");
    perf.field("snapshot", &snapshot);
    perf.field("root", ui_state.root);
    perf.field("path", &ui_state.path);
    perf.field("lens", ui_state.lens);
    perf.field("row_filter", &ui_state.row_filter);
    perf.field("selected_key", &selected_key);
    let mut browse_stats = BrowseFanoutStats::default();

    if let Some(progress) = progress.as_deref_mut() {
        progress.step(
            1,
            VIEW_LOAD_PHASE_TOTAL,
            "loading filter options".to_string(),
        );
    }
    let filter_options_started = Instant::now();
    let filter_options = cached_filter_options(query_cache, query_engine, &snapshot)?;
    perf.field(
        "filter_options_ms",
        filter_options_started.elapsed().as_secs_f64() * 1000.0,
    );

    if let Some(progress) = progress.as_deref_mut() {
        progress.step(2, VIEW_LOAD_PHASE_TOTAL, "sanitizing UI state".to_string());
    }
    let sanitize_started = Instant::now();
    sanitize_ui_state(&mut ui_state, &filter_options);
    perf.field(
        "sanitize_ui_state_ms",
        sanitize_started.elapsed().as_secs_f64() * 1000.0,
    );

    if let Some(progress) = progress.as_deref_mut() {
        progress.step(
            3,
            VIEW_LOAD_PHASE_TOTAL,
            "applying active filters".to_string(),
        );
    }
    let filters_started = Instant::now();
    let filters = current_query_filters_for(&ui_state, &snapshot)?;
    perf.field(
        "current_query_filters_ms",
        filters_started.elapsed().as_secs_f64() * 1000.0,
    );
    perf.field("filters", &filters);

    if let Some(progress) = progress.as_deref_mut() {
        progress.step(
            4,
            VIEW_LOAD_PHASE_TOTAL,
            "browsing current path".to_string(),
        );
    }
    let browse_started = Instant::now();
    let (path, raw_rows) = browse_rows_with_parent_fallback(
        query_cache,
        browse_cache,
        query_engine,
        &snapshot,
        &ui_state,
        &filters,
        &mut browse_stats,
    )?;
    perf.field(
        "browse_loop_ms",
        browse_started.elapsed().as_secs_f64() * 1000.0,
    );
    perf.field("raw_row_count", raw_rows.len());
    ui_state.path = path;

    if let Some(progress) = progress.as_deref_mut() {
        progress.step(
            5,
            VIEW_LOAD_PHASE_TOTAL,
            "resolving project root".to_string(),
        );
    }
    let project_root_started = Instant::now();
    let current_project_root = match project_id_from_path(&ui_state.path) {
        Some(project_id) => project_root_for_state(
            query_cache,
            browse_cache,
            query_engine,
            &snapshot,
            ui_state.lens,
            &filters,
            project_id,
            &mut browse_stats,
        )?,
        None => None,
    };
    perf.field(
        "project_root_ms",
        project_root_started.elapsed().as_secs_f64() * 1000.0,
    );

    if let Some(progress) = progress.as_deref_mut() {
        progress.step(6, VIEW_LOAD_PHASE_TOTAL, "building breadcrumbs".to_string());
    }
    let breadcrumb_started = Instant::now();
    let breadcrumb_targets = build_breadcrumb_targets(
        &ui_state.root,
        &ui_state.path,
        &filter_options,
        current_project_root.as_deref(),
    );
    perf.field(
        "breadcrumb_build_ms",
        breadcrumb_started.elapsed().as_secs_f64() * 1000.0,
    );
    perf.field("breadcrumb_count", breadcrumb_targets.len());

    if let Some(progress) = progress.as_deref_mut() {
        progress.step(
            7,
            VIEW_LOAD_PHASE_TOTAL,
            "recomputing map context".to_string(),
        );
    }
    let radial_started = Instant::now();
    let radial_context = build_radial_context_for_state(
        query_cache,
        browse_cache,
        query_engine,
        perf_logger.clone(),
        RadialContextRequest {
            snapshot: &snapshot,
            ui_state: &ui_state,
            active_path: &ui_state.path,
            filters: &filters,
            project_root: current_project_root.as_deref(),
        },
        &mut browse_stats,
    )?;
    perf.field(
        "radial_context_ms",
        radial_started.elapsed().as_secs_f64() * 1000.0,
    );

    if let Some(progress) = progress.as_mut() {
        progress.step(
            8,
            VIEW_LOAD_PHASE_TOTAL,
            "refreshing snapshot coverage".to_string(),
        );
    }
    let coverage_started = Instant::now();
    let snapshot_coverage = cached_snapshot_coverage_summary(query_cache, query_engine, &snapshot)?;
    perf.field(
        "snapshot_coverage_ms",
        coverage_started.elapsed().as_secs_f64() * 1000.0,
    );
    perf.field("browse_request_count", browse_stats.total_requests());
    perf.field(
        "distinct_browse_request_count",
        browse_stats.distinct_request_count(),
    );
    perf.field(
        "duplicate_browse_request_count",
        browse_stats.duplicate_request_count(),
    );
    perf.field(
        "repeated_browse_requests",
        browse_stats.repeated_requests(5),
    );
    perf.finish_ok();

    Ok(LoadedView {
        snapshot,
        snapshot_coverage,
        ui_state,
        filter_options,
        raw_rows,
        breadcrumb_targets,
        radial_context,
        current_project_root,
        selected_key,
    })
}

fn build_jump_targets_for_state(
    query_engine: &QueryEngine<'_>,
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    perf_logger: Option<PerfLogger>,
    snapshot: SnapshotBounds,
    ui_state: PersistedUiState,
    mut progress: Option<&mut dyn ProgressSink>,
) -> Result<Vec<JumpTarget>> {
    if let Some(progress) = progress.as_deref_mut() {
        progress.step(
            1,
            JUMP_TARGET_PHASE_TOTAL,
            "loading current filters".to_string(),
        );
    }
    let filters = current_query_filters_for(&ui_state, &snapshot)?;
    let mut perf = PerfScope::new(perf_logger, "tui.build_jump_targets");
    perf.field("snapshot", &snapshot);
    perf.field("root", ui_state.root);
    perf.field("path", &ui_state.path);
    perf.field("lens", ui_state.lens);
    perf.field("filters", &filters);
    let mut browse_stats = BrowseFanoutStats::default();
    let mut targets = Vec::new();
    let mut project_count = 0usize;
    let mut category_count = 0usize;
    let mut action_count = 0usize;
    let browse_started = Instant::now();

    if let Some(progress) = progress.as_deref_mut() {
        progress.step(
            2,
            JUMP_TARGET_PHASE_TOTAL,
            "walking project hierarchy".to_string(),
        );
    }
    let project_root_rows = cached_browse(
        query_cache,
        browse_cache,
        query_engine,
        BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: ui_state.lens,
            filters: filters.clone(),
            path: BrowsePath::Root,
        },
        &mut browse_stats,
    )?;
    let project_category_requests = project_root_rows
        .iter()
        .filter_map(|project| {
            project.project_id.map(|project_id| BrowseRequest {
                snapshot: snapshot.clone(),
                root: RootView::ProjectHierarchy,
                lens: ui_state.lens,
                filters: filters.clone(),
                path: BrowsePath::Project { project_id },
            })
        })
        .collect::<Vec<_>>();
    let project_category_sets = cached_browse_many(
        query_cache,
        browse_cache,
        query_engine,
        project_category_requests.clone(),
        &mut browse_stats,
    )?;
    let mut project_action_requests = Vec::new();

    for (project, categories) in project_root_rows.iter().zip(project_category_sets.iter()) {
        let Some(project_id) = project.project_id else {
            continue;
        };
        project_count += 1;
        targets.push(JumpTarget {
            label: project.label.clone(),
            detail: "project".to_string(),
            root: RootView::ProjectHierarchy,
            path: BrowsePath::Project { project_id },
        });

        for category in categories {
            let Some(category_name) = category.category.clone() else {
                continue;
            };
            category_count += 1;
            targets.push(JumpTarget {
                label: format!("{} / {}", project.label, category_name),
                detail: "project category".to_string(),
                root: RootView::ProjectHierarchy,
                path: BrowsePath::ProjectCategory {
                    project_id,
                    category: category_name.clone(),
                },
            });
            project_action_requests.push(BrowseRequest {
                snapshot: snapshot.clone(),
                root: RootView::ProjectHierarchy,
                lens: ui_state.lens,
                filters: filters.clone(),
                path: BrowsePath::ProjectCategory {
                    project_id,
                    category: category_name,
                },
            });
        }
    }
    let project_action_sets = cached_browse_many(
        query_cache,
        browse_cache,
        query_engine,
        project_action_requests.clone(),
        &mut browse_stats,
    )?;
    for (request, actions) in project_action_requests
        .iter()
        .zip(project_action_sets.into_iter())
    {
        let BrowsePath::ProjectCategory {
            project_id,
            category,
        } = &request.path
        else {
            continue;
        };
        for action in actions {
            let Some(action_key) = action.action.clone() else {
                continue;
            };
            action_count += 1;
            let project_label = project_root_rows
                .iter()
                .find(|project| project.project_id == Some(*project_id))
                .map(|project| project.label.as_str())
                .unwrap_or("project");
            targets.push(JumpTarget {
                label: format!("{project_label} / {category} / {}", action.label),
                detail: "project action".to_string(),
                root: RootView::ProjectHierarchy,
                path: BrowsePath::ProjectAction {
                    project_id: *project_id,
                    category: category.clone(),
                    action: action_key,
                    parent_path: None,
                },
            });
        }
    }

    if let Some(progress) = progress.as_deref_mut() {
        progress.step(
            3,
            JUMP_TARGET_PHASE_TOTAL,
            "walking category hierarchy".to_string(),
        );
    }
    let category_root_rows = cached_browse(
        query_cache,
        browse_cache,
        query_engine,
        BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::CategoryHierarchy,
            lens: ui_state.lens,
            filters: filters.clone(),
            path: BrowsePath::Root,
        },
        &mut browse_stats,
    )?;
    let category_action_requests = category_root_rows
        .iter()
        .filter_map(|category| {
            category
                .category
                .clone()
                .map(|category_name| BrowseRequest {
                    snapshot: snapshot.clone(),
                    root: RootView::CategoryHierarchy,
                    lens: ui_state.lens,
                    filters: filters.clone(),
                    path: BrowsePath::Category {
                        category: category_name,
                    },
                })
        })
        .collect::<Vec<_>>();
    let category_action_sets = cached_browse_many(
        query_cache,
        browse_cache,
        query_engine,
        category_action_requests.clone(),
        &mut browse_stats,
    )?;
    let mut category_project_requests = Vec::new();

    for (category, actions) in category_root_rows.iter().zip(category_action_sets.iter()) {
        let Some(category_name) = category.category.clone() else {
            continue;
        };
        category_count += 1;
        targets.push(JumpTarget {
            label: category_name.clone(),
            detail: "category".to_string(),
            root: RootView::CategoryHierarchy,
            path: BrowsePath::Category {
                category: category_name.clone(),
            },
        });

        for action in actions {
            let Some(action_key) = action.action.clone() else {
                continue;
            };
            action_count += 1;
            targets.push(JumpTarget {
                label: format!("{} / {}", category_name, action.label),
                detail: "category action".to_string(),
                root: RootView::CategoryHierarchy,
                path: BrowsePath::CategoryAction {
                    category: category_name.clone(),
                    action: action_key.clone(),
                },
            });
            category_project_requests.push(BrowseRequest {
                snapshot: snapshot.clone(),
                root: RootView::CategoryHierarchy,
                lens: ui_state.lens,
                filters: filters.clone(),
                path: BrowsePath::CategoryAction {
                    category: category_name.clone(),
                    action: action_key,
                },
            });
        }
    }
    let category_project_sets = cached_browse_many(
        query_cache,
        browse_cache,
        query_engine,
        category_project_requests.clone(),
        &mut browse_stats,
    )?;
    for (request, projects) in category_project_requests
        .iter()
        .zip(category_project_sets.into_iter())
    {
        let BrowsePath::CategoryAction { category, action } = &request.path else {
            continue;
        };
        for project in projects {
            let Some(project_id) = project.project_id else {
                continue;
            };
            project_count += 1;
            targets.push(JumpTarget {
                label: format!("{} / {} / {}", category, action.label(), project.label),
                detail: "category project".to_string(),
                root: RootView::CategoryHierarchy,
                path: BrowsePath::CategoryActionProject {
                    category: category.clone(),
                    action: project.action.clone().unwrap_or_else(|| action.clone()),
                    project_id,
                    parent_path: None,
                },
            });
        }
    }

    if let Some(progress) = progress.as_mut() {
        progress.step(
            4,
            JUMP_TARGET_PHASE_TOTAL,
            "finalizing jump targets".to_string(),
        );
    }
    perf.field(
        "browse_work_ms",
        browse_started.elapsed().as_secs_f64() * 1000.0,
    );
    perf.field("project_count", project_count);
    perf.field("category_count", category_count);
    perf.field("action_count", action_count);
    perf.field("target_count", targets.len());
    perf.field("browse_request_count", browse_stats.total_requests());
    perf.field(
        "distinct_browse_request_count",
        browse_stats.distinct_request_count(),
    );
    perf.field(
        "duplicate_browse_request_count",
        browse_stats.duplicate_request_count(),
    );
    perf.field(
        "repeated_browse_requests",
        browse_stats.repeated_requests(5),
    );
    perf.finish_ok();

    Ok(targets)
}

fn browse_rows_with_parent_fallback(
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    query_engine: &QueryEngine<'_>,
    snapshot: &SnapshotBounds,
    ui_state: &PersistedUiState,
    filters: &BrowseFilters,
    browse_stats: &mut BrowseFanoutStats,
) -> Result<(BrowsePath, Vec<RollupRow>)> {
    let mut path = ui_state.path.clone();

    loop {
        let rows = cached_browse(
            query_cache,
            browse_cache,
            query_engine,
            BrowseRequest {
                snapshot: snapshot.clone(),
                root: ui_state.root,
                lens: ui_state.lens,
                filters: filters.clone(),
                path: path.clone(),
            },
            browse_stats,
        )?;

        if !rows.is_empty() || matches!(path, BrowsePath::Root) {
            return Ok((path, rows));
        }

        let project_root = match project_id_from_path(&path) {
            Some(project_id) => project_root_for_state(
                query_cache,
                browse_cache,
                query_engine,
                snapshot,
                ui_state.lens,
                filters,
                project_id,
                browse_stats,
            )?,
            None => None,
        };
        let parent = parent_browse_path(&path, project_root.as_deref());
        if parent == path {
            return Ok((path, rows));
        }
        path = parent;
    }
}

struct RadialContextRequest<'a> {
    snapshot: &'a SnapshotBounds,
    ui_state: &'a PersistedUiState,
    active_path: &'a BrowsePath,
    filters: &'a BrowseFilters,
    project_root: Option<&'a str>,
}

fn build_radial_context_for_state(
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    query_engine: &QueryEngine<'_>,
    perf_logger: Option<PerfLogger>,
    request: RadialContextRequest<'_>,
    browse_stats: &mut BrowseFanoutStats,
) -> Result<RadialContext> {
    let mut perf = PerfScope::new_verbose(perf_logger, "tui.build_radial_context");
    perf.field("snapshot", request.snapshot);
    perf.field("root", request.ui_state.root);
    perf.field("path", request.active_path);
    perf.field("lens", request.ui_state.lens);
    perf.field("filters", request.filters);
    let steps = radial_ancestor_steps(
        &request.ui_state.root,
        request.active_path,
        request.project_root,
    );
    let mut ancestor_layers = Vec::new();
    let mut current_span = RadialSpan::full();
    let browse_started = Instant::now();

    for step in steps {
        let rows = cached_browse(
            query_cache,
            browse_cache,
            query_engine,
            BrowseRequest {
                snapshot: request.snapshot.clone(),
                root: request.ui_state.root,
                lens: request.ui_state.lens,
                filters: request.filters.clone(),
                path: step.query_path,
            },
            browse_stats,
        )?;
        let layer = build_sunburst_layer(
            &rows,
            request.ui_state.lens,
            Some(&step.selected_child),
            current_span,
        );
        current_span = radial_selected_child_span(&layer);
        ancestor_layers.push(layer);
    }

    perf.field(
        "browse_work_ms",
        browse_started.elapsed().as_secs_f64() * 1000.0,
    );
    perf.field("ancestor_layer_count", ancestor_layers.len());
    perf.field("browse_request_count", browse_stats.total_requests());
    perf.field(
        "distinct_browse_request_count",
        browse_stats.distinct_request_count(),
    );
    perf.field(
        "duplicate_browse_request_count",
        browse_stats.duplicate_request_count(),
    );
    perf.field(
        "repeated_browse_requests",
        browse_stats.repeated_requests(5),
    );
    perf.finish_ok();

    let descendant_layers = build_radial_descendant_layers(
        query_cache,
        browse_cache,
        query_engine,
        &request,
        current_span,
        browse_stats,
    )
    .unwrap_or_default();

    Ok(RadialContext {
        ancestor_layers,
        current_span,
        descendant_layers,
    })
}

fn build_radial_descendant_layers(
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    query_engine: &QueryEngine<'_>,
    request: &RadialContextRequest<'_>,
    initial_span: RadialSpan,
    browse_stats: &mut BrowseFanoutStats,
) -> Result<Vec<RadialLayer>> {
    if matches!(request.active_path, BrowsePath::Root) {
        return Ok(Vec::new());
    }

    let rows = cached_browse(
        query_cache,
        browse_cache,
        query_engine,
        BrowseRequest {
            snapshot: request.snapshot.clone(),
            root: request.ui_state.root,
            lens: request.ui_state.lens,
            filters: request.filters.clone(),
            path: request.active_path.clone(),
        },
        browse_stats,
    )?;

    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let layer = build_radial_layer(&rows, request.ui_state.lens, None, initial_span);
    Ok(vec![layer])
}

// Project-root lookup needs the live query engine, both cache layers, and the
// active browse filters together because it reuses the exact interactive browse
// path rather than maintaining a second lookup mechanism.
#[allow(clippy::too_many_arguments)]
fn project_root_for_state(
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    query_engine: &QueryEngine<'_>,
    snapshot: &SnapshotBounds,
    lens: MetricLens,
    filters: &BrowseFilters,
    project_id: i64,
    browse_stats: &mut BrowseFanoutStats,
) -> Result<Option<String>> {
    let relaxed_filters = BrowseFilters {
        time_window: filters.time_window.clone(),
        model: filters.model.clone(),
        project_id: None,
        action_category: None,
        action: None,
    };
    let root_rows = cached_browse(
        query_cache,
        browse_cache,
        query_engine,
        BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens,
            filters: relaxed_filters,
            path: BrowsePath::Root,
        },
        browse_stats,
    )?;

    Ok(root_rows
        .into_iter()
        .find(|row| row.project_id == Some(project_id))
        .and_then(|row| row.full_path))
}

#[allow(clippy::too_many_arguments)]
fn project_root_for_state_with_source(
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    query_engine: &QueryEngine<'_>,
    snapshot: &SnapshotBounds,
    lens: MetricLens,
    filters: &BrowseFilters,
    project_id: i64,
    browse_stats: &mut BrowseFanoutStats,
    cache_sources: &mut BrowseCacheSourceStats,
) -> Result<Option<String>> {
    let relaxed_filters = BrowseFilters {
        time_window: filters.time_window.clone(),
        model: filters.model.clone(),
        project_id: None,
        action_category: None,
        action: None,
    };
    let (root_rows, source) = cached_browse_with_source(
        query_cache,
        browse_cache,
        query_engine,
        BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens,
            filters: relaxed_filters,
            path: BrowsePath::Root,
        },
        browse_stats,
    )?;
    cache_sources.record(source);

    Ok(root_rows
        .into_iter()
        .find(|row| row.project_id == Some(project_id))
        .and_then(|row| row.full_path))
}

fn build_radial_context_for_state_with_source(
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    query_engine: &QueryEngine<'_>,
    perf_logger: Option<PerfLogger>,
    request: RadialContextRequest<'_>,
    browse_stats: &mut BrowseFanoutStats,
    cache_sources: &mut BrowseCacheSourceStats,
) -> Result<RadialContext> {
    let mut perf = PerfScope::new_verbose(perf_logger, "tui.build_radial_context");
    perf.field("snapshot", request.snapshot);
    perf.field("root", request.ui_state.root);
    perf.field("path", request.active_path);
    perf.field("lens", request.ui_state.lens);
    perf.field("filters", request.filters);
    let steps = radial_ancestor_steps(
        &request.ui_state.root,
        request.active_path,
        request.project_root,
    );
    let mut ancestor_layers = Vec::new();
    let mut current_span = RadialSpan::full();
    let browse_started = Instant::now();

    for step in steps {
        let (rows, source) = cached_browse_with_source(
            query_cache,
            browse_cache,
            query_engine,
            BrowseRequest {
                snapshot: request.snapshot.clone(),
                root: request.ui_state.root,
                lens: request.ui_state.lens,
                filters: request.filters.clone(),
                path: step.query_path,
            },
            browse_stats,
        )?;
        cache_sources.record(source);
        let layer = build_sunburst_layer(
            &rows,
            request.ui_state.lens,
            Some(&step.selected_child),
            current_span,
        );
        current_span = radial_selected_child_span(&layer);
        ancestor_layers.push(layer);
    }

    perf.field(
        "browse_work_ms",
        browse_started.elapsed().as_secs_f64() * 1000.0,
    );
    perf.field("ancestor_layer_count", ancestor_layers.len());
    perf.field("browse_request_count", browse_stats.total_requests());
    perf.field(
        "distinct_browse_request_count",
        browse_stats.distinct_request_count(),
    );
    perf.field(
        "duplicate_browse_request_count",
        browse_stats.duplicate_request_count(),
    );
    perf.field(
        "repeated_browse_requests",
        browse_stats.repeated_requests(5),
    );
    perf.field("cache_memory_hit_count", cache_sources.memory_hits);
    perf.field("cache_persisted_hit_count", cache_sources.persisted_hits);
    perf.field("cache_live_query_count", cache_sources.live_queries);
    perf.finish_ok();

    let descendant_layers = build_radial_descendant_layers_with_source(
        query_cache,
        browse_cache,
        query_engine,
        &request,
        current_span,
        browse_stats,
        cache_sources,
    )
    .unwrap_or_default();

    Ok(RadialContext {
        ancestor_layers,
        current_span,
        descendant_layers,
    })
}

fn build_radial_descendant_layers_with_source(
    query_cache: &mut QueryResultCache,
    browse_cache: &mut BrowseCacheStore,
    query_engine: &QueryEngine<'_>,
    request: &RadialContextRequest<'_>,
    initial_span: RadialSpan,
    browse_stats: &mut BrowseFanoutStats,
    cache_sources: &mut BrowseCacheSourceStats,
) -> Result<Vec<RadialLayer>> {
    if matches!(request.active_path, BrowsePath::Root) {
        return Ok(Vec::new());
    }

    let (rows, source) = cached_browse_with_source(
        query_cache,
        browse_cache,
        query_engine,
        BrowseRequest {
            snapshot: request.snapshot.clone(),
            root: request.ui_state.root,
            lens: request.ui_state.lens,
            filters: request.filters.clone(),
            path: request.active_path.clone(),
        },
        browse_stats,
    )?;
    cache_sources.record(source);

    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let layer = build_radial_layer(&rows, request.ui_state.lens, None, initial_span);
    Ok(vec![layer])
}

fn current_query_filters_for(
    ui_state: &PersistedUiState,
    snapshot: &SnapshotBounds,
) -> Result<BrowseFilters> {
    Ok(BrowseFilters {
        time_window: ui_state.time_window.to_filter(snapshot)?,
        model: ui_state.model.clone(),
        project_id: ui_state.project_id,
        action_category: ui_state.action_category.clone(),
        action: ui_state.action.clone(),
    })
}

fn sanitize_ui_state(ui_state: &mut PersistedUiState, filter_options: &FilterOptions) {
    if !matches_path_root(&ui_state.root, &ui_state.path) {
        ui_state.path = BrowsePath::Root;
    }

    if ui_state.model.as_ref().is_some_and(|model| {
        !filter_options
            .models
            .iter()
            .any(|candidate| candidate == model)
    }) {
        ui_state.model = None;
    }

    if ui_state.project_id.is_some_and(|project_id| {
        !filter_options
            .projects
            .iter()
            .any(|project| project.id == project_id)
    }) {
        ui_state.project_id = None;
    }

    if ui_state.action_category.as_ref().is_some_and(|category| {
        !filter_options
            .categories
            .iter()
            .any(|candidate| candidate == category)
    }) {
        ui_state.action_category = None;
    }

    if ui_state.action.as_ref().is_some_and(|action| {
        !filter_options.actions.iter().any(|option| {
            option.action == *action
                && ui_state
                    .action_category
                    .as_ref()
                    .is_none_or(|category| option.category == *category)
        })
    }) {
        ui_state.action = None;
    }

    normalize_columns(&mut ui_state.enabled_columns);
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;

        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;

        Ok(Self { terminal })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedUiState {
    root: RootView,
    path: BrowsePath,
    lens: MetricLens,
    pane_mode: PaneMode,
    time_window: TimeWindowPreset,
    model: Option<String>,
    project_id: Option<i64>,
    action_category: Option<String>,
    action: Option<ActionKey>,
    row_filter: String,
    enabled_columns: Vec<OptionalColumn>,
    #[serde(default)]
    opportunity_filter: Option<OpportunityCategory>,
}

impl Default for PersistedUiState {
    fn default() -> Self {
        Self {
            root: RootView::ProjectHierarchy,
            path: BrowsePath::Root,
            lens: MetricLens::UncachedInput,
            pane_mode: PaneMode::Table,
            time_window: TimeWindowPreset::All,
            model: None,
            project_id: None,
            action_category: None,
            action: None,
            row_filter: String::new(),
            enabled_columns: default_enabled_columns(),
            opportunity_filter: None,
        }
    }
}

impl PersistedUiState {
    fn load(path: &Path) -> Result<Option<Self>> {
        match fs::read_to_string(path) {
            Ok(raw) => {
                let mut state: Self = serde_json::from_str(&raw)
                    .with_context(|| format!("unable to parse {}", path.display()))?;
                normalize_columns(&mut state.enabled_columns);
                Ok(Some(state))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).with_context(|| format!("unable to read {}", path.display())),
        }
    }

    fn save(&self, path: &Path) -> Result<()> {
        let serialized = serde_json::to_string_pretty(self)
            .context("unable to serialize persisted TUI state")?;
        fs::write(path, serialized).with_context(|| format!("unable to write {}", path.display()))
    }

    fn apply_startup_browse_state(&mut self, startup_browse_state: Option<StartupBrowseState>) {
        match startup_browse_state {
            Some(startup_browse_state) => {
                self.root = startup_browse_state.root;
                self.path = startup_browse_state.path;
            }
            None => {
                self.path = BrowsePath::Root;
            }
        }

        self.clear_scoped_filters();
    }

    fn clear_filters(&mut self) {
        self.time_window = TimeWindowPreset::All;
        self.model = None;
        self.clear_scoped_filters();
        self.row_filter.clear();
        self.opportunity_filter = None;
    }

    fn clear_scoped_filters(&mut self) {
        self.project_id = None;
        self.action_category = None;
        self.action = None;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum PaneMode {
    Table,
    #[serde(alias = "Details")]
    Radial,
}

impl PaneMode {
    fn from_focus(focus: PaneFocus) -> Self {
        match focus {
            PaneFocus::Table => Self::Table,
            PaneFocus::Radial => Self::Radial,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PaneFocus {
    Table,
    Radial,
}

impl PaneFocus {
    fn toggle(self) -> Self {
        match self {
            Self::Table => Self::Radial,
            Self::Radial => Self::Table,
        }
    }

    fn from_pane_mode(mode: PaneMode) -> Self {
        match mode {
            PaneMode::Table => Self::Table,
            PaneMode::Radial => Self::Radial,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RadialContext {
    ancestor_layers: Vec<RadialLayer>,
    current_span: RadialSpan,
    descendant_layers: Vec<RadialLayer>,
}

type RadialModel = SunburstModel;
type RadialLayer = SunburstLayer;
type RadialSpan = SunburstSpan;

#[cfg(test)]
type RadialCenter = SunburstCenter;

#[cfg(test)]
type RadialSegment = SunburstSegment;

#[cfg(test)]
type RadialBucket = SunburstBucket;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RadialAncestorStep {
    query_path: BrowsePath,
    selected_child: String,
}

pub(crate) fn pane_block(title: &str, focused: bool) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_style(pane_border_style(focused))
        .title(pane_title(title, focused))
}

fn pane_border_style(focused: bool) -> Style {
    if focused {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Gray).add_modifier(Modifier::DIM)
    }
}

fn pane_title(title: &str, focused: bool) -> Line<'static> {
    let label = if focused {
        title.to_uppercase()
    } else {
        title.to_string()
    };

    let prefix = if focused { "◆" } else { "◦" };
    Line::from(vec![
        Span::styled(format!("{prefix} "), pane_title_style(focused)),
        Span::styled(label, pane_title_style(focused)),
    ])
}

fn pane_title_style(focused: bool) -> Style {
    if focused {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::DIM)
    }
}

fn table_header_style(focused: bool) -> Style {
    if focused {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Gray).add_modifier(Modifier::DIM)
    }
}

fn table_row_highlight_style(focused: bool) -> Style {
    if focused {
        Style::default()
            .bg(Color::DarkGray)
            .fg(Color::White)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().bg(Color::DarkGray).fg(Color::Gray)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum TimeWindowPreset {
    All,
    Last5Hours,
    Last24Hours,
    LastWeek,
}

impl TimeWindowPreset {
    fn next(self) -> Self {
        match self {
            Self::All => Self::Last5Hours,
            Self::Last5Hours => Self::Last24Hours,
            Self::Last24Hours => Self::LastWeek,
            Self::LastWeek => Self::All,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Last5Hours => "last 5h",
            Self::Last24Hours => "last 24h",
            Self::LastWeek => "last 1w",
        }
    }

    fn to_filter(self, snapshot: &SnapshotBounds) -> Result<Option<TimeWindowFilter>> {
        let Some(upper_bound) = snapshot.upper_bound_utc.as_deref() else {
            return Ok(None);
        };
        let upper_bound = snapshot
            .upper_bound_timestamp()?
            .with_context(|| format!("unable to parse snapshot upper bound {upper_bound}"))?;

        let start = match self {
            Self::All => return Ok(None),
            Self::Last5Hours => upper_bound.checked_sub(5.hours()).ok(),
            Self::Last24Hours => upper_bound.checked_sub(24.hours()).ok(),
            Self::LastWeek => upper_bound.checked_sub(168.hours()).ok(),
        };

        Ok(start.map(|start| TimeWindowFilter {
            start_at_utc: Some(start.to_string()),
            end_at_utc: None,
        }))
    }
}

fn snapshot_summary_text(snapshot: &SnapshotBounds, has_newer_snapshot: bool) -> String {
    if snapshot.is_bootstrap() {
        return if has_newer_snapshot {
            "snapshot: no imported data is visible yet (newer data is ready)".to_string()
        } else {
            "snapshot: no imported data is visible yet".to_string()
        };
    }

    if has_newer_snapshot {
        format!(
            "snapshot: showing imported data {} (newer data is ready)",
            snapshot_coverage_tail(snapshot)
        )
    } else {
        format!(
            "snapshot: showing imported data {}",
            snapshot_coverage_tail(snapshot)
        )
    }
}

fn view_line(targets: &[BreadcrumbTarget]) -> Line<'static> {
    if targets.is_empty() {
        return Line::from("view: none");
    }

    let mut spans = vec![Span::styled(
        "view: ",
        Style::default().add_modifier(Modifier::BOLD),
    )];
    for (index, target) in targets.iter().enumerate() {
        if index > 0 {
            spans.push(Span::styled(" > ", Style::default().fg(Color::DarkGray)));
        }

        let style = if index + 1 == targets.len() {
            Style::default().add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        spans.push(Span::styled(target.label.clone(), style));
    }

    Line::from(spans)
}

fn snapshot_coverage_tail(snapshot: &SnapshotBounds) -> String {
    match snapshot.upper_bound_timestamp().ok().flatten() {
        Some(timestamp) => format!("through {timestamp}"),
        None => snapshot
            .upper_bound_utc
            .as_deref()
            .map(|upper_bound| format!("through {upper_bound}"))
            .unwrap_or_else(|| "with an unknown upper bound".to_string()),
    }
}

fn snapshot_refresh_text(
    snapshot: &SnapshotBounds,
    _latest_snapshot: &SnapshotBounds,
    startup_open_reason: StartupOpenReason,
    has_newer_snapshot: bool,
) -> String {
    if has_newer_snapshot {
        return if snapshot.is_bootstrap() {
            "refresh: imported data is ready. Press r to load it; this empty view stays in place until you refresh."
                .to_string()
        } else {
            "refresh: newer imported data is ready. Press r to switch to it; this view stays pinned until you refresh."
                .to_string()
        };
    }

    match startup_open_reason {
        StartupOpenReason::Last24hReady => {
            "refresh: manual only. Background import never changes the visible view until you press r."
                .to_string()
        }
        StartupOpenReason::TimedOut => {
            "refresh: manual only. Startup opened before the last 24 hours finished importing, so this snapshot may still be partial."
                .to_string()
        }
        StartupOpenReason::FullImportReady => {
            "refresh: manual only. Startup waited for the full import to finish, so this snapshot is already current."
                .to_string()
        }
    }
}

fn snapshot_coverage_footer_text(summary: &SnapshotCoverageSummary) -> String {
    if summary == &SnapshotCoverageSummary::default() {
        return "coverage: no imported data is visible yet".to_string();
    }

    format!(
        "coverage: {}, {} across {}, {}, {}",
        count_label(summary.project_count, "project"),
        count_label(summary.project_day_count, "project-day"),
        count_label(summary.day_count, "day"),
        count_label(summary.session_count, "session"),
        count_label(summary.turn_count, "turn"),
    )
}

fn count_label(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("1 {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

fn format_storage_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

    let bytes_f64 = bytes as f64;
    if bytes_f64 >= GIB {
        format!("{:.1} GiB", bytes_f64 / GIB)
    } else if bytes_f64 >= MIB {
        format!("{:.1} MiB", bytes_f64 / MIB)
    } else if bytes_f64 >= KIB {
        format!("{:.1} KiB", bytes_f64 / KIB)
    } else {
        format!("{bytes} B")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputMode {
    Normal,
    FilterInput,
    JumpInput,
    BreadcrumbPicker,
    ColumnChooser,
    BrowseCacheMenu,
    BrowseCacheConfirm,
    DatabaseMenu,
    DatabaseConfirm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfirmationAction {
    ClearBrowseCache,
    RebuildBrowseCache,
    RebuildDatabase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum OptionalColumn {
    Kind,
    GrossInput,
    Output,
    Total,
    Last5Hours,
    LastWeek,
    UncachedReference,
    Items,
    TopOpportunity,
    OppScore,
    Confidence,
    OppSessionSetup,
    OppTaskSetup,
    OppHistoryDrag,
    OppDelegation,
    OppModelMismatch,
    OppPromptYield,
    OppSearchChurn,
    OppToolResultBloat,
}

impl OptionalColumn {
    fn priority(self) -> usize {
        match self {
            Self::Kind => 0,
            Self::Last5Hours => 1,
            Self::LastWeek => 2,
            Self::UncachedReference => 3,
            Self::GrossInput => 4,
            Self::Output => 5,
            Self::Total => 6,
            Self::Items => 7,
            Self::TopOpportunity => 8,
            Self::OppScore => 9,
            Self::Confidence => 10,
            Self::OppSessionSetup => 11,
            Self::OppTaskSetup => 12,
            Self::OppHistoryDrag => 13,
            Self::OppDelegation => 14,
            Self::OppModelMismatch => 15,
            Self::OppPromptYield => 16,
            Self::OppSearchChurn => 17,
            Self::OppToolResultBloat => 18,
        }
    }

    fn short_label(self) -> &'static str {
        match self {
            Self::Kind => "kind",
            Self::GrossInput => "all input",
            Self::Output => "output",
            Self::Total => "total",
            Self::Last5Hours => "5h",
            Self::LastWeek => "1w",
            Self::UncachedReference => "ref",
            Self::Items => "items",
            Self::TopOpportunity => "top",
            Self::OppScore => "score",
            Self::Confidence => "conf",
            Self::OppSessionSetup => "sess",
            Self::OppTaskSetup => "task",
            Self::OppHistoryDrag => "hist",
            Self::OppDelegation => "deleg",
            Self::OppModelMismatch => "model",
            Self::OppPromptYield => "yield",
            Self::OppSearchChurn => "srch",
            Self::OppToolResultBloat => "bloat",
        }
    }
}

#[derive(Debug, Default)]
struct JumpState {
    query: String,
    matches: Vec<JumpTarget>,
    selected: usize,
}

#[derive(Debug, Clone)]
struct JumpTarget {
    label: String,
    detail: String,
    root: RootView,
    path: BrowsePath,
}

#[derive(Debug, Clone)]
struct BreadcrumbTarget {
    label: String,
    display: String,
    root: RootView,
    path: BrowsePath,
}

#[derive(Debug, Default)]
struct BreadcrumbPickerState {
    selected: usize,
}

#[derive(Debug, Clone)]
struct StatusMessage {
    text: String,
    tone: StatusTone,
}

impl StatusMessage {
    fn info(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            tone: StatusTone::Info,
        }
    }

    fn error(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            tone: StatusTone::Error,
        }
    }
}

fn compact_status_text(text: impl AsRef<str>) -> String {
    text.as_ref()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Debug, Clone, Copy)]
enum StatusTone {
    Info,
    Error,
}

fn status_tone_style(tone: StatusTone) -> Style {
    match tone {
        StatusTone::Info => Style::default().fg(Color::Cyan),
        StatusTone::Error => Style::default().fg(Color::Red),
    }
}

#[derive(Debug, Clone)]
struct ColumnSpec {
    key: ColumnKey,
    title: String,
    constraint: Constraint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnKey {
    Label,
    SelectedLens,
    Optional(OptionalColumn),
}

fn default_enabled_columns() -> Vec<OptionalColumn> {
    vec![
        OptionalColumn::Kind,
        OptionalColumn::Last5Hours,
        OptionalColumn::LastWeek,
        OptionalColumn::UncachedReference,
        OptionalColumn::GrossInput,
        OptionalColumn::Output,
        OptionalColumn::Items,
    ]
}

fn normalize_columns(columns: &mut Vec<OptionalColumn>) {
    columns.sort_by_key(|column| column.priority());
    columns.dedup();
}

fn toggle_column(columns: &mut Vec<OptionalColumn>, column: OptionalColumn) {
    if let Some(index) = columns.iter().position(|candidate| *candidate == column) {
        columns.remove(index);
    } else {
        columns.push(column);
        normalize_columns(columns);
    }
}

fn toggle_mark(columns: &[OptionalColumn], column: OptionalColumn) -> &'static str {
    if columns.contains(&column) {
        "☑"
    } else {
        "☐"
    }
}

fn enabled_column_summary(columns: &[OptionalColumn]) -> String {
    if columns.is_empty() {
        return "(none)".to_string();
    }

    columns
        .iter()
        .map(|column| column.short_label())
        .collect::<Vec<_>>()
        .join(", ")
}

fn active_columns(
    width: u16,
    lens: MetricLens,
    enabled_columns: &[OptionalColumn],
) -> Vec<ColumnSpec> {
    let mut columns = vec![
        ColumnSpec {
            key: ColumnKey::Label,
            title: "label".to_string(),
            constraint: Constraint::Min(24),
        },
        ColumnSpec {
            key: ColumnKey::SelectedLens,
            title: metric_lens_label(lens).to_string(),
            constraint: Constraint::Length(11),
        },
    ];

    let mut used_width = 35;
    let optional_specs = enabled_columns
        .iter()
        .map(optional_column_spec)
        .collect::<Vec<_>>();

    for spec in optional_specs {
        let column_width = match spec.constraint {
            Constraint::Length(length) => length,
            _ => 10,
        };
        if used_width + column_width + 1 > width {
            continue;
        }
        used_width += column_width + 1;
        columns.push(spec);
    }

    columns
}

fn optional_column_spec(column: &OptionalColumn) -> ColumnSpec {
    match column {
        OptionalColumn::Kind => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::Kind),
            title: "kind".to_string(),
            constraint: Constraint::Length(9),
        },
        OptionalColumn::GrossInput => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::GrossInput),
            title: "all input".to_string(),
            constraint: Constraint::Length(10),
        },
        OptionalColumn::Output => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::Output),
            title: "output".to_string(),
            constraint: Constraint::Length(10),
        },
        OptionalColumn::Total => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::Total),
            title: "total".to_string(),
            constraint: Constraint::Length(10),
        },
        OptionalColumn::Last5Hours => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::Last5Hours),
            title: "5h".to_string(),
            constraint: Constraint::Length(8),
        },
        OptionalColumn::LastWeek => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::LastWeek),
            title: "1w".to_string(),
            constraint: Constraint::Length(8),
        },
        OptionalColumn::UncachedReference => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::UncachedReference),
            title: "ref".to_string(),
            constraint: Constraint::Length(10),
        },
        OptionalColumn::Items => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::Items),
            title: "items".to_string(),
            constraint: Constraint::Length(7),
        },
        OptionalColumn::TopOpportunity => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::TopOpportunity),
            title: "top opp".to_string(),
            constraint: Constraint::Length(18),
        },
        OptionalColumn::OppScore => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::OppScore),
            title: "opp score".to_string(),
            constraint: Constraint::Length(10),
        },
        OptionalColumn::Confidence => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::Confidence),
            title: "conf".to_string(),
            constraint: Constraint::Length(8),
        },
        OptionalColumn::OppSessionSetup => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::OppSessionSetup),
            title: "sess".to_string(),
            constraint: Constraint::Length(8),
        },
        OptionalColumn::OppTaskSetup => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::OppTaskSetup),
            title: "task".to_string(),
            constraint: Constraint::Length(8),
        },
        OptionalColumn::OppHistoryDrag => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::OppHistoryDrag),
            title: "hist".to_string(),
            constraint: Constraint::Length(8),
        },
        OptionalColumn::OppDelegation => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::OppDelegation),
            title: "deleg".to_string(),
            constraint: Constraint::Length(8),
        },
        OptionalColumn::OppModelMismatch => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::OppModelMismatch),
            title: "model".to_string(),
            constraint: Constraint::Length(8),
        },
        OptionalColumn::OppPromptYield => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::OppPromptYield),
            title: "yield".to_string(),
            constraint: Constraint::Length(8),
        },
        OptionalColumn::OppSearchChurn => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::OppSearchChurn),
            title: "srch".to_string(),
            constraint: Constraint::Length(8),
        },
        OptionalColumn::OppToolResultBloat => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::OppToolResultBloat),
            title: "bloat".to_string(),
            constraint: Constraint::Length(8),
        },
    }
}

fn render_column_value(column: ColumnKey, row: &RollupRow, lens: MetricLens) -> String {
    match column {
        ColumnKey::Label => row.label.clone(),
        ColumnKey::SelectedLens => format_metric(row.metrics.lens_value(lens)),
        ColumnKey::Optional(OptionalColumn::Kind) => row_kind_label(row.kind).to_string(),
        ColumnKey::Optional(OptionalColumn::GrossInput) => format_metric(row.metrics.gross_input),
        ColumnKey::Optional(OptionalColumn::Output) => format_metric(row.metrics.output),
        ColumnKey::Optional(OptionalColumn::Total) => format_metric(row.metrics.total),
        ColumnKey::Optional(OptionalColumn::Last5Hours) => {
            format_metric(row.indicators.selected_lens_last_5_hours)
        }
        ColumnKey::Optional(OptionalColumn::LastWeek) => {
            format_metric(row.indicators.selected_lens_last_week)
        }
        ColumnKey::Optional(OptionalColumn::UncachedReference) => {
            format_metric(row.indicators.uncached_input_reference)
        }
        ColumnKey::Optional(OptionalColumn::Items) => row.item_count.to_string(),
        ColumnKey::Optional(OptionalColumn::TopOpportunity) => row
            .opportunities
            .top_category
            .map(opportunity_category_label)
            .unwrap_or("")
            .to_string(),
        ColumnKey::Optional(OptionalColumn::OppScore) => {
            if row.opportunities.total_score > 0.0 {
                format!("{:.1}", row.opportunities.total_score)
            } else {
                String::new()
            }
        }
        ColumnKey::Optional(OptionalColumn::Confidence) => row
            .opportunities
            .top_confidence
            .map(opportunity_confidence_label)
            .unwrap_or("")
            .to_string(),
        ColumnKey::Optional(OptionalColumn::OppSessionSetup) => {
            format_category_score(&row.opportunities, OpportunityCategory::SessionSetup)
        }
        ColumnKey::Optional(OptionalColumn::OppTaskSetup) => {
            format_category_score(&row.opportunities, OpportunityCategory::TaskSetup)
        }
        ColumnKey::Optional(OptionalColumn::OppHistoryDrag) => {
            format_category_score(&row.opportunities, OpportunityCategory::HistoryDrag)
        }
        ColumnKey::Optional(OptionalColumn::OppDelegation) => {
            format_category_score(&row.opportunities, OpportunityCategory::Delegation)
        }
        ColumnKey::Optional(OptionalColumn::OppModelMismatch) => {
            format_category_score(&row.opportunities, OpportunityCategory::ModelMismatch)
        }
        ColumnKey::Optional(OptionalColumn::OppPromptYield) => {
            format_category_score(&row.opportunities, OpportunityCategory::PromptYield)
        }
        ColumnKey::Optional(OptionalColumn::OppSearchChurn) => {
            format_category_score(&row.opportunities, OpportunityCategory::SearchChurn)
        }
        ColumnKey::Optional(OptionalColumn::OppToolResultBloat) => {
            format_category_score(&row.opportunities, OpportunityCategory::ToolResultBloat)
        }
    }
}

fn format_category_score(
    summary: &gnomon_core::opportunity::OpportunitySummary,
    category: OpportunityCategory,
) -> String {
    summary
        .annotations
        .iter()
        .find(|ann| ann.category == category)
        .map(|ann| {
            if ann.score > 0.0 {
                format!("{:.1}", ann.score)
            } else {
                String::new()
            }
        })
        .unwrap_or_default()
}

fn render_tree_label(row: &TreeRow) -> String {
    let indent = "  ".repeat(row.depth);
    let glyph = match (row.is_expandable(), row.is_expanded) {
        (true, true) => "▾ ",
        (true, false) => "▸ ",
        (false, _) => "  ",
    };
    format!("{indent}{glyph}{}", row.row.label)
}

#[cfg(test)]
fn drillability_glyph(root: &RootView, current_path: &BrowsePath, row: &RollupRow) -> &'static str {
    if next_browse_path(root, current_path, row).is_some() {
        "▸ "
    } else {
        "  "
    }
}

pub(crate) fn metric_lens_label(lens: MetricLens) -> &'static str {
    match lens {
        MetricLens::UncachedInput => "uncached",
        MetricLens::GrossInput => "all input",
        MetricLens::Output => "output",
        MetricLens::Total => "total",
    }
}

fn next_metric_lens(lens: MetricLens) -> MetricLens {
    match lens {
        MetricLens::UncachedInput => MetricLens::GrossInput,
        MetricLens::GrossInput => MetricLens::Output,
        MetricLens::Output => MetricLens::Total,
        MetricLens::Total => MetricLens::UncachedInput,
    }
}

pub(crate) fn row_kind_label(kind: RollupRowKind) -> &'static str {
    match kind {
        RollupRowKind::Project => "project",
        RollupRowKind::ActionCategory => "category",
        RollupRowKind::Action => "action",
        RollupRowKind::Directory => "dir",
        RollupRowKind::File => "file",
    }
}

fn build_radial_model(
    context: &RadialContext,
    visible_rows: &[RollupRow],
    selected_row: Option<&RollupRow>,
    root: &RootView,
    path: &BrowsePath,
    filter_options: &FilterOptions,
    lens: MetricLens,
) -> RadialModel {
    build_sunburst_model(
        context.ancestor_layers.clone(),
        context.current_span,
        visible_rows,
        selected_row,
        &context.descendant_layers,
        build_sunburst_scope_label(root, path, filter_options),
        lens,
    )
}

fn build_radial_layer(
    rows: &[RollupRow],
    lens: MetricLens,
    selected_key: Option<&str>,
    span: RadialSpan,
) -> RadialLayer {
    build_sunburst_layer(rows, lens, selected_key, span)
}

fn radial_ancestor_steps(
    root: &RootView,
    path: &BrowsePath,
    project_root: Option<&str>,
) -> Vec<RadialAncestorStep> {
    match (root, path) {
        (_, BrowsePath::Root) => Vec::new(),
        (RootView::ProjectHierarchy, BrowsePath::Project { project_id }) => {
            vec![RadialAncestorStep {
                query_path: BrowsePath::Root,
                selected_child: project_row_key(*project_id),
            }]
        }
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectCategory {
                project_id,
                category,
            },
        ) => vec![
            RadialAncestorStep {
                query_path: BrowsePath::Root,
                selected_child: project_row_key(*project_id),
            },
            RadialAncestorStep {
                query_path: BrowsePath::Project {
                    project_id: *project_id,
                },
                selected_child: category_row_key(category),
            },
        ],
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectAction {
                project_id,
                category,
                action,
                parent_path,
            },
        ) => {
            let mut steps = vec![
                RadialAncestorStep {
                    query_path: BrowsePath::Root,
                    selected_child: project_row_key(*project_id),
                },
                RadialAncestorStep {
                    query_path: BrowsePath::Project {
                        project_id: *project_id,
                    },
                    selected_child: category_row_key(category),
                },
                RadialAncestorStep {
                    query_path: BrowsePath::ProjectCategory {
                        project_id: *project_id,
                        category: category.clone(),
                    },
                    selected_child: action_row_key(action),
                },
            ];
            steps.extend(project_path_ancestor_steps(
                *project_id,
                category,
                action,
                parent_path.as_deref(),
                project_root,
            ));
            steps
        }
        (RootView::CategoryHierarchy, BrowsePath::Category { category }) => {
            vec![RadialAncestorStep {
                query_path: BrowsePath::Root,
                selected_child: category_row_key(category),
            }]
        }
        (RootView::CategoryHierarchy, BrowsePath::CategoryAction { category, action }) => vec![
            RadialAncestorStep {
                query_path: BrowsePath::Root,
                selected_child: category_row_key(category),
            },
            RadialAncestorStep {
                query_path: BrowsePath::Category {
                    category: category.clone(),
                },
                selected_child: action_row_key(action),
            },
        ],
        (
            RootView::CategoryHierarchy,
            BrowsePath::CategoryActionProject {
                category,
                action,
                project_id,
                parent_path,
            },
        ) => {
            let mut steps = vec![
                RadialAncestorStep {
                    query_path: BrowsePath::Root,
                    selected_child: category_row_key(category),
                },
                RadialAncestorStep {
                    query_path: BrowsePath::Category {
                        category: category.clone(),
                    },
                    selected_child: action_row_key(action),
                },
                RadialAncestorStep {
                    query_path: BrowsePath::CategoryAction {
                        category: category.clone(),
                        action: action.clone(),
                    },
                    selected_child: project_row_key(*project_id),
                },
            ];
            steps.extend(category_project_path_ancestor_steps(
                category,
                action,
                *project_id,
                parent_path.as_deref(),
                project_root,
            ));
            steps
        }
        _ => Vec::new(),
    }
}

fn project_path_ancestor_steps(
    project_id: i64,
    category: &str,
    action: &ActionKey,
    parent_path: Option<&str>,
    project_root: Option<&str>,
) -> Vec<RadialAncestorStep> {
    let Some(project_root) = project_root else {
        return Vec::new();
    };
    let Some(parent_path) = parent_path else {
        return Vec::new();
    };

    let mut query_parent = None;
    path_chain(project_root, parent_path)
        .into_iter()
        .map(|selected_path| {
            let step = RadialAncestorStep {
                query_path: BrowsePath::ProjectAction {
                    project_id,
                    category: category.to_string(),
                    action: action.clone(),
                    parent_path: query_parent.clone(),
                },
                selected_child: path_row_key(&selected_path),
            };
            query_parent = Some(selected_path);
            step
        })
        .collect()
}

fn category_project_path_ancestor_steps(
    category: &str,
    action: &ActionKey,
    project_id: i64,
    parent_path: Option<&str>,
    project_root: Option<&str>,
) -> Vec<RadialAncestorStep> {
    let Some(project_root) = project_root else {
        return Vec::new();
    };
    let Some(parent_path) = parent_path else {
        return Vec::new();
    };

    let mut query_parent = None;
    path_chain(project_root, parent_path)
        .into_iter()
        .map(|selected_path| {
            let step = RadialAncestorStep {
                query_path: BrowsePath::CategoryActionProject {
                    category: category.to_string(),
                    action: action.clone(),
                    project_id,
                    parent_path: query_parent.clone(),
                },
                selected_child: path_row_key(&selected_path),
            };
            query_parent = Some(selected_path);
            step
        })
        .collect()
}

fn path_chain(project_root: &str, parent_path: &str) -> Vec<String> {
    let project_root = Path::new(project_root);
    let parent_path = Path::new(parent_path);
    let Ok(relative_path) = parent_path.strip_prefix(project_root) else {
        return Vec::new();
    };

    let mut current = project_root.to_path_buf();
    let mut paths = Vec::new();
    for component in relative_path.components() {
        current.push(component.as_os_str());
        paths.push(current.to_string_lossy().to_string());
    }

    paths
}

fn radial_selected_child_span(layer: &RadialLayer) -> RadialSpan {
    sunburst_selected_child_span(layer, SunburstDistortionPolicy::default())
}

#[cfg(test)]
fn radial_segment_at_angle(layer: &RadialLayer, angle: f64) -> Option<&RadialSegment> {
    sunburst_segment_at_angle(layer, angle, SunburstDistortionPolicy::default())
}

#[cfg(test)]
fn radial_center_label_area(inner: Rect) -> Rect {
    sunburst_center_label_area(inner, SunburstRenderConfig::default())
}

#[cfg(test)]
fn radial_center_label_style(focused: bool) -> Style {
    sunburst_center_label_style(focused)
}

fn project_row_key(project_id: i64) -> String {
    format!("project:{project_id}")
}

fn category_row_key(category: &str) -> String {
    format!("category:{category}")
}

fn action_row_key(action: &ActionKey) -> String {
    format!("action:{}", stable_action_key(action))
}

fn path_row_key(path: &str) -> String {
    format!("path:{path}")
}

fn stable_action_key(action: &ActionKey) -> String {
    [
        classification_state_key(action.classification_state),
        action.normalized_action.as_deref().unwrap_or_default(),
        action.command_family.as_deref().unwrap_or_default(),
        action.base_command.as_deref().unwrap_or_default(),
    ]
    .join("|")
}

fn classification_state_key(state: ClassificationState) -> &'static str {
    match state {
        ClassificationState::Classified => "classified",
        ClassificationState::Mixed => "mixed",
        ClassificationState::Unclassified => "unclassified",
    }
}

#[cfg(test)]
fn filter_rows(rows: &[RollupRow], filter: &str) -> Vec<RollupRow> {
    let needle = filter.trim().to_ascii_lowercase();
    if needle.is_empty() {
        return rows.to_vec();
    }

    rows.iter()
        .filter(|row| row_search_text(row).contains(&needle))
        .cloned()
        .collect()
}

fn row_search_text(row: &RollupRow) -> String {
    let mut parts = vec![row.label.to_ascii_lowercase()];
    if let Some(category) = &row.category {
        parts.push(category.to_ascii_lowercase());
    }
    if let Some(action) = &row.action {
        parts.push(action_label(action).to_ascii_lowercase());
    }
    if let Some(path) = &row.full_path {
        parts.push(path.to_ascii_lowercase());
    }
    parts.join(" ")
}

fn selected_node_metadata_text(
    row: &RollupRow,
    project_identity: Option<&SelectedProjectIdentity>,
) -> Option<String> {
    let mut parts = Vec::new();

    match row.kind {
        RollupRowKind::Project => {
            let root_path = project_identity
                .map(|identity| identity.root_path.as_str())
                .or(row.full_path.as_deref())?;
            parts.push(format!("root {root_path}"));
            if let Some(identity) = project_identity {
                parts.push(format!("identity {}", identity.identity_kind.as_str()));
                if let Some(reason) = identity.identity_reason.as_deref() {
                    parts.push(format!("fallback {reason}"));
                }
            }
        }
        RollupRowKind::Directory | RollupRowKind::File => {
            let path = row.full_path.as_deref()?;
            parts.push(format!("path {path}"));
        }
        _ => {
            let path = row.full_path.as_deref()?;
            parts.push(format!("path {path}"));
        }
    }

    Some(parts.join("  |  "))
}

fn build_jump_matches(query: &str, targets: Vec<JumpTarget>) -> Vec<JumpTarget> {
    if query.trim().is_empty() {
        return targets.into_iter().take(JUMP_MATCH_LIMIT).collect();
    }

    let mut matcher = Matcher::new(MatcherConfig::DEFAULT.match_paths());
    let pattern = Pattern::parse(query, CaseMatching::Ignore, Normalization::Smart);
    let indexed = targets
        .iter()
        .enumerate()
        .map(|(index, target)| JumpMatcherCandidate {
            index,
            haystack: format!("{} {}", target.label, target.detail),
        })
        .collect::<Vec<_>>();

    pattern
        .match_list(indexed, &mut matcher)
        .into_iter()
        .take(JUMP_MATCH_LIMIT)
        .filter_map(|(candidate, _)| targets.get(candidate.index).cloned())
        .collect()
}

#[derive(Debug, Clone)]
struct JumpMatcherCandidate {
    index: usize,
    haystack: String,
}

impl AsRef<str> for JumpMatcherCandidate {
    fn as_ref(&self) -> &str {
        &self.haystack
    }
}

fn cycle_option(current: Option<String>, options: &[String]) -> Option<String> {
    if options.is_empty() {
        return None;
    }

    match current {
        None => options.first().cloned(),
        Some(current) => {
            let Some(index) = options.iter().position(|option| *option == current) else {
                return options.first().cloned();
            };
            if index + 1 >= options.len() {
                None
            } else {
                options.get(index + 1).cloned()
            }
        }
    }
}

fn cycle_project(
    current: Option<i64>,
    options: &[gnomon_core::query::ProjectFilterOption],
) -> Option<i64> {
    if options.is_empty() {
        return None;
    }

    match current {
        None => options.first().map(|project| project.id),
        Some(current) => {
            let Some(index) = options.iter().position(|project| project.id == current) else {
                return options.first().map(|project| project.id);
            };
            if index + 1 >= options.len() {
                None
            } else {
                options.get(index + 1).map(|project| project.id)
            }
        }
    }
}

fn cycle_action(
    current: Option<ActionKey>,
    options: &[gnomon_core::query::ActionFilterOption],
    category: Option<&str>,
) -> Option<ActionKey> {
    let filtered = options
        .iter()
        .filter(|option| category.is_none_or(|category| option.category == category))
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        return None;
    }

    match current {
        None => filtered.first().map(|option| option.action.clone()),
        Some(current) => {
            let Some(index) = filtered.iter().position(|option| option.action == current) else {
                return filtered.first().map(|option| option.action.clone());
            };
            if index + 1 >= filtered.len() {
                None
            } else {
                filtered.get(index + 1).map(|option| option.action.clone())
            }
        }
    }
}

const OPPORTUNITY_CATEGORIES: [OpportunityCategory; 8] = [
    OpportunityCategory::SessionSetup,
    OpportunityCategory::TaskSetup,
    OpportunityCategory::HistoryDrag,
    OpportunityCategory::Delegation,
    OpportunityCategory::ModelMismatch,
    OpportunityCategory::PromptYield,
    OpportunityCategory::SearchChurn,
    OpportunityCategory::ToolResultBloat,
];

fn cycle_opportunity_filter(current: Option<OpportunityCategory>) -> Option<OpportunityCategory> {
    match current {
        None => Some(OPPORTUNITY_CATEGORIES[0]),
        Some(current) => {
            let index = OPPORTUNITY_CATEGORIES
                .iter()
                .position(|cat| *cat == current)
                .unwrap_or(0);
            if index + 1 >= OPPORTUNITY_CATEGORIES.len() {
                None
            } else {
                Some(OPPORTUNITY_CATEGORIES[index + 1])
            }
        }
    }
}

fn action_label(action: &ActionKey) -> String {
    action.label()
}

fn build_breadcrumb_targets(
    root: &RootView,
    path: &BrowsePath,
    filter_options: &FilterOptions,
    project_root: Option<&str>,
) -> Vec<BreadcrumbTarget> {
    let mut targets = Vec::new();
    let mut display_parts = Vec::new();
    let mut push_target = |label: String, root: RootView, path: BrowsePath| {
        display_parts.push(label.clone());
        targets.push(BreadcrumbTarget {
            label,
            display: display_parts.join(" > "),
            root,
            path,
        });
    };

    push_target(
        breadcrumb_root_label(root).to_string(),
        *root,
        BrowsePath::Root,
    );

    match (root, path) {
        (_, BrowsePath::Root) => {}
        (RootView::ProjectHierarchy, BrowsePath::Project { project_id }) => {
            push_target(
                project_name(*project_id, filter_options),
                *root,
                BrowsePath::Project {
                    project_id: *project_id,
                },
            );
        }
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectCategory {
                project_id,
                category,
            },
        ) => {
            push_target(
                project_name(*project_id, filter_options),
                *root,
                BrowsePath::Project {
                    project_id: *project_id,
                },
            );
            push_target(
                category.clone(),
                *root,
                BrowsePath::ProjectCategory {
                    project_id: *project_id,
                    category: category.clone(),
                },
            );
        }
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectAction {
                project_id,
                category,
                action,
                parent_path,
            },
        ) => {
            push_target(
                project_name(*project_id, filter_options),
                *root,
                BrowsePath::Project {
                    project_id: *project_id,
                },
            );
            push_target(
                category.clone(),
                *root,
                BrowsePath::ProjectCategory {
                    project_id: *project_id,
                    category: category.clone(),
                },
            );
            push_target(
                action_label(action),
                *root,
                BrowsePath::ProjectAction {
                    project_id: *project_id,
                    category: category.clone(),
                    action: action.clone(),
                    parent_path: None,
                },
            );
            if let Some(project_root) = project_root {
                for selected_path in path_chain(project_root, parent_path.as_deref().unwrap_or(""))
                {
                    push_target(
                        breadcrumb_path_label(&selected_path),
                        *root,
                        BrowsePath::ProjectAction {
                            project_id: *project_id,
                            category: category.clone(),
                            action: action.clone(),
                            parent_path: Some(selected_path),
                        },
                    );
                }
            }
        }
        (RootView::CategoryHierarchy, BrowsePath::Category { category }) => {
            push_target(
                category.clone(),
                *root,
                BrowsePath::Category {
                    category: category.clone(),
                },
            );
        }
        (RootView::CategoryHierarchy, BrowsePath::CategoryAction { category, action }) => {
            push_target(
                category.clone(),
                *root,
                BrowsePath::Category {
                    category: category.clone(),
                },
            );
            push_target(
                action_label(action),
                *root,
                BrowsePath::CategoryAction {
                    category: category.clone(),
                    action: action.clone(),
                },
            );
        }
        (
            RootView::CategoryHierarchy,
            BrowsePath::CategoryActionProject {
                category,
                action,
                project_id,
                parent_path,
            },
        ) => {
            push_target(
                category.clone(),
                *root,
                BrowsePath::Category {
                    category: category.clone(),
                },
            );
            push_target(
                action_label(action),
                *root,
                BrowsePath::CategoryAction {
                    category: category.clone(),
                    action: action.clone(),
                },
            );
            push_target(
                project_name(*project_id, filter_options),
                *root,
                BrowsePath::CategoryActionProject {
                    category: category.clone(),
                    action: action.clone(),
                    project_id: *project_id,
                    parent_path: None,
                },
            );
            if let Some(project_root) = project_root {
                for selected_path in path_chain(project_root, parent_path.as_deref().unwrap_or(""))
                {
                    push_target(
                        breadcrumb_path_label(&selected_path),
                        *root,
                        BrowsePath::CategoryActionProject {
                            category: category.clone(),
                            action: action.clone(),
                            project_id: *project_id,
                            parent_path: Some(selected_path),
                        },
                    );
                }
            }
        }
        _ => {}
    }

    targets
}

fn breadcrumb_root_label(root: &RootView) -> &'static str {
    match root {
        RootView::ProjectHierarchy => "all projects",
        RootView::CategoryHierarchy => "all categories",
    }
}

fn breadcrumb_path_label(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| path.to_string())
}

fn next_browse_path(root: &RootView, current: &BrowsePath, row: &RollupRow) -> Option<BrowsePath> {
    match (root, current, row.kind) {
        (RootView::ProjectHierarchy, BrowsePath::Root, RollupRowKind::Project) => row
            .project_id
            .map(|project_id| BrowsePath::Project { project_id }),
        (
            RootView::ProjectHierarchy,
            BrowsePath::Project { project_id },
            RollupRowKind::ActionCategory,
        ) => row
            .category
            .clone()
            .map(|category| BrowsePath::ProjectCategory {
                project_id: *project_id,
                category,
            }),
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectCategory {
                project_id,
                category,
            },
            RollupRowKind::Action,
        ) => row.action.clone().map(|action| BrowsePath::ProjectAction {
            project_id: *project_id,
            category: category.clone(),
            action,
            parent_path: None,
        }),
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectAction {
                project_id,
                category,
                action,
                ..
            },
            RollupRowKind::Directory,
        ) => row
            .full_path
            .clone()
            .map(|parent_path| BrowsePath::ProjectAction {
                project_id: *project_id,
                category: category.clone(),
                action: action.clone(),
                parent_path: Some(parent_path),
            }),
        (RootView::CategoryHierarchy, BrowsePath::Root, RollupRowKind::ActionCategory) => row
            .category
            .clone()
            .map(|category| BrowsePath::Category { category }),
        (RootView::CategoryHierarchy, BrowsePath::Category { category }, RollupRowKind::Action) => {
            row.action.clone().map(|action| BrowsePath::CategoryAction {
                category: category.clone(),
                action,
            })
        }
        (
            RootView::CategoryHierarchy,
            BrowsePath::CategoryAction { category, action },
            RollupRowKind::Project,
        ) => row
            .project_id
            .map(|project_id| BrowsePath::CategoryActionProject {
                category: category.clone(),
                action: action.clone(),
                project_id,
                parent_path: None,
            }),
        (
            RootView::CategoryHierarchy,
            BrowsePath::CategoryActionProject {
                category,
                action,
                project_id,
                ..
            },
            RollupRowKind::Directory,
        ) => row
            .full_path
            .clone()
            .map(|parent_path| BrowsePath::CategoryActionProject {
                category: category.clone(),
                action: action.clone(),
                project_id: *project_id,
                parent_path: Some(parent_path),
            }),
        _ => None,
    }
}

fn prefetch_child_requests(request: &BrowseRequest, rows: &[RollupRow]) -> Vec<BrowseRequest> {
    rows.iter()
        .filter_map(|row| next_browse_path(&request.root, &request.path, row))
        .take(PREFETCH_RECURSION_BREADTH_LIMIT)
        .map(|path| BrowseRequest {
            snapshot: request.snapshot.clone(),
            root: request.root,
            lens: request.lens,
            filters: request.filters.clone(),
            path,
        })
        .collect()
}

fn parent_browse_path(path: &BrowsePath, project_root: Option<&str>) -> BrowsePath {
    match path {
        BrowsePath::Root => BrowsePath::Root,
        BrowsePath::Project { .. } | BrowsePath::Category { .. } => BrowsePath::Root,
        BrowsePath::ProjectCategory { project_id, .. } => BrowsePath::Project {
            project_id: *project_id,
        },
        BrowsePath::CategoryAction { category, .. } => BrowsePath::Category {
            category: category.clone(),
        },
        BrowsePath::ProjectAction {
            project_id,
            category,
            action,
            parent_path,
        } => match parent_path {
            Some(parent_path) => {
                let parent = Path::new(parent_path)
                    .parent()
                    .map(|parent| parent.to_string_lossy().to_string());
                let next_parent = match (parent, project_root) {
                    (Some(parent), Some(project_root)) if parent == project_root => None,
                    (Some(parent), Some(_)) => Some(parent),
                    _ => None,
                };
                BrowsePath::ProjectAction {
                    project_id: *project_id,
                    category: category.clone(),
                    action: action.clone(),
                    parent_path: next_parent,
                }
            }
            None => BrowsePath::ProjectCategory {
                project_id: *project_id,
                category: category.clone(),
            },
        },
        BrowsePath::CategoryActionProject {
            category,
            action,
            project_id,
            parent_path,
        } => match parent_path {
            Some(parent_path) => {
                let parent = Path::new(parent_path)
                    .parent()
                    .map(|parent| parent.to_string_lossy().to_string());
                let next_parent = match (parent, project_root) {
                    (Some(parent), Some(project_root)) if parent == project_root => None,
                    (Some(parent), Some(_)) => Some(parent),
                    _ => None,
                };
                BrowsePath::CategoryActionProject {
                    category: category.clone(),
                    action: action.clone(),
                    project_id: *project_id,
                    parent_path: next_parent,
                }
            }
            None => BrowsePath::CategoryAction {
                category: category.clone(),
                action: action.clone(),
            },
        },
    }
}

fn path_is_within(candidate: &BrowsePath, ancestor: &BrowsePath) -> bool {
    match (candidate, ancestor) {
        (_, BrowsePath::Root) => true,
        (
            BrowsePath::Project { project_id },
            BrowsePath::Project {
                project_id: ancestor_project,
            },
        ) => project_id == ancestor_project,
        (
            BrowsePath::ProjectCategory {
                project_id,
                category: _,
            },
            BrowsePath::Project {
                project_id: ancestor_project,
            },
        ) => project_id == ancestor_project,
        (
            BrowsePath::ProjectCategory {
                project_id,
                category,
            },
            BrowsePath::ProjectCategory {
                project_id: ancestor_project,
                category: ancestor_category,
            },
        ) => project_id == ancestor_project && category == ancestor_category,
        (
            BrowsePath::ProjectAction {
                project_id,
                category: _,
                action: _,
                parent_path: _,
            },
            BrowsePath::Project {
                project_id: ancestor_project,
            },
        ) => project_id == ancestor_project,
        (
            BrowsePath::ProjectAction {
                project_id,
                category,
                action: _,
                parent_path: _,
            },
            BrowsePath::ProjectCategory {
                project_id: ancestor_project,
                category: ancestor_category,
            },
        ) => project_id == ancestor_project && category == ancestor_category,
        (
            BrowsePath::ProjectAction {
                project_id,
                category,
                action,
                parent_path,
            },
            BrowsePath::ProjectAction {
                project_id: ancestor_project,
                category: ancestor_category,
                action: ancestor_action,
                parent_path: ancestor_parent_path,
            },
        ) => {
            project_id == ancestor_project
                && category == ancestor_category
                && action == ancestor_action
                && directory_path_is_within(parent_path.as_deref(), ancestor_parent_path.as_deref())
        }
        (
            BrowsePath::Category { category },
            BrowsePath::Category {
                category: ancestor_category,
            },
        ) => category == ancestor_category,
        (
            BrowsePath::CategoryAction {
                category,
                action: _,
            },
            BrowsePath::Category {
                category: ancestor_category,
            },
        ) => category == ancestor_category,
        (
            BrowsePath::CategoryAction { category, action },
            BrowsePath::CategoryAction {
                category: ancestor_category,
                action: ancestor_action,
            },
        ) => category == ancestor_category && action == ancestor_action,
        (
            BrowsePath::CategoryActionProject {
                category,
                action: _,
                project_id: _,
                parent_path: _,
            },
            BrowsePath::Category {
                category: ancestor_category,
            },
        ) => category == ancestor_category,
        (
            BrowsePath::CategoryActionProject {
                category,
                action,
                project_id: _,
                parent_path: _,
            },
            BrowsePath::CategoryAction {
                category: ancestor_category,
                action: ancestor_action,
            },
        ) => category == ancestor_category && action == ancestor_action,
        (
            BrowsePath::CategoryActionProject {
                category,
                action,
                project_id,
                parent_path,
            },
            BrowsePath::CategoryActionProject {
                category: ancestor_category,
                action: ancestor_action,
                project_id: ancestor_project_id,
                parent_path: ancestor_parent_path,
            },
        ) => {
            category == ancestor_category
                && action == ancestor_action
                && project_id == ancestor_project_id
                && directory_path_is_within(parent_path.as_deref(), ancestor_parent_path.as_deref())
        }
        _ => false,
    }
}

fn directory_path_is_within(candidate: Option<&str>, ancestor: Option<&str>) -> bool {
    match (candidate, ancestor) {
        (_, None) => true,
        (Some(candidate), Some(ancestor)) => {
            candidate == ancestor
                || candidate
                    .strip_prefix(ancestor)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        }
        (None, Some(_)) => false,
    }
}

fn matches_path_root(root: &RootView, path: &BrowsePath) -> bool {
    matches!(
        (root, path),
        (_, BrowsePath::Root)
            | (RootView::ProjectHierarchy, BrowsePath::Project { .. })
            | (
                RootView::ProjectHierarchy,
                BrowsePath::ProjectCategory { .. }
            )
            | (RootView::ProjectHierarchy, BrowsePath::ProjectAction { .. })
            | (RootView::CategoryHierarchy, BrowsePath::Category { .. })
            | (
                RootView::CategoryHierarchy,
                BrowsePath::CategoryAction { .. }
            )
            | (
                RootView::CategoryHierarchy,
                BrowsePath::CategoryActionProject { .. }
            )
    )
}

fn project_id_from_path(path: &BrowsePath) -> Option<i64> {
    match path {
        BrowsePath::Project { project_id }
        | BrowsePath::ProjectCategory { project_id, .. }
        | BrowsePath::ProjectAction { project_id, .. }
        | BrowsePath::CategoryActionProject { project_id, .. } => Some(*project_id),
        BrowsePath::Root | BrowsePath::Category { .. } | BrowsePath::CategoryAction { .. } => None,
    }
}

pub(crate) fn describe_browse_path(
    root: &RootView,
    path: &BrowsePath,
    filter_options: &FilterOptions,
) -> String {
    match (root, path) {
        (RootView::ProjectHierarchy, BrowsePath::Root) => "all projects".to_string(),
        (RootView::CategoryHierarchy, BrowsePath::Root) => "all categories".to_string(),
        (_, BrowsePath::Project { project_id }) => project_name(*project_id, filter_options),
        (
            _,
            BrowsePath::ProjectCategory {
                project_id,
                category,
            },
        ) => {
            format!("{}/{}", project_name(*project_id, filter_options), category)
        }
        (
            _,
            BrowsePath::ProjectAction {
                project_id,
                category,
                action,
                parent_path,
            },
        ) => {
            let path_label = parent_path
                .as_ref()
                .and_then(|path| {
                    Path::new(path)
                        .file_name()
                        .map(|name| name.to_string_lossy().to_string())
                })
                .map(|path| format!("/{path}"))
                .unwrap_or_default();
            format!(
                "{}/{}/{}{}",
                project_name(*project_id, filter_options),
                category,
                action_label(action),
                path_label
            )
        }
        (_, BrowsePath::Category { category }) => category.clone(),
        (_, BrowsePath::CategoryAction { category, action }) => {
            format!("{}/{}", category, action_label(action))
        }
        (
            _,
            BrowsePath::CategoryActionProject {
                category,
                action,
                project_id,
                parent_path,
            },
        ) => {
            let path_label = parent_path
                .as_ref()
                .and_then(|path| {
                    Path::new(path)
                        .file_name()
                        .map(|name| name.to_string_lossy().to_string())
                })
                .map(|path| format!("/{path}"))
                .unwrap_or_default();
            format!(
                "{}/{}/{}{}",
                category,
                action_label(action),
                project_name(*project_id, filter_options),
                path_label
            )
        }
    }
}

fn project_name(project_id: i64, filter_options: &FilterOptions) -> String {
    filter_options
        .projects
        .iter()
        .find(|project| project.id == project_id)
        .map(|project| project.display_name.clone())
        .unwrap_or_else(|| format!("#{project_id}"))
}

pub(crate) fn format_metric(value: f64) -> String {
    if (value.fract()).abs() < f64::EPSILON {
        format!("{value:.0}")
    } else {
        format!("{value:.1}")
    }
}

fn centered_rect(area: Rect, width: u16, height: u16) -> Rect {
    let popup_width = width.min(area.width.saturating_sub(4)).max(10);
    let popup_height = height.min(area.height.saturating_sub(4)).max(6);
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(area.height.saturating_sub(popup_height) / 2),
            Constraint::Length(popup_height),
            Constraint::Min(0),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(area.width.saturating_sub(popup_width) / 2),
            Constraint::Length(popup_width),
            Constraint::Min(0),
        ])
        .split(vertical[1]);
    horizontal[1]
}

fn wide_layout_panes(area: Rect) -> Rc<[Rect]> {
    let map_width = wide_layout_map_pane_width(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(map_width), Constraint::Min(0)])
        .split(area)
}

fn wide_layout_map_pane_width(area: Rect) -> u16 {
    let max_map_width = area.width.saturating_sub(STATISTICS_PANE_MIN_WIDTH);
    if max_map_width == 0 {
        return area.width;
    }

    let ideal_inner_width = area
        .height
        .saturating_sub(2)
        .saturating_mul(MAP_PANE_INNER_ASPECT_NUMERATOR)
        / MAP_PANE_INNER_ASPECT_DENOMINATOR;
    let ideal_map_width = ideal_inner_width.saturating_add(2);

    ideal_map_width.clamp(MAP_PANE_MIN_WIDTH.min(max_map_width), max_map_width)
}

#[cfg(test)]
mod tests {
    use anyhow::{Context, Result};
    use gnomon_core::config::RuntimeConfig;
    use gnomon_core::db::Database;
    use gnomon_core::import::StartupOpenReason;
    use gnomon_core::opportunity::{
        OpportunityAnnotation, OpportunityCategory, OpportunityConfidence, OpportunitySummary,
    };
    use gnomon_core::perf::PerfLogger;
    use gnomon_core::query::{ClassificationState, QueryEngine, SnapshotBounds};
    use gnomon_core::sources::ConfiguredSources;
    use gnomon_core::validation::{ScaleValidationSpec, run_scale_validation};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use rusqlite::params;
    use serde_json::Value;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn persisted_state_round_trips() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("tui-state.json");
        let state = PersistedUiState {
            root: RootView::CategoryHierarchy,
            path: BrowsePath::Category {
                category: "editing".to_string(),
            },
            lens: MetricLens::Total,
            pane_mode: PaneMode::Radial,
            time_window: TimeWindowPreset::LastWeek,
            model: Some("claude-opus".to_string()),
            project_id: Some(7),
            action_category: Some("editing".to_string()),
            action: Some(sample_action("read file")),
            row_filter: "src".to_string(),
            enabled_columns: vec![OptionalColumn::Kind, OptionalColumn::Items],
            opportunity_filter: Some(OpportunityCategory::HistoryDrag),
        };

        state.save(&path)?;
        let loaded = PersistedUiState::load(&path)?.context("missing persisted state")?;
        assert_eq!(loaded.row_filter, "src");
        assert_eq!(
            loaded.opportunity_filter,
            Some(OpportunityCategory::HistoryDrag)
        );
        assert_eq!(loaded.lens, MetricLens::Total);
        assert_eq!(loaded.pane_mode, PaneMode::Radial);
        assert_eq!(
            loaded.enabled_columns,
            vec![OptionalColumn::Kind, OptionalColumn::Items]
        );
        Ok(())
    }

    #[test]
    fn persisted_state_loads_legacy_details_pane_name() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("tui-state.json");
        fs::write(
            &path,
            r#"{
  "root": "ProjectHierarchy",
  "path": "Root",
  "lens": "UncachedInput",
  "pane_mode": "Details",
  "time_window": "All",
  "model": null,
  "project_id": null,
  "action_category": null,
  "action": null,
  "row_filter": "",
  "enabled_columns": ["Kind"]
}"#,
        )?;

        let loaded = PersistedUiState::load(&path)?.context("missing persisted state")?;
        assert_eq!(loaded.pane_mode, PaneMode::Radial);
        Ok(())
    }

    #[test]
    fn row_filter_matches_labels_and_paths_case_insensitively() {
        let rows = vec![
            sample_row("src", Some("/tmp/project/src".to_string())),
            sample_row("README.md", Some("/tmp/project/README.md".to_string())),
        ];

        let filtered = filter_rows(&rows, "README.md");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].label, "README.md");

        let filtered = filter_rows(&rows, "PROJECT/SRC");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].label, "src");
    }

    #[test]
    fn apply_row_filter_flattens_expanded_tree_descendants() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.ui_state.root = RootView::ProjectHierarchy;
        app.ui_state.path = BrowsePath::ProjectAction {
            project_id: 1,
            category: "editing".to_string(),
            action: sample_action("read file"),
            parent_path: None,
        };
        app.raw_rows = vec![
            sample_row("src", Some("/tmp/project/src".to_string())),
            sample_row("tests", Some("/tmp/project/tests".to_string())),
        ];
        app.cache_rows(app.ui_state.path.clone(), app.raw_rows.clone());
        let src_path = BrowsePath::ProjectAction {
            project_id: 1,
            category: "editing".to_string(),
            action: sample_action("read file"),
            parent_path: Some("/tmp/project/src".to_string()),
        };
        app.expanded_paths.push(src_path.clone());
        app.cache_rows(
            src_path,
            vec![RollupRow {
                kind: RollupRowKind::File,
                key: "path:/tmp/project/src/lib.rs".to_string(),
                label: "lib.rs".to_string(),
                metrics: gnomon_core::query::MetricTotals {
                    uncached_input: 3.0,
                    cached_input: 0.0,
                    gross_input: 3.0,
                    output: 0.0,
                    total: 3.0,
                },
                indicators: gnomon_core::query::MetricIndicators {
                    selected_lens_last_5_hours: 3.0,
                    selected_lens_last_week: 3.0,
                    uncached_input_reference: 3.0,
                },
                item_count: 1,
                opportunities: OpportunitySummary::default(),
                skill_attribution: None,
                project_id: Some(1),
                project_identity: None,
                category: Some("editing".to_string()),
                action: Some(sample_action("read file")),
                full_path: Some("/tmp/project/src/lib.rs".to_string()),
            }],
        );

        app.apply_row_filter()?;

        let labels = app
            .visible_rows
            .iter()
            .map(|row| (row.row.label.clone(), row.depth, row.is_expanded))
            .collect::<Vec<_>>();
        assert_eq!(
            labels,
            vec![
                ("src".to_string(), 0, true),
                ("lib.rs".to_string(), 1, false),
                ("tests".to_string(), 0, false),
            ]
        );
        Ok(())
    }

    #[test]
    fn collapse_selected_prunes_descendant_expansions() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.ui_state.root = RootView::ProjectHierarchy;
        app.ui_state.path = BrowsePath::ProjectAction {
            project_id: 1,
            category: "editing".to_string(),
            action: sample_action("read file"),
            parent_path: None,
        };
        let src_row = sample_row("src", Some("/tmp/project/src".to_string()));
        app.visible_rows = vec![TreeRow {
            row: src_row,
            parent_path: app.ui_state.path.clone(),
            node_path: Some(BrowsePath::ProjectAction {
                project_id: 1,
                category: "editing".to_string(),
                action: sample_action("read file"),
                parent_path: Some("/tmp/project/src".to_string()),
            }),
            depth: 0,
            is_expanded: true,
        }];
        app.table_state.select(Some(0));
        app.expanded_paths = vec![
            BrowsePath::ProjectAction {
                project_id: 1,
                category: "editing".to_string(),
                action: sample_action("read file"),
                parent_path: Some("/tmp/project/src".to_string()),
            },
            BrowsePath::ProjectAction {
                project_id: 1,
                category: "editing".to_string(),
                action: sample_action("read file"),
                parent_path: Some("/tmp/project/src/lib".to_string()),
            },
        ];
        app.raw_rows = vec![sample_row("src", Some("/tmp/project/src".to_string()))];
        app.cache_rows(app.ui_state.path.clone(), app.raw_rows.clone());

        app.collapse_selected()?;

        assert!(app.expanded_paths.is_empty());
        Ok(())
    }

    #[test]
    fn jump_matches_use_fuzzy_sorting() {
        let matches = build_jump_matches(
            "proj edit",
            vec![
                JumpTarget {
                    label: "project-a / editing".to_string(),
                    detail: "project category".to_string(),
                    root: RootView::ProjectHierarchy,
                    path: BrowsePath::Root,
                },
                JumpTarget {
                    label: "documentation writing".to_string(),
                    detail: "category".to_string(),
                    root: RootView::CategoryHierarchy,
                    path: BrowsePath::Root,
                },
            ],
        );

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].label, "project-a / editing");
    }

    #[test]
    fn parent_browse_path_unwinds_directory_state() {
        let parent = parent_browse_path(
            &BrowsePath::ProjectAction {
                project_id: 1,
                category: "editing".to_string(),
                action: sample_action("read file"),
                parent_path: Some("/tmp/project-a/src".to_string()),
            },
            Some("/tmp/project-a"),
        );

        assert_eq!(
            parent,
            BrowsePath::ProjectAction {
                project_id: 1,
                category: "editing".to_string(),
                action: sample_action("read file"),
                parent_path: None,
            }
        );
    }

    #[test]
    fn active_columns_hide_low_priority_entries_when_narrow() {
        let columns = active_columns(48, MetricLens::UncachedInput, &default_enabled_columns());
        assert!(columns.len() < default_enabled_columns().len() + 2);
        assert_eq!(columns[0].key, ColumnKey::Label);
        assert_eq!(columns[1].key, ColumnKey::SelectedLens);
    }

    #[test]
    fn gross_metric_labels_use_all_input_wording() {
        assert_eq!(metric_lens_label(MetricLens::GrossInput), "all input");
        assert_eq!(OptionalColumn::GrossInput.short_label(), "all input");
        assert_eq!(
            optional_column_spec(&OptionalColumn::GrossInput).title,
            "all input"
        );
    }

    #[test]
    fn label_column_marks_drillable_rows() {
        let row = RollupRow {
            kind: RollupRowKind::Project,
            key: "project:1".to_string(),
            label: "project-a".to_string(),
            metrics: gnomon_core::query::MetricTotals {
                uncached_input: 5.0,
                cached_input: 0.0,
                gross_input: 5.0,
                output: 0.0,
                total: 5.0,
            },
            indicators: gnomon_core::query::MetricIndicators {
                selected_lens_last_5_hours: 5.0,
                selected_lens_last_week: 5.0,
                uncached_input_reference: 5.0,
            },
            item_count: 1,
            opportunities: OpportunitySummary::default(),
            skill_attribution: None,
            project_id: Some(1),
            project_identity: None,
            category: None,
            action: None,
            full_path: None,
        };

        let rendered = render_column_value(ColumnKey::Label, &row, MetricLens::UncachedInput);

        assert_eq!(rendered, "project-a");
        assert_eq!(
            drillability_glyph(&RootView::ProjectHierarchy, &BrowsePath::Root, &row),
            "▸ "
        );
    }

    #[test]
    fn label_column_leaves_leaf_rows_unmarked() {
        let row = RollupRow {
            kind: RollupRowKind::File,
            key: "path:/tmp/project-a/src/lib.rs".to_string(),
            label: "lib.rs".to_string(),
            metrics: gnomon_core::query::MetricTotals {
                uncached_input: 5.0,
                cached_input: 0.0,
                gross_input: 5.0,
                output: 0.0,
                total: 5.0,
            },
            indicators: gnomon_core::query::MetricIndicators {
                selected_lens_last_5_hours: 5.0,
                selected_lens_last_week: 5.0,
                uncached_input_reference: 5.0,
            },
            item_count: 1,
            opportunities: OpportunitySummary::default(),
            skill_attribution: None,
            project_id: Some(1),
            project_identity: None,
            category: Some("editing".to_string()),
            action: Some(sample_action("read file")),
            full_path: Some("/tmp/project-a/src/lib.rs".to_string()),
        };

        let rendered = render_column_value(ColumnKey::Label, &row, MetricLens::UncachedInput);

        assert_eq!(rendered, "lib.rs");
        assert_eq!(
            drillability_glyph(
                &RootView::ProjectHierarchy,
                &BrowsePath::ProjectAction {
                    project_id: 1,
                    category: "editing".to_string(),
                    action: sample_action("read file"),
                    parent_path: Some("/tmp/project-a/src".to_string()),
                },
                &row,
            ),
            "  "
        );
    }

    #[test]
    fn pane_focus_round_trips_through_pane_mode() {
        assert_eq!(
            PaneFocus::from_pane_mode(PaneMode::from_focus(PaneFocus::Table)),
            PaneFocus::Table
        );
        assert_eq!(
            PaneFocus::from_pane_mode(PaneMode::from_focus(PaneFocus::Radial)),
            PaneFocus::Radial
        );
    }

    #[test]
    fn radial_ancestor_steps_include_directory_chain() {
        let action = sample_action("read file");
        let steps = radial_ancestor_steps(
            &RootView::ProjectHierarchy,
            &BrowsePath::ProjectAction {
                project_id: 1,
                category: "editing".to_string(),
                action: action.clone(),
                parent_path: Some("/tmp/project-a/src/lib".to_string()),
            },
            Some("/tmp/project-a"),
        );

        assert_eq!(steps.len(), 5);
        assert_eq!(
            steps[0],
            RadialAncestorStep {
                query_path: BrowsePath::Root,
                selected_child: "project:1".to_string(),
            }
        );
        assert_eq!(
            steps[1],
            RadialAncestorStep {
                query_path: BrowsePath::Project { project_id: 1 },
                selected_child: "category:editing".to_string(),
            }
        );
        assert_eq!(
            steps[2],
            RadialAncestorStep {
                query_path: BrowsePath::ProjectCategory {
                    project_id: 1,
                    category: "editing".to_string(),
                },
                selected_child: action_row_key(&action),
            }
        );
        assert_eq!(
            steps[3],
            RadialAncestorStep {
                query_path: BrowsePath::ProjectAction {
                    project_id: 1,
                    category: "editing".to_string(),
                    action: action.clone(),
                    parent_path: None,
                },
                selected_child: "path:/tmp/project-a/src".to_string(),
            }
        );
        assert_eq!(
            steps[4],
            RadialAncestorStep {
                query_path: BrowsePath::ProjectAction {
                    project_id: 1,
                    category: "editing".to_string(),
                    action,
                    parent_path: Some("/tmp/project-a/src".to_string()),
                },
                selected_child: "path:/tmp/project-a/src/lib".to_string(),
            }
        );
    }

    #[test]
    fn radial_context_uses_active_path_not_ui_state_path() -> Result<()> {
        // Regression test for issue #56: build_radial_context must use the
        // active_path parameter (derived from tree selection), not
        // ui_state.path.  When a user selects an expanded child row the radial
        // reveal should reflect that child's path.
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        // ui_state.path stays at Root – this is the stale path that was
        // previously (incorrectly) used for radial context.
        app.ui_state.root = RootView::ProjectHierarchy;
        app.ui_state.path = BrowsePath::Root;

        // The active path simulates a tree row selected inside a project's
        // category.  radial_ancestor_steps returns 2 steps for this path (Root
        // → Project, Project → Category).
        let active_path = BrowsePath::ProjectCategory {
            project_id: 1,
            category: "editing".to_string(),
        };

        let filters = current_query_filters_for(&app.ui_state, &app.snapshot)?;
        let mut browse_stats = BrowseFanoutStats::default();

        let ctx = app.build_radial_context(&filters, &active_path, None, &mut browse_stats)?;

        // With the fix, ancestor_layers length == number of ancestor steps for
        // `active_path` (2 for ProjectCategory).  Before the fix, ui_state.path
        // was Root which produces 0 ancestor steps.
        assert_eq!(
            ctx.ancestor_layers.len(),
            2,
            "radial context should reflect active_path (ProjectCategory → 2 ancestor layers), \
             not ui_state.path (Root → 0)"
        );
        Ok(())
    }

    #[test]
    fn radial_context_for_top_level_project_selection() -> Result<()> {
        // Regression test for issue #56: selecting a top-level project row
        // should produce a radial context with 1 ancestor layer (Root →
        // Project), even when ui_state.path is Root.
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        app.ui_state.root = RootView::ProjectHierarchy;
        app.ui_state.path = BrowsePath::Root;

        let active_path = BrowsePath::Project { project_id: 1 };

        let filters = current_query_filters_for(&app.ui_state, &app.snapshot)?;
        let mut browse_stats = BrowseFanoutStats::default();

        let ctx = app.build_radial_context(&filters, &active_path, None, &mut browse_stats)?;

        assert_eq!(
            ctx.ancestor_layers.len(),
            1,
            "selecting a project row should yield 1 ancestor layer (Root → Project)"
        );
        Ok(())
    }

    #[test]
    fn radial_descendant_layers_show_children_not_largest_branch() -> Result<()> {
        // Regression test for issue #70: selecting a node should produce
        // exactly one descendant layer (its immediate children), not
        // auto-follow the largest child branch deeper.
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        app.ui_state.root = RootView::ProjectHierarchy;
        app.ui_state.path = BrowsePath::Root;

        // Select a project node — its children (categories) should appear as
        // exactly one descendant layer.  The old code would auto-follow the
        // largest category and produce up to 3 descendant layers.
        let active_path = BrowsePath::Project { project_id: 1 };
        let filters = current_query_filters_for(&app.ui_state, &app.snapshot)?;
        let mut browse_stats = BrowseFanoutStats::default();

        let ctx = app.build_radial_context(&filters, &active_path, None, &mut browse_stats)?;

        assert!(
            ctx.descendant_layers.len() <= 1,
            "descendant layers should contain at most 1 layer (immediate children), \
             got {} — the renderer must not auto-follow the largest child branch",
            ctx.descendant_layers.len()
        );
        Ok(())
    }

    #[test]
    fn radial_model_marks_selected_outer_segment() {
        let rows = vec![
            sample_row("src", Some("/tmp/project/src".to_string())),
            sample_row("tests", Some("/tmp/project/tests".to_string())),
        ];
        let filter_options = sample_filter_options();
        let model = build_radial_model(
            &RadialContext::default(),
            &rows,
            rows.get(1),
            &RootView::ProjectHierarchy,
            &BrowsePath::Project { project_id: 1 },
            &filter_options,
            MetricLens::UncachedInput,
        );

        assert_eq!(model.layers.len(), 1);
        assert_eq!(
            model.layers[0]
                .segments
                .iter()
                .filter(|segment| segment.is_selected)
                .count(),
            1
        );
        assert_eq!(
            model.center.selection_label,
            "selected: tests (dir, uncached 5)"
        );
        assert!(model.layers[0].segments[1].is_selected);
        assert_eq!(model.center.scope_label, "project-a");
    }

    #[test]
    fn breadcrumb_targets_include_project_and_directory_ancestors() {
        let targets = build_breadcrumb_targets(
            &RootView::ProjectHierarchy,
            &BrowsePath::ProjectAction {
                project_id: 1,
                category: "editing".to_string(),
                action: sample_action("read file"),
                parent_path: Some("/tmp/project-a/src/lib".to_string()),
            },
            &sample_filter_options(),
            Some("/tmp/project-a"),
        );

        let labels = targets
            .iter()
            .map(|target| target.label.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            labels,
            vec![
                "all projects",
                "project-a",
                "editing",
                "read file",
                "src",
                "lib",
            ]
        );
        assert_eq!(targets[0].path, BrowsePath::Root);
        assert_eq!(targets[1].path, BrowsePath::Project { project_id: 1 });
        assert_eq!(
            targets[3].path,
            BrowsePath::ProjectAction {
                project_id: 1,
                category: "editing".to_string(),
                action: sample_action("read file"),
                parent_path: None,
            }
        );
        assert_eq!(
            targets[5].path,
            BrowsePath::ProjectAction {
                project_id: 1,
                category: "editing".to_string(),
                action: sample_action("read file"),
                parent_path: Some("/tmp/project-a/src/lib".to_string()),
            }
        );
    }

    #[test]
    fn breadcrumb_targets_include_category_hierarchy_ancestors() {
        let targets = build_breadcrumb_targets(
            &RootView::CategoryHierarchy,
            &BrowsePath::CategoryActionProject {
                category: "Documentation".to_string(),
                action: sample_action("write file"),
                project_id: 2,
                parent_path: Some("/tmp/project-b/src".to_string()),
            },
            &FilterOptions {
                projects: vec![gnomon_core::query::ProjectFilterOption {
                    id: 2,
                    display_name: "project-b".to_string(),
                }],
                models: Vec::new(),
                categories: Vec::new(),
                actions: Vec::new(),
            },
            Some("/tmp/project-b"),
        );

        let labels = targets
            .iter()
            .map(|target| target.label.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            labels,
            vec![
                "all categories",
                "Documentation",
                "write file",
                "project-b",
                "src",
            ]
        );
        assert_eq!(targets[0].path, BrowsePath::Root);
        assert_eq!(
            targets[3].path,
            BrowsePath::CategoryActionProject {
                category: "Documentation".to_string(),
                action: sample_action("write file"),
                project_id: 2,
                parent_path: None,
            }
        );
        assert_eq!(
            targets[4].path,
            BrowsePath::CategoryActionProject {
                category: "Documentation".to_string(),
                action: sample_action("write file"),
                project_id: 2,
                parent_path: Some("/tmp/project-b/src".to_string()),
            }
        );
    }

    #[test]
    fn radial_selected_child_span_tracks_nested_drilldown_arcs() {
        let project_layer = build_radial_layer(
            &[
                radial_row(RadialRowSpec {
                    key: "project:1",
                    label: "project-a",
                    kind: RollupRowKind::Project,
                    value: 10.0,
                    project_id: Some(1),
                    category: None,
                    action: None,
                    full_path: None,
                }),
                radial_row(RadialRowSpec {
                    key: "project:2",
                    label: "project-b",
                    kind: RollupRowKind::Project,
                    value: 10.0,
                    project_id: Some(2),
                    category: None,
                    action: None,
                    full_path: None,
                }),
            ],
            MetricLens::UncachedInput,
            Some("project:1"),
            RadialSpan::full(),
        );
        assert_span_close(radial_selected_child_span(&project_layer), 0.0, TAU / 2.0);

        let category_layer = build_radial_layer(
            &[
                radial_row(RadialRowSpec {
                    key: "category:editing",
                    label: "editing",
                    kind: RollupRowKind::ActionCategory,
                    value: 8.0,
                    project_id: Some(1),
                    category: Some("editing"),
                    action: None,
                    full_path: None,
                }),
                radial_row(RadialRowSpec {
                    key: "category:Documentation",
                    label: "Documentation",
                    kind: RollupRowKind::ActionCategory,
                    value: 8.0,
                    project_id: Some(1),
                    category: Some("Documentation"),
                    action: None,
                    full_path: None,
                }),
            ],
            MetricLens::UncachedInput,
            Some("category:Documentation"),
            radial_selected_child_span(&project_layer),
        );
        assert_span_close(
            radial_selected_child_span(&category_layer),
            TAU / 4.0,
            TAU / 4.0,
        );

        let action_layer = build_radial_layer(
            &[
                radial_row(RadialRowSpec {
                    key: "action:read",
                    label: "read",
                    kind: RollupRowKind::Action,
                    value: 4.0,
                    project_id: Some(1),
                    category: Some("Documentation"),
                    action: Some(sample_action("read file")),
                    full_path: None,
                }),
                radial_row(RadialRowSpec {
                    key: "action:write",
                    label: "write",
                    kind: RollupRowKind::Action,
                    value: 4.0,
                    project_id: Some(1),
                    category: Some("Documentation"),
                    action: Some(sample_action("write file")),
                    full_path: None,
                }),
            ],
            MetricLens::UncachedInput,
            Some("action:write"),
            radial_selected_child_span(&category_layer),
        );
        assert_span_close(
            radial_selected_child_span(&action_layer),
            3.0 * TAU / 8.0,
            TAU / 8.0,
        );

        let directory_layer = build_radial_layer(
            &[
                radial_row(RadialRowSpec {
                    key: "path:/tmp/project-a/src",
                    label: "src",
                    kind: RollupRowKind::Directory,
                    value: 2.0,
                    project_id: Some(1),
                    category: Some("Documentation"),
                    action: Some(sample_action("read file")),
                    full_path: Some("/tmp/project-a/src"),
                }),
                radial_row(RadialRowSpec {
                    key: "path:/tmp/project-a/src/lib",
                    label: "lib",
                    kind: RollupRowKind::Directory,
                    value: 2.0,
                    project_id: Some(1),
                    category: Some("Documentation"),
                    action: Some(sample_action("read file")),
                    full_path: Some("/tmp/project-a/src/lib"),
                }),
            ],
            MetricLens::UncachedInput,
            Some("path:/tmp/project-a/src/lib"),
            radial_selected_child_span(&action_layer),
        );
        let directory_span = radial_selected_child_span(&directory_layer);
        assert_span_close(directory_span, 7.0 * TAU / 16.0, TAU / 16.0);
        assert!(
            radial_segment_at_angle(&directory_layer, 0.1).is_none(),
            "child layer should not render outside the inherited arc"
        );
        assert!(
            radial_segment_at_angle(
                &directory_layer,
                directory_span.start + directory_span.sweep / 2.0
            )
            .is_some(),
            "child layer should render inside the inherited arc"
        );
    }

    #[test]
    fn radial_selected_child_span_expands_tiny_selected_segments() {
        let layer = build_radial_layer(
            &[
                radial_row(RadialRowSpec {
                    key: "project:1",
                    label: "project-a",
                    kind: RollupRowKind::Project,
                    value: 99.0,
                    project_id: Some(1),
                    category: None,
                    action: None,
                    full_path: None,
                }),
                radial_row(RadialRowSpec {
                    key: "project:2",
                    label: "project-b",
                    kind: RollupRowKind::Project,
                    value: 1.0,
                    project_id: Some(2),
                    category: None,
                    action: None,
                    full_path: None,
                }),
            ],
            MetricLens::UncachedInput,
            Some("project:2"),
            RadialSpan::full(),
        );

        let span = radial_selected_child_span(&layer);
        let expected = TAU * (1.0 / 100.0);
        assert!(
            (span.sweep - expected).abs() < 0.001,
            "expected undistorted 1% sweep ({:.4}), got {:.4}",
            expected,
            span.sweep,
        );
    }

    #[test]
    fn radial_center_labels_stay_inside_fit_area_on_narrow_layout() -> Result<()> {
        let model = RadialModel {
            center: RadialCenter {
                scope_label: "QQQQQQ".to_string(),
                lens_label: "WWWWWW".to_string(),
                selection_label: "ZZZZZZZZZZ".to_string(),
            },
            layers: vec![RadialLayer {
                span: RadialSpan::full(),
                segments: vec![
                    RadialSegment {
                        value: 8.0,
                        bucket: RadialBucket::Project,
                        is_selected: true,
                    },
                    RadialSegment {
                        value: 4.0,
                        bucket: RadialBucket::Category,
                        is_selected: false,
                    },
                ],
                total_value: 12.0,
            }],
        };

        let width = 24;
        let height = 12;
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend)?;

        terminal.draw(|frame| {
            let pane = SunburstPane {
                model: &model,
                focused: false,
                config: SunburstRenderConfig::default(),
            };
            frame.render_widget(&pane, frame.area());
        })?;

        let buffer = terminal.backend().buffer();
        let inner = pane_block("Map", false).inner(Rect::new(0, 0, width, height));
        let center_area = radial_center_label_area(inner);

        let mut saw_center_text = false;
        for (index, cell) in buffer.content.iter().enumerate() {
            let x = (index as u16) % width;
            let y = (index as u16) / width;
            let symbol = cell.symbol();
            if matches!(symbol, "Q" | "W" | "Z") {
                saw_center_text = true;
                assert!(
                    x >= center_area.x
                        && x < center_area.x + center_area.width
                        && y >= center_area.y
                        && y < center_area.y + center_area.height,
                    "center label character escaped the fitted area at ({x}, {y}): {symbol:?}"
                );
            }
        }

        assert!(
            saw_center_text,
            "expected the fitted center labels to render"
        );
        Ok(())
    }

    #[test]
    fn radial_pane_renders_coarse_segments_through_coarse_raster_pipeline() -> Result<()> {
        let model = RadialModel {
            center: RadialCenter::default(),
            layers: vec![RadialLayer {
                span: RadialSpan::full(),
                segments: vec![
                    RadialSegment {
                        value: 8.0,
                        bucket: RadialBucket::Project,
                        is_selected: true,
                    },
                    RadialSegment {
                        value: 4.0,
                        bucket: RadialBucket::Category,
                        is_selected: false,
                    },
                ],
                total_value: 12.0,
            }],
        };

        let backend = TestBackend::new(24, 12);
        let mut terminal = Terminal::new(backend)?;
        terminal.draw(|frame| {
            frame.render_widget(
                &SunburstPane {
                    model: &model,
                    focused: true,
                    config: SunburstRenderConfig {
                        mode: SunburstRenderMode::Coarse,
                        ..SunburstRenderConfig::default()
                    },
                },
                frame.area(),
            );
        })?;

        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<Vec<_>>();
        assert!(
            rendered.contains(&"█"),
            "expected the coarse raster pipeline to emit the uniform fill glyph"
        );
        Ok(())
    }

    #[test]
    fn radial_pane_emits_braille_glyphs_when_braille_mode_is_selected() -> Result<()> {
        let model = RadialModel {
            center: RadialCenter::default(),
            layers: vec![RadialLayer {
                span: RadialSpan::full(),
                segments: vec![
                    RadialSegment {
                        value: 3.0,
                        bucket: RadialBucket::Project,
                        is_selected: true,
                    },
                    RadialSegment {
                        value: 1.0,
                        bucket: RadialBucket::Category,
                        is_selected: false,
                    },
                ],
                total_value: 4.0,
            }],
        };

        let backend = TestBackend::new(24, 12);
        let mut terminal = Terminal::new(backend)?;
        terminal.draw(|frame| {
            frame.render_widget(
                &SunburstPane {
                    model: &model,
                    focused: true,
                    config: SunburstRenderConfig {
                        mode: SunburstRenderMode::Braille,
                        ..SunburstRenderConfig::default()
                    },
                },
                frame.area(),
            );
        })?;

        let has_braille = terminal.backend().buffer().content.iter().any(|cell| {
            cell.symbol()
                .chars()
                .next()
                .is_some_and(|ch| ('\u{2801}'..='\u{28ff}').contains(&ch))
        });
        assert!(
            has_braille,
            "expected Braille render mode to emit Braille glyphs"
        );
        Ok(())
    }

    #[test]
    fn radial_pane_emits_quadrant_glyphs_when_quadrant_mode_is_selected() -> Result<()> {
        let model = RadialModel {
            center: RadialCenter::default(),
            layers: vec![RadialLayer {
                span: RadialSpan::full(),
                segments: vec![
                    RadialSegment {
                        value: 3.0,
                        bucket: RadialBucket::Project,
                        is_selected: true,
                    },
                    RadialSegment {
                        value: 1.0,
                        bucket: RadialBucket::Category,
                        is_selected: false,
                    },
                ],
                total_value: 4.0,
            }],
        };

        let backend = TestBackend::new(24, 12);
        let mut terminal = Terminal::new(backend)?;
        terminal.draw(|frame| {
            frame.render_widget(
                &SunburstPane {
                    model: &model,
                    focused: true,
                    config: SunburstRenderConfig {
                        mode: SunburstRenderMode::Quadrant,
                        ..SunburstRenderConfig::default()
                    },
                },
                frame.area(),
            );
        })?;

        let has_quadrant = terminal.backend().buffer().content.iter().any(|cell| {
            matches!(
                cell.symbol().chars().next(),
                Some(
                    '▘' | '▝'
                        | '▖'
                        | '▗'
                        | '▀'
                        | '▄'
                        | '▌'
                        | '▐'
                        | '▚'
                        | '▞'
                        | '▛'
                        | '▜'
                        | '▙'
                        | '▟'
                        | '█'
                )
            )
        });
        assert!(
            has_quadrant,
            "expected quadrant render mode to emit quadrant or half-block glyphs"
        );
        Ok(())
    }

    #[test]
    fn coarse_cached_indicator_uses_a_single_subtle_glyph() -> Result<()> {
        let model = RadialModel {
            center: RadialCenter::default(),
            layers: vec![RadialLayer {
                span: RadialSpan::full(),
                segments: vec![RadialSegment {
                    value: 1.0,
                    bucket: RadialBucket::Category,
                    is_selected: false,
                }],
                total_value: 1.0,
            }],
        };

        let backend = TestBackend::new(24, 12);
        let mut terminal = Terminal::new(backend)?;
        terminal.draw(|frame| {
            frame.render_widget(
                &SunburstPane {
                    model: &model,
                    focused: true,
                    config: SunburstRenderConfig {
                        mode: SunburstRenderMode::Coarse,
                        ..SunburstRenderConfig::default()
                    },
                },
                frame.area(),
            );
        })?;

        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<Vec<_>>();
        assert!(
            rendered.contains(&"█"),
            "expected cached-heavy coarse cells to use the uniform fill glyph"
        );
        assert!(
            !rendered.contains(&"·"),
            "expected no texture glyphs — coarse mode uses uniform fill"
        );
        Ok(())
    }

    #[test]
    fn render_header_shows_segmented_breadcrumbs() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.breadcrumb_targets = build_breadcrumb_targets(
            &RootView::ProjectHierarchy,
            &BrowsePath::ProjectAction {
                project_id: 1,
                category: "editing".to_string(),
                action: sample_action("read file"),
                parent_path: Some("/tmp/project-a/src".to_string()),
            },
            &sample_filter_options(),
            Some("/tmp/project-a"),
        );

        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend)?;
        terminal.draw(|frame| app.render(frame))?;

        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol().to_string())
            .collect::<String>();
        assert!(content.contains("view:"));
        assert!(content.contains("all projects"));
        assert!(content.contains("project-a"));
        assert!(content.contains(" > "));

        Ok(())
    }

    #[test]
    fn app_new_with_perf_logger_emits_tui_and_query_events() -> Result<()> {
        let temp = tempdir()?;
        let log_path = temp.path().join("perf.jsonl");
        let logger = PerfLogger::open_jsonl(log_path.clone())?;

        let _app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            Some(logger),
        )?;

        let operations = fs::read_to_string(log_path)?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|payload| {
                payload["operation"]
                    .as_str()
                    .map(std::string::ToString::to_string)
            })
            .collect::<Vec<_>>();

        assert!(operations.iter().any(|op| op == "tui.reload_view"));
        assert!(
            operations
                .iter()
                .any(|op| op == "tui.refresh_snapshot_status")
        );
        assert!(operations.iter().any(|op| op == "query.filter_options"));
        Ok(())
    }

    #[test]
    fn browse_fanout_logs_duplicate_reload_requests_on_project_path() -> Result<()> {
        let temp = tempdir()?;
        let validation = run_scale_validation(
            temp.path(),
            ScaleValidationSpec {
                project_count: 1,
                day_count: 2,
                sessions_per_day: 1,
            },
        )?;

        let log_path = temp.path().join("perf.jsonl");
        let logger = PerfLogger::open_jsonl(log_path.clone())?;

        let project_id = {
            let database = Database::open_read_only(&validation.db_path)?;
            let engine = QueryEngine::new(database.connection());
            engine
                .filter_options(&validation.final_snapshot)?
                .projects
                .first()
                .context("expected a visible project in the validation fixture")?
                .id
        };

        let _app = App::new(
            RuntimeConfig {
                app_name: "gnomon",
                state_dir: temp.path().to_path_buf(),
                config_path: temp.path().join("config.toml"),
                db_path: validation.db_path.clone(),
                source_root: validation.source_root.clone(),
                sources: ConfiguredSources::legacy_claude(&validation.source_root),
                project_identity: Default::default(),
                project_filters: Vec::new(),
            },
            validation.final_snapshot.clone(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            Some(StartupBrowseState {
                root: RootView::ProjectHierarchy,
                path: BrowsePath::Project { project_id },
            }),
            None,
            None,
            Some(logger.clone()),
        )?;

        let mut query_cache = QueryResultCache::default();
        let mut browse_cache = BrowseCacheStore::open(default_browse_cache_path(temp.path()))?;
        let database = Database::open_read_only(&validation.db_path)?;
        let query_engine = QueryEngine::with_perf(database.connection(), Some(logger.clone()));
        let ui_state = PersistedUiState {
            root: RootView::ProjectHierarchy,
            path: BrowsePath::Project { project_id },
            project_id: None,
            ..PersistedUiState::default()
        };

        let _targets = build_jump_targets_for_state(
            &query_engine,
            &mut query_cache,
            &mut browse_cache,
            Some(logger),
            validation.final_snapshot.clone(),
            ui_state,
            None,
        )?;

        let payloads = std::fs::read_to_string(log_path)?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let reload = payloads
            .iter()
            .find(|payload| payload["operation"] == "tui.reload_view")
            .context("missing reload perf event")?;
        let jump = payloads
            .iter()
            .find(|payload| payload["operation"] == "tui.build_jump_targets")
            .context("missing jump-target perf event")?;

        println!(
            "reload fanout: total={} distinct={} duplicate={} repeated={:?}",
            reload["browse_request_count"].as_u64().unwrap_or(0),
            reload["distinct_browse_request_count"]
                .as_u64()
                .unwrap_or(0),
            reload["duplicate_browse_request_count"]
                .as_u64()
                .unwrap_or(0),
            reload["repeated_browse_requests"],
        );
        println!(
            "jump fanout: total={} distinct={} duplicate={} repeated={:?}",
            jump["browse_request_count"].as_u64().unwrap_or(0),
            jump["distinct_browse_request_count"].as_u64().unwrap_or(0),
            jump["duplicate_browse_request_count"].as_u64().unwrap_or(0),
            jump["repeated_browse_requests"],
        );

        assert!(
            reload["duplicate_browse_request_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert!(
            reload["repeated_browse_requests"]
                .as_array()
                .is_some_and(|requests| requests
                    .iter()
                    .any(|request| request["count"].as_u64().unwrap_or(0) > 1))
        );
        assert!(jump["browse_request_count"].as_u64().unwrap_or(0) > 0);

        Ok(())
    }

    #[test]
    fn selection_move_logs_selection_change_and_context_load() -> Result<()> {
        let temp = tempdir()?;
        let validation = run_scale_validation(
            temp.path(),
            ScaleValidationSpec {
                project_count: 12,
                day_count: 8,
                sessions_per_day: 4,
            },
        )?;

        let log_path = temp.path().join("perf.jsonl");
        let logger = PerfLogger::open_jsonl(log_path.clone())?;

        let project_id = {
            let database = Database::open_read_only(&validation.db_path)?;
            let engine = QueryEngine::new(database.connection());
            engine
                .filter_options(&validation.final_snapshot)?
                .projects
                .first()
                .context("expected a visible project in the validation fixture")?
                .id
        };

        let mut app = App::new(
            RuntimeConfig {
                app_name: "gnomon",
                state_dir: temp.path().to_path_buf(),
                config_path: temp.path().join("config.toml"),
                db_path: validation.db_path.clone(),
                source_root: validation.source_root.clone(),
                sources: ConfiguredSources::legacy_claude(&validation.source_root),
                project_identity: Default::default(),
                project_filters: Vec::new(),
            },
            validation.final_snapshot.clone(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            Some(StartupBrowseState {
                root: RootView::ProjectHierarchy,
                path: BrowsePath::Project { project_id },
            }),
            None,
            None,
            Some(logger),
        )?;

        for _ in 0..5 {
            app.handle_normal_key(KeyEvent::from(KeyCode::Down))?;
        }

        let operations = fs::read_to_string(log_path)?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|payload| {
                payload["operation"]
                    .as_str()
                    .map(std::string::ToString::to_string)
            })
            .collect::<Vec<_>>();

        assert!(
            operations.iter().any(|op| op == "tui.selection_change"),
            "selection change should emit a perf event"
        );
        assert!(
            operations
                .iter()
                .any(|op| op == "tui.selection_context_load"),
            "selection context load should emit a perf event"
        );
        Ok(())
    }

    #[test]
    fn repeated_view_load_reuses_cached_query_results_for_same_snapshot() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.query_cache = QueryResultCache::default();
        let perf_logger = app.perf_logger.clone();
        let conn = app.database.connection();
        let query_engine = QueryEngine::with_perf(conn, perf_logger.clone());

        let _first = load_view_for_state(
            &query_engine,
            &mut app.query_cache,
            &mut app.browse_cache,
            perf_logger.clone(),
            app.snapshot.clone(),
            app.ui_state.clone(),
            None,
            None,
        )?;
        assert_eq!(
            app.query_cache.stats_for(QueryCacheDomain::FilterOptions),
            QueryCacheStats { hits: 0, misses: 1 }
        );
        assert_eq!(
            app.query_cache.stats_for(QueryCacheDomain::Browse),
            QueryCacheStats { hits: 0, misses: 1 }
        );
        assert_eq!(
            app.query_cache
                .stats_for(QueryCacheDomain::SnapshotCoverage),
            QueryCacheStats { hits: 0, misses: 1 }
        );

        let _second = load_view_for_state(
            &query_engine,
            &mut app.query_cache,
            &mut app.browse_cache,
            perf_logger,
            app.snapshot.clone(),
            app.ui_state.clone(),
            None,
            None,
        )?;
        assert_eq!(
            app.query_cache.stats_for(QueryCacheDomain::FilterOptions),
            QueryCacheStats { hits: 1, misses: 1 }
        );
        assert_eq!(
            app.query_cache.stats_for(QueryCacheDomain::Browse),
            QueryCacheStats { hits: 1, misses: 1 }
        );
        assert_eq!(
            app.query_cache
                .stats_for(QueryCacheDomain::SnapshotCoverage),
            QueryCacheStats { hits: 1, misses: 1 }
        );

        Ok(())
    }

    #[test]
    fn selection_context_cache_memoize_reuses_same_key() -> Result<()> {
        let mut cache = SelectionContextCache::default();
        let key = SelectionContextKey {
            snapshot: SnapshotBounds {
                max_publish_seq: 42,
                published_chunk_count: 1,
                upper_bound_utc: Some("2026-03-28T10:00:00Z".to_string()),
            },
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            selected_row_key: Some(TreeRowKey {
                parent_path: BrowsePath::Root,
                row_key: "project:1".to_string(),
            }),
            active_path: BrowsePath::Project { project_id: 1 },
        };
        let mut build_count = 0usize;

        let first = cache.memoize(key.clone(), || {
            build_count += 1;
            Ok::<_, anyhow::Error>(SelectionContextValue {
                active_path: BrowsePath::Project { project_id: 1 },
                current_project_root: Some("/tmp/project-a".to_string()),
                breadcrumb_targets: Vec::new(),
                radial_context: RadialContext::default(),
                selected_project_identity: None,
            })
        })?;
        let second = cache.memoize(key, || {
            build_count += 1;
            Ok::<_, anyhow::Error>(SelectionContextValue {
                active_path: BrowsePath::Project { project_id: 1 },
                current_project_root: Some("/tmp/project-a".to_string()),
                breadcrumb_targets: Vec::new(),
                radial_context: RadialContext::default(),
                selected_project_identity: None,
            })
        })?;

        assert_eq!(build_count, 1);
        assert_eq!(
            cache.stats(),
            SelectionContextCacheStats { hits: 1, misses: 1 }
        );
        assert_eq!(first.active_path, second.active_path);
        Ok(())
    }

    #[test]
    fn selection_context_cache_reuses_context_when_selection_returns_to_row() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.selection_context_cache = SelectionContextCache::default();
        app.ui_state.root = RootView::ProjectHierarchy;
        app.ui_state.path = BrowsePath::Root;

        let row_a = radial_row(RadialRowSpec {
            key: "project:1",
            label: "project-a",
            kind: RollupRowKind::Project,
            value: 9.0,
            project_id: Some(1),
            category: None,
            action: None,
            full_path: Some("/tmp/project-a"),
        });
        let row_b = radial_row(RadialRowSpec {
            key: "project:2",
            label: "project-b",
            kind: RollupRowKind::Project,
            value: 8.0,
            project_id: Some(2),
            category: None,
            action: None,
            full_path: Some("/tmp/project-b"),
        });

        app.raw_rows = vec![row_a.clone(), row_b.clone()];
        app.cache_rows(BrowsePath::Root, app.raw_rows.clone());
        app.visible_rows = vec![
            TreeRow {
                row: row_a,
                parent_path: BrowsePath::Root,
                node_path: Some(BrowsePath::Project { project_id: 1 }),
                depth: 0,
                is_expanded: false,
            },
            TreeRow {
                row: row_b,
                parent_path: BrowsePath::Root,
                node_path: Some(BrowsePath::Project { project_id: 2 }),
                depth: 0,
                is_expanded: false,
            },
        ];

        app.table_state.select(Some(0));
        app.refresh_active_context_for_selection();
        assert_eq!(
            app.selection_context_cache.stats(),
            SelectionContextCacheStats { hits: 0, misses: 1 }
        );

        app.table_state.select(Some(1));
        app.refresh_active_context_for_selection();
        assert_eq!(
            app.selection_context_cache.stats(),
            SelectionContextCacheStats { hits: 0, misses: 2 }
        );

        app.table_state.select(Some(0));
        app.refresh_active_context_for_selection();
        assert_eq!(
            app.selection_context_cache.stats(),
            SelectionContextCacheStats { hits: 1, misses: 2 }
        );
        assert_eq!(
            app.radial_context_path,
            BrowsePath::Project { project_id: 1 }
        );
        Ok(())
    }

    #[test]
    fn drilldown_selection_defaults_to_first_on_first_visit_and_restores_on_revisit() -> Result<()>
    {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        let root_ui_state = PersistedUiState {
            root: RootView::ProjectHierarchy,
            path: BrowsePath::Root,
            ..PersistedUiState::default()
        };
        let child_path = BrowsePath::Project { project_id: 4 };
        let child_ui_state = PersistedUiState {
            root: RootView::ProjectHierarchy,
            path: child_path.clone(),
            ..PersistedUiState::default()
        };
        let root_rows = vec![
            radial_row(RadialRowSpec {
                key: "project:1",
                label: "project-1",
                kind: RollupRowKind::Project,
                value: 1.0,
                project_id: Some(1),
                category: None,
                action: None,
                full_path: Some("/tmp/project-1"),
            }),
            radial_row(RadialRowSpec {
                key: "project:2",
                label: "project-2",
                kind: RollupRowKind::Project,
                value: 2.0,
                project_id: Some(2),
                category: None,
                action: None,
                full_path: Some("/tmp/project-2"),
            }),
            radial_row(RadialRowSpec {
                key: "project:3",
                label: "project-3",
                kind: RollupRowKind::Project,
                value: 3.0,
                project_id: Some(3),
                category: None,
                action: None,
                full_path: Some("/tmp/project-3"),
            }),
            radial_row(RadialRowSpec {
                key: "project:4",
                label: "project-4",
                kind: RollupRowKind::Project,
                value: 4.0,
                project_id: Some(4),
                category: None,
                action: None,
                full_path: Some("/tmp/project-4"),
            }),
        ];
        let child_rows = vec![
            radial_row(RadialRowSpec {
                key: "category:editing",
                label: "editing",
                kind: RollupRowKind::ActionCategory,
                value: 1.0,
                project_id: Some(4),
                category: Some("editing"),
                action: None,
                full_path: Some("/tmp/project-4"),
            }),
            radial_row(RadialRowSpec {
                key: "category:debugging",
                label: "debugging",
                kind: RollupRowKind::ActionCategory,
                value: 1.0,
                project_id: Some(4),
                category: Some("debugging"),
                action: None,
                full_path: Some("/tmp/project-4"),
            }),
            radial_row(RadialRowSpec {
                key: "category:tests",
                label: "tests",
                kind: RollupRowKind::ActionCategory,
                value: 1.0,
                project_id: Some(4),
                category: Some("tests"),
                action: None,
                full_path: Some("/tmp/project-4"),
            }),
            radial_row(RadialRowSpec {
                key: "category:docs",
                label: "docs",
                kind: RollupRowKind::ActionCategory,
                value: 1.0,
                project_id: Some(4),
                category: Some("docs"),
                action: None,
                full_path: Some("/tmp/project-4"),
            }),
        ];

        app.apply_loaded_view(loaded_view_for_test(
            root_ui_state.clone(),
            root_rows.clone(),
            None,
        ))?;
        assert_eq!(app.table_state.selected(), Some(0));

        app.table_state.select(Some(3));
        app.refresh_active_context_for_selection();
        let parent_selected_key = app.selected_tree_row_key();

        app.apply_loaded_view(loaded_view_for_test(
            child_ui_state.clone(),
            child_rows.clone(),
            parent_selected_key.clone(),
        ))?;
        assert_eq!(app.table_state.selected(), Some(0));

        app.table_state.select(Some(3));
        app.refresh_active_context_for_selection();
        let child_selected_key = app.selected_tree_row_key();

        app.apply_loaded_view(loaded_view_for_test(
            root_ui_state,
            root_rows,
            child_selected_key,
        ))?;
        assert_eq!(app.table_state.selected(), Some(3));

        app.apply_loaded_view(loaded_view_for_test(
            child_ui_state,
            child_rows,
            parent_selected_key,
        ))?;
        assert_eq!(app.table_state.selected(), Some(3));
        Ok(())
    }

    #[test]
    fn prefetch_batch_logs_source_counts() -> Result<()> {
        let temp = tempdir()?;
        let log_path = temp.path().join("perf.jsonl");
        let logger = PerfLogger::open_jsonl(log_path.clone())?;

        let mut perf = prefetch_batch_perf_scope(Some(logger));
        std::thread::sleep(Duration::from_millis(20));
        perf.field("request_count", 4usize);
        perf.field("browse_request_count", 6usize);
        perf.field("distinct_browse_request_count", 4usize);
        perf.field("duplicate_browse_request_count", 2usize);
        perf.field("cache_memory_hit_count", 1usize);
        perf.field("cache_persisted_hit_count", 2usize);
        perf.field("cache_live_query_count", 3usize);
        perf.finish_ok();

        let payloads = fs::read_to_string(log_path)?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let prefetch = payloads
            .iter()
            .find(|payload| payload["operation"] == "tui.prefetch_batch")
            .context("missing prefetch batch perf event")?;

        assert_eq!(prefetch["request_count"].as_u64(), Some(4));
        assert_eq!(prefetch["browse_request_count"].as_u64(), Some(6));
        assert_eq!(prefetch["distinct_browse_request_count"].as_u64(), Some(4));
        assert_eq!(prefetch["duplicate_browse_request_count"].as_u64(), Some(2));
        assert_eq!(prefetch["cache_memory_hit_count"].as_u64(), Some(1));
        assert_eq!(prefetch["cache_persisted_hit_count"].as_u64(), Some(2));
        assert_eq!(prefetch["cache_live_query_count"].as_u64(), Some(3));
        assert!(prefetch["duration_ms"].as_f64().unwrap_or(0.0) >= 10.0);
        Ok(())
    }

    #[test]
    fn query_result_cache_swaps_snapshot_buckets_cleanly() -> Result<()> {
        let snapshot_a = SnapshotBounds {
            max_publish_seq: 1,
            published_chunk_count: 1,
            upper_bound_utc: Some("2026-03-28T10:00:00Z".to_string()),
        };
        let snapshot_b = SnapshotBounds {
            max_publish_seq: 2,
            published_chunk_count: 2,
            upper_bound_utc: Some("2026-03-28T11:00:00Z".to_string()),
        };
        let mut cache = QueryResultCache::default();
        let mut build_count = 0usize;

        let first = cache.memoize(QueryCacheDomain::Browse, &snapshot_a, &"root", || {
            build_count += 1;
            Ok::<_, anyhow::Error>(vec!["snapshot-a".to_string()])
        })?;
        let second = cache.memoize(QueryCacheDomain::Browse, &snapshot_a, &"root", || {
            build_count += 1;
            Ok::<_, anyhow::Error>(vec!["should-not-run".to_string()])
        })?;
        let refreshed = cache.memoize(QueryCacheDomain::Browse, &snapshot_b, &"root", || {
            build_count += 1;
            Ok::<_, anyhow::Error>(vec!["snapshot-b".to_string()])
        })?;

        assert_eq!(first, vec!["snapshot-a".to_string()]);
        assert_eq!(second, vec!["snapshot-a".to_string()]);
        assert_eq!(refreshed, vec!["snapshot-b".to_string()]);
        assert_eq!(build_count, 2);
        assert_eq!(
            cache.stats_for(QueryCacheDomain::Browse),
            QueryCacheStats { hits: 1, misses: 2 }
        );
        assert_eq!(cache.snapshot_count(), 2);

        cache.retain_snapshot(&snapshot_b);

        assert_eq!(cache.snapshot_count(), 1);
        Ok(())
    }

    fn sample_row(label: &str, full_path: Option<String>) -> RollupRow {
        RollupRow {
            kind: RollupRowKind::Directory,
            key: format!("path:{label}"),
            label: label.to_string(),
            metrics: gnomon_core::query::MetricTotals {
                uncached_input: 5.0,
                cached_input: 1.0,
                gross_input: 6.0,
                output: 2.0,
                total: 8.0,
            },
            indicators: gnomon_core::query::MetricIndicators {
                selected_lens_last_5_hours: 5.0,
                selected_lens_last_week: 6.0,
                uncached_input_reference: 5.0,
            },
            item_count: 1,
            opportunities: OpportunitySummary::default(),
            skill_attribution: None,
            project_id: Some(1),
            project_identity: None,
            category: Some("editing".to_string()),
            action: Some(sample_action("read file")),
            full_path,
        }
    }

    struct RadialRowSpec<'a> {
        key: &'a str,
        label: &'a str,
        kind: RollupRowKind,
        value: f64,
        project_id: Option<i64>,
        category: Option<&'a str>,
        action: Option<ActionKey>,
        full_path: Option<&'a str>,
    }

    fn radial_row(spec: RadialRowSpec<'_>) -> RollupRow {
        RollupRow {
            kind: spec.kind,
            key: spec.key.to_string(),
            label: spec.label.to_string(),
            metrics: gnomon_core::query::MetricTotals {
                uncached_input: spec.value,
                cached_input: 0.0,
                gross_input: spec.value,
                output: 0.0,
                total: spec.value,
            },
            indicators: gnomon_core::query::MetricIndicators {
                selected_lens_last_5_hours: spec.value,
                selected_lens_last_week: spec.value,
                uncached_input_reference: spec.value,
            },
            item_count: 1,
            opportunities: OpportunitySummary::default(),
            skill_attribution: None,
            project_id: spec.project_id,
            project_identity: None,
            category: spec.category.map(str::to_string),
            action: spec.action,
            full_path: spec.full_path.map(str::to_string),
        }
    }

    fn sample_filter_options() -> FilterOptions {
        FilterOptions {
            projects: vec![gnomon_core::query::ProjectFilterOption {
                id: 1,
                display_name: "project-a".to_string(),
            }],
            models: Vec::new(),
            categories: Vec::new(),
            actions: Vec::new(),
        }
    }

    fn loaded_view_for_test(
        ui_state: PersistedUiState,
        raw_rows: Vec<RollupRow>,
        selected_key: Option<TreeRowKey>,
    ) -> LoadedView {
        LoadedView {
            snapshot: SnapshotBounds::bootstrap(),
            snapshot_coverage: SnapshotCoverageSummary::default(),
            ui_state,
            filter_options: sample_filter_options(),
            raw_rows,
            breadcrumb_targets: Vec::new(),
            radial_context: RadialContext::default(),
            current_project_root: None,
            selected_key,
        }
    }

    fn assert_span_close(span: RadialSpan, expected_start: f64, expected_sweep: f64) {
        assert!(
            (span.start - expected_start).abs() < 1e-9,
            "unexpected span start: {} != {}",
            span.start,
            expected_start
        );
        assert!(
            (span.sweep - expected_sweep).abs() < 1e-9,
            "unexpected span sweep: {} != {}",
            span.sweep,
            expected_sweep
        );
    }

    fn sample_action(label: &str) -> ActionKey {
        ActionKey {
            classification_state: ClassificationState::Classified,
            normalized_action: Some(label.to_string()),
            command_family: None,
            base_command: None,
        }
    }

    fn make_test_config(dir: &std::path::Path) -> RuntimeConfig {
        let source_root = dir.join("source");
        RuntimeConfig {
            app_name: "gnomon",
            state_dir: dir.to_path_buf(),
            config_path: dir.join("config.toml"),
            db_path: dir.join("test.sqlite3"),
            source_root: source_root.clone(),
            sources: ConfiguredSources::legacy_claude(&source_root),
            project_identity: Default::default(),
            project_filters: Vec::new(),
        }
    }

    fn render_to_string(
        config: RuntimeConfig,
        snapshot: SnapshotBounds,
        startup_open_reason: StartupOpenReason,
        latest_snapshot: Option<SnapshotBounds>,
    ) -> Result<String> {
        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend)?;
        let mut app = App::new(
            config,
            snapshot.clone(),
            startup_open_reason,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        if let Some(latest_snapshot) = latest_snapshot {
            app.latest_snapshot = latest_snapshot;
            app.has_newer_snapshot = app.latest_snapshot.max_publish_seq > snapshot.max_publish_seq;
        }
        terminal.draw(|frame| app.render(frame))?;
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol().to_string())
            .collect::<String>();
        Ok(content)
    }

    fn render_app_to_string(app: &mut App, width: u16, height: u16) -> Result<String> {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend)?;
        terminal.draw(|frame| app.render(frame))?;
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol().to_string())
            .collect::<String>();
        Ok(content)
    }

    #[test]
    fn startup_browse_state_defaults_fresh_sessions_to_root() {
        let mut state = PersistedUiState {
            root: RootView::CategoryHierarchy,
            path: BrowsePath::Category {
                category: "editing".to_string(),
            },
            project_id: Some(1),
            action_category: Some("editing".to_string()),
            action: Some(sample_action("read file")),
            row_filter: "src".to_string(),
            ..PersistedUiState::default()
        };

        state.apply_startup_browse_state(None);

        assert_eq!(state.root, RootView::CategoryHierarchy);
        assert_eq!(state.path, BrowsePath::Root);
        assert_eq!(state.project_id, None);
        assert_eq!(state.action_category, None);
        assert_eq!(state.action, None);
        assert_eq!(state.row_filter, "src");
    }

    #[test]
    fn startup_browse_state_applies_explicit_drill_down() {
        let mut state = PersistedUiState {
            project_id: Some(1),
            action_category: Some("editing".to_string()),
            action: Some(sample_action("read file")),
            row_filter: "src".to_string(),
            ..PersistedUiState::default()
        };
        let startup_browse_state = StartupBrowseState {
            root: RootView::ProjectHierarchy,
            path: BrowsePath::ProjectCategory {
                project_id: 7,
                category: "editing".to_string(),
            },
        };

        state.apply_startup_browse_state(Some(startup_browse_state));

        assert_eq!(state.root, RootView::ProjectHierarchy);
        assert_eq!(
            state.path,
            BrowsePath::ProjectCategory {
                project_id: 7,
                category: "editing".to_string(),
            }
        );
        assert_eq!(state.project_id, None);
        assert_eq!(state.action_category, None);
        assert_eq!(state.action, None);
        assert_eq!(state.row_filter, "src");
    }

    #[test]
    fn filter_summary_describes_structural_scope_without_filter_language() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.ui_state.project_id = Some(1);
        app.ui_state.action_category = Some("editing".to_string());
        app.ui_state.action = Some(sample_action("read file"));
        app.ui_state.row_filter = "src".to_string();

        let summary = app.filter_summary();

        assert!(summary.contains("project: #1"));
        assert!(summary.contains("category: editing"));
        assert!(summary.contains("action: read file"));
        assert!(summary.contains("row src"));
        assert!(!summary.contains("project scope"));
        assert!(!summary.contains("category scope"));
        assert!(!summary.contains("action scope"));
        Ok(())
    }

    #[test]
    fn render_produces_three_pane_layout() -> Result<()> {
        let temp = tempdir()?;
        let content = render_to_string(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
        )?;
        assert!(
            content.contains("Status"),
            "Status pane header not rendered"
        );
        assert!(
            content.contains("[snapshot]"),
            "snapshot badge not rendered"
        );
        assert!(
            content.contains("no imported data is visible yet"),
            "empty snapshot state not rendered in header"
        );
        assert!(content.contains("Keys"), "Keys pane header not rendered");
        Ok(())
    }

    #[test]
    fn render_shows_startup_context_in_header() -> Result<()> {
        let temp = tempdir()?;
        let content = render_to_string(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
        )?;
        assert!(
            content.contains("▣ gnomon"),
            "app name not rendered in header"
        );
        assert!(
            content.contains("[refresh]"),
            "refresh badge not rendered in header"
        );
        assert!(
            content.contains("[snapshot]"),
            "snapshot badge not rendered in header"
        );
        assert!(
            content.contains("Background import never changes the visible view"),
            "refresh policy not rendered in header"
        );
        Ok(())
    }

    #[test]
    fn render_omits_status_focus_label() -> Result<()> {
        let temp = tempdir()?;
        let content = render_to_string(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
        )?;

        assert!(
            !content.contains("focus:"),
            "focus should be communicated by pane styling, not status text"
        );
        Ok(())
    }

    #[test]
    fn wide_layout_map_width_tracks_terminal_height() {
        let main_area = Rect::new(0, 0, 240, 49);

        let panes = wide_layout_panes(main_area);

        assert_eq!(panes[0].width, 107);
        assert_eq!(panes[1].width, 133);
    }

    #[test]
    fn wide_layout_map_width_preserves_statistics_minimum_width() {
        let main_area = Rect::new(0, 0, 120, 29);

        let panes = wide_layout_panes(main_area);

        assert!(panes[1].width >= STATISTICS_PANE_MIN_WIDTH);
        assert!(panes[0].width < main_area.width);
    }

    #[test]
    fn wide_layout_marks_table_as_the_focused_pane() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        let content = render_app_to_string(&mut app, 140, 40)?;

        assert!(
            content.contains("◆ STATISTICS"),
            "focused statistics title should stand out"
        );
        assert!(
            content.contains("◦ Map"),
            "unfocused map title should remain visible"
        );
        assert!(
            !content.contains("focus:"),
            "wide layout should not rely on header focus copy"
        );
        Ok(())
    }

    #[test]
    fn narrow_layout_marks_radial_as_the_focused_pane() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.focused_pane = PaneFocus::Radial;
        app.ui_state.pane_mode = PaneMode::Radial;

        let content = render_app_to_string(&mut app, 100, 40)?;

        assert!(
            content.contains("◆ MAP"),
            "focused map title should stand out"
        );
        assert!(
            !content.contains("Statistics"),
            "narrow map layout should show one pane"
        );
        assert!(
            !content.contains("focus:"),
            "narrow layout should not reintroduce status focus copy"
        );
        Ok(())
    }

    #[test]
    fn pane_styles_make_unfocused_panes_visibly_quieter() {
        assert_eq!(pane_border_style(true).fg, Some(Color::Cyan));
        assert_eq!(pane_border_style(false).fg, Some(Color::Gray));
        assert_eq!(pane_title_style(true).fg, Some(Color::Cyan));
        assert_eq!(pane_title_style(false).fg, Some(Color::DarkGray));
        assert_eq!(table_header_style(true).fg, Some(Color::Cyan));
        assert_eq!(table_header_style(false).fg, Some(Color::Gray));
        assert_eq!(radial_center_label_style(true).fg, Some(Color::White));
        assert_eq!(radial_center_label_style(false).fg, Some(Color::Gray));
        assert_eq!(badge_style(BadgeTone::Accent).fg, Some(Color::Cyan));
        assert_eq!(badge_style(BadgeTone::Info).fg, Some(Color::White));
        assert_eq!(badge_style(BadgeTone::Success).fg, Some(Color::Green));
        assert_eq!(badge_style(BadgeTone::Warning).fg, Some(Color::Yellow));
        assert_eq!(badge_style(BadgeTone::Muted).fg, Some(Color::DarkGray));
    }

    #[test]
    fn toggle_mark_uses_checkbox_glyphs() {
        assert_eq!(
            toggle_mark(&[OptionalColumn::Kind], OptionalColumn::Kind),
            "☑"
        );
        assert_eq!(toggle_mark(&[], OptionalColumn::Kind), "☐");
    }

    #[test]
    fn render_tree_label_uses_box_drawing_glyphs() {
        let row = TreeRow {
            row: sample_row("src", Some("/tmp/project/src".to_string())),
            parent_path: BrowsePath::Root,
            node_path: Some(BrowsePath::Project { project_id: 1 }),
            depth: 1,
            is_expanded: false,
        };

        assert_eq!(render_tree_label(&row), "  ▸ src");
    }

    #[test]
    fn render_keeps_layout_intact_with_long_import_status_message() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.status_message = Some(StatusMessage::error(
            "deferred import failed for 2 chunks; first error: unable to normalize source file source/path/to/session.jsonl: unexpected EOF while parsing malformed json",
        ));

        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend)?;
        terminal.draw(|frame| app.render(frame))?;
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol().to_string())
            .collect::<String>();

        assert!(
            content.contains("Status"),
            "status pane header not rendered"
        );
        assert!(content.contains("Keys"), "keys pane header not rendered");
        Ok(())
    }

    #[test]
    fn startup_import_failures_do_not_set_tui_status_message() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            Some("startup import failed for 1 chunk".to_string()),
            None,
            None,
            None,
            None,
            None,
        )?;

        assert!(
            app.status_message.is_none(),
            "startup import failures should not be rendered in the TUI"
        );

        app.apply_status_update(StartupWorkerEvent::DeferredFailures {
            deferred_status_message: Some("deferred import failed for 2 chunks".to_string()),
        });
        assert!(
            app.status_message.is_none(),
            "deferred import failures should not be rendered in the TUI"
        );

        app.apply_status_update(StartupWorkerEvent::StartupSettled {
            startup_status_message: Some("startup import failed for 1 chunk".to_string()),
        });
        assert!(
            app.status_message.is_none(),
            "settled startup failures should not be rendered in the TUI"
        );

        Ok(())
    }

    #[test]
    fn stale_view_result_does_not_overwrite_newer_pending_view() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        let original_root = app.ui_state.root;
        let mut query_cache = QueryResultCache::default();
        let perf_logger = app.perf_logger.clone();
        let conn = app.database.connection();
        let query_engine = QueryEngine::with_perf(conn, perf_logger.clone());

        let mut loaded_view = load_view_for_state(
            &query_engine,
            &mut query_cache,
            &mut app.browse_cache,
            perf_logger,
            app.snapshot.clone(),
            app.ui_state.clone(),
            None,
            None,
        )?;
        loaded_view.ui_state.root = RootView::CategoryHierarchy;
        app.pending_async_work.begin_view(2, "loading view");

        app.apply_query_result(QueryWorkerResult::ViewLoaded(ViewLoadResult {
            sequence: 1,
            loaded_view,
        }))?;

        assert_eq!(app.ui_state.root, original_root);
        assert_eq!(
            app.pending_async_work
                .view
                .current
                .map(|request| request.sequence),
            Some(2)
        );
        Ok(())
    }

    #[test]
    fn stale_view_progress_does_not_overwrite_newer_pending_view() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.pending_async_work.begin_view(2, "loading view");

        app.apply_query_result(QueryWorkerResult::Progress(QueryProgressUpdate {
            sequence: 1,
            task: PendingTaskKind::View,
            phase: "browsing current path".to_string(),
            progress: Some(PhaseProgress {
                current: 4,
                total: VIEW_LOAD_PHASE_TOTAL,
            }),
        }))?;

        let request = app
            .pending_async_work
            .view
            .current
            .expect("pending view request");
        assert_eq!(request.sequence, 2);
        assert_eq!(request.state, PendingTaskState::Queued);
        assert!(request.phase.is_none());
        assert!(request.progress.is_none());
        Ok(())
    }

    #[test]
    fn stale_jump_result_does_not_overwrite_newer_pending_query() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.jump_state.query = "doc".to_string();
        app.pending_async_work
            .begin_jump(4, "building jump targets");

        app.apply_query_result(QueryWorkerResult::JumpTargetsBuilt(JumpTargetsResult {
            sequence: 3,
            targets: vec![JumpTarget {
                label: "documentation".to_string(),
                detail: "category".to_string(),
                root: RootView::CategoryHierarchy,
                path: BrowsePath::Category {
                    category: "Documentation".to_string(),
                },
            }],
        }))?;

        assert!(app.jump_state.matches.is_empty());
        assert_eq!(
            app.pending_async_work
                .jump
                .current
                .map(|request| request.sequence),
            Some(4)
        );
        Ok(())
    }

    #[test]
    fn progress_update_marks_request_running_with_phase_details() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.pending_async_work
            .begin_jump(3, "building jump targets");

        app.apply_query_result(QueryWorkerResult::Progress(QueryProgressUpdate {
            sequence: 3,
            task: PendingTaskKind::Jump,
            phase: "walking category hierarchy".to_string(),
            progress: Some(PhaseProgress {
                current: 3,
                total: JUMP_TARGET_PHASE_TOTAL,
            }),
        }))?;

        let request = app
            .pending_async_work
            .jump
            .current
            .expect("pending jump request");
        assert_eq!(request.state, PendingTaskState::Running);
        assert_eq!(request.phase, Some("walking category hierarchy"));
        assert_eq!(
            request
                .progress
                .map(|progress| (progress.current, progress.total)),
            Some((3, JUMP_TARGET_PHASE_TOTAL))
        );
        Ok(())
    }

    #[test]
    fn render_shows_pending_activity_immediately() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.pending_async_work.begin_view(1, "loading view");

        let content = render_app_to_string(&mut app, 120, 40)?;

        assert!(content.contains("[activity]"));
        assert!(content.contains("queued loading view"));
        Ok(())
    }

    #[test]
    fn render_shows_startup_import_activity_from_progress_updates() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::TimedOut,
            None,
            Some(StartupProgressUpdate {
                label: "rebuilding database",
                current: 2,
                total: 5,
                detail: "git:/projects/demo:2026-03-29".to_string(),
            }),
            None,
            None,
            None,
            None,
        )?;

        let content = render_app_to_string(&mut app, 120, 40)?;

        assert!(content.contains("[activity]"));
        assert!(content.contains("rebuilding database"));
        assert!(content.contains("[2/5]"));
        assert!(content.contains("git:/projects/demo:2026-03-29"));
        Ok(())
    }

    #[test]
    fn render_shows_running_and_superseding_view_requests_together() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.pending_async_work.begin_view(1, "loading view");
        app.apply_query_result(QueryWorkerResult::Progress(QueryProgressUpdate {
            sequence: 1,
            task: PendingTaskKind::View,
            phase: "browsing current path".to_string(),
            progress: Some(PhaseProgress {
                current: 4,
                total: VIEW_LOAD_PHASE_TOTAL,
            }),
        }))?;
        app.pending_async_work.begin_view(2, "refreshing snapshot");

        let content = render_app_to_string(&mut app, 120, 40)?;

        assert!(content.contains("[activity]"));
        assert!(content.contains("loading view"));
        assert!(content.contains("[4/8]"));
        assert!(content.contains("browsing current path"));
        assert!(content.contains("queued refreshing snapshot"));
        assert!(content.contains("superseding older request"));
        Ok(())
    }

    #[test]
    fn stale_view_completion_promotes_superseding_request_without_applying_result() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        let original_root = app.ui_state.root;

        app.pending_async_work.begin_view(1, "loading view");
        app.apply_query_result(QueryWorkerResult::Progress(QueryProgressUpdate {
            sequence: 1,
            task: PendingTaskKind::View,
            phase: "browsing current path".to_string(),
            progress: Some(PhaseProgress {
                current: 4,
                total: VIEW_LOAD_PHASE_TOTAL,
            }),
        }))?;
        app.pending_async_work.begin_view(2, "refreshing snapshot");
        let mut query_cache = QueryResultCache::default();
        let perf_logger = app.perf_logger.clone();
        let conn = app.database.connection();
        let query_engine = QueryEngine::with_perf(conn, perf_logger.clone());

        let mut loaded_view = load_view_for_state(
            &query_engine,
            &mut query_cache,
            &mut app.browse_cache,
            perf_logger,
            app.snapshot.clone(),
            app.ui_state.clone(),
            None,
            None,
        )?;
        loaded_view.ui_state.root = RootView::CategoryHierarchy;

        app.apply_query_result(QueryWorkerResult::ViewLoaded(ViewLoadResult {
            sequence: 1,
            loaded_view,
        }))?;

        assert_eq!(app.ui_state.root, original_root);
        let current = app
            .pending_async_work
            .view
            .current
            .expect("superseding view request should remain pending");
        assert_eq!(current.sequence, 2);
        assert_eq!(current.state, PendingTaskState::Queued);
        assert!(app.pending_async_work.view.next.is_none());
        Ok(())
    }

    #[test]
    fn render_shows_running_phase_text_in_narrow_layout() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.focused_pane = PaneFocus::Radial;
        app.ui_state.pane_mode = PaneMode::Radial;
        app.pending_async_work.begin_view(1, "refreshing snapshot");
        app.apply_query_result(QueryWorkerResult::Progress(QueryProgressUpdate {
            sequence: 1,
            task: PendingTaskKind::View,
            phase: "recomputing map context".to_string(),
            progress: Some(PhaseProgress {
                current: 7,
                total: VIEW_LOAD_PHASE_TOTAL,
            }),
        }))?;

        let detail = app
            .header_detail_line()
            .spans
            .into_iter()
            .map(|span| span.content.into_owned())
            .collect::<String>();

        assert!(detail.contains("[activity]"));
        assert!(detail.contains("refreshing snapshot"));
        assert!(detail.contains("[7/8]"));
        assert!(detail.contains("recomputing map context"));
        Ok(())
    }

    #[test]
    fn render_footer_demotes_structural_scope_controls() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        let content = render_app_to_string(&mut app, 200, 40)?;

        assert!(content.contains("p/c/a cycle scope"));
        assert!(content.contains("Space clear scope/root"));
        assert!(content.contains("t/m filters"));
        assert!(content.contains("quit"));
        Ok(())
    }

    #[test]
    fn render_footer_contains_quit_hint() -> Result<()> {
        let temp = tempdir()?;
        let content = render_to_string(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
        )?;
        assert!(
            content.contains("quit"),
            "footer should contain 'quit' key hint"
        );
        Ok(())
    }

    #[test]
    fn selected_node_metadata_text_uses_project_root_and_identity_reason() {
        let mut row = sample_row("project-a", Some("/tmp/project-a".to_string()));
        row.kind = RollupRowKind::Project;
        row.label = "project-a".to_string();
        let metadata = SelectedProjectIdentity {
            identity_kind: ProjectIdentityKind::Path,
            root_path: "/tmp/project-a".to_string(),
            identity_reason: Some("git root could not be resolved from cwd".to_string()),
        };

        let text = selected_node_metadata_text(&row, Some(&metadata))
            .expect("project rows should surface metadata");

        assert!(text.contains("root /tmp/project-a"));
        assert!(text.contains("identity path"));
        assert!(text.contains("fallback git root could not be resolved from cwd"));
    }

    #[test]
    fn selected_node_metadata_text_uses_full_path_for_file_rows() {
        let mut row = sample_row("lib.rs", Some("/tmp/project-a/src/lib.rs".to_string()));
        row.kind = RollupRowKind::File;
        row.full_path = Some("/tmp/project-a/src/lib.rs".to_string());
        row.label = "lib.rs".to_string();

        let text =
            selected_node_metadata_text(&row, None).expect("file rows should surface their path");

        assert_eq!(text, "path /tmp/project-a/src/lib.rs");
    }

    #[test]
    fn project_identity_for_reads_identity_metadata_from_project_table() -> Result<()> {
        let temp = tempdir()?;
        let app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.database.connection().execute(
            "
            INSERT INTO project (
                identity_kind,
                canonical_key,
                display_name,
                root_path,
                git_root_path,
                git_origin,
                identity_reason
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ",
            params![
                "path",
                "path:/tmp/project-a",
                "project-a",
                "/tmp/project-a",
                Option::<String>::None,
                Option::<String>::None,
                "git root could not be resolved from cwd",
            ],
        )?;

        let metadata = app
            .project_identity_for(1)?
            .context("project metadata should exist")?;

        assert_eq!(metadata.identity_kind, ProjectIdentityKind::Path);
        assert_eq!(metadata.root_path, "/tmp/project-a");
        assert_eq!(
            metadata.identity_reason.as_deref(),
            Some("git root could not be resolved from cwd")
        );
        Ok(())
    }

    #[test]
    fn sqlite_snapshot_upper_bound_builds_time_window_filter() -> Result<()> {
        let filter = TimeWindowPreset::Last24Hours.to_filter(&SnapshotBounds {
            max_publish_seq: 1,
            published_chunk_count: 1,
            upper_bound_utc: Some("2026-03-27 18:28:38".to_string()),
        })?;

        assert_eq!(
            filter,
            Some(TimeWindowFilter {
                start_at_utc: Some("2026-03-26T18:28:38Z".to_string()),
                end_at_utc: None,
            })
        );

        Ok(())
    }

    #[test]
    fn render_shows_partial_coverage_when_startup_times_out() -> Result<()> {
        let temp = tempdir()?;
        let content = render_to_string(
            make_test_config(temp.path()),
            SnapshotBounds {
                max_publish_seq: 2,
                published_chunk_count: 2,
                upper_bound_utc: Some("2026-03-27 18:28:38".to_string()),
            },
            StartupOpenReason::TimedOut,
            None,
        )?;

        assert!(
            content.contains("refresh:"),
            "refresh line not rendered in header"
        );
        assert_eq!(
            snapshot_refresh_text(
                &SnapshotBounds {
                    max_publish_seq: 2,
                    published_chunk_count: 2,
                    upper_bound_utc: Some("2026-03-27 18:28:38".to_string()),
                },
                &SnapshotBounds {
                    max_publish_seq: 2,
                    published_chunk_count: 2,
                    upper_bound_utc: Some("2026-03-27 18:28:38".to_string()),
                },
                StartupOpenReason::TimedOut,
                false,
            ),
            "refresh: manual only. Startup opened before the last 24 hours finished importing, so this snapshot may still be partial."
        );

        Ok(())
    }

    #[test]
    fn render_shows_full_coverage_when_startup_waits_for_full_import() -> Result<()> {
        let temp = tempdir()?;
        let content = render_to_string(
            make_test_config(temp.path()),
            SnapshotBounds {
                max_publish_seq: 4,
                published_chunk_count: 4,
                upper_bound_utc: Some("2026-03-27 18:28:38".to_string()),
            },
            StartupOpenReason::FullImportReady,
            None,
        )?;

        assert!(
            content.contains("refresh:"),
            "refresh line not rendered in header"
        );
        assert_eq!(
            snapshot_refresh_text(
                &SnapshotBounds {
                    max_publish_seq: 4,
                    published_chunk_count: 4,
                    upper_bound_utc: Some("2026-03-27 18:28:38".to_string()),
                },
                &SnapshotBounds {
                    max_publish_seq: 4,
                    published_chunk_count: 4,
                    upper_bound_utc: Some("2026-03-27 18:28:38".to_string()),
                },
                StartupOpenReason::FullImportReady,
                false,
            ),
            "refresh: manual only. Startup waited for the full import to finish, so this snapshot is already current."
        );

        Ok(())
    }

    #[test]
    fn render_shows_refresh_target_without_auto_applying_it() -> Result<()> {
        let temp = tempdir()?;
        let content = render_to_string(
            make_test_config(temp.path()),
            SnapshotBounds {
                max_publish_seq: 2,
                published_chunk_count: 2,
                upper_bound_utc: Some("2026-03-27T18:28:38Z".to_string()),
            },
            StartupOpenReason::Last24hReady,
            Some(SnapshotBounds {
                max_publish_seq: 3,
                published_chunk_count: 3,
                upper_bound_utc: Some("2026-03-27T19:45:00Z".to_string()),
            }),
        )?;

        assert!(
            content.contains(
                "snapshot: showing imported data through 2026-03-27T18:28:38Z (newer data is ready)"
            ),
            "current pinned snapshot summary not rendered"
        );
        assert!(
            content.contains("newer imported data is ready"),
            "newer snapshot availability not rendered"
        );
        assert!(
            content.contains("this view stays pinned until you refresh"),
            "manual refresh guarantee not rendered"
        );

        Ok(())
    }

    #[test]
    fn snapshot_coverage_footer_uses_meaningful_counts() {
        assert_eq!(
            snapshot_coverage_footer_text(&SnapshotCoverageSummary {
                project_count: 2,
                project_day_count: 5,
                day_count: 3,
                session_count: 7,
                turn_count: 11,
            }),
            "coverage: 2 projects, 5 project-days across 3 days, 7 sessions, 11 turns"
        );
    }

    #[test]
    fn snapshot_coverage_footer_handles_empty_snapshots() {
        assert_eq!(
            snapshot_coverage_footer_text(&SnapshotCoverageSummary::default()),
            "coverage: no imported data is visible yet"
        );
    }

    #[test]
    fn descendant_model_includes_ancestor_current_and_descendant_layers() {
        // Build a RadialContext with 1 ancestor layer and 2 descendant layers.
        let ancestor_layer = build_radial_layer(
            &[radial_row(RadialRowSpec {
                key: "project:1",
                label: "project-a",
                kind: RollupRowKind::Project,
                value: 10.0,
                project_id: Some(1),
                category: None,
                action: None,
                full_path: None,
            })],
            MetricLens::UncachedInput,
            Some("project:1"),
            RadialSpan::full(),
        );
        let descendant_layer_1 = build_radial_layer(
            &[
                radial_row(RadialRowSpec {
                    key: "category:editing",
                    label: "editing",
                    kind: RollupRowKind::ActionCategory,
                    value: 6.0,
                    project_id: Some(1),
                    category: Some("editing"),
                    action: None,
                    full_path: None,
                }),
                radial_row(RadialRowSpec {
                    key: "category:docs",
                    label: "docs",
                    kind: RollupRowKind::ActionCategory,
                    value: 4.0,
                    project_id: Some(1),
                    category: Some("docs"),
                    action: None,
                    full_path: None,
                }),
            ],
            MetricLens::UncachedInput,
            None,
            RadialSpan::full(),
        );
        let descendant_layer_2 = build_radial_layer(
            &[radial_row(RadialRowSpec {
                key: "action:read",
                label: "read",
                kind: RollupRowKind::Action,
                value: 6.0,
                project_id: Some(1),
                category: Some("editing"),
                action: Some(sample_action("read file")),
                full_path: None,
            })],
            MetricLens::UncachedInput,
            None,
            RadialSpan::full(),
        );

        let context = RadialContext {
            ancestor_layers: vec![ancestor_layer],
            current_span: RadialSpan::full(),
            descendant_layers: vec![descendant_layer_1, descendant_layer_2],
        };

        let current_rows = vec![radial_row(RadialRowSpec {
            key: "project:1",
            label: "project-a",
            kind: RollupRowKind::Project,
            value: 10.0,
            project_id: Some(1),
            category: None,
            action: None,
            full_path: None,
        })];

        let model = build_radial_model(
            &context,
            &current_rows,
            current_rows.first(),
            &RootView::ProjectHierarchy,
            &BrowsePath::Root,
            &sample_filter_options(),
            MetricLens::UncachedInput,
        );

        // 1 ancestor + 0 current (skipped, duplicates ancestor) + 2 descendant = 3 layers
        assert_eq!(model.layers.len(), 3);
    }

    #[test]
    fn model_skips_current_layer_when_ancestors_exist_and_selection_present() {
        // When ancestor layers exist and there's a selection, the current layer
        // duplicates the last ancestor (same browse-level rows at a narrower
        // span). Skipping it avoids an extra ring that causes visual overlap.
        let ancestor_layer = build_radial_layer(
            &[
                radial_row(RadialRowSpec {
                    key: "project:1",
                    label: "project-a",
                    kind: RollupRowKind::Project,
                    value: 70.0,
                    project_id: Some(1),
                    category: None,
                    action: None,
                    full_path: None,
                }),
                radial_row(RadialRowSpec {
                    key: "project:2",
                    label: "project-b",
                    kind: RollupRowKind::Project,
                    value: 30.0,
                    project_id: Some(2),
                    category: None,
                    action: None,
                    full_path: None,
                }),
            ],
            MetricLens::UncachedInput,
            Some("project:1"),
            RadialSpan::full(),
        );
        let descendant_layer = build_radial_layer(
            &[radial_row(RadialRowSpec {
                key: "category:editing",
                label: "editing",
                kind: RollupRowKind::ActionCategory,
                value: 70.0,
                project_id: Some(1),
                category: Some("editing"),
                action: None,
                full_path: None,
            })],
            MetricLens::UncachedInput,
            None,
            RadialSpan::full(),
        );

        let context = RadialContext {
            ancestor_layers: vec![ancestor_layer],
            current_span: RadialSpan::full(),
            descendant_layers: vec![descendant_layer],
        };

        // current_rows duplicate the ancestor (same root-level projects)
        let current_rows = vec![
            radial_row(RadialRowSpec {
                key: "project:1",
                label: "project-a",
                kind: RollupRowKind::Project,
                value: 70.0,
                project_id: Some(1),
                category: None,
                action: None,
                full_path: None,
            }),
            radial_row(RadialRowSpec {
                key: "project:2",
                label: "project-b",
                kind: RollupRowKind::Project,
                value: 30.0,
                project_id: Some(2),
                category: None,
                action: None,
                full_path: None,
            }),
        ];

        let model = build_radial_model(
            &context,
            &current_rows,
            current_rows.first(), // selected_row present
            &RootView::ProjectHierarchy,
            &BrowsePath::Root,
            &sample_filter_options(),
            MetricLens::UncachedInput,
        );

        // 1 ancestor + 0 current (skipped) + 1 descendant = 2 layers
        assert_eq!(
            model.layers.len(),
            2,
            "current layer should be skipped when it duplicates the last ancestor"
        );
    }

    #[test]
    fn model_keeps_current_layer_when_no_ancestors() {
        // Without ancestors, the current layer is the only representation
        // of the browse level — must be included.
        let context = RadialContext::default();
        let current_rows = vec![radial_row(RadialRowSpec {
            key: "project:1",
            label: "project-a",
            kind: RollupRowKind::Project,
            value: 10.0,
            project_id: Some(1),
            category: None,
            action: None,
            full_path: None,
        })];

        let model = build_radial_model(
            &context,
            &current_rows,
            current_rows.first(),
            &RootView::ProjectHierarchy,
            &BrowsePath::Root,
            &sample_filter_options(),
            MetricLens::UncachedInput,
        );

        assert_eq!(
            model.layers.len(),
            1,
            "current layer should be kept when no ancestors exist"
        );
    }

    #[test]
    fn model_skips_descendants_when_ancestors_exist_but_no_selection() {
        // When ancestors exist but nothing is selected, the descendant layers
        // would duplicate the current layer (both query the same browse level).
        let ancestor_layer = build_radial_layer(
            &[radial_row(RadialRowSpec {
                key: "project:1",
                label: "project-a",
                kind: RollupRowKind::Project,
                value: 10.0,
                project_id: Some(1),
                category: None,
                action: None,
                full_path: None,
            })],
            MetricLens::UncachedInput,
            Some("project:1"),
            RadialSpan::full(),
        );
        let descendant_layer = build_radial_layer(
            &[radial_row(RadialRowSpec {
                key: "category:editing",
                label: "editing",
                kind: RollupRowKind::ActionCategory,
                value: 10.0,
                project_id: Some(1),
                category: Some("editing"),
                action: None,
                full_path: None,
            })],
            MetricLens::UncachedInput,
            None,
            RadialSpan::full(),
        );

        let context = RadialContext {
            ancestor_layers: vec![ancestor_layer],
            current_span: RadialSpan::full(),
            descendant_layers: vec![descendant_layer],
        };

        let current_rows = vec![radial_row(RadialRowSpec {
            key: "category:editing",
            label: "editing",
            kind: RollupRowKind::ActionCategory,
            value: 10.0,
            project_id: Some(1),
            category: Some("editing"),
            action: None,
            full_path: None,
        })];

        let model = build_radial_model(
            &context,
            &current_rows,
            None, // no selection
            &RootView::ProjectHierarchy,
            &BrowsePath::Project { project_id: 1 },
            &sample_filter_options(),
            MetricLens::UncachedInput,
        );

        // 1 ancestor + 1 current + 0 descendant (skipped) = 2 layers
        assert_eq!(
            model.layers.len(),
            2,
            "descendant layers should be skipped when they duplicate the current layer"
        );
    }

    #[test]
    fn descendant_layers_for_selected_project() -> Result<()> {
        // Integration test: from Root with active_path = Project{1},
        // descendant layers should appear showing categories.
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        app.ui_state.root = RootView::ProjectHierarchy;
        app.ui_state.path = BrowsePath::Root;

        let active_path = BrowsePath::Project { project_id: 1 };

        let filters = current_query_filters_for(&app.ui_state, &app.snapshot)?;
        let mut browse_stats = BrowseFanoutStats::default();

        let ctx = app.build_radial_context(&filters, &active_path, None, &mut browse_stats)?;

        // With an empty database, ancestor layers should exist (1 for Root→Project)
        // but descendant layers will be empty since there's no data to query.
        assert_eq!(
            ctx.ancestor_layers.len(),
            1,
            "selecting a project row should yield 1 ancestor layer"
        );
        assert!(
            ctx.descendant_layers.is_empty(),
            "with no data, descendant layers should be empty"
        );
        Ok(())
    }

    #[test]
    fn no_descendant_layers_at_root_path() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        app.ui_state.root = RootView::ProjectHierarchy;
        app.ui_state.path = BrowsePath::Root;

        let active_path = BrowsePath::Root;
        let filters = current_query_filters_for(&app.ui_state, &app.snapshot)?;
        let mut browse_stats = BrowseFanoutStats::default();

        let ctx = app.build_radial_context(&filters, &active_path, None, &mut browse_stats)?;

        assert!(
            ctx.descendant_layers.is_empty(),
            "Root path should produce no descendant layers"
        );
        Ok(())
    }

    #[test]
    fn inspect_pane_shows_no_opportunities_message_for_empty_row() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.show_inspect_pane = true;
        app.visible_rows = vec![TreeRow {
            row: sample_row("src", None),
            parent_path: BrowsePath::Root,
            node_path: None,
            depth: 0,
            is_expanded: false,
        }];
        app.table_state.select(Some(0));

        let content = render_app_to_string(&mut app, 120, 40)?;

        assert!(
            content.contains("Opportunity Details"),
            "inspect pane title should be rendered"
        );
        assert!(
            content.contains("No opportunities detected"),
            "empty opportunity message should appear"
        );
        Ok(())
    }

    #[test]
    fn inspect_pane_renders_skill_attribution_for_selected_row() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.show_inspect_pane = true;
        let mut row = sample_row("src", None);
        row.skill_attribution = Some(gnomon_core::query::SkillAttributionSummary {
            skill_name: "planner".to_string(),
            confidence: SkillAttributionConfidence::High,
        });
        app.visible_rows = vec![TreeRow {
            row,
            parent_path: BrowsePath::Root,
            node_path: None,
            depth: 0,
            is_expanded: false,
        }];
        app.table_state.select(Some(0));

        let content = render_app_to_string(&mut app, 120, 40)?;

        assert!(content.contains("skill planner"));
        assert!(content.contains("confidence high"));
        Ok(())
    }

    #[test]
    fn inspect_pane_renders_opportunity_annotations() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.show_inspect_pane = true;
        let mut row = sample_row("src", None);
        row.opportunities = OpportunitySummary::from_annotations(vec![
            OpportunityAnnotation {
                category: OpportunityCategory::HistoryDrag,
                score: 0.7,
                confidence: OpportunityConfidence::High,
                evidence: vec!["later turns carry more context".to_string()],
                recommendation: Some("reset or split the session sooner".to_string()),
            },
            OpportunityAnnotation {
                category: OpportunityCategory::SearchChurn,
                score: 0.35,
                confidence: OpportunityConfidence::Medium,
                evidence: vec!["repeated search loops".to_string()],
                recommendation: None,
            },
        ]);
        app.visible_rows = vec![TreeRow {
            row,
            parent_path: BrowsePath::Root,
            node_path: None,
            depth: 0,
            is_expanded: false,
        }];
        app.table_state.select(Some(0));

        let content = render_app_to_string(&mut app, 120, 40)?;

        assert!(
            content.contains("Opportunity Details"),
            "inspect pane title should be rendered"
        );
        assert!(
            content.contains("history drag"),
            "history drag category should appear"
        );
        assert!(
            content.contains("search churn"),
            "search churn category should appear"
        );
        assert!(
            content.contains("0.70"),
            "history drag score should be formatted"
        );
        assert!(
            content.contains("later turns carry more context"),
            "evidence text should appear"
        );
        assert!(
            content.contains("reset or split the session sooner"),
            "recommendation text should appear"
        );
        Ok(())
    }

    #[test]
    fn render_column_value_top_opportunity_shows_category_label() {
        let mut row = sample_row("src", None);
        row.opportunities = OpportunitySummary::from_annotations(vec![OpportunityAnnotation {
            category: OpportunityCategory::HistoryDrag,
            score: 0.7,
            confidence: OpportunityConfidence::High,
            evidence: vec![],
            recommendation: None,
        }]);

        let value = render_column_value(
            ColumnKey::Optional(OptionalColumn::TopOpportunity),
            &row,
            MetricLens::UncachedInput,
        );
        assert_eq!(value, "history drag");
    }

    #[test]
    fn render_column_value_top_opportunity_empty_when_no_opportunities() {
        let row = sample_row("src", None);
        let value = render_column_value(
            ColumnKey::Optional(OptionalColumn::TopOpportunity),
            &row,
            MetricLens::UncachedInput,
        );
        assert_eq!(value, "");
    }

    #[test]
    fn render_column_value_opp_score_formats_to_one_decimal() {
        let mut row = sample_row("src", None);
        row.opportunities = OpportunitySummary::from_annotations(vec![
            OpportunityAnnotation {
                category: OpportunityCategory::HistoryDrag,
                score: 0.7,
                confidence: OpportunityConfidence::High,
                evidence: vec![],
                recommendation: None,
            },
            OpportunityAnnotation {
                category: OpportunityCategory::SearchChurn,
                score: 0.3,
                confidence: OpportunityConfidence::Medium,
                evidence: vec![],
                recommendation: None,
            },
        ]);

        let value = render_column_value(
            ColumnKey::Optional(OptionalColumn::OppScore),
            &row,
            MetricLens::UncachedInput,
        );
        assert_eq!(value, "1.0");
    }

    #[test]
    fn render_column_value_opp_score_empty_when_zero() {
        let row = sample_row("src", None);
        let value = render_column_value(
            ColumnKey::Optional(OptionalColumn::OppScore),
            &row,
            MetricLens::UncachedInput,
        );
        assert_eq!(value, "");
    }

    #[test]
    fn render_column_value_confidence_shows_label() {
        let mut row = sample_row("src", None);
        row.opportunities = OpportunitySummary::from_annotations(vec![OpportunityAnnotation {
            category: OpportunityCategory::SessionSetup,
            score: 0.5,
            confidence: OpportunityConfidence::Medium,
            evidence: vec![],
            recommendation: None,
        }]);

        let value = render_column_value(
            ColumnKey::Optional(OptionalColumn::Confidence),
            &row,
            MetricLens::UncachedInput,
        );
        assert_eq!(value, "medium");
    }

    #[test]
    fn render_column_value_per_category_shows_matching_score() {
        let mut row = sample_row("src", None);
        row.opportunities = OpportunitySummary::from_annotations(vec![
            OpportunityAnnotation {
                category: OpportunityCategory::HistoryDrag,
                score: 0.7,
                confidence: OpportunityConfidence::High,
                evidence: vec![],
                recommendation: None,
            },
            OpportunityAnnotation {
                category: OpportunityCategory::SearchChurn,
                score: 0.3,
                confidence: OpportunityConfidence::Medium,
                evidence: vec![],
                recommendation: None,
            },
        ]);

        let history = render_column_value(
            ColumnKey::Optional(OptionalColumn::OppHistoryDrag),
            &row,
            MetricLens::UncachedInput,
        );
        assert_eq!(history, "0.7");

        let churn = render_column_value(
            ColumnKey::Optional(OptionalColumn::OppSearchChurn),
            &row,
            MetricLens::UncachedInput,
        );
        assert_eq!(churn, "0.3");

        let delegation = render_column_value(
            ColumnKey::Optional(OptionalColumn::OppDelegation),
            &row,
            MetricLens::UncachedInput,
        );
        assert_eq!(delegation, "");
    }

    #[test]
    fn toggle_column_works_with_opportunity_columns() {
        let mut columns = default_enabled_columns();
        assert!(!columns.contains(&OptionalColumn::TopOpportunity));

        toggle_column(&mut columns, OptionalColumn::TopOpportunity);
        assert!(columns.contains(&OptionalColumn::TopOpportunity));

        toggle_column(&mut columns, OptionalColumn::TopOpportunity);
        assert!(!columns.contains(&OptionalColumn::TopOpportunity));
    }

    #[test]
    fn active_columns_includes_opportunity_columns_when_enabled() {
        let enabled = vec![OptionalColumn::TopOpportunity, OptionalColumn::OppScore];
        let columns = active_columns(200, MetricLens::UncachedInput, &enabled);
        let keys: Vec<_> = columns.iter().map(|c| c.key).collect();
        assert!(keys.contains(&ColumnKey::Optional(OptionalColumn::TopOpportunity)));
        assert!(keys.contains(&ColumnKey::Optional(OptionalColumn::OppScore)));
    }

    #[test]
    fn cycle_opportunity_filter_cycles_through_all_categories_and_back_to_none() {
        let mut current = None;
        current = cycle_opportunity_filter(current);
        assert_eq!(current, Some(OpportunityCategory::SessionSetup));

        current = cycle_opportunity_filter(current);
        assert_eq!(current, Some(OpportunityCategory::TaskSetup));

        // Cycle through remaining 6 categories to reach ToolResultBloat
        for _ in 0..6 {
            current = cycle_opportunity_filter(current);
        }
        assert_eq!(current, Some(OpportunityCategory::ToolResultBloat));

        // One more cycle wraps back to None
        current = cycle_opportunity_filter(current);
        assert_eq!(
            current, None,
            "should wrap back to None after last category"
        );
    }

    #[test]
    fn clear_filters_resets_opportunity_filter() {
        let mut state = PersistedUiState {
            opportunity_filter: Some(OpportunityCategory::Delegation),
            ..PersistedUiState::default()
        };
        state.clear_filters();
        assert_eq!(state.opportunity_filter, None);
    }

    #[test]
    fn space_clears_most_specific_structural_filter_first() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.ui_state.project_id = Some(1);
        app.ui_state.action_category = Some("editing".to_string());
        app.ui_state.action = Some(sample_action("read file"));

        app.handle_normal_key(KeyEvent::from(KeyCode::Char(' ')))?;
        assert_eq!(app.ui_state.action, None);
        assert_eq!(app.ui_state.action_category.as_deref(), Some("editing"));
        assert_eq!(app.ui_state.project_id, Some(1));

        app.handle_normal_key(KeyEvent::from(KeyCode::Char(' ')))?;
        assert_eq!(app.ui_state.action_category, None);
        assert_eq!(app.ui_state.project_id, Some(1));

        app.handle_normal_key(KeyEvent::from(KeyCode::Char(' ')))?;
        assert_eq!(app.ui_state.project_id, None);
        Ok(())
    }

    #[test]
    fn space_resets_drilled_path_to_root_when_no_structural_filter_is_active() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        app.ui_state.root = RootView::CategoryHierarchy;
        app.ui_state.path = BrowsePath::Category {
            category: "editing".to_string(),
        };
        app.expanded_paths = vec![BrowsePath::Category {
            category: "editing".to_string(),
        }];
        app.row_cache = vec![CachedRows {
            path: BrowsePath::Category {
                category: "editing".to_string(),
            },
            rows: Vec::new(),
        }];

        app.handle_normal_key(KeyEvent::from(KeyCode::Char(' ')))?;

        assert_eq!(app.ui_state.path, BrowsePath::Root);
        assert!(app.expanded_paths.is_empty());
        assert!(app.row_cache.is_empty());
        Ok(())
    }

    #[test]
    fn space_at_root_without_structural_filters_sets_status_message() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        app.handle_normal_key(KeyEvent::from(KeyCode::Char(' ')))?;

        assert_eq!(app.ui_state.path, BrowsePath::Root);
        assert_eq!(app.ui_state.project_id, None);
        assert_eq!(app.ui_state.action_category, None);
        assert_eq!(app.ui_state.action, None);
        assert_eq!(
            app.status_message
                .as_ref()
                .map(|message| message.text.as_str()),
            Some("Already at the unscoped root view.")
        );
        Ok(())
    }

    #[test]
    fn persisted_state_deserializes_without_opportunity_filter() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("tui-state.json");
        // Write state JSON without opportunity_filter field to test serde(default)
        let json = serde_json::json!({
            "root": "ProjectHierarchy",
            "path": "Root",
            "lens": "UncachedInput",
            "pane_mode": "Table",
            "time_window": "All",
            "model": null,
            "project_id": null,
            "action_category": null,
            "action": null,
            "row_filter": "",
            "enabled_columns": ["Kind"]
        });
        fs::write(&path, serde_json::to_string_pretty(&json)?)?;
        let loaded = PersistedUiState::load(&path)?.context("missing")?;
        assert_eq!(loaded.opportunity_filter, None);
        Ok(())
    }
}
