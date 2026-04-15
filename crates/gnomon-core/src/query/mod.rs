use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use jiff::{Timestamp, ToSpan};
use rusqlite::types::Value;
use rusqlite::{Connection, params, params_from_iter};
use serde::{Deserialize, Serialize};

use crate::opportunity::OpportunitySummary;
use crate::opportunity::history_drag::{self, HistoryDragTurn};
use crate::perf::{PerfLogger, PerfScope};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotBounds {
    pub max_publish_seq: u64,
    pub published_chunk_count: usize,
    pub upper_bound_utc: Option<String>,
}

impl SnapshotBounds {
    pub const fn bootstrap() -> Self {
        Self {
            max_publish_seq: 0,
            published_chunk_count: 0,
            upper_bound_utc: None,
        }
    }

    pub fn is_bootstrap(&self) -> bool {
        self.max_publish_seq == 0
    }

    pub fn load(conn: &Connection) -> Result<Self> {
        QueryEngine::new(conn).latest_snapshot_bounds()
    }

    pub fn upper_bound_timestamp(&self) -> Result<Option<Timestamp>> {
        parse_timestamp(self.upper_bound_utc.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SnapshotCoverageSummary {
    pub project_count: usize,
    pub project_day_count: usize,
    pub day_count: usize,
    pub session_count: usize,
    pub turn_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricLens {
    UncachedInput,
    GrossInput,
    Output,
    Total,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TimeWindowFilter {
    pub start_at_utc: Option<String>,
    pub end_at_utc: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BrowseFilters {
    pub time_window: Option<TimeWindowFilter>,
    pub model: Option<String>,
    pub project_id: Option<i64>,
    pub action_category: Option<String>,
    pub action: Option<ActionKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RootView {
    ProjectHierarchy,
    CategoryHierarchy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrowsePath {
    Root,
    Project {
        project_id: i64,
    },
    ProjectCategory {
        project_id: i64,
        category: String,
    },
    ProjectAction {
        project_id: i64,
        category: String,
        action: ActionKey,
        parent_path: Option<String>,
    },
    Category {
        category: String,
    },
    CategoryAction {
        category: String,
        action: ActionKey,
    },
    CategoryActionProject {
        category: String,
        action: ActionKey,
        project_id: i64,
        parent_path: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowseRequest {
    pub snapshot: SnapshotBounds,
    pub root: RootView,
    pub lens: MetricLens,
    pub filters: BrowseFilters,
    pub path: BrowsePath,
}

/// A batch request that shares immutable context (snapshot, root, lens, filters)
/// across many parent paths whose children should be fetched.
///
/// This is the primary API for prefetch: submit many paths in one call and receive
/// per-parent results that decompose into individual cache entries.
///
/// There is no hard limit on the number of paths, but callers should cap batch size
/// (the design default is 20 paths) to avoid holding the database connection too long.
/// Recursion depth limiting is the caller's responsibility (see the prefetch coordinator).
///
/// For path-drill paths (`ProjectAction`, `CategoryActionProject`), each subtree is
/// queried independently — batching amortizes filter compilation and snapshot setup
/// overhead but does not combine SQL queries across parents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchBrowseRequest {
    pub snapshot: SnapshotBounds,
    pub root: RootView,
    pub lens: MetricLens,
    pub filters: BrowseFilters,
    pub paths: Vec<BrowsePath>,
}

impl BatchBrowseRequest {
    /// Decompose into individual `BrowseRequest` values, one per path.
    ///
    /// Useful for callers that need per-parent cache keys (e.g., the prefetch
    /// coordinator writes each result under its own `PersistedBrowseRequest` key).
    pub fn to_individual_requests(&self) -> Vec<BrowseRequest> {
        self.paths
            .iter()
            .map(|path| BrowseRequest {
                snapshot: self.snapshot.clone(),
                root: self.root,
                lens: self.lens,
                filters: self.filters.clone(),
                path: path.clone(),
            })
            .collect()
    }
}

/// Per-parent results from a batched browse query, keyed by the original `BrowsePath`.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchBrowseResponse {
    pub results: Vec<BatchBrowseResult>,
}

impl BatchBrowseResponse {
    /// Decompose batch results into `(BrowseRequest, Vec<RollupRow>)` pairs suitable
    /// for writing into the per-parent browse cache.
    pub fn into_cache_pairs(
        self,
        request: &BatchBrowseRequest,
    ) -> Vec<(BrowseRequest, Vec<RollupRow>)> {
        self.results
            .into_iter()
            .map(|result| {
                let browse_request = BrowseRequest {
                    snapshot: request.snapshot.clone(),
                    root: request.root,
                    lens: request.lens,
                    filters: request.filters.clone(),
                    path: result.path,
                };
                (browse_request, result.rows)
            })
            .collect()
    }
}

/// A single parent's child rows from a batch browse query.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchBrowseResult {
    pub path: BrowsePath,
    pub rows: Vec<RollupRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct BrowseBatchGroupKey<'a> {
    snapshot: &'a SnapshotBounds,
    root: RootView,
    lens: MetricLens,
    filters: &'a BrowseFilters,
    strategy: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ClassificationState {
    Classified,
    Mixed,
    Unclassified,
}

impl ClassificationState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Classified => "classified",
            Self::Mixed => "mixed",
            Self::Unclassified => "unclassified",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ActionKey {
    pub classification_state: ClassificationState,
    pub normalized_action: Option<String>,
    pub command_family: Option<String>,
    pub base_command: Option<String>,
}

impl ActionKey {
    pub fn label(&self) -> String {
        match self.classification_state {
            ClassificationState::Classified => self
                .normalized_action
                .clone()
                .or_else(|| self.base_command.clone())
                .or_else(|| self.command_family.clone())
                .unwrap_or_else(|| "classified".to_string()),
            ClassificationState::Mixed => "[mixed]".to_string(),
            ClassificationState::Unclassified => "[unclassified]".to_string(),
        }
    }

    fn stable_key(&self) -> String {
        [
            self.classification_state.as_str(),
            self.normalized_action.as_deref().unwrap_or_default(),
            self.command_family.as_deref().unwrap_or_default(),
            self.base_command.as_deref().unwrap_or_default(),
        ]
        .join("|")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RollupRowKind {
    Project,
    ActionCategory,
    Action,
    Directory,
    File,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricTotals {
    pub uncached_input: f64,
    pub cached_input: f64,
    pub gross_input: f64,
    pub output: f64,
    pub total: f64,
}

impl MetricTotals {
    fn zero() -> Self {
        Self {
            uncached_input: 0.0,
            cached_input: 0.0,
            gross_input: 0.0,
            output: 0.0,
            total: 0.0,
        }
    }

    fn from_usage(
        input_tokens: i64,
        cache_creation_input_tokens: i64,
        cache_read_input_tokens: i64,
        output_tokens: i64,
    ) -> Self {
        let uncached_input = (input_tokens + cache_creation_input_tokens) as f64;
        let cached_input = cache_read_input_tokens as f64;
        let gross_input = uncached_input + cached_input;
        let output = output_tokens as f64;

        Self {
            uncached_input,
            cached_input,
            gross_input,
            output,
            total: gross_input + output,
        }
    }

    fn add_assign(&mut self, other: &Self) {
        self.uncached_input += other.uncached_input;
        self.cached_input += other.cached_input;
        self.gross_input += other.gross_input;
        self.output += other.output;
        self.total += other.total;
    }

    fn divided_by(&self, divisor: f64) -> Self {
        if divisor <= 0.0 {
            return Self::zero();
        }

        Self {
            uncached_input: self.uncached_input / divisor,
            cached_input: self.cached_input / divisor,
            gross_input: self.gross_input / divisor,
            output: self.output / divisor,
            total: self.total / divisor,
        }
    }

    pub fn lens_value(&self, lens: MetricLens) -> f64 {
        match lens {
            MetricLens::UncachedInput => self.uncached_input,
            MetricLens::GrossInput => self.gross_input,
            MetricLens::Output => self.output,
            MetricLens::Total => self.total,
        }
    }

    fn is_zero_value(&self) -> bool {
        self.uncached_input == 0.0
            && self.cached_input == 0.0
            && self.gross_input == 0.0
            && self.output == 0.0
            && self.total == 0.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricIndicators {
    pub selected_lens_last_5_hours: f64,
    pub selected_lens_last_week: f64,
    pub uncached_input_reference: f64,
}

impl MetricIndicators {
    fn is_zero_value(&self) -> bool {
        self.selected_lens_last_5_hours == 0.0
            && self.selected_lens_last_week == 0.0
            && self.uncached_input_reference == 0.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RollupRow {
    pub kind: RollupRowKind,
    pub key: String,
    pub label: String,
    pub metrics: MetricTotals,
    pub indicators: MetricIndicators,
    pub item_count: u64,
    #[serde(default)]
    pub opportunities: OpportunitySummary,
    #[serde(default)]
    pub skill_attribution: Option<SkillAttributionSummary>,
    pub project_id: Option<i64>,
    pub project_identity: Option<ProjectIdentity>,
    pub category: Option<String>,
    pub action: Option<ActionKey>,
    pub full_path: Option<String>,
}

impl RollupRow {
    fn is_zero_value(&self) -> bool {
        self.metrics.is_zero_value() && self.indicators.is_zero_value()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectFilterOption {
    pub id: i64,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionFilterOption {
    pub category: String,
    pub action: ActionKey,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterOptions {
    pub projects: Vec<ProjectFilterOption>,
    pub models: Vec<String>,
    pub categories: Vec<String>,
    pub actions: Vec<ActionFilterOption>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectIdentity {
    pub identity_kind: String,
    pub root_path: String,
    pub git_root_path: Option<String>,
    pub git_origin: Option<String>,
    pub identity_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrowseReport {
    pub snapshot: SnapshotBounds,
    pub request: BrowseRequest,
    pub rows: Vec<RollupRow>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpportunitiesReport {
    pub snapshot: SnapshotBounds,
    pub rows: Vec<OpportunitiesReportRow>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpportunitiesReportRow {
    pub key: String,
    pub label: String,
    pub project_id: i64,
    pub project_name: String,
    pub conversation_id: i64,
    pub conversation_title: Option<String>,
    pub turn_count: u64,
    pub metrics: MetricTotals,
    pub opportunities: OpportunitySummary,
}

#[derive(Debug, Clone, Default)]
pub struct OpportunitiesFilters {
    pub project_id: Option<i64>,
    pub start_at_utc: Option<String>,
    pub end_at_utc: Option<String>,
    pub include_empty: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HistoryEventFilters {
    pub session_id: Option<String>,
    pub start_at_utc: Option<String>,
    pub end_at_utc: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEventRow {
    pub source_file_id: i64,
    pub source_line_no: i64,
    pub session_id: Option<String>,
    pub recorded_at_utc: Option<String>,
    pub raw_project: Option<String>,
    pub display_text: Option<String>,
    pub pasted_contents_json: Option<String>,
    pub input_kind: String,
    pub slash_command_name: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkillInvocationFilters {
    pub skill_name: Option<String>,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillInvocationConfidence {
    Explicit,
    Confirmed,
    Inferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillTranscriptEvidenceKind {
    PromptText,
    ToolInputPath,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillConfidenceCounts {
    pub explicit: u64,
    pub confirmed: u64,
    pub inferred: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillInvocationRow {
    pub skill_name: String,
    pub session_id: String,
    pub recorded_at_utc: Option<String>,
    pub raw_project: Option<String>,
    pub conversation_id: Option<i64>,
    pub project_id: Option<i64>,
    pub project_name: Option<String>,
    pub conversation_title: Option<String>,
    pub confidence: SkillInvocationConfidence,
    pub transcript_evidence_kinds: Vec<SkillTranscriptEvidenceKind>,
    pub transcript_evidence_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SkillsPath {
    Root,
    Skill { skill_name: String },
    SkillProject { skill_name: String, project_id: i64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SkillsRowKind {
    Skill,
    Project,
    Session,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SkillAttributionConfidence {
    High,
}

impl SkillAttributionConfidence {
    fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "high" => Some(Self::High),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillAttributionSummary {
    pub skill_name: String,
    pub confidence: SkillAttributionConfidence,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkillsReport {
    pub snapshot: SnapshotBounds,
    pub path: SkillsPath,
    pub cost_scope: String,
    pub attributed_cost_scope: String,
    pub unmatched_invocation_count: u64,
    pub unmatched_session_count: u64,
    pub rows: Vec<SkillsReportRow>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkillsReportRow {
    pub kind: SkillsRowKind,
    pub key: String,
    pub label: String,
    pub skill_name: String,
    pub project_id: Option<i64>,
    pub project_name: Option<String>,
    pub session_id: Option<String>,
    pub conversation_id: Option<i64>,
    pub conversation_title: Option<String>,
    pub invocation_count: u64,
    pub session_count: u64,
    pub confidence_counts: SkillConfidenceCounts,
    pub transcript_evidence_count: u64,
    pub metrics: MetricTotals,
    pub attributed_action_count: u64,
    pub attributed_metrics: MetricTotals,
    pub top_attribution_confidence: Option<SkillAttributionConfidence>,
}

pub struct QueryEngine<'conn> {
    conn: &'conn Connection,
    perf_logger: Option<PerfLogger>,
}

#[derive(Debug, Clone)]
struct SkillSessionAssociation {
    skill_name: String,
    session_id: String,
    invocation_count: u64,
    conversation_id: Option<i64>,
    project_id: Option<i64>,
    project_name: Option<String>,
    conversation_title: Option<String>,
    confidence_counts: SkillConfidenceCounts,
    transcript_evidence_count: u64,
    metrics: Option<MetricTotals>,
    attributed_action_count: u64,
    attributed_metrics: MetricTotals,
    top_attribution_confidence: Option<SkillAttributionConfidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LoadedSkillTranscriptPart {
    session_id: String,
    conversation_id: i64,
    project_id: i64,
    project_name: String,
    conversation_title: Option<String>,
    recorded_at_utc: Option<String>,
    text_value: Option<String>,
    metadata_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SkillTranscriptEvidence {
    session_id: String,
    skill_name: String,
    kind: SkillTranscriptEvidenceKind,
    recorded_at_utc: Option<String>,
    conversation_id: i64,
    project_id: i64,
    project_name: String,
    conversation_title: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AggregatedSkillInvocation {
    invocation_count: u64,
    conversation_id: Option<i64>,
    project_id: Option<i64>,
    project_name: Option<String>,
    conversation_title: Option<String>,
    confidence_counts: SkillConfidenceCounts,
    transcript_evidence_count: u64,
}

const CONVERSATION_TURNS_SQL: &str = "
    SELECT
        c.id AS conversation_id,
        c.project_id,
        p.display_name AS project_name,
        c.title,
        t.sequence_no,
        COALESCE(t.input_tokens, 0) + COALESCE(t.cache_creation_input_tokens, 0) AS uncached_input,
        COALESCE(t.cache_read_input_tokens, 0) AS cached_input,
        COALESCE(t.output_tokens, 0) AS output_tokens
    FROM turn t
    JOIN conversation c ON c.id = t.conversation_id
    JOIN project p ON p.id = c.project_id
    JOIN import_chunk ic ON ic.id = t.import_chunk_id
    WHERE ic.state = 'complete'
      AND ic.publish_seq IS NOT NULL
      AND ic.publish_seq <= ?1
    ORDER BY c.project_id, c.id, t.sequence_no
";

const LATEST_SNAPSHOT_BOUNDS_SQL: &str = "
    SELECT
        MAX(publish_seq),
        COUNT(*),
        MAX(completed_at_utc)
    FROM import_chunk
    WHERE state = 'complete' AND publish_seq IS NOT NULL
";

const SNAPSHOT_COVERAGE_SUMMARY_SQL: &str = "
    SELECT
        COUNT(DISTINCT project_id),
        COUNT(*),
        COUNT(DISTINCT chunk_day_local),
        COALESCE(SUM(imported_conversation_count), 0),
        COALESCE(SUM(imported_turn_count), 0)
    FROM import_chunk
    WHERE state = 'complete'
      AND publish_seq IS NOT NULL
      AND publish_seq <= ?1
";

const DISPLAY_CATEGORY_SQL: &str = "
    CASE
        WHEN a.category IS NOT NULL THEN a.category
        WHEN a.classification_state = 'mixed' THEN '[mixed]'
        WHEN a.classification_state = 'unclassified' THEN '[unclassified]'
        ELSE 'classified'
    END
";

const ACTION_TIMESTAMP_SQL: &str = "COALESCE(a.ended_at_utc, a.started_at_utc)";

const LOAD_RECENT_ACTION_FACTS_SQL: &str = "
    SELECT
        p.id,
        p.display_name,
        p.root_path,
        p.identity_kind,
        p.git_root_path,
        p.git_origin,
        p.identity_reason,
        a.category,
        a.normalized_action,
        a.command_family,
        a.base_command,
        a.classification_state,
        COALESCE(a.ended_at_utc, a.started_at_utc),
        COALESCE(a.input_tokens, 0),
        COALESCE(a.cache_creation_input_tokens, 0),
        COALESCE(a.cache_read_input_tokens, 0),
        COALESCE(a.output_tokens, 0)
    FROM action a
    JOIN import_chunk ic ON ic.id = a.import_chunk_id
    JOIN project p ON p.id = ic.project_id
    WHERE ic.state = 'complete'
      AND ic.publish_seq IS NOT NULL
      AND ic.publish_seq <= ?1
      AND datetime(COALESCE(a.ended_at_utc, a.started_at_utc)) >= datetime(?2)
";

const FILTER_OPTIONS_PROJECT_CATEGORY_ACTION_SQL: &str = "
    SELECT DISTINCT
        p.id,
        p.display_name,
        car.display_category,
        car.classification_state,
        car.normalized_action,
        car.command_family,
        car.base_command
    FROM chunk_action_rollup car
    JOIN import_chunk ic ON ic.id = car.import_chunk_id
    JOIN project p ON p.id = ic.project_id
    WHERE ic.state = 'complete'
      AND ic.publish_seq IS NOT NULL
      AND ic.publish_seq <= ?1
    ORDER BY
        p.id,
        car.display_category,
        car.classification_state,
        car.normalized_action,
        car.command_family,
        car.base_command
";

const FILTER_OPTIONS_MODELS_SQL: &str = "
    SELECT DISTINCT m.model_name
    FROM message m
    JOIN import_chunk ic ON ic.id = m.import_chunk_id
    WHERE ic.state = 'complete'
      AND ic.publish_seq IS NOT NULL
      AND ic.publish_seq <= ?1
      AND m.model_name IS NOT NULL
    ORDER BY m.model_name
";

impl<'conn> QueryEngine<'conn> {
    pub fn new(conn: &'conn Connection) -> Self {
        Self {
            conn,
            perf_logger: None,
        }
    }

    pub fn with_perf(conn: &'conn Connection, perf_logger: Option<PerfLogger>) -> Self {
        Self { conn, perf_logger }
    }

    fn perf_scope(&self, operation: &str) -> PerfScope {
        PerfScope::new(self.perf_logger.clone(), operation)
    }

    fn verbose_perf_scope(&self, operation: &str) -> PerfScope {
        PerfScope::new_verbose(self.perf_logger.clone(), operation)
    }

    pub fn latest_snapshot_bounds(&self) -> Result<SnapshotBounds> {
        let mut perf = self.perf_scope("query.latest_snapshot_bounds");
        let row = self
            .conn
            .query_row(LATEST_SNAPSHOT_BOUNDS_SQL, [], |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .context("unable to compute the latest published snapshot bounds")?;

        let Some(max_publish_seq) = row.0 else {
            let snapshot = SnapshotBounds::bootstrap();
            perf.field("snapshot", &snapshot);
            perf.finish_ok();
            return Ok(snapshot);
        };

        let snapshot = SnapshotBounds {
            max_publish_seq: u64::try_from(max_publish_seq)
                .context("latest publish_seq was negative")?,
            published_chunk_count: usize::try_from(row.1)
                .context("published chunk count overflowed usize")?,
            upper_bound_utc: row.2,
        };
        perf.field("snapshot", &snapshot);
        perf.finish_ok();
        Ok(snapshot)
    }

    pub fn has_newer_snapshot(&self, snapshot: &SnapshotBounds) -> Result<bool> {
        Ok(self.latest_snapshot_bounds()?.max_publish_seq > snapshot.max_publish_seq)
    }

    pub fn snapshot_coverage_summary(
        &self,
        snapshot: &SnapshotBounds,
    ) -> Result<SnapshotCoverageSummary> {
        if snapshot.max_publish_seq == 0 {
            return Ok(SnapshotCoverageSummary::default());
        }

        let max_publish_seq = i64::try_from(snapshot.max_publish_seq)
            .context("snapshot publish_seq overflowed i64")?;
        let row = self
            .conn
            .query_row(SNAPSHOT_COVERAGE_SUMMARY_SQL, [max_publish_seq], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .context("unable to compute the visible snapshot coverage summary")?;

        Ok(SnapshotCoverageSummary {
            project_count: usize::try_from(row.0).context("project count overflowed usize")?,
            project_day_count: usize::try_from(row.1)
                .context("project-day count overflowed usize")?,
            day_count: usize::try_from(row.2).context("day count overflowed usize")?,
            session_count: usize::try_from(row.3).context("session count overflowed usize")?,
            turn_count: usize::try_from(row.4).context("turn count overflowed usize")?,
        })
    }

    pub fn filter_options(&self, snapshot: &SnapshotBounds) -> Result<FilterOptions> {
        let mut perf = self.perf_scope("query.filter_options");
        perf.field("snapshot", snapshot);

        let build_started = Instant::now();
        let mut projects = BTreeMap::new();
        let mut categories = BTreeSet::new();
        let mut actions = BTreeMap::new();
        let mut models = Vec::new();

        let rollup_load_started = Instant::now();
        if snapshot.max_publish_seq > 0 {
            let max_publish_seq = i64::try_from(snapshot.max_publish_seq)
                .context("snapshot publish_seq overflowed i64")?;
            let mut stmt = self
                .conn
                .prepare(FILTER_OPTIONS_PROJECT_CATEGORY_ACTION_SQL)
                .context("unable to prepare filter option rollup query")?;
            let rows = stmt.query_map([max_publish_seq], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })?;

            for row in rows {
                let (
                    project_id,
                    project_display_name,
                    category,
                    classification_state,
                    normalized_action,
                    command_family,
                    base_command,
                ) = row.context("unable to read filter option rollup row")?;
                let action = ActionKey {
                    classification_state: parse_classification_state(&classification_state)?,
                    normalized_action,
                    command_family,
                    base_command,
                };

                projects.entry(project_id).or_insert(project_display_name);
                categories.insert(category.clone());
                actions
                    .entry((category, action.clone()))
                    .or_insert_with(|| action.label());
            }

            let mut model_stmt = self
                .conn
                .prepare(FILTER_OPTIONS_MODELS_SQL)
                .context("unable to prepare filter option model query")?;
            let model_rows =
                model_stmt.query_map([max_publish_seq], |row| row.get::<_, String>(0))?;
            for row in model_rows {
                models.push(row.context("unable to read filter option model row")?);
            }
        }
        perf.field(
            "rollup_load_ms",
            rollup_load_started.elapsed().as_secs_f64() * 1000.0,
        );

        let options = FilterOptions {
            projects: projects
                .into_iter()
                .map(|(id, display_name)| ProjectFilterOption { id, display_name })
                .collect(),
            models,
            categories: categories.into_iter().collect(),
            actions: actions
                .into_iter()
                .map(|((category, action), label)| ActionFilterOption {
                    category,
                    action,
                    label,
                })
                .collect(),
        };
        perf.field("build_ms", build_started.elapsed().as_secs_f64() * 1000.0);
        perf.field("project_count", options.projects.len());
        perf.field("model_count", options.models.len());
        perf.field("category_count", options.categories.len());
        perf.field("action_count", options.actions.len());
        perf.finish_ok();
        Ok(options)
    }

    pub fn browse(&self, request: &BrowseRequest) -> Result<Vec<RollupRow>> {
        let mut perf = self.verbose_perf_scope("query.browse");
        record_browse_request_perf_fields(&mut perf, request);

        let filter_compile_started = Instant::now();
        let compiled_filters = CompiledFilters::compile(&request.filters)?;
        perf.field(
            "filter_compile_ms",
            filter_compile_started.elapsed().as_secs_f64() * 1000.0,
        );

        let windows_started = Instant::now();
        let windows = Windows::from_snapshot(&request.snapshot)?;
        perf.field(
            "window_build_ms",
            windows_started.elapsed().as_secs_f64() * 1000.0,
        );

        let aggregate_started = Instant::now();
        let mut rows = if is_path_browse(&request.path) {
            if can_use_path_rollups(&request.filters) {
                let path_load_started = Instant::now();
                let rows = self.load_path_rollup_rows(request)?;
                perf.field(
                    "path_rollup_load_ms",
                    path_load_started.elapsed().as_secs_f64() * 1000.0,
                );
                perf.field("path_rollup_count", rows.len());
                rows
            } else {
                let path_load_started = Instant::now();
                let path_facts = self.load_path_facts(request)?;
                perf.field(
                    "path_load_ms",
                    path_load_started.elapsed().as_secs_f64() * 1000.0,
                );
                perf.field("path_fact_count", path_facts.len());
                aggregate_path_request(request, &compiled_filters, &windows, &path_facts)?
            }
        } else if can_use_action_rollups(&request.filters) {
            let action_load_started = Instant::now();
            let action_rollups = self.load_grouped_action_rollup_rows(request)?;
            perf.field(
                "grouped_action_rollup_load_ms",
                action_load_started.elapsed().as_secs_f64() * 1000.0,
            );
            perf.field("grouped_action_rollup_count", action_rollups.len());

            let mut rows = action_rollups;

            if let Some(last_week_start) = windows.last_week_start {
                let recent_load_started = Instant::now();
                let recent_action_facts =
                    self.load_recent_action_facts(&request.snapshot, last_week_start)?;
                perf.field(
                    "recent_action_load_ms",
                    recent_load_started.elapsed().as_secs_f64() * 1000.0,
                );
                perf.field("recent_action_fact_count", recent_action_facts.len());
                let indicator_rows = aggregate_action_request(
                    request,
                    &compiled_filters,
                    &windows,
                    &recent_action_facts,
                )?;
                apply_indicator_rows(&mut rows, indicator_rows);
            }

            rows
        } else {
            let action_load_started = Instant::now();
            let action_facts = self.load_action_facts(request)?;
            perf.field(
                "action_load_ms",
                action_load_started.elapsed().as_secs_f64() * 1000.0,
            );
            perf.field("action_fact_count", action_facts.len());
            aggregate_action_request(request, &compiled_filters, &windows, &action_facts)?
        };
        perf.field(
            "aggregate_ms",
            aggregate_started.elapsed().as_secs_f64() * 1000.0,
        );

        // Hide rows that have no visible usage contribution; item counts alone should not keep
        // an aggregate row on screen.
        rows.retain(|row| !row.is_zero_value());

        let sort_started = Instant::now();
        rows.sort_by(|left, right| {
            right
                .metrics
                .lens_value(request.lens)
                .total_cmp(&left.metrics.lens_value(request.lens))
                .then_with(|| left.label.cmp(&right.label))
        });
        perf.field("sort_ms", sort_started.elapsed().as_secs_f64() * 1000.0);
        perf.field("row_count", rows.len());
        perf.finish_ok();

        Ok(rows)
    }

    pub fn browse_many(&self, requests: &[BrowseRequest]) -> Result<Vec<Vec<RollupRow>>> {
        let mut perf = self.perf_scope("query.browse_many");
        perf.field("request_count", requests.len());

        if requests.is_empty() {
            perf.finish_ok();
            return Ok(Vec::new());
        }

        let mut grouped_indexes = BTreeMap::<String, Vec<usize>>::new();
        let mut fallback_indexes = Vec::new();
        for (index, request) in requests.iter().enumerate() {
            let Some(strategy) = browse_batch_strategy(request) else {
                fallback_indexes.push(index);
                continue;
            };
            let key = serde_json::to_string(&BrowseBatchGroupKey {
                snapshot: &request.snapshot,
                root: request.root,
                lens: request.lens,
                filters: &request.filters,
                strategy,
            })
            .context("unable to serialize browse batch grouping key")?;
            grouped_indexes.entry(key).or_default().push(index);
        }

        let mut outputs = vec![Vec::new(); requests.len()];
        let mut grouped_request_count = 0usize;

        for indexes in grouped_indexes.into_values() {
            if indexes.len() == 1 {
                let index = indexes[0];
                outputs[index] = self.browse(&requests[index])?;
                continue;
            }

            grouped_request_count += indexes.len();
            let grouped_requests = indexes
                .iter()
                .map(|index| requests[*index].clone())
                .collect::<Vec<_>>();
            let grouped_rows = self.browse_many_compatible(&grouped_requests)?;
            for (batch_position, request_index) in indexes.into_iter().enumerate() {
                outputs[request_index] = grouped_rows
                    .get(batch_position)
                    .cloned()
                    .context("missing grouped browse batch output")?;
            }
        }

        for index in fallback_indexes {
            outputs[index] = self.browse(&requests[index])?;
        }

        perf.field("grouped_request_count", grouped_request_count);
        perf.field(
            "fallback_request_count",
            requests.len().saturating_sub(grouped_request_count),
        );
        perf.finish_ok();
        Ok(outputs)
    }

    /// Execute a batched browse query for many parent paths sharing the same
    /// snapshot, root, lens, and filters. Returns per-parent results that can be
    /// decomposed into individual cache entries via [`BatchBrowseResponse::into_cache_pairs`].
    pub fn browse_batch(&self, batch: &BatchBrowseRequest) -> Result<BatchBrowseResponse> {
        let mut perf = self.perf_scope("query.browse_batch");
        perf.field("path_count", batch.paths.len());

        let requests = batch.to_individual_requests();
        let row_sets = self.browse_many(&requests)?;

        let results = batch
            .paths
            .iter()
            .cloned()
            .zip(row_sets)
            .map(|(path, rows)| BatchBrowseResult { path, rows })
            .collect();

        perf.finish_ok();
        Ok(BatchBrowseResponse { results })
    }

    pub fn browse_report(&self, request: BrowseRequest) -> Result<BrowseReport> {
        let rows = self.browse(&request)?;
        Ok(BrowseReport {
            snapshot: request.snapshot.clone(),
            request,
            rows,
        })
    }

    pub fn opportunities_report(
        &self,
        snapshot: &SnapshotBounds,
        filters: &OpportunitiesFilters,
    ) -> Result<OpportunitiesReport> {
        let mut perf = self.perf_scope("query.opportunities_report");
        if snapshot.max_publish_seq == 0 {
            perf.finish_ok();
            return Ok(OpportunitiesReport {
                snapshot: snapshot.clone(),
                rows: Vec::new(),
            });
        }

        let max_publish_seq = i64::try_from(snapshot.max_publish_seq)
            .context("snapshot publish_seq overflowed i64")?;

        let mut sql = CONVERSATION_TURNS_SQL.to_string();
        let mut param_values: Vec<Value> = vec![Value::Integer(max_publish_seq)];

        if let Some(project_id) = filters.project_id {
            sql.push_str(&format!(" AND c.project_id = ?{}", param_values.len() + 1));
            param_values.push(Value::Integer(project_id));
        }
        if let Some(ref start) = filters.start_at_utc {
            sql.push_str(&format!(
                " AND datetime(COALESCE(t.ended_at_utc, t.started_at_utc)) >= datetime(?{})",
                param_values.len() + 1
            ));
            param_values.push(Value::Text(start.clone()));
        }
        if let Some(ref end) = filters.end_at_utc {
            sql.push_str(&format!(
                " AND datetime(COALESCE(t.ended_at_utc, t.started_at_utc)) <= datetime(?{})",
                param_values.len() + 1
            ));
            param_values.push(Value::Text(end.clone()));
        }

        // Re-append ORDER BY since we built on the base SQL (which already has one,
        // but additional WHERE clauses don't affect it — the base SQL's ORDER BY is
        // at the end and remains valid).

        let mut stmt = self.conn.prepare(&sql)?;
        let turn_rows = stmt
            .query_map(params_from_iter(param_values.iter()), |row| {
                Ok(ConversationTurnRow {
                    conversation_id: row.get(0)?,
                    project_id: row.get(1)?,
                    project_name: row.get(2)?,
                    conversation_title: row.get(3)?,
                    _sequence_no: row.get(4)?,
                    uncached_input: row.get(5)?,
                    cached_input: row.get(6)?,
                    output_tokens: row.get(7)?,
                })
            })
            .context("failed to query conversation turns")?
            .collect::<Result<Vec<_>, _>>()
            .context("failed to read conversation turn rows")?;

        let rows = build_opportunities_rows(&turn_rows, filters.include_empty);

        perf.field("row_count", rows.len());
        perf.finish_ok();

        Ok(OpportunitiesReport {
            snapshot: snapshot.clone(),
            rows,
        })
    }

    pub fn history_events(
        &self,
        snapshot: &SnapshotBounds,
        filters: &HistoryEventFilters,
    ) -> Result<Vec<HistoryEventRow>> {
        if snapshot.max_publish_seq == 0 {
            return Ok(Vec::new());
        }

        let max_publish_seq = i64::try_from(snapshot.max_publish_seq)
            .context("snapshot publish_seq overflowed i64")?;
        let mut sql = "
            SELECT
                he.source_file_id,
                he.source_line_no,
                he.session_id,
                he.recorded_at_utc,
                he.raw_project,
                he.display_text,
                he.pasted_contents_json,
                he.input_kind,
                he.slash_command_name
            FROM history_event he
            JOIN import_chunk ic ON ic.id = he.import_chunk_id
            WHERE ic.state = 'complete'
              AND ic.publish_seq IS NOT NULL
              AND ic.publish_seq <= ?1
        "
        .to_string();
        let mut params = vec![Value::Integer(max_publish_seq)];

        if let Some(session_id) = &filters.session_id {
            sql.push_str(&format!(" AND he.session_id = ?{}", params.len() + 1));
            params.push(Value::Text(session_id.clone()));
        }
        if let Some(start_at_utc) = &filters.start_at_utc {
            sql.push_str(&format!(
                " AND datetime(he.recorded_at_utc) >= datetime(?{})",
                params.len() + 1
            ));
            params.push(Value::Text(start_at_utc.clone()));
        }
        if let Some(end_at_utc) = &filters.end_at_utc {
            sql.push_str(&format!(
                " AND datetime(he.recorded_at_utc) <= datetime(?{})",
                params.len() + 1
            ));
            params.push(Value::Text(end_at_utc.clone()));
        }

        sql.push_str(" ORDER BY he.recorded_at_utc, he.source_file_id, he.source_line_no");
        let mut stmt = self
            .conn
            .prepare(&sql)
            .context("unable to prepare history event query")?;
        let rows = stmt
            .query_map(params_from_iter(params.iter()), |row| {
                Ok(HistoryEventRow {
                    source_file_id: row.get(0)?,
                    source_line_no: row.get(1)?,
                    session_id: row.get(2)?,
                    recorded_at_utc: row.get(3)?,
                    raw_project: row.get(4)?,
                    display_text: row.get(5)?,
                    pasted_contents_json: row.get(6)?,
                    input_kind: row.get(7)?,
                    slash_command_name: row.get(8)?,
                })
            })
            .context("unable to execute history event query")?;

        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("unable to read history event rows")
    }

    pub fn skill_invocations(
        &self,
        snapshot: &SnapshotBounds,
        filters: &SkillInvocationFilters,
    ) -> Result<Vec<SkillInvocationRow>> {
        if snapshot.max_publish_seq == 0 {
            return Ok(Vec::new());
        }

        let explicit_rows = self.load_explicit_skill_invocations(snapshot)?;
        let transcript_evidence = self.load_skill_transcript_evidence(snapshot)?;
        let mut evidence_by_skill_session =
            HashMap::<(String, String), Vec<SkillTranscriptEvidence>>::new();
        for evidence in transcript_evidence {
            evidence_by_skill_session
                .entry((evidence.skill_name.clone(), evidence.session_id.clone()))
                .or_default()
                .push(evidence);
        }

        let mut rows = Vec::new();
        let mut seen_pairs = HashSet::<(String, String)>::new();

        for explicit in explicit_rows {
            let evidence = evidence_by_skill_session
                .get(&(explicit.skill_name.clone(), explicit.session_id.clone()))
                .cloned()
                .unwrap_or_default();
            seen_pairs.insert((explicit.skill_name.clone(), explicit.session_id.clone()));
            rows.push(skill_invocation_row_from_explicit(explicit, &evidence));
        }

        for ((skill_name, session_id), evidence) in evidence_by_skill_session {
            if seen_pairs.contains(&(skill_name.clone(), session_id.clone())) {
                continue;
            }
            rows.push(skill_invocation_row_from_inferred(
                skill_name, session_id, &evidence,
            ));
        }

        rows.retain(|row| {
            filters
                .skill_name
                .as_ref()
                .is_none_or(|skill_name| &row.skill_name == skill_name)
                && filters
                    .session_id
                    .as_ref()
                    .is_none_or(|session_id| &row.session_id == session_id)
        });
        rows.sort_by(|left, right| {
            left.skill_name
                .cmp(&right.skill_name)
                .then_with(|| left.session_id.cmp(&right.session_id))
                .then_with(|| left.recorded_at_utc.cmp(&right.recorded_at_utc))
                .then_with(|| left.confidence.cmp(&right.confidence))
        });

        Ok(rows)
    }

    pub fn skills_report(
        &self,
        snapshot: &SnapshotBounds,
        path: SkillsPath,
    ) -> Result<SkillsReport> {
        let mut perf = self.perf_scope("query.skills_report");
        if snapshot.max_publish_seq == 0 {
            perf.finish_ok();
            return Ok(SkillsReport {
                snapshot: snapshot.clone(),
                path,
                cost_scope: "session-associated".to_string(),
                attributed_cost_scope: "action-attributed".to_string(),
                unmatched_invocation_count: 0,
                unmatched_session_count: 0,
                rows: Vec::new(),
            });
        }

        let associations = self.load_skill_session_associations(snapshot)?;
        let unmatched_invocation_count = associations
            .iter()
            .filter(|row| row.conversation_id.is_none())
            .map(|row| row.invocation_count)
            .sum();
        let unmatched_session_count = associations
            .iter()
            .filter(|row| row.conversation_id.is_none())
            .count() as u64;
        let matched = associations
            .iter()
            .filter(|row| row.conversation_id.is_some() && row.metrics.is_some())
            .collect::<Vec<_>>();

        let mut rows = match &path {
            SkillsPath::Root => build_skill_root_rows(&matched),
            SkillsPath::Skill { skill_name } => build_skill_project_rows(&matched, skill_name),
            SkillsPath::SkillProject {
                skill_name,
                project_id,
            } => build_skill_session_rows(&matched, skill_name, *project_id),
        };
        rows.sort_by(|left, right| {
            right
                .metrics
                .uncached_input
                .total_cmp(&left.metrics.uncached_input)
                .then_with(|| left.label.cmp(&right.label))
        });

        perf.field("row_count", rows.len());
        perf.field("unmatched_invocation_count", unmatched_invocation_count);
        perf.field("unmatched_session_count", unmatched_session_count);
        perf.finish_ok();

        Ok(SkillsReport {
            snapshot: snapshot.clone(),
            path,
            cost_scope: "session-associated".to_string(),
            attributed_cost_scope: "action-attributed".to_string(),
            unmatched_invocation_count,
            unmatched_session_count,
            rows,
        })
    }

    pub fn latest_snapshot_bounds_query_plan(&self) -> Result<Vec<String>> {
        self.explain_query_plan(LATEST_SNAPSHOT_BOUNDS_SQL)
    }

    pub fn grouped_action_rollup_browse_query_plan(
        &self,
        request: &BrowseRequest,
    ) -> Result<Vec<String>> {
        let (sql, query_params) = build_grouped_action_rollup_rows_query(request)?;
        self.explain_query_plan_with_params(&sql, query_params)
    }

    pub fn recent_action_facts_query_plan(&self, snapshot: &SnapshotBounds) -> Result<Vec<String>> {
        let Some(upper_bound) = snapshot.upper_bound_timestamp()? else {
            return Ok(Vec::new());
        };
        let Ok(last_week_start) = upper_bound.checked_sub(168.hours()) else {
            return Ok(Vec::new());
        };
        self.explain_query_plan_with_snapshot_and_timestamp(
            LOAD_RECENT_ACTION_FACTS_SQL,
            snapshot,
            &last_week_start.to_string(),
        )
    }

    pub fn action_browse_query_plan(&self, request: &BrowseRequest) -> Result<Vec<String>> {
        let (sql, query_params) = build_scoped_action_facts_query(request)?;
        self.explain_query_plan_with_params(&sql, query_params)
    }

    pub fn path_browse_query_plan(&self, request: &BrowseRequest) -> Result<Vec<String>> {
        let (sql, query_params) = if can_use_path_rollups(&request.filters) {
            build_path_rollup_rows_query(request)?
        } else {
            build_scoped_path_facts_query(request)?
        };
        self.explain_query_plan_with_params(&sql, query_params)
    }

    pub fn batched_non_path_browse_query_plan(
        &self,
        requests: &[BrowseRequest],
    ) -> Result<Vec<String>> {
        let (sql, query_params) = build_batched_grouped_action_rollup_rows_query(requests)?;
        self.explain_query_plan_with_params(&sql, query_params)
    }

    pub fn batched_path_browse_query_plan(
        &self,
        requests: &[BrowseRequest],
    ) -> Result<Vec<String>> {
        let (sql, query_params) = if requests
            .first()
            .is_some_and(|request| can_use_path_rollups(&request.filters))
        {
            build_batched_path_rollup_rows_query(requests)?
        } else {
            build_batched_path_facts_query(requests)?
        };
        self.explain_query_plan_with_params(&sql, query_params)
    }

    fn load_skill_session_associations(
        &self,
        snapshot: &SnapshotBounds,
    ) -> Result<Vec<SkillSessionAssociation>> {
        let max_publish_seq = i64::try_from(snapshot.max_publish_seq)
            .context("snapshot publish_seq overflowed i64")?;
        let sql = "
            WITH conversation_sessions AS (
                SELECT DISTINCT
                    c.id,
                    c.project_id,
                    p.display_name,
                    c.title,
                    substr(c.external_id, instr(c.external_id, ':session:') + 9) AS session_id
                FROM conversation c
                JOIN source_file sf
                    ON sf.id = c.source_file_id
                   AND sf.source_kind = 'transcript'
                JOIN stream s
                    ON s.conversation_id = c.id
                JOIN import_chunk cic
                    ON cic.id = s.import_chunk_id
                JOIN project p
                    ON p.id = c.project_id
                WHERE cic.state = 'complete'
                  AND cic.publish_seq IS NOT NULL
                  AND cic.publish_seq <= ?1
                  AND instr(c.external_id, ':session:') > 0
            ),
            conversation_metrics AS (
                SELECT
                    t.conversation_id,
                    SUM(COALESCE(t.input_tokens, 0) + COALESCE(t.cache_creation_input_tokens, 0)) AS uncached_input,
                    SUM(COALESCE(t.cache_read_input_tokens, 0)) AS cached_input,
                    SUM(COALESCE(t.output_tokens, 0)) AS output_tokens
                FROM turn t
                JOIN import_chunk tic
                    ON tic.id = t.import_chunk_id
                WHERE tic.state = 'complete'
                  AND tic.publish_seq IS NOT NULL
                  AND tic.publish_seq <= ?1
                GROUP BY t.conversation_id
            ),
            attributed_actions AS (
                SELECT
                    cs.session_id,
                    asa.skill_name,
                    COUNT(*) AS action_count,
                    SUM(COALESCE(a.input_tokens, 0) + COALESCE(a.cache_creation_input_tokens, 0)) AS uncached_input,
                    SUM(COALESCE(a.cache_read_input_tokens, 0)) AS cached_input,
                    SUM(COALESCE(a.output_tokens, 0)) AS output_tokens,
                    MAX(asa.confidence) AS top_confidence
                FROM action_skill_attribution asa
                JOIN action a
                    ON a.id = asa.action_id
                JOIN import_chunk aic
                    ON aic.id = a.import_chunk_id
                JOIN turn t
                    ON t.id = a.turn_id
                JOIN conversation_sessions cs
                    ON cs.id = t.conversation_id
                WHERE aic.state = 'complete'
                  AND aic.publish_seq IS NOT NULL
                  AND aic.publish_seq <= ?1
                GROUP BY cs.session_id, asa.skill_name
            ),
            skill_sessions AS (
                SELECT
                    si.skill_name,
                    si.session_id,
                    COUNT(*) AS invocation_count
                FROM skill_invocation si
                JOIN import_chunk sic
                    ON sic.id = si.import_chunk_id
                WHERE sic.state = 'complete'
                  AND sic.publish_seq IS NOT NULL
                  AND sic.publish_seq <= ?1
                GROUP BY si.skill_name, si.session_id
            )
            SELECT
                ss.skill_name,
                ss.session_id,
                ss.invocation_count,
                cs.id,
                cs.project_id,
                cs.display_name,
                cs.title,
                cm.uncached_input,
                cm.cached_input,
                cm.output_tokens,
                COALESCE(aa.action_count, 0),
                COALESCE(aa.uncached_input, 0),
                COALESCE(aa.cached_input, 0),
                COALESCE(aa.output_tokens, 0),
                aa.top_confidence
            FROM skill_sessions ss
            LEFT JOIN conversation_sessions cs
                ON cs.session_id = ss.session_id
            LEFT JOIN conversation_metrics cm
                ON cm.conversation_id = cs.id
            LEFT JOIN attributed_actions aa
                ON aa.session_id = ss.session_id
               AND aa.skill_name = ss.skill_name
            ORDER BY ss.skill_name, cs.project_id, ss.session_id
        ";

        let mut stmt = self
            .conn
            .prepare(sql)
            .context("unable to prepare skills report query")?;
        let rows = stmt
            .query_map([max_publish_seq], |row| {
                let uncached_input = row.get::<_, Option<f64>>(7)?;
                let cached_input = row.get::<_, Option<f64>>(8)?;
                let output = row.get::<_, Option<f64>>(9)?;
                let attributed_uncached_input = row.get::<_, f64>(11)?;
                let attributed_cached_input = row.get::<_, f64>(12)?;
                let attributed_output = row.get::<_, f64>(13)?;
                Ok(SkillSessionAssociation {
                    skill_name: row.get(0)?,
                    session_id: row.get(1)?,
                    invocation_count: row.get::<_, i64>(2)? as u64,
                    conversation_id: row.get(3)?,
                    project_id: row.get(4)?,
                    project_name: row.get(5)?,
                    conversation_title: row.get(6)?,
                    confidence_counts: SkillConfidenceCounts::default(),
                    transcript_evidence_count: 0,
                    metrics: uncached_input.map(|uncached_input| {
                        let cached_input = cached_input.unwrap_or_default();
                        let output = output.unwrap_or_default();
                        MetricTotals {
                            uncached_input,
                            cached_input,
                            gross_input: uncached_input + cached_input,
                            output,
                            total: uncached_input + cached_input + output,
                        }
                    }),
                    attributed_action_count: row.get::<_, i64>(10)? as u64,
                    attributed_metrics: MetricTotals {
                        uncached_input: attributed_uncached_input,
                        cached_input: attributed_cached_input,
                        gross_input: attributed_uncached_input + attributed_cached_input,
                        output: attributed_output,
                        total: attributed_uncached_input
                            + attributed_cached_input
                            + attributed_output,
                    },
                    top_attribution_confidence: row
                        .get::<_, Option<String>>(14)?
                        .as_deref()
                        .and_then(SkillAttributionConfidence::from_db_value),
                })
            })
            .context("unable to execute skills report query")?;

        let base_associations = rows
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("unable to read skills report rows")?;
        let metrics_by_conversation = self.load_conversation_metrics(snapshot)?;
        let invocation_rows =
            self.skill_invocations(snapshot, &SkillInvocationFilters::default())?;
        let mut aggregated = BTreeMap::<(String, String), AggregatedSkillInvocation>::new();
        for row in invocation_rows {
            let entry = aggregated
                .entry((row.skill_name.clone(), row.session_id.clone()))
                .or_insert_with(|| AggregatedSkillInvocation {
                    invocation_count: 0,
                    conversation_id: row.conversation_id,
                    project_id: row.project_id,
                    project_name: row.project_name.clone(),
                    conversation_title: row.conversation_title.clone(),
                    confidence_counts: SkillConfidenceCounts::default(),
                    transcript_evidence_count: 0,
                });
            entry.invocation_count += 1;
            entry.conversation_id = entry.conversation_id.or(row.conversation_id);
            entry.project_id = entry.project_id.or(row.project_id);
            entry.project_name = entry.project_name.clone().or(row.project_name.clone());
            entry.conversation_title = entry
                .conversation_title
                .clone()
                .or(row.conversation_title.clone());
            increment_skill_confidence_count(&mut entry.confidence_counts, row.confidence);
            entry.transcript_evidence_count += row.transcript_evidence_count;
        }

        let mut by_pair = base_associations
            .into_iter()
            .map(|association| {
                (
                    (
                        association.skill_name.clone(),
                        association.session_id.clone(),
                    ),
                    association,
                )
            })
            .collect::<BTreeMap<_, _>>();

        for ((skill_name, session_id), aggregate) in aggregated {
            let entry = by_pair
                .entry((skill_name.clone(), session_id.clone()))
                .or_insert_with(|| SkillSessionAssociation {
                    skill_name,
                    session_id,
                    invocation_count: 0,
                    conversation_id: aggregate.conversation_id,
                    project_id: aggregate.project_id,
                    project_name: aggregate.project_name.clone(),
                    conversation_title: aggregate.conversation_title.clone(),
                    confidence_counts: SkillConfidenceCounts::default(),
                    transcript_evidence_count: 0,
                    metrics: aggregate
                        .conversation_id
                        .and_then(|conversation_id| metrics_by_conversation.get(&conversation_id))
                        .cloned(),
                    attributed_action_count: 0,
                    attributed_metrics: MetricTotals::zero(),
                    top_attribution_confidence: None,
                });
            entry.invocation_count = aggregate.invocation_count;
            entry.conversation_id = entry.conversation_id.or(aggregate.conversation_id);
            entry.project_id = entry.project_id.or(aggregate.project_id);
            entry.project_name = entry
                .project_name
                .clone()
                .or(aggregate.project_name.clone());
            entry.conversation_title = entry
                .conversation_title
                .clone()
                .or(aggregate.conversation_title.clone());
            entry.confidence_counts = aggregate.confidence_counts;
            entry.transcript_evidence_count = aggregate.transcript_evidence_count;
            if entry.metrics.is_none() {
                entry.metrics = entry
                    .conversation_id
                    .and_then(|conversation_id| metrics_by_conversation.get(&conversation_id))
                    .cloned();
            }
        }

        Ok(by_pair.into_values().collect())
    }

    fn load_explicit_skill_invocations(
        &self,
        snapshot: &SnapshotBounds,
    ) -> Result<Vec<SkillInvocationRow>> {
        let max_publish_seq = i64::try_from(snapshot.max_publish_seq)
            .context("snapshot publish_seq overflowed i64")?;
        let sql = "
            SELECT
                si.skill_name,
                si.session_id,
                si.recorded_at_utc,
                si.raw_project,
                c.id,
                c.project_id,
                c.display_name,
                c.title
            FROM skill_invocation si
            JOIN import_chunk sic ON sic.id = si.import_chunk_id
            LEFT JOIN (
                SELECT DISTINCT
                    c.id,
                    c.title,
                    c.project_id,
                    p.display_name,
                    substr(c.external_id, instr(c.external_id, ':session:') + 9) AS session_id
                FROM conversation c
                JOIN source_file sf
                    ON sf.id = c.source_file_id
                   AND sf.source_kind = 'transcript'
                JOIN stream s
                    ON s.conversation_id = c.id
                JOIN import_chunk cic
                    ON cic.id = s.import_chunk_id
                JOIN project p
                    ON p.id = c.project_id
                WHERE cic.state = 'complete'
                  AND cic.publish_seq IS NOT NULL
                  AND cic.publish_seq <= ?1
                  AND instr(c.external_id, ':session:') > 0
            ) c
                ON c.session_id = si.session_id
            WHERE sic.state = 'complete'
              AND sic.publish_seq IS NOT NULL
              AND sic.publish_seq <= ?1
            ORDER BY si.skill_name, si.session_id, si.recorded_at_utc, c.id
        ";

        let mut stmt = self
            .conn
            .prepare(sql)
            .context("unable to prepare explicit skill invocation query")?;
        let rows = stmt
            .query_map([max_publish_seq], |row| {
                Ok(SkillInvocationRow {
                    skill_name: row.get(0)?,
                    session_id: row.get(1)?,
                    recorded_at_utc: row.get(2)?,
                    raw_project: row.get(3)?,
                    conversation_id: row.get(4)?,
                    project_id: row.get(5)?,
                    project_name: row.get(6)?,
                    conversation_title: row.get(7)?,
                    confidence: SkillInvocationConfidence::Explicit,
                    transcript_evidence_kinds: Vec::new(),
                    transcript_evidence_count: 0,
                })
            })
            .context("unable to execute explicit skill invocation query")?;

        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("unable to read explicit skill invocation rows")
    }

    fn load_skill_transcript_evidence(
        &self,
        snapshot: &SnapshotBounds,
    ) -> Result<Vec<SkillTranscriptEvidence>> {
        let max_publish_seq = i64::try_from(snapshot.max_publish_seq)
            .context("snapshot publish_seq overflowed i64")?;
        let sql = "
            SELECT
                substr(c.external_id, instr(c.external_id, ':session:') + 9) AS session_id,
                c.id,
                c.project_id,
                p.display_name,
                c.title,
                COALESCE(m.created_at_utc, m.completed_at_utc),
                mp.text_value,
                mp.metadata_json
            FROM conversation c
            JOIN source_file sf
                ON sf.id = c.source_file_id
               AND sf.source_kind = 'transcript'
            JOIN message m
                ON m.conversation_id = c.id
            JOIN import_chunk mic
                ON mic.id = m.import_chunk_id
               AND mic.state = 'complete'
               AND mic.publish_seq IS NOT NULL
               AND mic.publish_seq <= ?1
            JOIN message_part mp
                ON mp.message_id = m.id
            JOIN project p
                ON p.id = c.project_id
            WHERE instr(c.external_id, ':session:') > 0
              AND (
                  mp.text_value IS NOT NULL
                  OR mp.metadata_json IS NOT NULL
              )
            ORDER BY c.id, m.sequence_no, mp.ordinal
        ";
        let mut stmt = self
            .conn
            .prepare(sql)
            .context("unable to prepare transcript skill evidence query")?;
        let rows = stmt
            .query_map([max_publish_seq], |row| {
                Ok(LoadedSkillTranscriptPart {
                    session_id: row.get(0)?,
                    conversation_id: row.get(1)?,
                    project_id: row.get(2)?,
                    project_name: row.get(3)?,
                    conversation_title: row.get(4)?,
                    recorded_at_utc: row.get(5)?,
                    text_value: row.get(6)?,
                    metadata_json: row.get(7)?,
                })
            })
            .context("unable to execute transcript skill evidence query")?;

        let mut evidence = Vec::new();
        for row in rows {
            let row = row.context("unable to read transcript skill evidence row")?;
            evidence.extend(skill_transcript_evidence_from_part(&row));
        }

        Ok(evidence)
    }

    fn load_conversation_metrics(
        &self,
        snapshot: &SnapshotBounds,
    ) -> Result<HashMap<i64, MetricTotals>> {
        let max_publish_seq = i64::try_from(snapshot.max_publish_seq)
            .context("snapshot publish_seq overflowed i64")?;
        let sql = "
            SELECT
                t.conversation_id,
                SUM(COALESCE(t.input_tokens, 0) + COALESCE(t.cache_creation_input_tokens, 0)),
                SUM(COALESCE(t.cache_read_input_tokens, 0)),
                SUM(COALESCE(t.output_tokens, 0))
            FROM turn t
            JOIN import_chunk tic
                ON tic.id = t.import_chunk_id
            WHERE tic.state = 'complete'
              AND tic.publish_seq IS NOT NULL
              AND tic.publish_seq <= ?1
            GROUP BY t.conversation_id
        ";
        let mut stmt = self
            .conn
            .prepare(sql)
            .context("unable to prepare conversation metrics query")?;
        let rows = stmt
            .query_map([max_publish_seq], |row| {
                let conversation_id = row.get::<_, i64>(0)?;
                let uncached_input = row.get::<_, f64>(1)?;
                let cached_input = row.get::<_, f64>(2)?;
                let output = row.get::<_, f64>(3)?;
                Ok((
                    conversation_id,
                    MetricTotals {
                        uncached_input,
                        cached_input,
                        gross_input: uncached_input + cached_input,
                        output,
                        total: uncached_input + cached_input + output,
                    },
                ))
            })
            .context("unable to execute conversation metrics query")?;

        rows.collect::<rusqlite::Result<HashMap<_, _>>>()
            .context("unable to read conversation metrics rows")
    }

    fn load_action_facts(&self, request: &BrowseRequest) -> Result<Vec<ActionFact>> {
        let mut perf = self.verbose_perf_scope("query.load_action_facts");
        record_browse_request_perf_fields(&mut perf, request);
        if request.snapshot.max_publish_seq == 0 {
            perf.field("row_count", 0usize);
            perf.finish_ok();
            return Ok(Vec::new());
        }

        let (sql, query_params) = build_scoped_action_facts_query(request)?;
        let mut stmt = self.conn.prepare(&sql)?;

        let rows = stmt.query_map(params_from_iter(query_params.iter()), |row| {
            Ok(LoadedActionFact {
                project_id: row.get(0)?,
                project_display_name: row.get(1)?,
                project_root: row.get(2)?,
                project_identity: ProjectIdentity {
                    identity_kind: row.get(3)?,
                    root_path: row.get(2)?,
                    git_root_path: row.get(4)?,
                    git_origin: row.get(5)?,
                    identity_reason: row.get(6)?,
                },
                category: row.get(7)?,
                normalized_action: row.get(8)?,
                command_family: row.get(9)?,
                base_command: row.get(10)?,
                classification_state: row.get(11)?,
                timestamp: row.get(12)?,
                input_tokens: row.get(13)?,
                cache_creation_input_tokens: row.get(14)?,
                cache_read_input_tokens: row.get(15)?,
                output_tokens: row.get(16)?,
                model_names_csv: row.get(17)?,
                skill_name: row.get(18)?,
                skill_confidence: row.get(19)?,
            })
        })?;

        let facts = rows
            .map(|row| {
                let row = row.context("unable to read an action fact row")?;
                Ok(ActionFact {
                    project_id: row.project_id,
                    project_display_name: row.project_display_name,
                    project_root: row.project_root,
                    project_identity: row.project_identity,
                    category: display_category(row.category.as_deref(), &row.classification_state)?,
                    action: ActionKey {
                        classification_state: parse_classification_state(
                            &row.classification_state,
                        )?,
                        normalized_action: row.normalized_action,
                        command_family: row.command_family,
                        base_command: row.base_command,
                    },
                    timestamp: parse_timestamp(row.timestamp.as_deref())?,
                    metrics: MetricTotals::from_usage(
                        row.input_tokens,
                        row.cache_creation_input_tokens,
                        row.cache_read_input_tokens,
                        row.output_tokens,
                    ),
                    model_names: split_model_names(row.model_names_csv.as_deref()),
                    item_count: 1,
                    skill_attribution: parse_skill_attribution(
                        row.skill_name,
                        row.skill_confidence,
                    ),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        perf.field("row_count", facts.len());
        perf.finish_ok();
        Ok(facts)
    }

    fn load_grouped_action_rollup_rows(&self, request: &BrowseRequest) -> Result<Vec<RollupRow>> {
        let mut perf = self.verbose_perf_scope("query.load_grouped_action_rollup_rows");
        record_browse_request_perf_fields(&mut perf, request);
        if request.snapshot.max_publish_seq == 0 {
            perf.field("row_count", 0usize);
            perf.finish_ok();
            return Ok(Vec::new());
        }

        let (sql, query_params) = build_grouped_action_rollup_rows_query(request)?;
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(query_params.iter()), |row| {
            let project_root: Option<String> = row.get(2)?;
            let identity_kind: Option<String> = row.get(3)?;
            let git_root_path: Option<String> = row.get(4)?;
            let git_origin: Option<String> = row.get(5)?;
            let identity_reason: Option<String> = row.get(6)?;

            Ok(LoadedGroupedActionRollupRow {
                project_id: row.get(0)?,
                project_display_name: row.get(1)?,
                project_root: project_root.clone(),
                project_identity: match (project_root.clone(), identity_kind) {
                    (Some(root_path), Some(identity_kind)) => Some(ProjectIdentity {
                        identity_kind,
                        root_path,
                        git_root_path,
                        git_origin,
                        identity_reason,
                    }),
                    _ => None,
                },
                display_category: row.get(7)?,
                classification_state: row.get(8)?,
                normalized_action: row.get(9)?,
                command_family: row.get(10)?,
                base_command: row.get(11)?,
                input_tokens: row.get(12)?,
                cache_creation_input_tokens: row.get(13)?,
                cache_read_input_tokens: row.get(14)?,
                output_tokens: row.get(15)?,
                action_count: row.get(16)?,
            })
        })?;

        let facts = rows
            .map(|row| {
                let row = row.context("unable to read a grouped action rollup row")?;
                grouped_action_rollup_row_to_rollup_row(request, row)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut facts = facts;
        self.apply_action_rollup_skill_attributions(request, &mut facts)?;
        perf.field("row_count", facts.len());
        perf.finish_ok();
        Ok(facts)
    }

    fn browse_many_compatible(&self, requests: &[BrowseRequest]) -> Result<Vec<Vec<RollupRow>>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        if is_path_browse(&requests[0].path) {
            self.load_batched_path_browse_rows(requests)
        } else {
            self.load_batched_grouped_action_rollup_rows(requests)
        }
    }

    fn load_batched_grouped_action_rollup_rows(
        &self,
        requests: &[BrowseRequest],
    ) -> Result<Vec<Vec<RollupRow>>> {
        let mut perf = self.perf_scope("query.load_batched_grouped_action_rollup_rows");
        perf.field("request_count", requests.len());
        if requests.is_empty() {
            perf.finish_ok();
            return Ok(Vec::new());
        }

        if requests[0].snapshot.max_publish_seq == 0 {
            perf.finish_ok();
            return Ok(vec![Vec::new(); requests.len()]);
        }

        let (sql, query_params) = build_batched_grouped_action_rollup_rows_query(requests)?;
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(query_params.iter()), |row| {
            let project_root: Option<String> = row.get(3)?;
            let identity_kind: Option<String> = row.get(4)?;
            let git_root_path: Option<String> = row.get(5)?;
            let git_origin: Option<String> = row.get(6)?;
            let identity_reason: Option<String> = row.get(7)?;

            Ok((
                row.get::<_, i64>(0)?,
                LoadedGroupedActionRollupRow {
                    project_id: row.get(1)?,
                    project_display_name: row.get(2)?,
                    project_root: project_root.clone(),
                    project_identity: match (project_root.clone(), identity_kind) {
                        (Some(root_path), Some(identity_kind)) => Some(ProjectIdentity {
                            identity_kind,
                            root_path,
                            git_root_path,
                            git_origin,
                            identity_reason,
                        }),
                        _ => None,
                    },
                    display_category: row.get(8)?,
                    classification_state: row.get(9)?,
                    normalized_action: row.get(10)?,
                    command_family: row.get(11)?,
                    base_command: row.get(12)?,
                    input_tokens: row.get(13)?,
                    cache_creation_input_tokens: row.get(14)?,
                    cache_read_input_tokens: row.get(15)?,
                    output_tokens: row.get(16)?,
                    action_count: row.get(17)?,
                },
            ))
        })?;

        let mut row_sets = vec![Vec::new(); requests.len()];
        for row in rows {
            let (request_index, loaded_row) =
                row.context("unable to read a batched grouped action rollup row")?;
            let request_index = usize::try_from(request_index)
                .context("batched grouped action request index overflowed usize")?;
            let request = requests
                .get(request_index)
                .context("batched grouped action row referenced an unknown request")?;
            row_sets[request_index].push(grouped_action_rollup_row_to_rollup_row(
                request, loaded_row,
            )?);
        }

        if let Some(upper_bound) = requests[0].snapshot.upper_bound_timestamp()?
            && let Ok(last_week_start) = upper_bound.checked_sub(168.hours())
        {
            let recent_action_facts =
                self.load_recent_action_facts(&requests[0].snapshot, last_week_start)?;
            let windows = Windows::from_snapshot(&requests[0].snapshot)?;
            let compiled_filters = CompiledFilters::compile(&requests[0].filters)?;
            for (request, rows) in requests.iter().zip(row_sets.iter_mut()) {
                let indicator_rows = aggregate_action_request(
                    request,
                    &compiled_filters,
                    &windows,
                    &recent_action_facts,
                )?;
                apply_indicator_rows(rows, indicator_rows);
            }
        }

        for (request, rows) in requests.iter().zip(row_sets.iter_mut()) {
            self.apply_action_rollup_skill_attributions(request, rows)?;
            finalize_browse_rows(request, rows);
        }

        perf.field("row_set_count", row_sets.len());
        perf.finish_ok();
        Ok(row_sets)
    }

    fn load_batched_path_browse_rows(
        &self,
        requests: &[BrowseRequest],
    ) -> Result<Vec<Vec<RollupRow>>> {
        let mut perf = self.perf_scope("query.load_batched_path_browse_rows");
        perf.field("request_count", requests.len());
        if requests.is_empty() {
            perf.finish_ok();
            return Ok(Vec::new());
        }

        if requests[0].snapshot.max_publish_seq == 0 {
            perf.finish_ok();
            return Ok(vec![Vec::new(); requests.len()]);
        }

        if can_use_path_rollups(&requests[0].filters) {
            let (sql, query_params) = build_batched_path_rollup_rows_query(requests)?;
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(query_params.iter()), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    LoadedPathRollupRow {
                        child_path: row.get(1)?,
                        child_label: row.get(2)?,
                        child_kind: row.get(3)?,
                        input_tokens: row.get(4)?,
                        cache_creation_input_tokens: row.get(5)?,
                        cache_read_input_tokens: row.get(6)?,
                        output_tokens: row.get(7)?,
                        item_count: row.get(8)?,
                    },
                ))
            })?;

            let mut row_sets = vec![Vec::new(); requests.len()];
            for row in rows {
                let (request_index, loaded_row) =
                    row.context("unable to read a batched path rollup row")?;
                let request_index = usize::try_from(request_index)
                    .context("batched path rollup request index overflowed usize")?;
                let request = requests
                    .get(request_index)
                    .context("batched path rollup row referenced an unknown request")?;
                row_sets[request_index].push(path_rollup_row_to_rollup_row(request, loaded_row)?);
            }

            for (request, rows) in requests.iter().zip(row_sets.iter_mut()) {
                self.apply_path_rollup_skill_attributions(request, rows)?;
                finalize_browse_rows(request, rows);
            }

            perf.field("row_set_count", row_sets.len());
            perf.finish_ok();
            return Ok(row_sets);
        }

        let (sql, query_params) = build_batched_path_facts_query(requests)?;
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(query_params.iter()), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                LoadedPathFact {
                    action_id: row.get(1)?,
                    project_id: row.get(2)?,
                    project_root: row.get(3)?,
                    category: row.get(4)?,
                    normalized_action: row.get(5)?,
                    command_family: row.get(6)?,
                    base_command: row.get(7)?,
                    classification_state: row.get(8)?,
                    timestamp: row.get(9)?,
                    input_tokens: row.get(10)?,
                    cache_creation_input_tokens: row.get(11)?,
                    cache_read_input_tokens: row.get(12)?,
                    output_tokens: row.get(13)?,
                    model_name: row.get(14)?,
                    file_path: row.get(15)?,
                    ref_count: row.get(16)?,
                    skill_name: row.get(17)?,
                    skill_confidence: row.get(18)?,
                },
            ))
        })?;

        let mut facts_by_request = (0..requests.len()).map(|_| Vec::new()).collect::<Vec<_>>();
        for row in rows {
            let (request_index, loaded_row) =
                row.context("unable to read a batched path browse row")?;
            let request_index = usize::try_from(request_index)
                .context("batched path request index overflowed usize")?;
            let metrics = MetricTotals::from_usage(
                loaded_row.input_tokens,
                loaded_row.cache_creation_input_tokens,
                loaded_row.cache_read_input_tokens,
                loaded_row.output_tokens,
            )
            .divided_by(loaded_row.ref_count as f64);
            facts_by_request[request_index].push(PathFact {
                action_id: loaded_row.action_id,
                project_id: loaded_row.project_id,
                project_root: loaded_row.project_root,
                category: display_category(
                    loaded_row.category.as_deref(),
                    &loaded_row.classification_state,
                )?,
                action: ActionKey {
                    classification_state: parse_classification_state(
                        &loaded_row.classification_state,
                    )?,
                    normalized_action: loaded_row.normalized_action,
                    command_family: loaded_row.command_family,
                    base_command: loaded_row.base_command,
                },
                timestamp: parse_timestamp(loaded_row.timestamp.as_deref())?,
                model_name: loaded_row.model_name,
                file_path: loaded_row.file_path,
                metrics,
                skill_attribution: parse_skill_attribution(
                    loaded_row.skill_name,
                    loaded_row.skill_confidence,
                ),
            });
        }

        let compiled_filters = CompiledFilters::compile(&requests[0].filters)?;
        let windows = Windows::from_snapshot(&requests[0].snapshot)?;
        let mut row_sets = Vec::with_capacity(requests.len());
        for (request, facts) in requests.iter().zip(facts_by_request.iter()) {
            let mut rows = aggregate_path_request(request, &compiled_filters, &windows, facts)?;
            finalize_browse_rows(request, &mut rows);
            row_sets.push(rows);
        }

        perf.field("row_set_count", row_sets.len());
        perf.finish_ok();
        Ok(row_sets)
    }

    fn load_recent_action_facts(
        &self,
        snapshot: &SnapshotBounds,
        start_at: Timestamp,
    ) -> Result<Vec<ActionFact>> {
        let mut perf = self.verbose_perf_scope("query.load_recent_action_facts");
        perf.field("snapshot", snapshot);
        perf.field("start_at", start_at.to_string());
        if snapshot.max_publish_seq == 0 {
            perf.field("row_count", 0usize);
            perf.finish_ok();
            return Ok(Vec::new());
        }

        let mut stmt = self.conn.prepare(LOAD_RECENT_ACTION_FACTS_SQL)?;
        let rows = stmt.query_map(
            params![snapshot.max_publish_seq as i64, start_at.to_string()],
            |row| {
                Ok(LoadedActionFact {
                    project_id: row.get(0)?,
                    project_display_name: row.get(1)?,
                    project_root: row.get(2)?,
                    project_identity: ProjectIdentity {
                        identity_kind: row.get(3)?,
                        root_path: row.get(2)?,
                        git_root_path: row.get(4)?,
                        git_origin: row.get(5)?,
                        identity_reason: row.get(6)?,
                    },
                    category: row.get(7)?,
                    normalized_action: row.get(8)?,
                    command_family: row.get(9)?,
                    base_command: row.get(10)?,
                    classification_state: row.get(11)?,
                    timestamp: row.get(12)?,
                    input_tokens: row.get(13)?,
                    cache_creation_input_tokens: row.get(14)?,
                    cache_read_input_tokens: row.get(15)?,
                    output_tokens: row.get(16)?,
                    model_names_csv: None,
                    skill_name: None,
                    skill_confidence: None,
                })
            },
        )?;

        let facts = rows
            .map(|row| {
                let row = row.context("unable to read a recent action fact row")?;
                Ok(ActionFact {
                    project_id: row.project_id,
                    project_display_name: row.project_display_name,
                    project_root: row.project_root,
                    project_identity: row.project_identity,
                    category: display_category(row.category.as_deref(), &row.classification_state)?,
                    action: ActionKey {
                        classification_state: parse_classification_state(
                            &row.classification_state,
                        )?,
                        normalized_action: row.normalized_action,
                        command_family: row.command_family,
                        base_command: row.base_command,
                    },
                    timestamp: parse_timestamp(row.timestamp.as_deref())?,
                    metrics: MetricTotals::from_usage(
                        row.input_tokens,
                        row.cache_creation_input_tokens,
                        row.cache_read_input_tokens,
                        row.output_tokens,
                    ),
                    model_names: BTreeSet::new(),
                    item_count: 1,
                    skill_attribution: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        perf.field("row_count", facts.len());
        perf.finish_ok();
        Ok(facts)
    }

    fn load_path_facts(&self, request: &BrowseRequest) -> Result<Vec<PathFact>> {
        let mut perf = self.verbose_perf_scope("query.load_path_facts");
        record_browse_request_perf_fields(&mut perf, request);
        if request.snapshot.max_publish_seq == 0 {
            perf.field("row_count", 0usize);
            perf.finish_ok();
            return Ok(Vec::new());
        }

        let (sql, query_params) = build_scoped_path_facts_query(request)?;
        let mut stmt = self.conn.prepare(&sql)?;

        let rows = stmt.query_map(params_from_iter(query_params.iter()), |row| {
            Ok(LoadedPathFact {
                action_id: row.get(0)?,
                project_id: row.get(1)?,
                project_root: row.get(2)?,
                category: row.get(3)?,
                normalized_action: row.get(4)?,
                command_family: row.get(5)?,
                base_command: row.get(6)?,
                classification_state: row.get(7)?,
                timestamp: row.get(8)?,
                input_tokens: row.get(9)?,
                cache_creation_input_tokens: row.get(10)?,
                cache_read_input_tokens: row.get(11)?,
                output_tokens: row.get(12)?,
                model_name: row.get(13)?,
                file_path: row.get(14)?,
                ref_count: row.get(15)?,
                skill_name: row.get(16)?,
                skill_confidence: row.get(17)?,
            })
        })?;

        let facts = rows
            .map(|row| {
                let row = row.context("unable to read a path attribution fact row")?;
                let metrics = MetricTotals::from_usage(
                    row.input_tokens,
                    row.cache_creation_input_tokens,
                    row.cache_read_input_tokens,
                    row.output_tokens,
                )
                .divided_by(row.ref_count as f64);

                Ok(PathFact {
                    action_id: row.action_id,
                    project_id: row.project_id,
                    project_root: row.project_root,
                    category: display_category(row.category.as_deref(), &row.classification_state)?,
                    action: ActionKey {
                        classification_state: parse_classification_state(
                            &row.classification_state,
                        )?,
                        normalized_action: row.normalized_action,
                        command_family: row.command_family,
                        base_command: row.base_command,
                    },
                    timestamp: parse_timestamp(row.timestamp.as_deref())?,
                    model_name: row.model_name,
                    file_path: row.file_path,
                    metrics,
                    skill_attribution: parse_skill_attribution(
                        row.skill_name,
                        row.skill_confidence,
                    ),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        perf.field("row_count", facts.len());
        perf.finish_ok();
        Ok(facts)
    }

    fn load_path_rollup_rows(&self, request: &BrowseRequest) -> Result<Vec<RollupRow>> {
        let mut perf = self.verbose_perf_scope("query.load_path_rollup_rows");
        record_browse_request_perf_fields(&mut perf, request);
        if request.snapshot.max_publish_seq == 0 {
            perf.field("row_count", 0usize);
            perf.finish_ok();
            return Ok(Vec::new());
        }

        let (sql, query_params) = build_path_rollup_rows_query(request)?;
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(query_params.iter()), |row| {
            Ok(LoadedPathRollupRow {
                child_path: row.get(0)?,
                child_label: row.get(1)?,
                child_kind: row.get(2)?,
                input_tokens: row.get(3)?,
                cache_creation_input_tokens: row.get(4)?,
                cache_read_input_tokens: row.get(5)?,
                output_tokens: row.get(6)?,
                item_count: row.get(7)?,
            })
        })?;

        let rollup_rows = rows
            .map(|row| {
                let row = row.context("unable to read a path rollup row")?;
                path_rollup_row_to_rollup_row(request, row)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut rollup_rows = rollup_rows;
        self.apply_path_rollup_skill_attributions(request, &mut rollup_rows)?;
        perf.field("row_count", rollup_rows.len());
        perf.finish_ok();
        Ok(rollup_rows)
    }

    fn apply_action_rollup_skill_attributions(
        &self,
        request: &BrowseRequest,
        rows: &mut [RollupRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let category = match (&request.root, &request.path) {
            (
                RootView::ProjectHierarchy,
                BrowsePath::ProjectCategory {
                    project_id: _,
                    category,
                },
            )
            | (RootView::CategoryHierarchy, BrowsePath::Category { category }) => category,
            _ => return Ok(()),
        };

        let mut sql = String::from(
            "
            WITH requested_action(
                row_key,
                classification_state,
                normalized_action,
                command_family,
                base_command
            ) AS (VALUES
            ",
        );
        let mut query_params = Vec::new();
        let mut has_values = false;
        for row in rows.iter() {
            let Some(action) = row.action.as_ref() else {
                continue;
            };
            if has_values {
                sql.push_str(", ");
            }
            sql.push_str("(?, ?, ?, ?, ?)");
            has_values = true;
            query_params.push(Value::Text(row.key.clone()));
            query_params.push(Value::Text(
                action.classification_state.as_str().to_string(),
            ));
            match &action.normalized_action {
                Some(value) => query_params.push(Value::Text(value.clone())),
                None => query_params.push(Value::Null),
            }
            match &action.command_family {
                Some(value) => query_params.push(Value::Text(value.clone())),
                None => query_params.push(Value::Null),
            }
            match &action.base_command {
                Some(value) => query_params.push(Value::Text(value.clone())),
                None => query_params.push(Value::Null),
            }
        }
        if !has_values {
            return Ok(());
        }
        sql.push_str(
            ")
            SELECT
                ra.row_key,
                CASE
                    WHEN COUNT(DISTINCT a.id) > 0
                     AND COUNT(DISTINCT a.id) = COUNT(DISTINCT asa.action_id)
                     AND COUNT(DISTINCT asa.skill_name) = 1
                     AND COUNT(DISTINCT asa.confidence) = 1
                    THEN MIN(asa.skill_name)
                    ELSE NULL
                END AS skill_name,
                CASE
                    WHEN COUNT(DISTINCT a.id) > 0
                     AND COUNT(DISTINCT a.id) = COUNT(DISTINCT asa.action_id)
                     AND COUNT(DISTINCT asa.skill_name) = 1
                     AND COUNT(DISTINCT asa.confidence) = 1
                    THEN MIN(asa.confidence)
                    ELSE NULL
                END AS confidence
            FROM requested_action ra
            JOIN action a
            JOIN import_chunk ic ON ic.id = a.import_chunk_id
            JOIN project p ON p.id = ic.project_id
            LEFT JOIN action_skill_attribution asa ON asa.action_id = a.id
            WHERE ic.state = 'complete'
              AND ic.publish_seq IS NOT NULL
              AND ic.publish_seq <= ?
              AND ",
        );
        query_params.push(Value::Integer(
            i64::try_from(request.snapshot.max_publish_seq)
                .context("snapshot publish_seq overflowed i64")?,
        ));
        sql.push_str(DISPLAY_CATEGORY_SQL);
        sql.push_str(
            " = ?
              AND a.classification_state = ra.classification_state
              AND a.normalized_action IS ra.normalized_action
              AND a.command_family IS ra.command_family
              AND a.base_command IS ra.base_command
            ",
        );
        query_params.push(Value::Text(category.clone()));
        if let BrowsePath::ProjectCategory { project_id, .. } = &request.path {
            append_project_match(&mut sql, &mut query_params, *project_id);
        }
        if let Some(project_id) = request.filters.project_id {
            append_project_match(&mut sql, &mut query_params, project_id);
        }
        sql.push_str(" GROUP BY ra.row_key");

        let mut stmt = self.conn.prepare(&sql)?;
        let annotation_rows = stmt
            .query_map(params_from_iter(query_params.iter()), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    parse_skill_attribution(row.get(1)?, row.get(2)?),
                ))
            })?
            .collect::<rusqlite::Result<BTreeMap<_, _>>>()?;

        for row in rows {
            row.skill_attribution = annotation_rows.get(&row.key).cloned().flatten();
        }

        Ok(())
    }

    fn apply_path_rollup_skill_attributions(
        &self,
        request: &BrowseRequest,
        rows: &mut [RollupRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let (project_id, category, action, parent_path) = match (&request.root, &request.path) {
            (
                RootView::ProjectHierarchy,
                BrowsePath::ProjectAction {
                    project_id,
                    category,
                    action,
                    parent_path,
                },
            )
            | (
                RootView::CategoryHierarchy,
                BrowsePath::CategoryActionProject {
                    category,
                    action,
                    project_id,
                    parent_path,
                },
            ) => (*project_id, category, action, parent_path.as_deref()),
            _ => return Ok(()),
        };

        let mut sql = String::from(
            "
            WITH requested_child(row_key, child_path, child_kind) AS (VALUES
            ",
        );
        let mut query_params = Vec::new();
        let mut has_values = false;
        for row in rows.iter() {
            let Some(path) = row.full_path.as_ref() else {
                continue;
            };
            let kind = match row.kind {
                RollupRowKind::Directory => "directory",
                RollupRowKind::File => "file",
                _ => continue,
            };
            if has_values {
                sql.push_str(", ");
            }
            sql.push_str("(?, ?, ?)");
            has_values = true;
            query_params.push(Value::Text(row.key.clone()));
            query_params.push(Value::Text(path.clone()));
            query_params.push(Value::Text(kind.to_string()));
        }
        if !has_values {
            return Ok(());
        }
        sql.push_str(
            ")
            SELECT
                rc.row_key,
                CASE
                    WHEN COUNT(DISTINCT a.id) > 0
                     AND COUNT(DISTINCT a.id) = COUNT(DISTINCT asa.action_id)
                     AND COUNT(DISTINCT asa.skill_name) = 1
                     AND COUNT(DISTINCT asa.confidence) = 1
                    THEN MIN(asa.skill_name)
                    ELSE NULL
                END AS skill_name,
                CASE
                    WHEN COUNT(DISTINCT a.id) > 0
                     AND COUNT(DISTINCT a.id) = COUNT(DISTINCT asa.action_id)
                     AND COUNT(DISTINCT asa.skill_name) = 1
                     AND COUNT(DISTINCT asa.confidence) = 1
                    THEN MIN(asa.confidence)
                    ELSE NULL
                END AS confidence
            FROM requested_child rc
            JOIN action a
            JOIN import_chunk ic ON ic.id = a.import_chunk_id
            JOIN project p ON p.id = ic.project_id
            JOIN message m ON m.action_id = a.id
            JOIN message_path_ref mpr ON mpr.message_id = m.id
            JOIN path_node pn ON pn.id = mpr.path_node_id
            LEFT JOIN action_skill_attribution asa ON asa.action_id = a.id
            WHERE ic.state = 'complete'
              AND ic.publish_seq IS NOT NULL
              AND ic.publish_seq <= ?
              AND pn.node_kind = 'file'
              AND p.id = ?
              AND ",
        );
        query_params.push(Value::Integer(
            i64::try_from(request.snapshot.max_publish_seq)
                .context("snapshot publish_seq overflowed i64")?,
        ));
        query_params.push(Value::Integer(project_id));
        sql.push_str(DISPLAY_CATEGORY_SQL);
        sql.push_str(
            " = ?
              AND a.classification_state = ?
            ",
        );
        query_params.push(Value::Text(category.clone()));
        query_params.push(Value::Text(
            action.classification_state.as_str().to_string(),
        ));
        if let Some(normalized_action) = action.normalized_action.as_deref() {
            sql.push_str(" AND a.normalized_action = ?");
            query_params.push(Value::Text(normalized_action.to_string()));
        } else {
            sql.push_str(" AND a.normalized_action IS NULL");
        }
        if let Some(command_family) = action.command_family.as_deref() {
            sql.push_str(" AND a.command_family = ?");
            query_params.push(Value::Text(command_family.to_string()));
        } else {
            sql.push_str(" AND a.command_family IS NULL");
        }
        if let Some(base_command) = action.base_command.as_deref() {
            sql.push_str(" AND a.base_command = ?");
            query_params.push(Value::Text(base_command.to_string()));
        } else {
            sql.push_str(" AND a.base_command IS NULL");
        }
        if let Some(parent_path) = parent_path {
            sql.push_str(" AND substr(pn.full_path, 1, length(?) + 1) = ? || '/'");
            query_params.push(Value::Text(parent_path.to_string()));
            query_params.push(Value::Text(parent_path.to_string()));
        }
        sql.push_str(
            "
              AND (
                    (rc.child_kind = 'file' AND pn.full_path = rc.child_path)
                 OR (rc.child_kind = 'directory' AND substr(pn.full_path, 1, length(rc.child_path) + 1) = rc.child_path || '/')
              )
            GROUP BY rc.row_key
            ",
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let annotation_rows = stmt
            .query_map(params_from_iter(query_params.iter()), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    parse_skill_attribution(row.get(1)?, row.get(2)?),
                ))
            })?
            .collect::<rusqlite::Result<BTreeMap<_, _>>>()?;

        for row in rows {
            row.skill_attribution = annotation_rows.get(&row.key).cloned().flatten();
        }

        Ok(())
    }

    fn explain_query_plan(&self, sql: &str) -> Result<Vec<String>> {
        let explain_sql = format!("EXPLAIN QUERY PLAN {sql}");
        let mut stmt = self
            .conn
            .prepare(&explain_sql)
            .context("unable to prepare EXPLAIN QUERY PLAN statement")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(3))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("unable to read EXPLAIN QUERY PLAN rows")
    }

    fn explain_query_plan_with_snapshot_and_timestamp(
        &self,
        sql: &str,
        snapshot: &SnapshotBounds,
        timestamp: &str,
    ) -> Result<Vec<String>> {
        let explain_sql = format!("EXPLAIN QUERY PLAN {sql}");
        let max_publish_seq = i64::try_from(snapshot.max_publish_seq)
            .context("snapshot publish_seq overflowed i64 for EXPLAIN QUERY PLAN")?;
        let mut stmt = self
            .conn
            .prepare(&explain_sql)
            .context("unable to prepare EXPLAIN QUERY PLAN statement")?;
        let rows = stmt.query_map(params![max_publish_seq, timestamp], |row| {
            row.get::<_, String>(3)
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("unable to read EXPLAIN QUERY PLAN rows")
    }

    fn explain_query_plan_with_params(
        &self,
        sql: &str,
        query_params: Vec<Value>,
    ) -> Result<Vec<String>> {
        let explain_sql = format!("EXPLAIN QUERY PLAN {sql}");
        let mut stmt = self
            .conn
            .prepare(&explain_sql)
            .context("unable to prepare EXPLAIN QUERY PLAN statement")?;
        let rows = stmt.query_map(params_from_iter(query_params.iter()), |row| {
            row.get::<_, String>(3)
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("unable to read EXPLAIN QUERY PLAN rows")
    }
}

#[derive(Debug)]
struct LoadedActionFact {
    project_id: i64,
    project_display_name: String,
    project_root: String,
    project_identity: ProjectIdentity,
    category: Option<String>,
    normalized_action: Option<String>,
    command_family: Option<String>,
    base_command: Option<String>,
    classification_state: String,
    timestamp: Option<String>,
    input_tokens: i64,
    cache_creation_input_tokens: i64,
    cache_read_input_tokens: i64,
    output_tokens: i64,
    model_names_csv: Option<String>,
    skill_name: Option<String>,
    skill_confidence: Option<String>,
}

#[derive(Debug)]
struct ConversationTurnRow {
    conversation_id: i64,
    project_id: i64,
    project_name: String,
    conversation_title: Option<String>,
    _sequence_no: i64,
    uncached_input: i64,
    cached_input: i64,
    output_tokens: i64,
}

fn build_opportunities_rows(
    turn_rows: &[ConversationTurnRow],
    include_empty: bool,
) -> Vec<OpportunitiesReportRow> {
    let mut result = Vec::new();

    let mut idx = 0;
    while idx < turn_rows.len() {
        let conversation_id = turn_rows[idx].conversation_id;
        let project_id = turn_rows[idx].project_id;
        let project_name = turn_rows[idx].project_name.clone();
        let conversation_title = turn_rows[idx].conversation_title.clone();

        let mut turns = Vec::new();
        let mut metrics = MetricTotals::zero();
        let mut turn_count: u64 = 0;

        while idx < turn_rows.len() && turn_rows[idx].conversation_id == conversation_id {
            let row = &turn_rows[idx];
            turns.push(HistoryDragTurn {
                uncached_input: row.uncached_input as f64,
                cached_input: row.cached_input as f64,
            });
            let row_metrics = MetricTotals::from_usage(
                row.uncached_input, // SQL already sums input_tokens + cache_creation
                0,                  // cache_creation folded into uncached_input above
                row.cached_input,
                row.output_tokens,
            );
            metrics.add_assign(&row_metrics);
            turn_count += 1;
            idx += 1;
        }

        let opportunities = history_drag::detect_summary(&turns);

        if !include_empty && opportunities.is_empty() {
            continue;
        }

        result.push(OpportunitiesReportRow {
            key: format!("conversation:{conversation_id}"),
            label: conversation_title
                .clone()
                .unwrap_or_else(|| format!("session {conversation_id}")),
            project_id,
            project_name,
            conversation_id,
            conversation_title,
            turn_count,
            metrics,
            opportunities,
        });
    }

    result.sort_by(|a, b| {
        b.opportunities
            .total_score
            .total_cmp(&a.opportunities.total_score)
    });

    result
}

struct ActionFact {
    project_id: i64,
    project_display_name: String,
    project_root: String,
    project_identity: ProjectIdentity,
    category: String,
    action: ActionKey,
    timestamp: Option<Timestamp>,
    metrics: MetricTotals,
    model_names: BTreeSet<String>,
    item_count: u64,
    skill_attribution: Option<SkillAttributionSummary>,
}

#[derive(Debug)]
struct LoadedGroupedActionRollupRow {
    project_id: Option<i64>,
    project_display_name: Option<String>,
    project_root: Option<String>,
    project_identity: Option<ProjectIdentity>,
    display_category: Option<String>,
    classification_state: Option<String>,
    normalized_action: Option<String>,
    command_family: Option<String>,
    base_command: Option<String>,
    input_tokens: i64,
    cache_creation_input_tokens: i64,
    cache_read_input_tokens: i64,
    output_tokens: i64,
    action_count: i64,
}

#[derive(Debug)]
struct LoadedPathFact {
    action_id: i64,
    project_id: i64,
    project_root: String,
    category: Option<String>,
    normalized_action: Option<String>,
    command_family: Option<String>,
    base_command: Option<String>,
    classification_state: String,
    timestamp: Option<String>,
    input_tokens: i64,
    cache_creation_input_tokens: i64,
    cache_read_input_tokens: i64,
    output_tokens: i64,
    model_name: Option<String>,
    file_path: String,
    ref_count: i64,
    skill_name: Option<String>,
    skill_confidence: Option<String>,
}

#[derive(Debug)]
struct LoadedPathRollupRow {
    child_path: String,
    child_label: String,
    child_kind: String,
    input_tokens: f64,
    cache_creation_input_tokens: f64,
    cache_read_input_tokens: f64,
    output_tokens: f64,
    item_count: i64,
}

#[derive(Debug)]
struct PathFact {
    action_id: i64,
    project_id: i64,
    project_root: String,
    category: String,
    action: ActionKey,
    timestamp: Option<Timestamp>,
    model_name: Option<String>,
    file_path: String,
    metrics: MetricTotals,
    skill_attribution: Option<SkillAttributionSummary>,
}

#[derive(Debug)]
struct CompiledFilters {
    start_at: Option<Timestamp>,
    end_at: Option<Timestamp>,
    model: Option<String>,
    project_id: Option<i64>,
    action_category: Option<String>,
    action: Option<ActionKey>,
}

impl CompiledFilters {
    fn compile(filters: &BrowseFilters) -> Result<Self> {
        let (start_at, end_at) = match &filters.time_window {
            Some(window) => (
                parse_timestamp(window.start_at_utc.as_deref())?,
                parse_timestamp(window.end_at_utc.as_deref())?,
            ),
            None => (None, None),
        };

        Ok(Self {
            start_at,
            end_at,
            model: filters.model.clone(),
            project_id: filters.project_id,
            action_category: filters.action_category.clone(),
            action: filters.action.clone(),
        })
    }

    fn matches_action(
        &self,
        fact: &ActionFact,
        project_id: Option<i64>,
        category: Option<&str>,
        action: Option<&ActionKey>,
    ) -> bool {
        if self
            .project_id
            .is_some_and(|expected| fact.project_id != expected)
        {
            return false;
        }
        if project_id.is_some_and(|expected| fact.project_id != expected) {
            return false;
        }
        if self
            .action_category
            .as_deref()
            .is_some_and(|expected| fact.category != expected)
        {
            return false;
        }
        if category.is_some_and(|expected| fact.category != expected) {
            return false;
        }
        if self
            .action
            .as_ref()
            .is_some_and(|expected| &fact.action != expected)
        {
            return false;
        }
        if action.is_some_and(|expected| &fact.action != expected) {
            return false;
        }
        if self
            .model
            .as_ref()
            .is_some_and(|expected| !fact.model_names.contains(expected))
        {
            return false;
        }
        if !matches_timestamp(fact.timestamp, self.start_at, self.end_at) {
            return false;
        }

        true
    }

    fn matches_path(
        &self,
        fact: &PathFact,
        project_id: i64,
        category: &str,
        action: &ActionKey,
    ) -> bool {
        if self
            .project_id
            .is_some_and(|expected| fact.project_id != expected)
        {
            return false;
        }
        if fact.project_id != project_id {
            return false;
        }
        if self
            .action_category
            .as_deref()
            .is_some_and(|expected| fact.category != expected)
        {
            return false;
        }
        if fact.category != category {
            return false;
        }
        if self
            .action
            .as_ref()
            .is_some_and(|expected| &fact.action != expected)
        {
            return false;
        }
        if &fact.action != action {
            return false;
        }
        if self
            .model
            .as_ref()
            .is_some_and(|expected| fact.model_name.as_deref() != Some(expected.as_str()))
        {
            return false;
        }
        if !matches_timestamp(fact.timestamp, self.start_at, self.end_at) {
            return false;
        }

        true
    }
}

#[derive(Debug)]
struct Windows {
    last_5_hours_start: Option<Timestamp>,
    last_week_start: Option<Timestamp>,
}

impl Windows {
    fn from_snapshot(snapshot: &SnapshotBounds) -> Result<Self> {
        let Some(upper_bound) = snapshot.upper_bound_timestamp()? else {
            return Ok(Self {
                last_5_hours_start: None,
                last_week_start: None,
            });
        };

        Ok(Self {
            last_5_hours_start: upper_bound.checked_sub(5.hours()).ok(),
            last_week_start: upper_bound.checked_sub(168.hours()).ok(),
        })
    }
}

#[derive(Debug)]
struct RollupBuilder {
    kind: RollupRowKind,
    key: String,
    label: String,
    project: Option<ProjectRollupContext>,
    category: Option<String>,
    action: Option<ActionKey>,
    full_path: Option<String>,
    metrics: MetricTotals,
    selected_lens_last_5_hours: f64,
    selected_lens_last_week: f64,
    uncached_input_reference: f64,
    item_count: u64,
    skill_attribution: Option<SkillAttributionSummary>,
}

#[derive(Debug)]
struct ProjectRollupContext {
    project_id: i64,
    identity: Option<ProjectIdentity>,
}

impl RollupBuilder {
    fn new(
        kind: RollupRowKind,
        key: impl Into<String>,
        label: impl Into<String>,
        project: Option<ProjectRollupContext>,
        category: Option<String>,
        action: Option<ActionKey>,
        full_path: Option<String>,
    ) -> Self {
        Self {
            kind,
            key: key.into(),
            label: label.into(),
            project,
            category,
            action,
            full_path,
            metrics: MetricTotals::zero(),
            selected_lens_last_5_hours: 0.0,
            selected_lens_last_week: 0.0,
            uncached_input_reference: 0.0,
            item_count: 0,
            skill_attribution: None,
        }
    }

    fn add_metrics(
        &mut self,
        metrics: &MetricTotals,
        lens: MetricLens,
        timestamp: Option<Timestamp>,
        windows: &Windows,
        item_count: u64,
    ) {
        self.metrics.add_assign(metrics);
        self.item_count += item_count;
        self.uncached_input_reference += metrics.uncached_input;

        if within_window(timestamp, windows.last_5_hours_start) {
            self.selected_lens_last_5_hours += metrics.lens_value(lens);
        }
        if within_window(timestamp, windows.last_week_start) {
            self.selected_lens_last_week += metrics.lens_value(lens);
        }
    }

    fn build(self) -> RollupRow {
        RollupRow {
            kind: self.kind,
            key: self.key,
            label: self.label,
            metrics: self.metrics,
            indicators: MetricIndicators {
                selected_lens_last_5_hours: self.selected_lens_last_5_hours,
                selected_lens_last_week: self.selected_lens_last_week,
                uncached_input_reference: self.uncached_input_reference,
            },
            item_count: self.item_count,
            opportunities: OpportunitySummary::default(),
            skill_attribution: self.skill_attribution,
            project_id: self.project.as_ref().map(|project| project.project_id),
            project_identity: self.project.and_then(|project| project.identity),
            category: self.category,
            action: self.action,
            full_path: self.full_path,
        }
    }
}

fn parse_skill_attribution(
    skill_name: Option<String>,
    confidence: Option<String>,
) -> Option<SkillAttributionSummary> {
    Some(SkillAttributionSummary {
        skill_name: skill_name?,
        confidence: SkillAttributionConfidence::from_db_value(confidence.as_deref()?)?,
    })
}

fn summarize_skill_attributions<'a>(
    attributions: impl IntoIterator<Item = &'a Option<SkillAttributionSummary>>,
) -> Option<SkillAttributionSummary> {
    let mut iter = attributions.into_iter();
    let first = iter.next()?.as_ref()?.clone();
    if iter.all(|candidate| candidate.as_ref() == Some(&first)) {
        Some(first)
    } else {
        None
    }
}

fn build_skill_root_rows(associations: &[&SkillSessionAssociation]) -> Vec<SkillsReportRow> {
    let mut rows = BTreeMap::<String, SkillsReportRow>::new();

    for association in associations {
        let Some(metrics) = association.metrics.as_ref() else {
            continue;
        };
        let row = rows
            .entry(association.skill_name.clone())
            .or_insert_with(|| SkillsReportRow {
                kind: SkillsRowKind::Skill,
                key: format!("skill:{}", association.skill_name),
                label: association.skill_name.clone(),
                skill_name: association.skill_name.clone(),
                project_id: None,
                project_name: None,
                session_id: None,
                conversation_id: None,
                conversation_title: None,
                invocation_count: 0,
                session_count: 0,
                confidence_counts: SkillConfidenceCounts::default(),
                transcript_evidence_count: 0,
                metrics: MetricTotals::zero(),
                attributed_action_count: 0,
                attributed_metrics: MetricTotals::zero(),
                top_attribution_confidence: None,
            });
        row.invocation_count += association.invocation_count;
        row.session_count += 1;
        row.transcript_evidence_count += association.transcript_evidence_count;
        add_skill_confidence_counts(&mut row.confidence_counts, &association.confidence_counts);
        row.metrics.add_assign(metrics);
        row.attributed_action_count += association.attributed_action_count;
        row.attributed_metrics
            .add_assign(&association.attributed_metrics);
        row.top_attribution_confidence = row
            .top_attribution_confidence
            .or(association.top_attribution_confidence);
    }

    rows.into_values().collect()
}

fn build_skill_project_rows(
    associations: &[&SkillSessionAssociation],
    skill_name: &str,
) -> Vec<SkillsReportRow> {
    let mut rows = BTreeMap::<i64, SkillsReportRow>::new();

    for association in associations
        .iter()
        .copied()
        .filter(|row| row.skill_name == skill_name)
    {
        let (Some(project_id), Some(project_name), Some(metrics)) = (
            association.project_id,
            association.project_name.as_ref(),
            association.metrics.as_ref(),
        ) else {
            continue;
        };
        let row = rows.entry(project_id).or_insert_with(|| SkillsReportRow {
            kind: SkillsRowKind::Project,
            key: format!("skill:{skill_name}:project:{project_id}"),
            label: project_name.clone(),
            skill_name: skill_name.to_string(),
            project_id: Some(project_id),
            project_name: Some(project_name.clone()),
            session_id: None,
            conversation_id: None,
            conversation_title: None,
            invocation_count: 0,
            session_count: 0,
            confidence_counts: SkillConfidenceCounts::default(),
            transcript_evidence_count: 0,
            metrics: MetricTotals::zero(),
            attributed_action_count: 0,
            attributed_metrics: MetricTotals::zero(),
            top_attribution_confidence: None,
        });
        row.invocation_count += association.invocation_count;
        row.session_count += 1;
        row.transcript_evidence_count += association.transcript_evidence_count;
        add_skill_confidence_counts(&mut row.confidence_counts, &association.confidence_counts);
        row.metrics.add_assign(metrics);
        row.attributed_action_count += association.attributed_action_count;
        row.attributed_metrics
            .add_assign(&association.attributed_metrics);
        row.top_attribution_confidence = row
            .top_attribution_confidence
            .or(association.top_attribution_confidence);
    }

    rows.into_values().collect()
}

fn build_skill_session_rows(
    associations: &[&SkillSessionAssociation],
    skill_name: &str,
    project_id: i64,
) -> Vec<SkillsReportRow> {
    associations
        .iter()
        .copied()
        .filter(|row| row.skill_name == skill_name && row.project_id == Some(project_id))
        .filter_map(|association| {
            let metrics = association.metrics.clone()?;
            Some(SkillsReportRow {
                kind: SkillsRowKind::Session,
                key: format!("skill:{skill_name}:session:{}", association.session_id),
                label: association
                    .conversation_title
                    .clone()
                    .unwrap_or_else(|| association.session_id.clone()),
                skill_name: skill_name.to_string(),
                project_id: association.project_id,
                project_name: association.project_name.clone(),
                session_id: Some(association.session_id.clone()),
                conversation_id: association.conversation_id,
                conversation_title: association.conversation_title.clone(),
                invocation_count: association.invocation_count,
                session_count: 1,
                confidence_counts: association.confidence_counts.clone(),
                transcript_evidence_count: association.transcript_evidence_count,
                metrics,
                attributed_action_count: association.attributed_action_count,
                attributed_metrics: association.attributed_metrics.clone(),
                top_attribution_confidence: association.top_attribution_confidence,
            })
        })
        .collect()
}

fn skill_invocation_row_from_explicit(
    mut row: SkillInvocationRow,
    evidence: &[SkillTranscriptEvidence],
) -> SkillInvocationRow {
    row.confidence = if evidence.is_empty() {
        SkillInvocationConfidence::Explicit
    } else {
        SkillInvocationConfidence::Confirmed
    };
    row.transcript_evidence_kinds = transcript_evidence_kinds(evidence);
    row.transcript_evidence_count = evidence.len() as u64;
    row
}

fn skill_invocation_row_from_inferred(
    skill_name: String,
    session_id: String,
    evidence: &[SkillTranscriptEvidence],
) -> SkillInvocationRow {
    let first = evidence.first().expect("inferred row requires evidence");
    SkillInvocationRow {
        skill_name,
        session_id,
        recorded_at_utc: first.recorded_at_utc.clone(),
        raw_project: None,
        conversation_id: Some(first.conversation_id),
        project_id: Some(first.project_id),
        project_name: Some(first.project_name.clone()),
        conversation_title: first.conversation_title.clone(),
        confidence: SkillInvocationConfidence::Inferred,
        transcript_evidence_kinds: transcript_evidence_kinds(evidence),
        transcript_evidence_count: evidence.len() as u64,
    }
}

fn transcript_evidence_kinds(
    evidence: &[SkillTranscriptEvidence],
) -> Vec<SkillTranscriptEvidenceKind> {
    let mut kinds = evidence.iter().map(|row| row.kind).collect::<Vec<_>>();
    kinds.sort();
    kinds.dedup();
    kinds
}

fn increment_skill_confidence_count(
    counts: &mut SkillConfidenceCounts,
    confidence: SkillInvocationConfidence,
) {
    match confidence {
        SkillInvocationConfidence::Explicit => counts.explicit += 1,
        SkillInvocationConfidence::Confirmed => counts.confirmed += 1,
        SkillInvocationConfidence::Inferred => counts.inferred += 1,
    }
}

fn add_skill_confidence_counts(target: &mut SkillConfidenceCounts, source: &SkillConfidenceCounts) {
    target.explicit += source.explicit;
    target.confirmed += source.confirmed;
    target.inferred += source.inferred;
}

fn skill_transcript_evidence_from_part(
    row: &LoadedSkillTranscriptPart,
) -> Vec<SkillTranscriptEvidence> {
    let mut evidence = Vec::new();
    let mut seen = HashSet::<(String, SkillTranscriptEvidenceKind)>::new();

    if let Some(text_value) = row.text_value.as_deref() {
        for skill_name in skill_names_from_transcript_text(text_value) {
            if seen.insert((skill_name.clone(), SkillTranscriptEvidenceKind::PromptText)) {
                evidence.push(SkillTranscriptEvidence {
                    session_id: row.session_id.clone(),
                    skill_name,
                    kind: SkillTranscriptEvidenceKind::PromptText,
                    recorded_at_utc: row.recorded_at_utc.clone(),
                    conversation_id: row.conversation_id,
                    project_id: row.project_id,
                    project_name: row.project_name.clone(),
                    conversation_title: row.conversation_title.clone(),
                });
            }
        }
    }

    if let Some(metadata_json) = row.metadata_json.as_deref() {
        for skill_name in skill_names_from_tool_metadata(metadata_json) {
            if seen.insert((
                skill_name.clone(),
                SkillTranscriptEvidenceKind::ToolInputPath,
            )) {
                evidence.push(SkillTranscriptEvidence {
                    session_id: row.session_id.clone(),
                    skill_name,
                    kind: SkillTranscriptEvidenceKind::ToolInputPath,
                    recorded_at_utc: row.recorded_at_utc.clone(),
                    conversation_id: row.conversation_id,
                    project_id: row.project_id,
                    project_name: row.project_name.clone(),
                    conversation_title: row.conversation_title.clone(),
                });
            }
        }
    }

    evidence
}

fn skill_names_from_transcript_text(text: &str) -> Vec<String> {
    let normalized = text.replace('\r', "");
    let trimmed = normalized.trim();
    if !trimmed.starts_with("---\n") {
        return Vec::new();
    }

    let mut lines = trimmed.lines();
    if lines.next() != Some("---") {
        return Vec::new();
    }

    let mut frontmatter = Vec::new();
    for line in lines {
        if line == "---" {
            break;
        }
        frontmatter.push(line);
    }

    let has_description = frontmatter
        .iter()
        .any(|line| line.starts_with("description:"));
    if !has_description {
        return Vec::new();
    }

    frontmatter
        .iter()
        .find_map(|line| line.strip_prefix("name:"))
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| vec![name.to_string()])
        .unwrap_or_default()
}

fn skill_names_from_tool_metadata(metadata_json: &str) -> Vec<String> {
    let Ok(metadata) = serde_json::from_str::<serde_json::Value>(metadata_json) else {
        return Vec::new();
    };
    let mut names = BTreeSet::new();
    collect_skill_names_from_json_value(&metadata, &mut names);
    names.into_iter().collect()
}

fn collect_skill_names_from_json_value(value: &serde_json::Value, names: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::String(text) => {
            for token in text.split_whitespace() {
                if let Some(skill_name) = skill_name_from_path_like_text(token) {
                    names.insert(skill_name);
                }
            }
            if let Some(skill_name) = skill_name_from_path_like_text(text) {
                names.insert(skill_name);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_skill_names_from_json_value(item, names);
            }
        }
        serde_json::Value::Object(map) => {
            for value in map.values() {
                collect_skill_names_from_json_value(value, names);
            }
        }
        _ => {}
    }
}

fn skill_name_from_path_like_text(text: &str) -> Option<String> {
    let trimmed = text.trim_matches(|character: char| {
        matches!(
            character,
            '"' | '\'' | ',' | ')' | '(' | '[' | ']' | '{' | '}' | ';'
        )
    });
    let path = Path::new(trimmed);
    let components = path
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    if let Some(skill_md_index) = components
        .iter()
        .position(|component| component == "SKILL.md")
        && skill_md_index >= 1
    {
        let skill_name = components[skill_md_index - 1].trim();
        if !skill_name.is_empty() {
            return Some(skill_name.to_string());
        }
    }

    let skills_index = components
        .iter()
        .position(|component| component == "skills")?;
    let tail = &components[skills_index + 1..];
    if tail.is_empty() {
        return None;
    }
    let candidate = if tail[0].starts_with('.') {
        tail.get(1)?
    } else {
        &tail[0]
    };
    let candidate = candidate.trim();
    if candidate.is_empty() {
        None
    } else {
        Some(candidate.to_string())
    }
}

fn aggregate_projects(
    facts: &[ActionFact],
    filters: &CompiledFilters,
    windows: &Windows,
    lens: MetricLens,
    project_id: Option<i64>,
    category: Option<&str>,
    action: Option<&ActionKey>,
) -> Vec<RollupRow> {
    let mut builders = BTreeMap::<i64, RollupBuilder>::new();

    for fact in facts
        .iter()
        .filter(|fact| filters.matches_action(fact, project_id, category, action))
    {
        builders
            .entry(fact.project_id)
            .or_insert_with(|| {
                RollupBuilder::new(
                    RollupRowKind::Project,
                    format!("project:{}", fact.project_id),
                    fact.project_display_name.clone(),
                    Some(ProjectRollupContext {
                        project_id: fact.project_id,
                        identity: Some(fact.project_identity.clone()),
                    }),
                    None,
                    None,
                    Some(fact.project_root.clone()),
                )
            })
            .add_metrics(
                &fact.metrics,
                lens,
                fact.timestamp,
                windows,
                fact.item_count,
            );
    }

    builders.into_values().map(RollupBuilder::build).collect()
}

fn aggregate_categories(
    facts: &[ActionFact],
    filters: &CompiledFilters,
    windows: &Windows,
    lens: MetricLens,
    project_id: Option<i64>,
    category: Option<&str>,
    action: Option<&ActionKey>,
) -> Vec<RollupRow> {
    let mut builders = BTreeMap::<String, RollupBuilder>::new();

    for fact in facts
        .iter()
        .filter(|fact| filters.matches_action(fact, project_id, category, action))
    {
        builders
            .entry(fact.category.clone())
            .or_insert_with(|| {
                RollupBuilder::new(
                    RollupRowKind::ActionCategory,
                    format!("category:{}", fact.category),
                    fact.category.clone(),
                    None,
                    Some(fact.category.clone()),
                    None,
                    None,
                )
            })
            .add_metrics(
                &fact.metrics,
                lens,
                fact.timestamp,
                windows,
                fact.item_count,
            );
    }

    builders.into_values().map(RollupBuilder::build).collect()
}

fn aggregate_actions(
    facts: &[ActionFact],
    filters: &CompiledFilters,
    windows: &Windows,
    lens: MetricLens,
    project_id: Option<i64>,
    category: Option<&str>,
    action: Option<&ActionKey>,
) -> Vec<RollupRow> {
    let mut builders =
        BTreeMap::<ActionKey, (RollupBuilder, Vec<Option<SkillAttributionSummary>>)>::new();

    for fact in facts
        .iter()
        .filter(|fact| filters.matches_action(fact, project_id, category, action))
    {
        builders
            .entry(fact.action.clone())
            .or_insert_with(|| {
                (
                    RollupBuilder::new(
                        RollupRowKind::Action,
                        format!("action:{}", fact.action.stable_key()),
                        fact.action.label(),
                        project_id.map(|project_id| ProjectRollupContext {
                            project_id,
                            identity: None,
                        }),
                        Some(fact.category.clone()),
                        Some(fact.action.clone()),
                        None,
                    ),
                    Vec::new(),
                )
            })
            .0
            .add_metrics(
                &fact.metrics,
                lens,
                fact.timestamp,
                windows,
                fact.item_count,
            );
        if let Some((_, skill_attributions)) = builders.get_mut(&fact.action) {
            skill_attributions.push(fact.skill_attribution.clone());
        }
    }

    builders
        .into_values()
        .map(|(mut builder, skill_attributions)| {
            builder.skill_attribution = summarize_skill_attributions(&skill_attributions);
            builder.build()
        })
        .collect()
}

fn aggregate_paths(
    facts: &[PathFact],
    filters: &CompiledFilters,
    windows: &Windows,
    lens: MetricLens,
    scope: PathBrowseScope<'_>,
) -> Vec<RollupRow> {
    let Some(project_root) = facts
        .iter()
        .find(|fact| fact.project_id == scope.project_id)
        .map(|fact| fact.project_root.clone())
    else {
        return Vec::new();
    };

    let base_path = scope.parent_path.unwrap_or(project_root.as_str());
    let base_path = Path::new(base_path);
    let mut builders = BTreeMap::<
        String,
        (
            RollupBuilder,
            BTreeSet<String>,
            BTreeMap<i64, Option<SkillAttributionSummary>>,
        ),
    >::new();

    for fact in facts
        .iter()
        .filter(|fact| filters.matches_path(fact, scope.project_id, scope.category, scope.action))
    {
        let file_path = Path::new(&fact.file_path);
        let Ok(relative_path) = file_path.strip_prefix(base_path) else {
            continue;
        };
        if relative_path.as_os_str().is_empty() {
            continue;
        }

        let mut components = relative_path.components();
        let Some(first_component) = components.next() else {
            continue;
        };
        let mut child_path = PathBuf::from(base_path);
        child_path.push(first_component.as_os_str());
        let is_file = components.next().is_none();
        let label = first_component.as_os_str().to_string_lossy().to_string();
        let child_path_string = child_path.to_string_lossy().to_string();
        let kind = if is_file {
            RollupRowKind::File
        } else {
            RollupRowKind::Directory
        };

        let entry = builders
            .entry(child_path_string.clone())
            .or_insert_with(|| {
                (
                    RollupBuilder::new(
                        kind,
                        format!("path:{child_path_string}"),
                        label,
                        Some(ProjectRollupContext {
                            project_id: scope.project_id,
                            identity: None,
                        }),
                        Some(scope.category.to_string()),
                        Some(scope.action.clone()),
                        Some(child_path_string.clone()),
                    ),
                    BTreeSet::new(),
                    BTreeMap::new(),
                )
            });

        entry
            .0
            .add_metrics(&fact.metrics, lens, fact.timestamp, windows, 1);
        entry.1.insert(fact.file_path.clone());
        entry
            .2
            .entry(fact.action_id)
            .or_insert_with(|| fact.skill_attribution.clone());
    }

    builders
        .into_values()
        .map(|(mut builder, leaf_paths, action_skills)| {
            builder.item_count = leaf_paths.len() as u64;
            builder.skill_attribution = summarize_skill_attributions(action_skills.values());
            builder.build()
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct PathBrowseScope<'a> {
    project_id: i64,
    category: &'a str,
    action: &'a ActionKey,
    parent_path: Option<&'a str>,
}

fn can_use_action_rollups(filters: &BrowseFilters) -> bool {
    filters.time_window.is_none() && filters.model.is_none()
}

fn can_use_path_rollups(filters: &BrowseFilters) -> bool {
    filters.time_window.is_none() && filters.model.is_none()
}

fn record_browse_request_perf_fields(perf: &mut PerfScope, request: &BrowseRequest) {
    perf.field("root", request.root);
    perf.field("path", &request.path);
    perf.field("lens", request.lens);
    perf.field("snapshot_publish_seq", request.snapshot.max_publish_seq);
    perf.field(
        "filter_project_id",
        request
            .filters
            .project_id
            .map_or_else(|| "any".to_string(), |id| id.to_string()),
    );
    perf.field(
        "filter_category",
        request
            .filters
            .action_category
            .clone()
            .unwrap_or_else(|| "any".to_string()),
    );
    perf.field(
        "filter_model",
        request
            .filters
            .model
            .clone()
            .unwrap_or_else(|| "any".to_string()),
    );
    if let Some(time_window) = &request.filters.time_window {
        perf.field("filter_start_at_utc", &time_window.start_at_utc);
        perf.field("filter_end_at_utc", &time_window.end_at_utc);
    }
}

fn is_path_browse(path: &BrowsePath) -> bool {
    matches!(
        path,
        BrowsePath::ProjectAction { .. } | BrowsePath::CategoryActionProject { .. }
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupedActionRollupShape {
    Project,
    Category,
    Action,
}

fn grouped_action_rollup_shape(request: &BrowseRequest) -> Result<GroupedActionRollupShape> {
    match (&request.root, &request.path) {
        (RootView::ProjectHierarchy, BrowsePath::Root)
        | (RootView::CategoryHierarchy, BrowsePath::CategoryAction { .. }) => {
            Ok(GroupedActionRollupShape::Project)
        }
        (RootView::ProjectHierarchy, BrowsePath::Project { .. })
        | (RootView::CategoryHierarchy, BrowsePath::Root) => Ok(GroupedActionRollupShape::Category),
        (RootView::ProjectHierarchy, BrowsePath::ProjectCategory { .. })
        | (RootView::CategoryHierarchy, BrowsePath::Category { .. }) => {
            Ok(GroupedActionRollupShape::Action)
        }
        _ => {
            bail!(
                "browse path {:?} is incompatible with {:?}",
                request.path,
                request.root
            )
        }
    }
}

fn build_grouped_action_rollup_rows_query(request: &BrowseRequest) -> Result<(String, Vec<Value>)> {
    let shape = grouped_action_rollup_shape(request)?;
    let max_publish_seq = i64::try_from(request.snapshot.max_publish_seq)
        .context("snapshot publish_seq overflowed i64")?;
    let mut sql = match shape {
        GroupedActionRollupShape::Project => String::from(
            "
        SELECT
            p.id,
            p.display_name,
            p.root_path,
            p.identity_kind,
            p.git_root_path,
            p.git_origin,
            p.identity_reason,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            COALESCE(SUM(car.input_tokens), 0),
            COALESCE(SUM(car.cache_creation_input_tokens), 0),
            COALESCE(SUM(car.cache_read_input_tokens), 0),
            COALESCE(SUM(car.output_tokens), 0),
            COALESCE(SUM(car.action_count), 0)
        FROM chunk_action_rollup car
        JOIN import_chunk ic ON ic.id = car.import_chunk_id
        JOIN project p ON p.id = ic.project_id
        WHERE ic.state = 'complete'
          AND ic.publish_seq IS NOT NULL
          AND ic.publish_seq <= ?
        ",
        ),
        GroupedActionRollupShape::Category => String::from(
            "
        SELECT
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            car.display_category,
            NULL,
            NULL,
            NULL,
            NULL,
            COALESCE(SUM(car.input_tokens), 0),
            COALESCE(SUM(car.cache_creation_input_tokens), 0),
            COALESCE(SUM(car.cache_read_input_tokens), 0),
            COALESCE(SUM(car.output_tokens), 0),
            COALESCE(SUM(car.action_count), 0)
        FROM chunk_action_rollup car
        JOIN import_chunk ic ON ic.id = car.import_chunk_id
        JOIN project p ON p.id = ic.project_id
        WHERE ic.state = 'complete'
          AND ic.publish_seq IS NOT NULL
          AND ic.publish_seq <= ?
        ",
        ),
        GroupedActionRollupShape::Action => String::from(
            "
        SELECT
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            car.display_category,
            car.classification_state,
            car.normalized_action,
            car.command_family,
            car.base_command,
            COALESCE(SUM(car.input_tokens), 0),
            COALESCE(SUM(car.cache_creation_input_tokens), 0),
            COALESCE(SUM(car.cache_read_input_tokens), 0),
            COALESCE(SUM(car.output_tokens), 0),
            COALESCE(SUM(car.action_count), 0)
        FROM chunk_action_rollup car
        JOIN import_chunk ic ON ic.id = car.import_chunk_id
        JOIN project p ON p.id = ic.project_id
        WHERE ic.state = 'complete'
          AND ic.publish_seq IS NOT NULL
          AND ic.publish_seq <= ?
        ",
        ),
    };
    let mut query_params = vec![Value::Integer(max_publish_seq)];

    match (&request.root, &request.path) {
        (RootView::ProjectHierarchy, BrowsePath::Root)
        | (RootView::CategoryHierarchy, BrowsePath::Root) => {}
        (RootView::ProjectHierarchy, BrowsePath::Project { project_id }) => {
            append_project_match(&mut sql, &mut query_params, *project_id);
        }
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectCategory {
                project_id,
                category,
            },
        ) => {
            append_project_match(&mut sql, &mut query_params, *project_id);
            append_grouped_category_match(&mut sql, &mut query_params, category);
        }
        (RootView::CategoryHierarchy, BrowsePath::Category { category }) => {
            append_grouped_category_match(&mut sql, &mut query_params, category);
        }
        (RootView::CategoryHierarchy, BrowsePath::CategoryAction { category, action }) => {
            append_grouped_category_match(&mut sql, &mut query_params, category);
            append_grouped_action_key_match(&mut sql, &mut query_params, action);
        }
        _ => {
            bail!(
                "browse path {:?} is incompatible with {:?}",
                request.path,
                request.root
            )
        }
    }

    if let Some(project_id) = request.filters.project_id {
        append_project_match(&mut sql, &mut query_params, project_id);
    }
    if let Some(category) = request.filters.action_category.as_deref() {
        append_grouped_category_match(&mut sql, &mut query_params, category);
    }
    if let Some(action) = request.filters.action.as_ref() {
        append_grouped_action_key_match(&mut sql, &mut query_params, action);
    }

    sql.push_str(match shape {
        GroupedActionRollupShape::Project => {
            "
        GROUP BY
            p.id,
            p.display_name,
            p.root_path,
            p.identity_kind,
            p.git_root_path,
            p.git_origin,
            p.identity_reason
        "
        }
        GroupedActionRollupShape::Category => {
            "
        GROUP BY
            car.display_category
        "
        }
        GroupedActionRollupShape::Action => {
            "
        GROUP BY
            car.display_category,
            car.classification_state,
            car.normalized_action,
            car.command_family,
            car.base_command
        "
        }
    });

    Ok((sql, query_params))
}

fn build_batched_grouped_action_rollup_rows_query(
    requests: &[BrowseRequest],
) -> Result<(String, Vec<Value>)> {
    let first = requests
        .first()
        .context("batched grouped browse requires at least one request")?;
    let max_publish_seq = i64::try_from(first.snapshot.max_publish_seq)
        .context("snapshot publish_seq overflowed i64")?;
    let mut sql = String::from("WITH requested_parent(request_index");
    match (&first.root, &first.path) {
        (RootView::ProjectHierarchy, BrowsePath::Project { .. }) => {
            sql.push_str(", project_id) AS (VALUES ");
        }
        (RootView::ProjectHierarchy, BrowsePath::ProjectCategory { .. }) => {
            sql.push_str(", project_id, category) AS (VALUES ");
        }
        (RootView::CategoryHierarchy, BrowsePath::Category { .. }) => {
            sql.push_str(", category) AS (VALUES ");
        }
        (RootView::CategoryHierarchy, BrowsePath::CategoryAction { .. }) => {
            sql.push_str(
                ", category, classification_state, normalized_action, command_family, \
                 base_command) AS (VALUES ",
            );
        }
        _ => bail!(
            "batched grouped browse is incompatible with {:?}",
            first.path
        ),
    }

    let mut query_params = Vec::new();
    for (index, request) in requests.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        match (&request.root, &request.path) {
            (RootView::ProjectHierarchy, BrowsePath::Project { project_id }) => {
                sql.push_str("(?, ?)");
                query_params.push(Value::Integer(
                    i64::try_from(index).context("browse batch index overflowed i64")?,
                ));
                query_params.push(Value::Integer(*project_id));
            }
            (
                RootView::ProjectHierarchy,
                BrowsePath::ProjectCategory {
                    project_id,
                    category,
                },
            ) => {
                sql.push_str("(?, ?, ?)");
                query_params.push(Value::Integer(
                    i64::try_from(index).context("browse batch index overflowed i64")?,
                ));
                query_params.push(Value::Integer(*project_id));
                query_params.push(Value::Text(category.clone()));
            }
            (RootView::CategoryHierarchy, BrowsePath::Category { category }) => {
                sql.push_str("(?, ?)");
                query_params.push(Value::Integer(
                    i64::try_from(index).context("browse batch index overflowed i64")?,
                ));
                query_params.push(Value::Text(category.clone()));
            }
            (RootView::CategoryHierarchy, BrowsePath::CategoryAction { category, action }) => {
                sql.push_str("(?, ?, ?, ?, ?, ?)");
                query_params.push(Value::Integer(
                    i64::try_from(index).context("browse batch index overflowed i64")?,
                ));
                query_params.push(Value::Text(category.clone()));
                query_params.push(Value::Text(
                    action.classification_state.as_str().to_string(),
                ));
                match &action.normalized_action {
                    Some(value) => query_params.push(Value::Text(value.clone())),
                    None => query_params.push(Value::Null),
                }
                match &action.command_family {
                    Some(value) => query_params.push(Value::Text(value.clone())),
                    None => query_params.push(Value::Null),
                }
                match &action.base_command {
                    Some(value) => query_params.push(Value::Text(value.clone())),
                    None => query_params.push(Value::Null),
                }
            }
            _ => bail!("batched grouped browse requires compatible requests"),
        }
    }
    sql.push_str(
        ")
        SELECT
            rp.request_index,
            ",
    );
    let shape = grouped_action_rollup_shape(first)?;
    sql.push_str(match shape {
        GroupedActionRollupShape::Project => {
            "
            p.id,
            p.display_name,
            p.root_path,
            p.identity_kind,
            p.git_root_path,
            p.git_origin,
            p.identity_reason,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            COALESCE(SUM(car.input_tokens), 0),
            COALESCE(SUM(car.cache_creation_input_tokens), 0),
            COALESCE(SUM(car.cache_read_input_tokens), 0),
            COALESCE(SUM(car.output_tokens), 0),
            COALESCE(SUM(car.action_count), 0)
        "
        }
        GroupedActionRollupShape::Category => {
            "
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            car.display_category,
            NULL,
            NULL,
            NULL,
            NULL,
            COALESCE(SUM(car.input_tokens), 0),
            COALESCE(SUM(car.cache_creation_input_tokens), 0),
            COALESCE(SUM(car.cache_read_input_tokens), 0),
            COALESCE(SUM(car.output_tokens), 0),
            COALESCE(SUM(car.action_count), 0)
        "
        }
        GroupedActionRollupShape::Action => {
            "
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            car.display_category,
            car.classification_state,
            car.normalized_action,
            car.command_family,
            car.base_command,
            COALESCE(SUM(car.input_tokens), 0),
            COALESCE(SUM(car.cache_creation_input_tokens), 0),
            COALESCE(SUM(car.cache_read_input_tokens), 0),
            COALESCE(SUM(car.output_tokens), 0),
            COALESCE(SUM(car.action_count), 0)
        "
        }
    });
    sql.push_str(
        "
        FROM requested_parent rp
        JOIN chunk_action_rollup car
        JOIN import_chunk ic ON ic.id = car.import_chunk_id
        JOIN project p ON p.id = ic.project_id
        WHERE ic.state = 'complete'
          AND ic.publish_seq IS NOT NULL
          AND ic.publish_seq <= ?
        ",
    );
    query_params.push(Value::Integer(max_publish_seq));

    match (&first.root, &first.path) {
        (RootView::ProjectHierarchy, BrowsePath::Project { .. }) => {
            sql.push_str(" AND p.id = rp.project_id");
        }
        (RootView::ProjectHierarchy, BrowsePath::ProjectCategory { .. }) => {
            sql.push_str(" AND p.id = rp.project_id AND car.display_category = rp.category");
        }
        (RootView::CategoryHierarchy, BrowsePath::Category { .. }) => {
            sql.push_str(" AND car.display_category = rp.category");
        }
        (RootView::CategoryHierarchy, BrowsePath::CategoryAction { .. }) => {
            sql.push_str(
                "
                AND car.display_category = rp.category
                AND car.classification_state = rp.classification_state
                AND car.normalized_action IS rp.normalized_action
                AND car.command_family IS rp.command_family
                AND car.base_command IS rp.base_command
                ",
            );
        }
        _ => {}
    }

    if let Some(project_id) = first.filters.project_id {
        append_project_match(&mut sql, &mut query_params, project_id);
    }
    if let Some(category) = first.filters.action_category.as_deref() {
        append_grouped_category_match(&mut sql, &mut query_params, category);
    }
    if let Some(action) = first.filters.action.as_ref() {
        append_grouped_action_key_match(&mut sql, &mut query_params, action);
    }

    sql.push_str(match shape {
        GroupedActionRollupShape::Project => {
            "
        GROUP BY
            rp.request_index,
            p.id,
            p.display_name,
            p.root_path,
            p.identity_kind,
            p.git_root_path,
            p.git_origin,
            p.identity_reason
        "
        }
        GroupedActionRollupShape::Category => {
            "
        GROUP BY
            rp.request_index,
            car.display_category
        "
        }
        GroupedActionRollupShape::Action => {
            "
        GROUP BY
            rp.request_index,
            car.display_category,
            car.classification_state,
            car.normalized_action,
            car.command_family,
            car.base_command
        "
        }
    });

    Ok((sql, query_params))
}

fn grouped_action_rollup_row_to_rollup_row(
    request: &BrowseRequest,
    row: LoadedGroupedActionRollupRow,
) -> Result<RollupRow> {
    let metrics = MetricTotals::from_usage(
        row.input_tokens,
        row.cache_creation_input_tokens,
        row.cache_read_input_tokens,
        row.output_tokens,
    );
    let uncached_input_reference = metrics.uncached_input;
    let item_count = u64::try_from(row.action_count)
        .context("grouped action rollup action_count overflowed u64")?;

    match (&request.root, &request.path) {
        (RootView::ProjectHierarchy, BrowsePath::Root)
        | (RootView::CategoryHierarchy, BrowsePath::CategoryAction { .. }) => {
            let project_id = row
                .project_id
                .context("grouped project rollup row is missing project_id")?;
            let project_display_name = row
                .project_display_name
                .context("grouped project rollup row is missing project_display_name")?;
            let project_root = row
                .project_root
                .context("grouped project rollup row is missing project_root")?;

            Ok(RollupRow {
                kind: RollupRowKind::Project,
                key: format!("project:{project_id}"),
                label: project_display_name,
                metrics,
                indicators: MetricIndicators {
                    selected_lens_last_5_hours: 0.0,
                    selected_lens_last_week: 0.0,
                    uncached_input_reference,
                },
                item_count,
                opportunities: OpportunitySummary::default(),
                skill_attribution: None,
                project_id: Some(project_id),
                project_identity: row.project_identity,
                category: None,
                action: None,
                full_path: Some(project_root),
            })
        }
        (RootView::ProjectHierarchy, BrowsePath::Project { .. })
        | (RootView::CategoryHierarchy, BrowsePath::Root) => {
            let display_category = row
                .display_category
                .context("grouped category rollup row is missing display_category")?;

            Ok(RollupRow {
                kind: RollupRowKind::ActionCategory,
                key: format!("category:{display_category}"),
                label: display_category.clone(),
                metrics,
                indicators: MetricIndicators {
                    selected_lens_last_5_hours: 0.0,
                    selected_lens_last_week: 0.0,
                    uncached_input_reference,
                },
                item_count,
                opportunities: OpportunitySummary::default(),
                skill_attribution: None,
                project_id: None,
                project_identity: None,
                category: Some(display_category),
                action: None,
                full_path: None,
            })
        }
        (RootView::ProjectHierarchy, BrowsePath::ProjectCategory { project_id, .. }) => {
            build_grouped_action_row(row, metrics, item_count, Some(*project_id))
        }
        (RootView::CategoryHierarchy, BrowsePath::Category { .. }) => {
            build_grouped_action_row(row, metrics, item_count, None)
        }
        _ => {
            bail!(
                "browse path {:?} is incompatible with {:?}",
                request.path,
                request.root
            )
        }
    }
}

fn path_rollup_row_to_rollup_row(
    request: &BrowseRequest,
    row: LoadedPathRollupRow,
) -> Result<RollupRow> {
    let (project_id, category, action) = match (&request.root, &request.path) {
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectAction {
                project_id,
                category,
                action,
                ..
            },
        ) => (*project_id, category.clone(), action.clone()),
        (
            RootView::CategoryHierarchy,
            BrowsePath::CategoryActionProject {
                category,
                action,
                project_id,
                ..
            },
        ) => (*project_id, category.clone(), action.clone()),
        _ => {
            bail!(
                "browse path {:?} is incompatible with {:?}",
                request.path,
                request.root
            )
        }
    };

    let kind = match row.child_kind.as_str() {
        "directory" => RollupRowKind::Directory,
        "file" => RollupRowKind::File,
        other => bail!("unexpected child kind {other} in path rollup"),
    };
    let item_count = u64::try_from(row.item_count).context("path rollup item_count overflowed")?;
    let metrics = MetricTotals {
        uncached_input: row.input_tokens + row.cache_creation_input_tokens,
        cached_input: row.cache_read_input_tokens,
        gross_input: row.input_tokens
            + row.cache_creation_input_tokens
            + row.cache_read_input_tokens,
        output: row.output_tokens,
        total: row.input_tokens
            + row.cache_creation_input_tokens
            + row.cache_read_input_tokens
            + row.output_tokens,
    };

    Ok(RollupRow {
        kind,
        key: format!("path:{}", row.child_path),
        label: row.child_label,
        metrics: metrics.clone(),
        indicators: MetricIndicators {
            selected_lens_last_5_hours: 0.0,
            selected_lens_last_week: 0.0,
            uncached_input_reference: metrics.uncached_input,
        },
        item_count,
        opportunities: OpportunitySummary::default(),
        skill_attribution: None,
        project_id: Some(project_id),
        project_identity: None,
        category: Some(category),
        action: Some(action),
        full_path: Some(row.child_path),
    })
}

fn build_grouped_action_row(
    row: LoadedGroupedActionRollupRow,
    metrics: MetricTotals,
    item_count: u64,
    project_id: Option<i64>,
) -> Result<RollupRow> {
    let uncached_input_reference = metrics.uncached_input;
    let display_category = row
        .display_category
        .context("grouped action rollup row is missing display_category")?;
    let classification_state = row
        .classification_state
        .context("grouped action rollup row is missing classification_state")?;
    let action = ActionKey {
        classification_state: parse_classification_state(&classification_state)?,
        normalized_action: row.normalized_action,
        command_family: row.command_family,
        base_command: row.base_command,
    };

    Ok(RollupRow {
        kind: RollupRowKind::Action,
        key: format!("action:{}", action.stable_key()),
        label: action.label(),
        metrics,
        indicators: MetricIndicators {
            selected_lens_last_5_hours: 0.0,
            selected_lens_last_week: 0.0,
            uncached_input_reference,
        },
        item_count,
        opportunities: OpportunitySummary::default(),
        skill_attribution: None,
        project_id,
        project_identity: None,
        category: Some(display_category),
        action: Some(action),
        full_path: None,
    })
}

fn aggregate_action_request(
    request: &BrowseRequest,
    filters: &CompiledFilters,
    windows: &Windows,
    facts: &[ActionFact],
) -> Result<Vec<RollupRow>> {
    let rows = match (&request.root, &request.path) {
        (RootView::ProjectHierarchy, BrowsePath::Root) => {
            aggregate_projects(facts, filters, windows, request.lens, None, None, None)
        }
        (RootView::ProjectHierarchy, BrowsePath::Project { project_id }) => aggregate_categories(
            facts,
            filters,
            windows,
            request.lens,
            Some(*project_id),
            None,
            None,
        ),
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectCategory {
                project_id,
                category,
            },
        ) => aggregate_actions(
            facts,
            filters,
            windows,
            request.lens,
            Some(*project_id),
            Some(category),
            None,
        ),
        (RootView::CategoryHierarchy, BrowsePath::Root) => {
            aggregate_categories(facts, filters, windows, request.lens, None, None, None)
        }
        (RootView::CategoryHierarchy, BrowsePath::Category { category }) => aggregate_actions(
            facts,
            filters,
            windows,
            request.lens,
            None,
            Some(category),
            None,
        ),
        (RootView::CategoryHierarchy, BrowsePath::CategoryAction { category, action }) => {
            aggregate_projects(
                facts,
                filters,
                windows,
                request.lens,
                None,
                Some(category),
                Some(action),
            )
        }
        _ => {
            bail!(
                "browse path {:?} is incompatible with {:?}",
                request.path,
                request.root
            )
        }
    };

    Ok(rows)
}

fn aggregate_path_request(
    request: &BrowseRequest,
    filters: &CompiledFilters,
    windows: &Windows,
    facts: &[PathFact],
) -> Result<Vec<RollupRow>> {
    let rows = match (&request.root, &request.path) {
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectAction {
                project_id,
                category,
                action,
                parent_path,
            },
        ) => aggregate_paths(
            facts,
            filters,
            windows,
            request.lens,
            PathBrowseScope {
                project_id: *project_id,
                category,
                action,
                parent_path: parent_path.as_deref(),
            },
        ),
        (
            RootView::CategoryHierarchy,
            BrowsePath::CategoryActionProject {
                category,
                action,
                project_id,
                parent_path,
            },
        ) => aggregate_paths(
            facts,
            filters,
            windows,
            request.lens,
            PathBrowseScope {
                project_id: *project_id,
                category,
                action,
                parent_path: parent_path.as_deref(),
            },
        ),
        _ => {
            bail!(
                "browse path {:?} is incompatible with {:?}",
                request.path,
                request.root
            )
        }
    };

    Ok(rows)
}

fn apply_indicator_rows(rows: &mut [RollupRow], indicator_rows: Vec<RollupRow>) {
    let indicators_by_key = indicator_rows
        .into_iter()
        .map(|row| (row.key, row.indicators))
        .collect::<BTreeMap<_, _>>();

    for row in rows {
        if let Some(indicators) = indicators_by_key.get(&row.key) {
            row.indicators.selected_lens_last_5_hours = indicators.selected_lens_last_5_hours;
            row.indicators.selected_lens_last_week = indicators.selected_lens_last_week;
        }
    }
}

fn finalize_browse_rows(request: &BrowseRequest, rows: &mut Vec<RollupRow>) {
    rows.retain(|row| !row.is_zero_value());
    rows.sort_by(|left, right| {
        right
            .metrics
            .lens_value(request.lens)
            .total_cmp(&left.metrics.lens_value(request.lens))
            .then_with(|| left.label.cmp(&right.label))
    });
}

fn browse_batch_strategy(request: &BrowseRequest) -> Option<&'static str> {
    match (&request.root, &request.path) {
        (RootView::ProjectHierarchy, BrowsePath::Project { .. })
            if can_use_action_rollups(&request.filters) =>
        {
            Some("project-categories")
        }
        (RootView::ProjectHierarchy, BrowsePath::ProjectCategory { .. })
            if can_use_action_rollups(&request.filters) =>
        {
            Some("project-category-actions")
        }
        (RootView::CategoryHierarchy, BrowsePath::Category { .. })
            if can_use_action_rollups(&request.filters) =>
        {
            Some("category-actions")
        }
        (RootView::CategoryHierarchy, BrowsePath::CategoryAction { .. })
            if can_use_action_rollups(&request.filters) =>
        {
            Some("category-action-projects")
        }
        (RootView::ProjectHierarchy, BrowsePath::ProjectAction { .. }) => Some("project-path"),
        (RootView::CategoryHierarchy, BrowsePath::CategoryActionProject { .. }) => {
            Some("category-project-path")
        }
        _ => None,
    }
}

fn build_scoped_action_facts_query(request: &BrowseRequest) -> Result<(String, Vec<Value>)> {
    let max_publish_seq = i64::try_from(request.snapshot.max_publish_seq)
        .context("snapshot publish_seq overflowed i64")?;
    let mut sql = format!(
        "
        SELECT
            p.id,
            p.display_name,
            p.root_path,
            p.identity_kind,
            p.git_root_path,
            p.git_origin,
            p.identity_reason,
            a.category,
            a.normalized_action,
            a.command_family,
            a.base_command,
            a.classification_state,
            {ACTION_TIMESTAMP_SQL},
            COALESCE(a.input_tokens, 0),
            COALESCE(a.cache_creation_input_tokens, 0),
            COALESCE(a.cache_read_input_tokens, 0),
            COALESCE(a.output_tokens, 0),
            GROUP_CONCAT(DISTINCT m.model_name),
            asa.skill_name,
            asa.confidence
        FROM action a
        JOIN import_chunk ic ON ic.id = a.import_chunk_id
        JOIN project p ON p.id = ic.project_id
        LEFT JOIN message m ON m.action_id = a.id
        LEFT JOIN action_skill_attribution asa ON asa.action_id = a.id
        WHERE ic.state = 'complete'
          AND ic.publish_seq IS NOT NULL
          AND ic.publish_seq <= ?
        "
    );
    let mut query_params = vec![Value::Integer(max_publish_seq)];

    match (&request.root, &request.path) {
        (RootView::ProjectHierarchy, BrowsePath::Root)
        | (RootView::CategoryHierarchy, BrowsePath::Root) => {}
        (RootView::ProjectHierarchy, BrowsePath::Project { project_id }) => {
            append_project_match(&mut sql, &mut query_params, *project_id);
        }
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectCategory {
                project_id,
                category,
            },
        ) => {
            append_project_match(&mut sql, &mut query_params, *project_id);
            append_category_match(&mut sql, &mut query_params, category);
        }
        (RootView::CategoryHierarchy, BrowsePath::Category { category }) => {
            append_category_match(&mut sql, &mut query_params, category);
        }
        (RootView::CategoryHierarchy, BrowsePath::CategoryAction { category, action }) => {
            append_category_match(&mut sql, &mut query_params, category);
            append_action_key_match(&mut sql, &mut query_params, action);
        }
        _ => {
            bail!(
                "browse path {:?} is incompatible with {:?}",
                request.path,
                request.root
            )
        }
    }

    if let Some(project_id) = request.filters.project_id {
        append_project_match(&mut sql, &mut query_params, project_id);
    }
    if let Some(category) = request.filters.action_category.as_deref() {
        append_category_match(&mut sql, &mut query_params, category);
    }
    if let Some(action) = request.filters.action.as_ref() {
        append_action_key_match(&mut sql, &mut query_params, action);
    }
    if let Some(model) = request.filters.model.as_deref() {
        sql.push_str(
            "
            AND EXISTS (
                SELECT 1
                FROM message m_filter
                WHERE m_filter.action_id = a.id
                  AND m_filter.model_name = ?
            )
            ",
        );
        query_params.push(Value::Text(model.to_string()));
    }
    if let Some(time_window) = request.filters.time_window.as_ref() {
        if let Some(start_at) = time_window.start_at_utc.as_deref() {
            sql.push_str(&format!(
                " AND datetime({ACTION_TIMESTAMP_SQL}) >= datetime(?)"
            ));
            query_params.push(Value::Text(start_at.to_string()));
        }
        if let Some(end_at) = time_window.end_at_utc.as_deref() {
            sql.push_str(&format!(
                " AND datetime({ACTION_TIMESTAMP_SQL}) <= datetime(?)"
            ));
            query_params.push(Value::Text(end_at.to_string()));
        }
    }

    sql.push_str(
        "
        GROUP BY
            a.id,
            p.id,
            p.display_name,
            p.root_path,
            p.identity_kind,
            p.git_root_path,
            p.git_origin,
            p.identity_reason,
            a.category,
            a.normalized_action,
            a.command_family,
            a.base_command,
            a.classification_state,
            a.ended_at_utc,
            a.started_at_utc,
            a.input_tokens,
            a.cache_creation_input_tokens,
            a.cache_read_input_tokens,
            a.output_tokens,
            asa.skill_name,
            asa.confidence
        ",
    );

    Ok((sql, query_params))
}

fn build_scoped_path_facts_query(request: &BrowseRequest) -> Result<(String, Vec<Value>)> {
    let max_publish_seq = i64::try_from(request.snapshot.max_publish_seq)
        .context("snapshot publish_seq overflowed i64")?;
    let (scope_project_id, scope_category, scope_action, parent_path) =
        match (&request.root, &request.path) {
            (
                RootView::ProjectHierarchy,
                BrowsePath::ProjectAction {
                    project_id,
                    category,
                    action,
                    parent_path,
                },
            ) => (
                *project_id,
                category.as_str(),
                action,
                parent_path.as_deref(),
            ),
            (
                RootView::CategoryHierarchy,
                BrowsePath::CategoryActionProject {
                    category,
                    action,
                    project_id,
                    parent_path,
                },
            ) => (
                *project_id,
                category.as_str(),
                action,
                parent_path.as_deref(),
            ),
            _ => {
                bail!(
                    "browse path {:?} is incompatible with {:?}",
                    request.path,
                    request.root
                )
            }
        };

    let mut sql = "
        SELECT
            a.id,
            p.id,
            p.root_path,
            a.category,
            a.normalized_action,
            a.command_family,
            a.base_command,
            a.classification_state,
            COALESCE(m.completed_at_utc, m.created_at_utc),
            COALESCE(m.input_tokens, 0),
            COALESCE(m.cache_creation_input_tokens, 0),
            COALESCE(m.cache_read_input_tokens, 0),
            COALESCE(m.output_tokens, 0),
            m.model_name,
            pn.full_path,
            (
                SELECT COUNT(*)
                FROM message_path_ref ref_count
                WHERE ref_count.message_id = m.id
            ) AS ref_count
            ,
            asa.skill_name,
            asa.confidence
        FROM action a
        JOIN import_chunk ic ON ic.id = a.import_chunk_id
        JOIN project p ON p.id = ic.project_id
        JOIN message m ON m.action_id = a.id
        JOIN message_path_ref mpr ON mpr.message_id = m.id
        JOIN path_node pn ON pn.id = mpr.path_node_id
        LEFT JOIN action_skill_attribution asa ON asa.action_id = a.id
        WHERE ic.state = 'complete'
          AND ic.publish_seq IS NOT NULL
          AND ic.publish_seq <= ?
          AND pn.node_kind = 'file'
        "
    .to_string();
    let mut query_params = vec![Value::Integer(max_publish_seq)];

    append_project_match(&mut sql, &mut query_params, scope_project_id);
    append_category_match(&mut sql, &mut query_params, scope_category);
    append_action_key_match(&mut sql, &mut query_params, scope_action);

    if let Some(project_id) = request.filters.project_id {
        append_project_match(&mut sql, &mut query_params, project_id);
    }
    if let Some(category) = request.filters.action_category.as_deref() {
        append_category_match(&mut sql, &mut query_params, category);
    }
    if let Some(action) = request.filters.action.as_ref() {
        append_action_key_match(&mut sql, &mut query_params, action);
    }
    if let Some(model) = request.filters.model.as_deref() {
        sql.push_str(" AND m.model_name = ?");
        query_params.push(Value::Text(model.to_string()));
    }
    if let Some(time_window) = request.filters.time_window.as_ref() {
        if let Some(start_at) = time_window.start_at_utc.as_deref() {
            sql.push_str(
                " AND datetime(COALESCE(m.completed_at_utc, m.created_at_utc)) >= datetime(?)",
            );
            query_params.push(Value::Text(start_at.to_string()));
        }
        if let Some(end_at) = time_window.end_at_utc.as_deref() {
            sql.push_str(
                " AND datetime(COALESCE(m.completed_at_utc, m.created_at_utc)) <= datetime(?)",
            );
            query_params.push(Value::Text(end_at.to_string()));
        }
    }
    if let Some(parent_path) = parent_path {
        sql.push_str(" AND substr(pn.full_path, 1, length(?) + 1) = ? || '/'");
        query_params.push(Value::Text(parent_path.to_string()));
        query_params.push(Value::Text(parent_path.to_string()));
    }

    Ok((sql, query_params))
}

fn build_path_rollup_rows_query(request: &BrowseRequest) -> Result<(String, Vec<Value>)> {
    let max_publish_seq = i64::try_from(request.snapshot.max_publish_seq)
        .context("snapshot publish_seq overflowed i64")?;
    let (scope_project_id, scope_category, scope_action, parent_path) =
        match (&request.root, &request.path) {
            (
                RootView::ProjectHierarchy,
                BrowsePath::ProjectAction {
                    project_id,
                    category,
                    action,
                    parent_path,
                },
            ) => (
                *project_id,
                category.as_str(),
                action,
                parent_path.as_deref(),
            ),
            (
                RootView::CategoryHierarchy,
                BrowsePath::CategoryActionProject {
                    category,
                    action,
                    project_id,
                    parent_path,
                },
            ) => (
                *project_id,
                category.as_str(),
                action,
                parent_path.as_deref(),
            ),
            _ => {
                bail!(
                    "browse path {:?} is incompatible with {:?}",
                    request.path,
                    request.root
                )
            }
        };

    let mut sql = "
        SELECT
            cpr.child_path,
            cpr.child_label,
            cpr.child_kind,
            COALESCE(SUM(cpr.input_tokens), 0.0),
            COALESCE(SUM(cpr.cache_creation_input_tokens), 0.0),
            COALESCE(SUM(cpr.cache_read_input_tokens), 0.0),
            COALESCE(SUM(cpr.output_tokens), 0.0),
            COUNT(DISTINCT cpr.leaf_file_path)
        FROM chunk_path_rollup cpr
        JOIN import_chunk ic ON ic.id = cpr.import_chunk_id
        JOIN project p ON p.id = ic.project_id
        WHERE ic.state = 'complete'
          AND ic.publish_seq IS NOT NULL
          AND ic.publish_seq <= ?
          AND p.id = ?
          AND cpr.display_category = ?
        "
    .to_string();
    let mut query_params = vec![
        Value::Integer(max_publish_seq),
        Value::Integer(scope_project_id),
        Value::Text(scope_category.to_string()),
    ];
    append_path_rollup_action_match(&mut sql, &mut query_params, scope_action);
    append_path_rollup_parent_match(&mut sql, &mut query_params, parent_path);

    if let Some(project_id) = request.filters.project_id {
        append_project_match(&mut sql, &mut query_params, project_id);
    }
    if let Some(category) = request.filters.action_category.as_deref() {
        sql.push_str(" AND cpr.display_category = ?");
        query_params.push(Value::Text(category.to_string()));
    }
    if let Some(action) = request.filters.action.as_ref() {
        append_path_rollup_action_match(&mut sql, &mut query_params, action);
    }

    sql.push_str(
        "
        GROUP BY cpr.child_path, cpr.child_label, cpr.child_kind
        ",
    );

    Ok((sql, query_params))
}

fn build_batched_path_facts_query(requests: &[BrowseRequest]) -> Result<(String, Vec<Value>)> {
    let first = requests
        .first()
        .context("batched path browse requires at least one request")?;
    let max_publish_seq = i64::try_from(first.snapshot.max_publish_seq)
        .context("snapshot publish_seq overflowed i64")?;
    let mut sql = String::from(
        "
        WITH requested_parent(
            request_index,
            project_id,
            category,
            classification_state,
            normalized_action,
            command_family,
            base_command,
            parent_path
        ) AS (VALUES
        ",
    );
    let mut query_params = Vec::new();

    for (index, request) in requests.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        sql.push_str("(?, ?, ?, ?, ?, ?, ?, ?)");
        let (project_id, category, action, parent_path) = match (&request.root, &request.path) {
            (
                RootView::ProjectHierarchy,
                BrowsePath::ProjectAction {
                    project_id,
                    category,
                    action,
                    parent_path,
                },
            ) => (*project_id, category, action, parent_path.as_ref()),
            (
                RootView::CategoryHierarchy,
                BrowsePath::CategoryActionProject {
                    category,
                    action,
                    project_id,
                    parent_path,
                },
            ) => (*project_id, category, action, parent_path.as_ref()),
            _ => bail!("batched path browse requires compatible requests"),
        };
        query_params.push(Value::Integer(
            i64::try_from(index).context("browse batch index overflowed i64")?,
        ));
        query_params.push(Value::Integer(project_id));
        query_params.push(Value::Text(category.clone()));
        query_params.push(Value::Text(
            action.classification_state.as_str().to_string(),
        ));
        match &action.normalized_action {
            Some(value) => query_params.push(Value::Text(value.clone())),
            None => query_params.push(Value::Null),
        }
        match &action.command_family {
            Some(value) => query_params.push(Value::Text(value.clone())),
            None => query_params.push(Value::Null),
        }
        match &action.base_command {
            Some(value) => query_params.push(Value::Text(value.clone())),
            None => query_params.push(Value::Null),
        }
        match parent_path {
            Some(value) => query_params.push(Value::Text(value.clone())),
            None => query_params.push(Value::Null),
        }
    }

    sql.push_str(
        ")
        SELECT
            rp.request_index,
            a.id,
            p.id,
            p.root_path,
            a.category,
            a.normalized_action,
            a.command_family,
            a.base_command,
            a.classification_state,
            COALESCE(m.completed_at_utc, m.created_at_utc),
            COALESCE(m.input_tokens, 0),
            COALESCE(m.cache_creation_input_tokens, 0),
            COALESCE(m.cache_read_input_tokens, 0),
            COALESCE(m.output_tokens, 0),
            m.model_name,
            pn.full_path,
            (
                SELECT COUNT(*)
                FROM message_path_ref ref_count
                WHERE ref_count.message_id = m.id
            ) AS ref_count
            ,
            asa.skill_name,
            asa.confidence
        FROM requested_parent rp
        JOIN action a
        JOIN import_chunk ic ON ic.id = a.import_chunk_id
        JOIN project p ON p.id = ic.project_id
        JOIN message m ON m.action_id = a.id
        JOIN message_path_ref mpr ON mpr.message_id = m.id
        JOIN path_node pn ON pn.id = mpr.path_node_id
        LEFT JOIN action_skill_attribution asa ON asa.action_id = a.id
        WHERE ic.state = 'complete'
          AND ic.publish_seq IS NOT NULL
          AND ic.publish_seq <= ?
          AND pn.node_kind = 'file'
          AND p.id = rp.project_id
          AND ",
    );
    query_params.push(Value::Integer(max_publish_seq));
    sql.push_str(DISPLAY_CATEGORY_SQL);
    sql.push_str(
        " = rp.category
          AND a.classification_state = rp.classification_state
          AND a.normalized_action IS rp.normalized_action
          AND a.command_family IS rp.command_family
          AND a.base_command IS rp.base_command
          AND (
              rp.parent_path IS NULL
              OR substr(pn.full_path, 1, length(rp.parent_path) + 1) = rp.parent_path || '/'
          )
        ",
    );

    if let Some(project_id) = first.filters.project_id {
        append_project_match(&mut sql, &mut query_params, project_id);
    }
    if let Some(category) = first.filters.action_category.as_deref() {
        append_category_match(&mut sql, &mut query_params, category);
    }
    if let Some(action) = first.filters.action.as_ref() {
        append_action_key_match(&mut sql, &mut query_params, action);
    }
    if let Some(model) = first.filters.model.as_deref() {
        sql.push_str(" AND m.model_name = ?");
        query_params.push(Value::Text(model.to_string()));
    }
    if let Some(time_window) = first.filters.time_window.as_ref() {
        if let Some(start_at) = time_window.start_at_utc.as_deref() {
            sql.push_str(
                " AND datetime(COALESCE(m.completed_at_utc, m.created_at_utc)) >= datetime(?)",
            );
            query_params.push(Value::Text(start_at.to_string()));
        }
        if let Some(end_at) = time_window.end_at_utc.as_deref() {
            sql.push_str(
                " AND datetime(COALESCE(m.completed_at_utc, m.created_at_utc)) <= datetime(?)",
            );
            query_params.push(Value::Text(end_at.to_string()));
        }
    }

    Ok((sql, query_params))
}

fn build_batched_path_rollup_rows_query(
    requests: &[BrowseRequest],
) -> Result<(String, Vec<Value>)> {
    let first = requests
        .first()
        .context("batched path browse requires at least one request")?;
    let max_publish_seq = i64::try_from(first.snapshot.max_publish_seq)
        .context("snapshot publish_seq overflowed i64")?;
    let mut sql = String::from(
        "
        WITH requested_parent(
            request_index,
            project_id,
            category,
            classification_state,
            normalized_action,
            command_family,
            base_command,
            parent_path
        ) AS (VALUES
        ",
    );
    let mut query_params = Vec::new();

    for (index, request) in requests.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        sql.push_str("(?, ?, ?, ?, ?, ?, ?, ?)");
        let (project_id, category, action, parent_path) = match (&request.root, &request.path) {
            (
                RootView::ProjectHierarchy,
                BrowsePath::ProjectAction {
                    project_id,
                    category,
                    action,
                    parent_path,
                },
            ) => (*project_id, category, action, parent_path.as_ref()),
            (
                RootView::CategoryHierarchy,
                BrowsePath::CategoryActionProject {
                    category,
                    action,
                    project_id,
                    parent_path,
                },
            ) => (*project_id, category, action, parent_path.as_ref()),
            _ => bail!("batched path browse requires compatible requests"),
        };
        query_params.push(Value::Integer(
            i64::try_from(index).context("browse batch index overflowed i64")?,
        ));
        query_params.push(Value::Integer(project_id));
        query_params.push(Value::Text(category.clone()));
        query_params.push(Value::Text(
            action.classification_state.as_str().to_string(),
        ));
        match &action.normalized_action {
            Some(value) => query_params.push(Value::Text(value.clone())),
            None => query_params.push(Value::Null),
        }
        match &action.command_family {
            Some(value) => query_params.push(Value::Text(value.clone())),
            None => query_params.push(Value::Null),
        }
        match &action.base_command {
            Some(value) => query_params.push(Value::Text(value.clone())),
            None => query_params.push(Value::Null),
        }
        match parent_path {
            Some(value) => query_params.push(Value::Text(value.clone())),
            None => query_params.push(Value::Null),
        }
    }

    sql.push_str(
        ")
        SELECT
            rp.request_index,
            cpr.child_path,
            cpr.child_label,
            cpr.child_kind,
            COALESCE(SUM(cpr.input_tokens), 0.0),
            COALESCE(SUM(cpr.cache_creation_input_tokens), 0.0),
            COALESCE(SUM(cpr.cache_read_input_tokens), 0.0),
            COALESCE(SUM(cpr.output_tokens), 0.0),
            COUNT(DISTINCT cpr.leaf_file_path)
        FROM requested_parent rp
        JOIN chunk_path_rollup cpr
        JOIN import_chunk ic ON ic.id = cpr.import_chunk_id
        JOIN project p ON p.id = ic.project_id
        WHERE ic.state = 'complete'
          AND ic.publish_seq IS NOT NULL
          AND ic.publish_seq <= ?
          AND p.id = rp.project_id
          AND cpr.display_category = rp.category
          AND cpr.classification_state = rp.classification_state
          AND cpr.normalized_action IS rp.normalized_action
          AND cpr.command_family IS rp.command_family
          AND cpr.base_command IS rp.base_command
          AND (
              (rp.parent_path IS NULL AND cpr.parent_path IS NULL)
              OR cpr.parent_path = rp.parent_path
          )
        ",
    );
    query_params.push(Value::Integer(max_publish_seq));

    if let Some(project_id) = first.filters.project_id {
        append_project_match(&mut sql, &mut query_params, project_id);
    }
    if let Some(category) = first.filters.action_category.as_deref() {
        sql.push_str(" AND cpr.display_category = ?");
        query_params.push(Value::Text(category.to_string()));
    }
    if let Some(action) = first.filters.action.as_ref() {
        append_path_rollup_action_match(&mut sql, &mut query_params, action);
    }

    sql.push_str(
        "
        GROUP BY rp.request_index, cpr.child_path, cpr.child_label, cpr.child_kind
        ",
    );

    Ok((sql, query_params))
}

fn append_project_match(sql: &mut String, query_params: &mut Vec<Value>, project_id: i64) {
    sql.push_str(" AND p.id = ?");
    query_params.push(Value::Integer(project_id));
}

fn append_category_match(sql: &mut String, query_params: &mut Vec<Value>, category: &str) {
    sql.push_str(&format!(" AND {DISPLAY_CATEGORY_SQL} = ?"));
    query_params.push(Value::Text(category.to_string()));
}

fn append_action_key_match(sql: &mut String, query_params: &mut Vec<Value>, action: &ActionKey) {
    sql.push_str(" AND a.classification_state = ?");
    query_params.push(Value::Text(
        action.classification_state.as_str().to_string(),
    ));

    if let Some(normalized_action) = action.normalized_action.as_deref() {
        sql.push_str(" AND a.normalized_action = ?");
        query_params.push(Value::Text(normalized_action.to_string()));
    } else {
        sql.push_str(" AND a.normalized_action IS NULL");
    }

    if let Some(command_family) = action.command_family.as_deref() {
        sql.push_str(" AND a.command_family = ?");
        query_params.push(Value::Text(command_family.to_string()));
    } else {
        sql.push_str(" AND a.command_family IS NULL");
    }

    if let Some(base_command) = action.base_command.as_deref() {
        sql.push_str(" AND a.base_command = ?");
        query_params.push(Value::Text(base_command.to_string()));
    } else {
        sql.push_str(" AND a.base_command IS NULL");
    }
}

fn append_path_rollup_action_match(
    sql: &mut String,
    query_params: &mut Vec<Value>,
    action: &ActionKey,
) {
    sql.push_str(" AND cpr.classification_state = ?");
    query_params.push(Value::Text(
        action.classification_state.as_str().to_string(),
    ));

    if let Some(normalized_action) = action.normalized_action.as_deref() {
        sql.push_str(" AND cpr.normalized_action = ?");
        query_params.push(Value::Text(normalized_action.to_string()));
    } else {
        sql.push_str(" AND cpr.normalized_action IS NULL");
    }

    if let Some(command_family) = action.command_family.as_deref() {
        sql.push_str(" AND cpr.command_family = ?");
        query_params.push(Value::Text(command_family.to_string()));
    } else {
        sql.push_str(" AND cpr.command_family IS NULL");
    }

    if let Some(base_command) = action.base_command.as_deref() {
        sql.push_str(" AND cpr.base_command = ?");
        query_params.push(Value::Text(base_command.to_string()));
    } else {
        sql.push_str(" AND cpr.base_command IS NULL");
    }
}

fn append_path_rollup_parent_match(
    sql: &mut String,
    query_params: &mut Vec<Value>,
    parent_path: Option<&str>,
) {
    match parent_path {
        Some(parent_path) => {
            sql.push_str(" AND cpr.parent_path = ?");
            query_params.push(Value::Text(parent_path.to_string()));
        }
        None => sql.push_str(" AND cpr.parent_path IS NULL"),
    }
}

fn append_grouped_category_match(sql: &mut String, query_params: &mut Vec<Value>, category: &str) {
    sql.push_str(" AND car.display_category = ?");
    query_params.push(Value::Text(category.to_string()));
}

fn append_grouped_action_key_match(
    sql: &mut String,
    query_params: &mut Vec<Value>,
    action: &ActionKey,
) {
    sql.push_str(" AND car.classification_state = ?");
    query_params.push(Value::Text(
        action.classification_state.as_str().to_string(),
    ));

    if let Some(normalized_action) = action.normalized_action.as_deref() {
        sql.push_str(" AND car.normalized_action = ?");
        query_params.push(Value::Text(normalized_action.to_string()));
    } else {
        sql.push_str(" AND car.normalized_action IS NULL");
    }

    if let Some(command_family) = action.command_family.as_deref() {
        sql.push_str(" AND car.command_family = ?");
        query_params.push(Value::Text(command_family.to_string()));
    } else {
        sql.push_str(" AND car.command_family IS NULL");
    }

    if let Some(base_command) = action.base_command.as_deref() {
        sql.push_str(" AND car.base_command = ?");
        query_params.push(Value::Text(base_command.to_string()));
    } else {
        sql.push_str(" AND car.base_command IS NULL");
    }
}

fn matches_timestamp(
    timestamp: Option<Timestamp>,
    start_at: Option<Timestamp>,
    end_at: Option<Timestamp>,
) -> bool {
    let Some(timestamp) = timestamp else {
        return start_at.is_none() && end_at.is_none();
    };
    if start_at.is_some_and(|start| timestamp < start) {
        return false;
    }
    if end_at.is_some_and(|end| timestamp > end) {
        return false;
    }

    true
}

fn within_window(timestamp: Option<Timestamp>, start_at: Option<Timestamp>) -> bool {
    match (timestamp, start_at) {
        (Some(timestamp), Some(start_at)) => timestamp >= start_at,
        _ => false,
    }
}

fn parse_timestamp(raw: Option<&str>) -> Result<Option<Timestamp>> {
    raw.map(parse_timestamp_value).transpose()
}

fn parse_timestamp_value(raw: &str) -> Result<Timestamp> {
    if let Ok(parsed) = raw.parse::<Timestamp>() {
        return Ok(parsed);
    }

    let sqlite_utc = format!("{}Z", raw.replace(' ', "T"));
    sqlite_utc
        .parse::<Timestamp>()
        .with_context(|| format!("unable to parse timestamp {raw}"))
}

fn parse_classification_state(raw: &str) -> Result<ClassificationState> {
    match raw {
        "classified" => Ok(ClassificationState::Classified),
        "mixed" => Ok(ClassificationState::Mixed),
        "unclassified" => Ok(ClassificationState::Unclassified),
        _ => bail!("unexpected classification_state {raw}"),
    }
}

fn display_category(raw_category: Option<&str>, classification_state: &str) -> Result<String> {
    if let Some(category) = raw_category {
        return Ok(category.to_string());
    }

    match parse_classification_state(classification_state)? {
        ClassificationState::Mixed => Ok("[mixed]".to_string()),
        ClassificationState::Unclassified => Ok("[unclassified]".to_string()),
        ClassificationState::Classified => Ok("classified".to_string()),
    }
}

fn split_model_names(raw: Option<&str>) -> BTreeSet<String> {
    raw.into_iter()
        .flat_map(|raw| raw.split(','))
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use anyhow::{Context, Result};
    use rusqlite::types::Value;
    use rusqlite::{Connection, OptionalExtension, params};
    use serde_json::Value as JsonValue;
    use tempfile::tempdir;

    use crate::db::Database;
    use crate::opportunity::{
        OpportunityAnnotation, OpportunityCategory, OpportunityConfidence, OpportunitySummary,
    };
    use crate::perf::PerfLogger;

    use super::{
        ActionKey, BatchBrowseRequest, BatchBrowseResult, BrowseFilters, BrowsePath, BrowseRequest,
        ClassificationState, ConversationTurnRow, FilterOptions, HistoryEventFilters, MetricLens,
        QueryEngine, RootView, SkillAttributionConfidence, SkillAttributionSummary,
        SkillInvocationConfidence, SkillInvocationFilters, SkillTranscriptEvidenceKind, SkillsPath,
        SnapshotBounds, SnapshotCoverageSummary, TimeWindowFilter,
        build_grouped_action_rollup_rows_query, build_opportunities_rows,
        build_scoped_action_facts_query, build_scoped_path_facts_query, display_category,
    };

    #[test]
    fn latest_snapshot_bounds_detects_newer_published_chunks() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let engine = QueryEngine::new(conn);

        let latest = engine.latest_snapshot_bounds()?;
        assert_eq!(latest.max_publish_seq, 2);
        assert_eq!(latest.published_chunk_count, 2);
        assert_eq!(
            latest.upper_bound_utc.as_deref(),
            Some("2026-03-26T14:00:00Z")
        );

        let older = SnapshotBounds {
            max_publish_seq: 1,
            published_chunk_count: 1,
            upper_bound_utc: Some("2026-03-26T12:00:00Z".to_string()),
        };
        assert!(engine.has_newer_snapshot(&older)?);
        assert!(!engine.has_newer_snapshot(&latest)?);

        conn.execute(
            "
            INSERT INTO import_chunk (
                project_id,
                chunk_day_local,
                state,
                publish_seq,
                started_at_utc,
                completed_at_utc
            )
            VALUES (?1, '2026-03-28', 'complete', 3, '2026-03-28T08:00:00Z', '2026-03-28T09:00:00Z')
            ",
            [fixture.project_a_id],
        )?;

        assert!(engine.has_newer_snapshot(&latest)?);

        Ok(())
    }

    #[test]
    fn opportunity_summary_defaults_to_no_annotations() {
        assert_eq!(
            OpportunitySummary::default(),
            OpportunitySummary::from_annotations(vec![])
        );
    }

    #[test]
    fn opportunity_summary_derives_top_category_and_total() {
        let summary = OpportunitySummary::from_annotations(vec![
            OpportunityAnnotation {
                category: OpportunityCategory::SearchChurn,
                score: 0.45,
                confidence: OpportunityConfidence::Medium,
                evidence: vec!["looping over the same search path".to_string()],
                recommendation: Some("narrow the search target earlier".to_string()),
            },
            OpportunityAnnotation {
                category: OpportunityCategory::HistoryDrag,
                score: 0.7,
                confidence: OpportunityConfidence::High,
                evidence: vec!["later turns carry more prior context".to_string()],
                recommendation: Some("reset after the task boundary".to_string()),
            },
        ]);

        assert_eq!(summary.top_category, Some(OpportunityCategory::HistoryDrag));
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::High));
        assert!((summary.total_score - 1.15).abs() < 1e-9);
        assert_eq!(summary.annotations.len(), 2);
    }

    #[test]
    fn snapshot_coverage_summary_uses_user_facing_units() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        seed_query_fixture(conn, temp.path())?;
        let engine = QueryEngine::new(conn);

        let coverage = engine.snapshot_coverage_summary(&engine.latest_snapshot_bounds()?)?;
        assert_eq!(
            coverage,
            SnapshotCoverageSummary {
                project_count: 2,
                project_day_count: 2,
                day_count: 1,
                session_count: 4,
                turn_count: 4,
            }
        );

        Ok(())
    }

    #[test]
    fn bootstrap_snapshot_has_empty_coverage_summary() -> Result<()> {
        let temp = tempdir()?;
        let db = Database::open(temp.path().join("usage.sqlite3"))?;
        let engine = QueryEngine::new(db.connection());

        assert_eq!(
            engine.snapshot_coverage_summary(&SnapshotBounds::bootstrap())?,
            SnapshotCoverageSummary::default()
        );

        Ok(())
    }

    #[test]
    fn history_events_are_queryable_by_session_id_and_timestamp() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;

        let source_file_id: i64 = conn.query_row(
            "
            INSERT INTO source_file (
                project_id,
                relative_path,
                source_kind,
                modified_at_utc,
                size_bytes,
                scan_warnings_json
            )
            VALUES (?1, '../history.jsonl', 'claude_history', '2026-03-26T09:00:00Z', 1, '[]')
            RETURNING id
            ",
            [fixture.project_a_id],
            |row| row.get(0),
        )?;
        let import_chunk_id: i64 = conn.query_row(
            "
            INSERT INTO import_chunk (
                project_id,
                chunk_day_local,
                state,
                publish_seq,
                started_at_utc,
                completed_at_utc
            )
            VALUES (?1, '2026-03-28', 'complete', 3, '2026-03-28T09:00:00Z', '2026-03-28T09:00:01Z')
            RETURNING id
            ",
            [fixture.project_a_id],
            |row| row.get(0),
        )?;
        conn.execute(
            "
            INSERT INTO history_event (
                import_chunk_id,
                source_file_id,
                source_line_no,
                session_id,
                recorded_at_utc,
                raw_project,
                display_text,
                pasted_contents_json,
                input_kind,
                slash_command_name,
                raw_json
            )
            VALUES
                (?1, ?2, 1, 'session-history-1', '2026-03-26T08:00:00Z', '/tmp/project-a', 'Investigate parser', '[{\"type\":\"text\",\"text\":\"trace\"}]', 'plain_prompt', NULL, '{}'),
                (?1, ?2, 2, 'session-history-1', '2026-03-26T08:01:00Z', '/tmp/project-a', '/skill planner --fast', '[]', 'slash_command', 'skill', '{}'),
                (?1, ?2, 3, 'session-history-2', '2026-03-26T08:02:00Z', '/tmp/project-b', NULL, NULL, 'other', NULL, '{}')
            ",
            params![import_chunk_id, source_file_id],
        )?;

        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let session_rows = engine.history_events(
            &snapshot,
            &HistoryEventFilters {
                session_id: Some("session-history-1".to_string()),
                ..HistoryEventFilters::default()
            },
        )?;
        assert_eq!(session_rows.len(), 2);
        assert_eq!(session_rows[0].input_kind, "plain_prompt");
        assert_eq!(session_rows[1].input_kind, "slash_command");
        assert_eq!(session_rows[1].slash_command_name.as_deref(), Some("skill"));

        let window_rows = engine.history_events(
            &snapshot,
            &HistoryEventFilters {
                start_at_utc: Some("2026-03-26T08:01:00Z".to_string()),
                end_at_utc: Some("2026-03-26T08:02:00Z".to_string()),
                ..HistoryEventFilters::default()
            },
        )?;
        assert_eq!(window_rows.len(), 2);
        assert_eq!(
            window_rows
                .iter()
                .map(|row| row.source_line_no)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );

        Ok(())
    }

    #[test]
    fn skill_invocations_join_to_sessions_and_preserve_unmatched_rows() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let (matched_conversation_id, matched_transcript_source_file_id): (i64, i64) = conn
            .query_row(
                "
                SELECT c.id, c.source_file_id
                FROM conversation c
                JOIN project p ON p.id = c.project_id
                WHERE p.id = ?1
                ORDER BY c.id
                LIMIT 1
                ",
                [fixture.project_a_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
        conn.execute(
            "
            UPDATE conversation
            SET external_id = ?2
            WHERE id = ?1
            ",
            params![
                matched_conversation_id,
                format!(
                    "source-file:{}:session:matched-session",
                    matched_transcript_source_file_id
                )
            ],
        )?;

        let source_file_id: i64 = conn.query_row(
            "
            INSERT INTO source_file (
                project_id,
                relative_path,
                source_kind,
                modified_at_utc,
                size_bytes,
                scan_warnings_json
            )
            VALUES (?1, '../history.jsonl', 'claude_history', '2026-03-28T09:00:00Z', 1, '[]')
            RETURNING id
            ",
            [fixture.project_a_id],
            |row| row.get(0),
        )?;
        let import_chunk_id: i64 = conn.query_row(
            "
            INSERT INTO import_chunk (
                project_id,
                chunk_day_local,
                state,
                publish_seq,
                started_at_utc,
                completed_at_utc
            )
            VALUES (?1, '2026-03-28', 'complete', 3, '2026-03-28T09:00:00Z', '2026-03-28T09:00:01Z')
            RETURNING id
            ",
            [fixture.project_a_id],
            |row| row.get(0),
        )?;

        let matched_history_event_id: i64 = conn.query_row(
            "
            INSERT INTO history_event (
                import_chunk_id,
                source_file_id,
                source_line_no,
                session_id,
                recorded_at_utc,
                raw_project,
                display_text,
                pasted_contents_json,
                input_kind,
                slash_command_name,
                raw_json
            )
            VALUES (?1, ?2, 1, 'matched-session', '2026-03-28T08:00:00Z', '/tmp/project-a', '/skill planner', '[]', 'slash_command', 'skill', '{}')
            RETURNING id
            ",
            params![import_chunk_id, source_file_id],
            |row| row.get(0),
        )?;
        let unmatched_history_event_id: i64 = conn.query_row(
            "
            INSERT INTO history_event (
                import_chunk_id,
                source_file_id,
                source_line_no,
                session_id,
                recorded_at_utc,
                raw_project,
                display_text,
                pasted_contents_json,
                input_kind,
                slash_command_name,
                raw_json
            )
            VALUES (?1, ?2, 2, 'missing-session', '2026-03-28T08:05:00Z', '/tmp/project-z', '/skill reviewer', '[]', 'slash_command', 'skill', '{}')
            RETURNING id
            ",
            params![import_chunk_id, source_file_id],
            |row| row.get(0),
        )?;

        conn.execute(
            "
            INSERT INTO skill_invocation (
                import_chunk_id,
                history_event_id,
                source_file_id,
                session_id,
                recorded_at_utc,
                raw_project,
                skill_name,
                invocation_kind
            )
            VALUES
                (?1, ?2, ?4, 'matched-session', '2026-03-28T08:00:00Z', '/tmp/project-a', 'planner', 'explicit_history'),
                (?1, ?3, ?4, 'missing-session', '2026-03-28T08:05:00Z', '/tmp/project-z', 'reviewer', 'explicit_history')
            ",
            params![
                import_chunk_id,
                matched_history_event_id,
                unmatched_history_event_id,
                source_file_id
            ],
        )?;

        let matched_message_id: i64 = conn.query_row(
            "
            SELECT m.id
            FROM message m
            WHERE m.conversation_id = ?1
            ORDER BY m.sequence_no
            LIMIT 1
            ",
            [matched_conversation_id],
            |row| row.get(0),
        )?;
        conn.execute(
            "
            INSERT INTO message_part (
                message_id,
                ordinal,
                part_kind,
                text_value,
                is_error
            )
            VALUES (?1, 99, 'text', ?2, 0)
            ",
            params![
                matched_message_id,
                "---\nname: planner\ndescription: confirm planner\n---\nUse the planner skill.\n"
            ],
        )?;

        let (inferred_conversation_id, inferred_source_file_id, inferred_message_id): (
            i64,
            i64,
            i64,
        ) = conn.query_row(
            "
            SELECT c.id, c.source_file_id, m.id
            FROM conversation c
            JOIN message m ON m.conversation_id = c.id
            WHERE c.id != ?1
            ORDER BY m.sequence_no
            LIMIT 1
            ",
            [matched_conversation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        conn.execute(
            "
            UPDATE conversation
            SET external_id = ?2
            WHERE id = ?1
            ",
            params![
                inferred_conversation_id,
                format!("source-file:{inferred_source_file_id}:session:reviewer-confirmed")
            ],
        )?;
        conn.execute(
            "
            INSERT INTO message_part (
                message_id,
                ordinal,
                part_kind,
                tool_name,
                metadata_json,
                is_error
            )
            VALUES (?1, 98, 'tool_use', 'Bash', ?2, 0)
            ",
            params![
                inferred_message_id,
                "{\"input\":{\"command\":\"python3 /tmp/.codex/plugins/cache/pkg/local/skills/reviewer/scripts/run.py\"}}"
            ],
        )?;

        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let rows = engine.skill_invocations(&snapshot, &SkillInvocationFilters::default())?;
        assert_eq!(rows.len(), 3);
        let planner_row = rows
            .iter()
            .find(|row| row.skill_name == "planner")
            .context("missing planner row")?;
        assert!(planner_row.conversation_id.is_some());
        assert_eq!(planner_row.conversation_id, Some(matched_conversation_id));
        assert_eq!(planner_row.project_name.as_deref(), Some("project-a"));
        assert_eq!(planner_row.confidence, SkillInvocationConfidence::Confirmed);
        assert_eq!(
            planner_row.transcript_evidence_kinds,
            vec![SkillTranscriptEvidenceKind::PromptText]
        );
        assert_eq!(planner_row.transcript_evidence_count, 1);

        let inferred_row = rows
            .iter()
            .find(|row| row.session_id == "reviewer-confirmed")
            .context("missing inferred reviewer row")?;
        assert_eq!(inferred_row.skill_name, "reviewer");
        assert!(inferred_row.conversation_id.is_some());
        assert_eq!(inferred_row.confidence, SkillInvocationConfidence::Inferred);
        assert_eq!(
            inferred_row.transcript_evidence_kinds,
            vec![SkillTranscriptEvidenceKind::ToolInputPath]
        );
        let unmatched_row = rows
            .iter()
            .find(|row| row.session_id == "missing-session")
            .context("missing unmatched reviewer row")?;
        assert_eq!(unmatched_row.skill_name, "reviewer");
        assert_eq!(unmatched_row.conversation_id, None);
        assert_eq!(
            unmatched_row.confidence,
            SkillInvocationConfidence::Explicit
        );

        let filtered = engine.skill_invocations(
            &snapshot,
            &SkillInvocationFilters {
                skill_name: Some("planner".to_string()),
                ..SkillInvocationFilters::default()
            },
        )?;
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].session_id, "matched-session");

        Ok(())
    }

    #[test]
    fn skills_report_aggregates_session_associated_metrics_by_skill_project_and_session()
    -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;

        let project_a_sessions = {
            let mut stmt = conn.prepare(
                "
                SELECT c.id, c.source_file_id
                FROM conversation c
                WHERE c.project_id = ?1
                ORDER BY c.started_at_utc
                ",
            )?;
            stmt.query_map([fixture.project_a_id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        let project_b_rows = {
            let mut stmt = conn.prepare(
                "
                SELECT c.id, c.source_file_id, c.project_id
                FROM conversation c
                WHERE c.project_id != ?1
                ORDER BY c.started_at_utc
                LIMIT 1
                ",
            )?;
            stmt.query_map([fixture.project_a_id], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        let [
            (project_a_session_1, source_file_a_1),
            (project_a_session_2, source_file_a_2),
        ] = project_a_sessions
            .as_slice()
            .try_into()
            .context("expected two project-a conversations")?;
        let [(project_b_session_1, source_file_b_1, project_b_id)] = project_b_rows
            .as_slice()
            .try_into()
            .context("expected one project-b conversation")?;

        conn.execute(
            "
            UPDATE conversation
            SET external_id = ?2
            WHERE id = ?1
            ",
            params![
                project_a_session_1,
                format!("source-file:{source_file_a_1}:session:planner-a1")
            ],
        )?;
        conn.execute(
            "
            UPDATE conversation
            SET external_id = ?2
            WHERE id = ?1
            ",
            params![
                project_a_session_2,
                format!("source-file:{source_file_a_2}:session:shared-a2")
            ],
        )?;
        conn.execute(
            "
            UPDATE conversation
            SET external_id = ?2
            WHERE id = ?1
            ",
            params![
                project_b_session_1,
                format!("source-file:{source_file_b_1}:session:reviewer-b1")
            ],
        )?;

        let planner_message_id: i64 = conn.query_row(
            "
            SELECT m.id
            FROM message m
            WHERE m.conversation_id = ?1
            ORDER BY m.sequence_no
            LIMIT 1
            ",
            [project_a_session_1],
            |row| row.get(0),
        )?;
        conn.execute(
            "
            INSERT INTO message_part (
                message_id,
                ordinal,
                part_kind,
                text_value,
                is_error
            )
            VALUES (?1, 98, 'text', ?2, 0)
            ",
            params![
                planner_message_id,
                "---\nname: planner\ndescription: planner body\n---\nFocus on planning.\n"
            ],
        )?;

        let reviewer_message_id: i64 = conn.query_row(
            "
            SELECT m.id
            FROM message m
            WHERE m.conversation_id = ?1
            ORDER BY m.sequence_no
            LIMIT 1
            ",
            [project_b_session_1],
            |row| row.get(0),
        )?;
        conn.execute(
            "
            INSERT INTO message_part (
                message_id,
                ordinal,
                part_kind,
                tool_name,
                metadata_json,
                is_error
            )
            VALUES (?1, 99, 'tool_use', 'Bash', ?2, 0)
            ",
            params![
                reviewer_message_id,
                "{\"input\":{\"command\":\"python3 /tmp/.codex/plugins/cache/pkg/local/skills/reviewer/scripts/run.py\"}}"
            ],
        )?;

        let source_file_id: i64 = conn.query_row(
            "
            INSERT INTO source_file (
                project_id,
                relative_path,
                source_kind,
                modified_at_utc,
                size_bytes,
                scan_warnings_json
            )
            VALUES (?1, '../history.jsonl', 'claude_history', '2026-03-28T09:00:00Z', 1, '[]')
            RETURNING id
            ",
            [fixture.project_a_id],
            |row| row.get(0),
        )?;
        let import_chunk_id: i64 = conn.query_row(
            "
            INSERT INTO import_chunk (
                project_id,
                chunk_day_local,
                state,
                publish_seq,
                started_at_utc,
                completed_at_utc
            )
            VALUES (?1, '2026-03-28', 'complete', 3, '2026-03-28T09:00:00Z', '2026-03-28T09:00:01Z')
            RETURNING id
            ",
            [fixture.project_a_id],
            |row| row.get(0),
        )?;

        let history_event_ids = [
            ("planner-a1", "planner", "2026-03-28T08:00:00Z"),
            ("planner-a1", "planner", "2026-03-28T08:01:00Z"),
            ("shared-a2", "planner", "2026-03-28T08:02:00Z"),
            ("shared-a2", "reviewer", "2026-03-28T08:03:00Z"),
            ("reviewer-b1", "reviewer", "2026-03-28T08:04:00Z"),
            ("missing-session", "ghost", "2026-03-28T08:05:00Z"),
        ]
        .into_iter()
        .enumerate()
        .map(|(idx, (session_id, skill_name, recorded_at_utc))| {
            let history_event_id: i64 = conn.query_row(
                "
                INSERT INTO history_event (
                    import_chunk_id,
                    source_file_id,
                    source_line_no,
                    session_id,
                    recorded_at_utc,
                    raw_project,
                    display_text,
                    pasted_contents_json,
                    input_kind,
                    slash_command_name,
                    raw_json
                )
                VALUES (?1, ?2, ?3, ?4, ?5, '/tmp/project', ?6, '[]', 'slash_command', 'skill', '{}')
                RETURNING id
                ",
                params![
                    import_chunk_id,
                    source_file_id,
                    idx as i64 + 1,
                    session_id,
                    recorded_at_utc,
                    format!("/skill {skill_name}")
                ],
                |row| row.get(0),
            )?;
            conn.execute(
                "
                INSERT INTO skill_invocation (
                    import_chunk_id,
                    history_event_id,
                    source_file_id,
                    session_id,
                    recorded_at_utc,
                    raw_project,
                    skill_name,
                    invocation_kind
                )
                VALUES (?1, ?2, ?3, ?4, ?5, '/tmp/project', ?6, 'explicit_history')
                ",
                params![
                    import_chunk_id,
                    history_event_id,
                    source_file_id,
                    session_id,
                    recorded_at_utc,
                    skill_name
                ],
            )?;
            Ok::<_, anyhow::Error>(history_event_id)
        })
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(history_event_ids.len(), 6);

        let planner_action_id: i64 = conn.query_row(
            "
            SELECT a.id
            FROM action a
            JOIN turn t ON t.id = a.turn_id
            WHERE t.conversation_id = ?1
            ",
            [project_a_session_1],
            |row| row.get(0),
        )?;
        let reviewer_action_id: i64 = conn.query_row(
            "
            SELECT a.id
            FROM action a
            JOIN turn t ON t.id = a.turn_id
            WHERE t.conversation_id = ?1
            ",
            [project_b_session_1],
            |row| row.get(0),
        )?;
        conn.execute(
            "
            INSERT INTO action_skill_attribution (action_id, skill_name, confidence)
            VALUES (?1, 'planner', 'high'), (?2, 'reviewer', 'high')
            ",
            params![planner_action_id, reviewer_action_id],
        )?;

        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let root_report = engine.skills_report(&snapshot, SkillsPath::Root)?;
        assert_eq!(root_report.cost_scope, "session-associated");
        assert_eq!(root_report.attributed_cost_scope, "action-attributed");
        assert_eq!(root_report.unmatched_invocation_count, 1);
        assert_eq!(root_report.unmatched_session_count, 1);
        assert_eq!(root_report.rows.len(), 2);
        assert_eq!(root_report.rows[0].skill_name, "planner");
        assert_eq!(root_report.rows[0].invocation_count, 3);
        assert_eq!(root_report.rows[0].session_count, 2);
        assert_eq!(root_report.rows[0].confidence_counts.confirmed, 2);
        assert_eq!(root_report.rows[0].confidence_counts.explicit, 1);
        assert_eq!(root_report.rows[0].transcript_evidence_count, 2);
        assert_eq!(root_report.rows[0].metrics.uncached_input, 10.0);
        assert_eq!(root_report.rows[0].attributed_action_count, 1);
        assert_eq!(root_report.rows[0].attributed_metrics.uncached_input, 5.0);
        assert_eq!(
            root_report.rows[0].top_attribution_confidence,
            Some(SkillAttributionConfidence::High)
        );
        assert_eq!(root_report.rows[1].skill_name, "reviewer");
        assert_eq!(root_report.rows[1].invocation_count, 2);
        assert_eq!(root_report.rows[1].session_count, 2);
        assert_eq!(root_report.rows[1].confidence_counts.confirmed, 1);
        assert_eq!(root_report.rows[1].confidence_counts.explicit, 1);
        assert_eq!(root_report.rows[1].transcript_evidence_count, 1);
        assert_eq!(root_report.rows[1].metrics.uncached_input, 9.0);
        assert_eq!(root_report.rows[1].attributed_action_count, 1);
        assert_eq!(root_report.rows[1].attributed_metrics.uncached_input, 4.0);
        assert_eq!(
            root_report.rows[1].top_attribution_confidence,
            Some(SkillAttributionConfidence::High)
        );

        let planner_projects = engine.skills_report(
            &snapshot,
            SkillsPath::Skill {
                skill_name: "planner".to_string(),
            },
        )?;
        assert_eq!(planner_projects.rows.len(), 1);
        assert_eq!(
            planner_projects.rows[0].project_id,
            Some(fixture.project_a_id)
        );
        assert_eq!(planner_projects.rows[0].invocation_count, 3);
        assert_eq!(planner_projects.rows[0].session_count, 2);
        assert_eq!(planner_projects.rows[0].confidence_counts.confirmed, 2);
        assert_eq!(planner_projects.rows[0].metrics.uncached_input, 10.0);
        assert_eq!(planner_projects.rows[0].attributed_action_count, 1);
        assert_eq!(
            planner_projects.rows[0].attributed_metrics.uncached_input,
            5.0
        );

        let reviewer_sessions = engine.skills_report(
            &snapshot,
            SkillsPath::SkillProject {
                skill_name: "reviewer".to_string(),
                project_id: project_b_id,
            },
        )?;
        assert_eq!(reviewer_sessions.rows.len(), 1);
        assert_eq!(
            reviewer_sessions.rows[0].session_id.as_deref(),
            Some("reviewer-b1")
        );
        assert_eq!(reviewer_sessions.rows[0].invocation_count, 1);
        assert_eq!(reviewer_sessions.rows[0].session_count, 1);
        assert_eq!(reviewer_sessions.rows[0].confidence_counts.confirmed, 1);
        assert_eq!(reviewer_sessions.rows[0].transcript_evidence_count, 1);
        assert_eq!(reviewer_sessions.rows[0].metrics.uncached_input, 4.0);
        assert_eq!(reviewer_sessions.rows[0].attributed_action_count, 1);
        assert_eq!(
            reviewer_sessions.rows[0].attributed_metrics.uncached_input,
            4.0
        );
        assert_eq!(
            reviewer_sessions.rows[0].top_attribution_confidence,
            Some(SkillAttributionConfidence::High)
        );

        Ok(())
    }

    #[test]
    fn snapshot_bounds_ignore_incomplete_chunks() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;

        let project_id = db.connection_mut().query_row(
            "
            INSERT INTO project (identity_kind, canonical_key, display_name, root_path)
            VALUES ('path', 'project-key', 'project', '/tmp/project')
            RETURNING id
            ",
            [],
            |row| row.get::<_, i64>(0),
        )?;

        db.connection_mut().execute(
            "
            INSERT INTO import_chunk (project_id, chunk_day_local, state, publish_seq)
            VALUES
                (?1, '2026-03-26', 'complete', 1),
                (?1, '2026-03-25', 'running', NULL),
                (?1, '2026-03-24', 'pending', 99),
                (?1, '2026-03-23', 'complete', 2)
            ",
            [project_id],
        )?;

        assert_eq!(
            SnapshotBounds::load(db.connection())?,
            SnapshotBounds {
                max_publish_seq: 2,
                published_chunk_count: 2,
                upper_bound_utc: None,
            }
        );

        Ok(())
    }

    #[test]
    fn pinned_snapshot_ignores_newer_chunks_until_refreshed() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let engine = QueryEngine::new(conn);
        let project_a_root = temp.path().join("project-a");
        let pinned_snapshot = engine.latest_snapshot_bounds()?;

        let before_refresh = engine.browse(&BrowseRequest {
            snapshot: pinned_snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::Root,
        })?;
        assert_eq!(before_refresh[0].label, "project-a");
        assert_eq!(before_refresh[0].metrics.uncached_input, 10.0);

        let refreshed_chunk_id = insert_import_chunk(
            conn,
            fixture.project_a_id,
            "2026-03-28",
            "complete",
            Some(3),
            Some("2026-03-28T09:00:00Z"),
        )?;
        seed_action(
            conn,
            SeedAction {
                project_id: fixture.project_a_id,
                project_root: &project_a_root,
                import_chunk_id: refreshed_chunk_id,
                category: Some("editing"),
                normalized_action: Some("read file"),
                command_family: None,
                base_command: None,
                classification_state: "classified",
                timestamp_utc: "2026-03-28T09:00:00Z",
                model_name: Some("claude-opus"),
                input_tokens: 7,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                output_tokens: 1,
                path_refs: vec![project_a_root.join("src").join("lib.rs")],
            },
        )?;
        refresh_chunk_fixture_aggregates(conn, refreshed_chunk_id)?;

        let still_pinned = engine.browse(&BrowseRequest {
            snapshot: pinned_snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::Root,
        })?;
        assert_eq!(still_pinned[0].metrics.uncached_input, 10.0);
        assert!(engine.has_newer_snapshot(&pinned_snapshot)?);

        let refreshed_snapshot = engine.latest_snapshot_bounds()?;
        let after_refresh = engine.browse(&BrowseRequest {
            snapshot: refreshed_snapshot,
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::Root,
        })?;
        assert_eq!(after_refresh[0].metrics.uncached_input, 17.0);

        Ok(())
    }

    #[test]
    fn project_hierarchy_rolls_up_metrics_and_filters() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let root_rows = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::Root,
        })?;
        assert_eq!(root_rows.len(), 2);
        assert_eq!(root_rows[0].label, "project-a");
        assert_eq!(root_rows[0].metrics.uncached_input, 10.0);
        assert_eq!(root_rows[0].metrics.gross_input, 11.0);
        assert_eq!(root_rows[0].metrics.output, 3.0);
        assert_eq!(root_rows[0].indicators.selected_lens_last_5_hours, 10.0);
        assert_eq!(root_rows[0].indicators.selected_lens_last_week, 10.0);
        assert_eq!(root_rows[0].indicators.uncached_input_reference, 10.0);

        assert_eq!(root_rows[1].label, "project-b");
        assert_eq!(root_rows[1].metrics.uncached_input, 6.0);
        assert_eq!(root_rows[1].indicators.selected_lens_last_5_hours, 0.0);
        assert_eq!(root_rows[1].indicators.selected_lens_last_week, 2.0);

        let model_filtered = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters {
                model: Some("claude-opus".to_string()),
                ..BrowseFilters::default()
            },
            path: BrowsePath::Root,
        })?;
        assert_eq!(model_filtered.len(), 2);
        assert_eq!(model_filtered[0].label, "project-a");
        assert_eq!(model_filtered[0].metrics.uncached_input, 5.0);
        assert_eq!(model_filtered[1].label, "project-b");
        assert_eq!(model_filtered[1].metrics.uncached_input, 2.0);

        let time_filtered = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::CategoryHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters {
                time_window: Some(TimeWindowFilter {
                    start_at_utc: Some("2026-03-26T00:00:00Z".to_string()),
                    end_at_utc: None,
                }),
                ..BrowseFilters::default()
            },
            path: BrowsePath::Root,
        })?;
        let labels: Vec<_> = time_filtered.iter().map(|row| row.label.as_str()).collect();
        assert_eq!(labels, vec!["editing", "test/build/run"]);

        let category_rows = engine.browse(&BrowseRequest {
            snapshot,
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::Project {
                project_id: fixture.project_a_id,
            },
        })?;
        assert_eq!(category_rows.len(), 2);
        assert_eq!(category_rows[0].label, "editing");
        assert_eq!(category_rows[0].metrics.uncached_input, 5.0);

        Ok(())
    }

    #[test]
    fn zero_value_aggregate_rows_are_hidden_across_roots_and_filters() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let project_a_root = temp.path().join("project-a");

        let zero_chunk_id = insert_import_chunk(
            conn,
            fixture.project_a_id,
            "2026-03-28",
            "complete",
            Some(3),
            Some("2026-03-28T09:00:00Z"),
        )?;
        seed_action(
            conn,
            SeedAction {
                project_id: fixture.project_a_id,
                project_root: &project_a_root,
                import_chunk_id: zero_chunk_id,
                category: Some("Inactive"),
                normalized_action: Some("do nothing"),
                command_family: None,
                base_command: None,
                classification_state: "classified",
                timestamp_utc: "2026-03-28T09:00:00Z",
                model_name: Some("claude-zero"),
                input_tokens: 0,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                output_tokens: 0,
                path_refs: vec![project_a_root.join("src").join("idle.rs")],
            },
        )?;
        refresh_chunk_fixture_aggregates(conn, zero_chunk_id)?;

        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let project_rows = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::Project {
                project_id: fixture.project_a_id,
            },
        })?;
        let project_labels = project_rows
            .iter()
            .map(|row| row.label.as_str())
            .collect::<Vec<_>>();
        assert_eq!(project_labels, vec!["editing", "test/build/run"]);

        let category_rows = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::CategoryHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::Root,
        })?;
        let category_labels = category_rows
            .iter()
            .map(|row| row.label.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            category_labels,
            vec![
                "editing",
                "test/build/run",
                "[mixed]",
                "documentation writing",
            ]
        );

        let zero_category_filtered = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters {
                action_category: Some("Inactive".to_string()),
                ..BrowseFilters::default()
            },
            path: BrowsePath::Project {
                project_id: fixture.project_a_id,
            },
        })?;
        assert!(zero_category_filtered.is_empty());

        let zero_category_filtered_root = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::CategoryHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters {
                action_category: Some("Inactive".to_string()),
                ..BrowseFilters::default()
            },
            path: BrowsePath::Root,
        })?;
        assert!(zero_category_filtered_root.is_empty());

        let zero_category_rows = engine.browse(&BrowseRequest {
            snapshot,
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::ProjectCategory {
                project_id: fixture.project_a_id,
                category: "Inactive".to_string(),
            },
        })?;
        assert!(zero_category_rows.is_empty());

        Ok(())
    }

    #[test]
    fn category_hierarchy_and_path_rollups_respect_explicit_file_refs() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let category_rows = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::CategoryHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::Root,
        })?;
        let labels: Vec<_> = category_rows.iter().map(|row| row.label.as_str()).collect();
        assert_eq!(
            labels,
            vec![
                "editing",
                "test/build/run",
                "[mixed]",
                "documentation writing"
            ]
        );

        let editing_action = ActionKey {
            classification_state: ClassificationState::Classified,
            normalized_action: Some("read file".to_string()),
            command_family: None,
            base_command: None,
        };
        let action_rows = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::CategoryHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::Category {
                category: "editing".to_string(),
            },
        })?;
        assert_eq!(action_rows.len(), 1);
        assert_eq!(action_rows[0].action.as_ref(), Some(&editing_action));

        let project_rows = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::CategoryHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::CategoryAction {
                category: "editing".to_string(),
                action: editing_action.clone(),
            },
        })?;
        assert_eq!(project_rows.len(), 1);
        assert_eq!(project_rows[0].label, "project-a");

        let path_rows = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::ProjectAction {
                project_id: fixture.project_a_id,
                category: "editing".to_string(),
                action: editing_action.clone(),
                parent_path: None,
            },
        })?;
        assert_eq!(path_rows.len(), 1);
        assert_eq!(path_rows[0].label, "src");
        assert_eq!(path_rows[0].metrics.uncached_input, 5.0);
        assert_eq!(path_rows[0].item_count, 1);

        let file_rows = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::ProjectAction {
                project_id: fixture.project_a_id,
                category: "editing".to_string(),
                action: editing_action,
                parent_path: path_rows[0].full_path.clone(),
            },
        })?;
        assert_eq!(file_rows.len(), 1);
        assert_eq!(file_rows[0].label, "lib.rs");
        assert_eq!(file_rows[0].metrics.uncached_input, 5.0);

        let no_path_action = ActionKey {
            classification_state: ClassificationState::Classified,
            normalized_action: Some("cargo test".to_string()),
            command_family: Some("cargo".to_string()),
            base_command: Some("cargo".to_string()),
        };
        let no_path_rows = engine.browse(&BrowseRequest {
            snapshot,
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::ProjectAction {
                project_id: fixture.project_a_id,
                category: "test/build/run".to_string(),
                action: no_path_action,
                parent_path: None,
            },
        })?;
        assert!(no_path_rows.is_empty());

        Ok(())
    }

    #[test]
    fn project_category_rows_surface_skill_attribution_only_for_attributed_actions() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let project_a_root = temp.path().join("project-a");
        let chunk_a_id: i64 = conn.query_row(
            "
            SELECT id
            FROM import_chunk
            WHERE project_id = ?1 AND state = 'complete'
            ORDER BY publish_seq
            LIMIT 1
            ",
            [fixture.project_a_id],
            |row| row.get(0),
        )?;

        let read_file_action_id: i64 = conn.query_row(
            "
            SELECT a.id
            FROM action a
            JOIN import_chunk ic ON ic.id = a.import_chunk_id
            WHERE ic.project_id = ?1
              AND a.category = 'editing'
              AND a.normalized_action = 'read file'
            LIMIT 1
            ",
            [fixture.project_a_id],
            |row| row.get(0),
        )?;
        conn.execute(
            "
            INSERT INTO action_skill_attribution (action_id, skill_name, confidence)
            VALUES (?1, 'planner', 'high')
            ",
            [read_file_action_id],
        )?;

        seed_action(
            conn,
            SeedAction {
                project_id: fixture.project_a_id,
                project_root: &project_a_root,
                import_chunk_id: chunk_a_id,
                category: Some("editing"),
                normalized_action: Some("write file"),
                command_family: None,
                base_command: None,
                classification_state: "classified",
                timestamp_utc: "2026-03-26T10:00:00Z",
                model_name: Some("claude-opus"),
                input_tokens: 1,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                output_tokens: 1,
                path_refs: vec![project_a_root.join("src").join("main.rs")],
            },
        )?;
        refresh_chunk_fixture_aggregates(conn, chunk_a_id)?;

        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;
        let rows = engine.browse(&BrowseRequest {
            snapshot,
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::ProjectCategory {
                project_id: fixture.project_a_id,
                category: "editing".to_string(),
            },
        })?;

        assert_eq!(rows.len(), 2);
        let read_file_row = rows
            .iter()
            .find(|row| row.label == "read file")
            .context("expected read file row")?;
        assert_eq!(
            read_file_row.skill_attribution,
            Some(SkillAttributionSummary {
                skill_name: "planner".to_string(),
                confidence: SkillAttributionConfidence::High,
            })
        );
        let write_file_row = rows
            .iter()
            .find(|row| row.label == "write file")
            .context("expected write file row")?;
        assert_eq!(write_file_row.skill_attribution, None);

        Ok(())
    }

    #[test]
    fn project_action_path_rows_surface_skill_attribution_per_attributed_leaf() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let project_a_root = temp.path().join("project-a");
        let chunk_a_id: i64 = conn.query_row(
            "
            SELECT id
            FROM import_chunk
            WHERE project_id = ?1 AND state = 'complete'
            ORDER BY publish_seq
            LIMIT 1
            ",
            [fixture.project_a_id],
            |row| row.get(0),
        )?;
        let read_file_action_id: i64 = conn.query_row(
            "
            SELECT a.id
            FROM action a
            JOIN import_chunk ic ON ic.id = a.import_chunk_id
            WHERE ic.project_id = ?1
              AND a.category = 'editing'
              AND a.normalized_action = 'read file'
            LIMIT 1
            ",
            [fixture.project_a_id],
            |row| row.get(0),
        )?;
        conn.execute(
            "
            INSERT INTO action_skill_attribution (action_id, skill_name, confidence)
            VALUES (?1, 'planner', 'high')
            ",
            [read_file_action_id],
        )?;

        seed_action(
            conn,
            SeedAction {
                project_id: fixture.project_a_id,
                project_root: &project_a_root,
                import_chunk_id: chunk_a_id,
                category: Some("editing"),
                normalized_action: Some("read file"),
                command_family: None,
                base_command: None,
                classification_state: "classified",
                timestamp_utc: "2026-03-26T09:30:00Z",
                model_name: Some("claude-opus"),
                input_tokens: 1,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                output_tokens: 1,
                path_refs: vec![project_a_root.join("README.md")],
            },
        )?;
        refresh_chunk_fixture_aggregates(conn, chunk_a_id)?;

        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;
        let editing_action = ActionKey {
            classification_state: ClassificationState::Classified,
            normalized_action: Some("read file".to_string()),
            command_family: None,
            base_command: None,
        };
        let rows = engine.browse(&BrowseRequest {
            snapshot,
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::ProjectAction {
                project_id: fixture.project_a_id,
                category: "editing".to_string(),
                action: editing_action,
                parent_path: None,
            },
        })?;

        assert_eq!(rows.len(), 2);
        let src_row = rows
            .iter()
            .find(|row| row.label == "src")
            .context("expected src row")?;
        assert_eq!(
            src_row.skill_attribution,
            Some(SkillAttributionSummary {
                skill_name: "planner".to_string(),
                confidence: SkillAttributionConfidence::High,
            })
        );
        let readme_row = rows
            .iter()
            .find(|row| row.label == "README.md")
            .context("expected README row")?;
        assert_eq!(readme_row.skill_attribution, None);

        Ok(())
    }

    #[test]
    fn browse_many_matches_individual_non_path_and_path_requests() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let editing_action = ActionKey {
            classification_state: ClassificationState::Classified,
            normalized_action: Some("read file".to_string()),
            command_family: None,
            base_command: None,
        };
        let non_path_requests = vec![
            BrowseRequest {
                snapshot: snapshot.clone(),
                root: RootView::ProjectHierarchy,
                lens: MetricLens::UncachedInput,
                filters: BrowseFilters::default(),
                path: BrowsePath::Project {
                    project_id: fixture.project_a_id,
                },
            },
            BrowseRequest {
                snapshot: snapshot.clone(),
                root: RootView::CategoryHierarchy,
                lens: MetricLens::UncachedInput,
                filters: BrowseFilters::default(),
                path: BrowsePath::Category {
                    category: "editing".to_string(),
                },
            },
        ];
        let non_path_batch = engine.browse_many(&non_path_requests)?;
        for (request, rows) in non_path_requests.iter().zip(non_path_batch.iter()) {
            assert_eq!(*rows, engine.browse(request)?);
        }

        let root_path_request = BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::ProjectAction {
                project_id: fixture.project_a_id,
                category: "editing".to_string(),
                action: editing_action.clone(),
                parent_path: None,
            },
        };
        let root_path_rows = engine.browse(&root_path_request)?;
        let child_path_request = BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::ProjectAction {
                project_id: fixture.project_a_id,
                category: "editing".to_string(),
                action: editing_action,
                parent_path: root_path_rows[0].full_path.clone(),
            },
        };
        let path_requests = vec![root_path_request, child_path_request];
        let path_batch = engine.browse_many(&path_requests)?;
        for (request, rows) in path_requests.iter().zip(path_batch.iter()) {
            assert_eq!(*rows, engine.browse(request)?);
        }
        Ok(())
    }

    #[test]
    fn browse_batch_single_path_matches_individual_browse() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let editing_action = ActionKey {
            classification_state: ClassificationState::Classified,
            normalized_action: Some("read file".to_string()),
            command_family: None,
            base_command: None,
        };
        let path = BrowsePath::ProjectAction {
            project_id: fixture.project_a_id,
            category: "editing".to_string(),
            action: editing_action.clone(),
            parent_path: None,
        };

        let individual = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: path.clone(),
        })?;

        let batch_response = engine.browse_batch(&BatchBrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            paths: vec![path.clone()],
        })?;

        assert_eq!(batch_response.results.len(), 1);
        assert_eq!(batch_response.results[0].path, path);
        assert_eq!(batch_response.results[0].rows, individual);

        Ok(())
    }

    #[test]
    fn browse_batch_multi_parent_path_drill() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let editing_action = ActionKey {
            classification_state: ClassificationState::Classified,
            normalized_action: Some("read file".to_string()),
            command_family: None,
            base_command: None,
        };

        let root_path = BrowsePath::ProjectAction {
            project_id: fixture.project_a_id,
            category: "editing".to_string(),
            action: editing_action.clone(),
            parent_path: None,
        };
        let root_rows = engine.browse(&BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: root_path.clone(),
        })?;

        let child_path = BrowsePath::ProjectAction {
            project_id: fixture.project_a_id,
            category: "editing".to_string(),
            action: editing_action,
            parent_path: root_rows[0].full_path.clone(),
        };

        let batch_response = engine.browse_batch(&BatchBrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            paths: vec![root_path.clone(), child_path.clone()],
        })?;

        assert_eq!(batch_response.results.len(), 2);
        assert_eq!(batch_response.results[0].path, root_path);
        assert_eq!(batch_response.results[1].path, child_path);
        assert_eq!(batch_response.results[0].rows, root_rows);

        // Each result is attributable to its specific parent path
        for result in &batch_response.results {
            assert!(
                matches!(&result.path, BrowsePath::ProjectAction { .. }),
                "result path must be a ProjectAction variant"
            );
        }

        Ok(())
    }

    #[test]
    fn browse_batch_into_cache_pairs_produces_valid_browse_requests() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let editing_action = ActionKey {
            classification_state: ClassificationState::Classified,
            normalized_action: Some("read file".to_string()),
            command_family: None,
            base_command: None,
        };

        let batch_request = BatchBrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            paths: vec![BrowsePath::ProjectAction {
                project_id: fixture.project_a_id,
                category: "editing".to_string(),
                action: editing_action.clone(),
                parent_path: None,
            }],
        };
        let batch_response = engine.browse_batch(&batch_request)?;
        let cache_pairs = batch_response.into_cache_pairs(&batch_request);

        assert_eq!(cache_pairs.len(), 1);
        let (cached_request, cached_rows) = &cache_pairs[0];

        // The decomposed request should produce identical results when used individually
        let individual_rows = engine.browse(cached_request)?;
        assert_eq!(*cached_rows, individual_rows);

        Ok(())
    }

    #[test]
    fn browse_batch_empty_paths_returns_empty_response() -> Result<()> {
        let temp = tempdir()?;
        let db = Database::open(temp.path().join("usage.sqlite3"))?;
        let engine = QueryEngine::new(db.connection());

        let batch_response = engine.browse_batch(&BatchBrowseRequest {
            snapshot: SnapshotBounds::bootstrap(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            paths: Vec::new(),
        })?;

        assert!(batch_response.results.is_empty());

        Ok(())
    }

    #[test]
    fn verbose_perf_logging_captures_per_node_browse_context() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let log_path = temp.path().join("perf.jsonl");
        let logger = PerfLogger::open_jsonl(log_path.clone())?;
        let engine = QueryEngine::with_perf(conn, Some(logger));
        let snapshot = engine.latest_snapshot_bounds()?;

        let _rows = engine.browse(&BrowseRequest {
            snapshot,
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::Project {
                project_id: fixture.project_a_id,
            },
        })?;

        let payloads = fs::read_to_string(log_path)?
            .lines()
            .map(serde_json::from_str::<JsonValue>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let browse = payloads
            .iter()
            .find(|payload| payload["operation"] == "query.browse")
            .context("missing query.browse perf event")?;

        assert_eq!(browse["root"], "ProjectHierarchy");
        assert_eq!(browse["lens"], "UncachedInput");
        assert_eq!(
            browse["path"]["Project"]["project_id"],
            fixture.project_a_id
        );
        assert_eq!(browse["granularity"], "verbose");
        assert!(browse["row_count"].as_u64().unwrap_or(0) > 0);
        Ok(())
    }

    #[test]
    fn filter_options_expose_visible_projects_models_categories_and_actions() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        seed_query_fixture(conn, temp.path())?;
        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let options = engine.filter_options(&snapshot)?;
        assert_eq!(
            options,
            FilterOptions {
                projects: vec![
                    super::ProjectFilterOption {
                        id: 1,
                        display_name: "project-a".to_string(),
                    },
                    super::ProjectFilterOption {
                        id: 2,
                        display_name: "project-b".to_string(),
                    },
                ],
                models: vec!["claude-haiku".to_string(), "claude-opus".to_string()],
                categories: vec![
                    "[mixed]".to_string(),
                    "documentation writing".to_string(),
                    "editing".to_string(),
                    "test/build/run".to_string(),
                ],
                actions: vec![
                    super::ActionFilterOption {
                        category: "[mixed]".to_string(),
                        action: ActionKey {
                            classification_state: ClassificationState::Mixed,
                            normalized_action: None,
                            command_family: None,
                            base_command: None,
                        },
                        label: "[mixed]".to_string(),
                    },
                    super::ActionFilterOption {
                        category: "documentation writing".to_string(),
                        action: ActionKey {
                            classification_state: ClassificationState::Classified,
                            normalized_action: Some("document write".to_string()),
                            command_family: None,
                            base_command: None,
                        },
                        label: "document write".to_string(),
                    },
                    super::ActionFilterOption {
                        category: "editing".to_string(),
                        action: ActionKey {
                            classification_state: ClassificationState::Classified,
                            normalized_action: Some("read file".to_string()),
                            command_family: None,
                            base_command: None,
                        },
                        label: "read file".to_string(),
                    },
                    super::ActionFilterOption {
                        category: "test/build/run".to_string(),
                        action: ActionKey {
                            classification_state: ClassificationState::Classified,
                            normalized_action: Some("cargo test".to_string()),
                            command_family: Some("cargo".to_string()),
                            base_command: Some("cargo".to_string()),
                        },
                        label: "cargo test".to_string(),
                    },
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn special_state_labels_are_bracketed() {
        assert_eq!(
            ActionKey {
                classification_state: ClassificationState::Mixed,
                normalized_action: None,
                command_family: None,
                base_command: None,
            }
            .label(),
            "[mixed]"
        );
        assert_eq!(
            ActionKey {
                classification_state: ClassificationState::Unclassified,
                normalized_action: None,
                command_family: None,
                base_command: None,
            }
            .label(),
            "[unclassified]"
        );
        assert_eq!(
            display_category(None, "mixed").expect("mixed should map to a display label"),
            "[mixed]"
        );
        assert_eq!(
            display_category(None, "unclassified")
                .expect("unclassified should map to a display label"),
            "[unclassified]"
        );
    }

    #[test]
    fn scoped_action_facts_query_applies_path_and_filter_clauses() -> Result<()> {
        let request = BrowseRequest {
            snapshot: SnapshotBounds {
                max_publish_seq: 42,
                published_chunk_count: 3,
                upper_bound_utc: Some("2026-03-27T00:00:00Z".to_string()),
            },
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters {
                model: Some("claude-opus".to_string()),
                time_window: Some(TimeWindowFilter {
                    start_at_utc: Some("2026-03-20T00:00:00Z".to_string()),
                    end_at_utc: Some("2026-03-27T00:00:00Z".to_string()),
                }),
                ..BrowseFilters::default()
            },
            path: BrowsePath::ProjectCategory {
                project_id: 7,
                category: "editing".to_string(),
            },
        };

        let (sql, query_params) = build_scoped_action_facts_query(&request)?;

        assert!(sql.contains("AND ic.publish_seq <= ?"));
        assert!(sql.contains("AND p.id = ?"));
        assert!(sql.contains("AND EXISTS ("));
        assert!(sql.contains("m_filter.model_name = ?"));
        assert!(
            sql.contains("datetime(COALESCE(a.ended_at_utc, a.started_at_utc)) >= datetime(?)")
        );
        assert!(
            sql.contains("datetime(COALESCE(a.ended_at_utc, a.started_at_utc)) <= datetime(?)")
        );
        assert!(sql.contains("GROUP_CONCAT(DISTINCT m.model_name)"));
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("WHEN a.category IS NOT NULL THEN a.category"));

        assert_eq!(
            query_params,
            vec![
                Value::Integer(42),
                Value::Integer(7),
                Value::Text("editing".to_string()),
                Value::Text("claude-opus".to_string()),
                Value::Text("2026-03-20T00:00:00Z".to_string()),
                Value::Text("2026-03-27T00:00:00Z".to_string()),
            ]
        );

        Ok(())
    }

    #[test]
    fn grouped_action_rollup_rows_query_groups_project_root_rows() -> Result<()> {
        let request = BrowseRequest {
            snapshot: SnapshotBounds {
                max_publish_seq: 42,
                published_chunk_count: 3,
                upper_bound_utc: Some("2026-03-27T00:00:00Z".to_string()),
            },
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::Root,
        };

        let (sql, query_params) = build_grouped_action_rollup_rows_query(&request)?;

        assert!(sql.contains("FROM chunk_action_rollup car"));
        assert!(sql.contains("JOIN import_chunk ic ON ic.id = car.import_chunk_id"));
        assert!(sql.contains("JOIN project p ON p.id = ic.project_id"));
        assert!(sql.contains("COALESCE(SUM(car.action_count), 0)"));
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("p.display_name"));
        assert_eq!(query_params, vec![Value::Integer(42)]);

        Ok(())
    }

    #[test]
    fn grouped_action_rollup_rows_query_groups_action_rows_and_applies_filters() -> Result<()> {
        let request = BrowseRequest {
            snapshot: SnapshotBounds {
                max_publish_seq: 42,
                published_chunk_count: 3,
                upper_bound_utc: Some("2026-03-27T00:00:00Z".to_string()),
            },
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters {
                action: Some(ActionKey {
                    classification_state: ClassificationState::Classified,
                    normalized_action: Some("read file".to_string()),
                    command_family: None,
                    base_command: None,
                }),
                ..BrowseFilters::default()
            },
            path: BrowsePath::ProjectCategory {
                project_id: 7,
                category: "editing".to_string(),
            },
        };

        let (sql, query_params) = build_grouped_action_rollup_rows_query(&request)?;

        assert!(sql.contains("car.display_category"));
        assert!(sql.contains("car.classification_state"));
        assert!(sql.contains("car.normalized_action"));
        assert!(sql.contains("car.command_family"));
        assert!(sql.contains("car.base_command"));
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("AND p.id = ?"));
        assert!(sql.contains("AND car.display_category = ?"));
        assert!(sql.contains("AND car.classification_state = ?"));
        assert_eq!(
            query_params,
            vec![
                Value::Integer(42),
                Value::Integer(7),
                Value::Text("editing".to_string()),
                Value::Text("classified".to_string()),
                Value::Text("read file".to_string()),
            ]
        );

        Ok(())
    }

    #[test]
    fn scoped_path_facts_query_applies_scope_parent_and_filters() -> Result<()> {
        let action = ActionKey {
            classification_state: ClassificationState::Classified,
            normalized_action: Some("read file".to_string()),
            command_family: None,
            base_command: None,
        };
        let request = BrowseRequest {
            snapshot: SnapshotBounds {
                max_publish_seq: 12,
                published_chunk_count: 2,
                upper_bound_utc: Some("2026-03-27T00:00:00Z".to_string()),
            },
            root: RootView::CategoryHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters {
                model: Some("claude-haiku".to_string()),
                time_window: Some(TimeWindowFilter {
                    start_at_utc: Some("2026-03-21T00:00:00Z".to_string()),
                    end_at_utc: Some("2026-03-27T00:00:00Z".to_string()),
                }),
                ..BrowseFilters::default()
            },
            path: BrowsePath::CategoryActionProject {
                category: "editing".to_string(),
                action,
                project_id: 9,
                parent_path: Some("/tmp/project/src".to_string()),
            },
        };

        let (sql, query_params) = build_scoped_path_facts_query(&request)?;

        assert!(sql.contains("AND pn.node_kind = 'file'"));
        assert!(sql.contains("AND p.id = ?"));
        assert!(sql.contains("AND m.model_name = ?"));
        assert!(sql.contains("AND substr(pn.full_path, 1, length(?) + 1) = ? || '/'"));
        assert!(
            sql.contains("datetime(COALESCE(m.completed_at_utc, m.created_at_utc)) >= datetime(?)")
        );
        assert!(
            sql.contains("datetime(COALESCE(m.completed_at_utc, m.created_at_utc)) <= datetime(?)")
        );
        assert!(sql.contains("SELECT COUNT(*)"));
        assert!(sql.contains("AND a.classification_state = ?"));

        assert_eq!(
            query_params,
            vec![
                Value::Integer(12),
                Value::Integer(9),
                Value::Text("editing".to_string()),
                Value::Text("classified".to_string()),
                Value::Text("read file".to_string()),
                Value::Text("claude-haiku".to_string()),
                Value::Text("2026-03-21T00:00:00Z".to_string()),
                Value::Text("2026-03-27T00:00:00Z".to_string()),
                Value::Text("/tmp/project/src".to_string()),
                Value::Text("/tmp/project/src".to_string()),
            ]
        );

        Ok(())
    }

    #[derive(Debug)]
    struct Fixture {
        project_a_id: i64,
    }

    fn seed_query_fixture(conn: &mut Connection, root: &Path) -> Result<Fixture> {
        let project_a_root = root.join("project-a");
        let project_b_root = root.join("project-b");
        fs::create_dir_all(project_a_root.join("src"))?;
        fs::create_dir_all(project_b_root.join("docs"))?;

        let project_a_id = insert_project(conn, "project-a", &project_a_root)?;
        let project_b_id = insert_project(conn, "project-b", &project_b_root)?;

        let chunk_a_id = insert_import_chunk(
            conn,
            project_a_id,
            "2026-03-26",
            "complete",
            Some(1),
            Some("2026-03-26T12:00:00Z"),
        )?;
        let chunk_b_id = insert_import_chunk(
            conn,
            project_b_id,
            "2026-03-26",
            "complete",
            Some(2),
            Some("2026-03-26T14:00:00Z"),
        )?;
        let _ignored_chunk =
            insert_import_chunk(conn, project_a_id, "2026-03-27", "pending", None, None)?;

        seed_action(
            conn,
            SeedAction {
                project_id: project_a_id,
                project_root: &project_a_root,
                import_chunk_id: chunk_a_id,
                category: Some("editing"),
                normalized_action: Some("read file"),
                command_family: None,
                base_command: None,
                classification_state: "classified",
                timestamp_utc: "2026-03-26T09:00:00Z",
                model_name: Some("claude-opus"),
                input_tokens: 2,
                cache_creation_input_tokens: 3,
                cache_read_input_tokens: 0,
                output_tokens: 1,
                path_refs: vec![project_a_root.join("src").join("lib.rs")],
            },
        )?;
        refresh_chunk_fixture_aggregates(conn, chunk_a_id)?;
        seed_action(
            conn,
            SeedAction {
                project_id: project_a_id,
                project_root: &project_a_root,
                import_chunk_id: chunk_a_id,
                category: Some("test/build/run"),
                normalized_action: Some("cargo test"),
                command_family: Some("cargo"),
                base_command: Some("cargo"),
                classification_state: "classified",
                timestamp_utc: "2026-03-26T13:00:00Z",
                model_name: Some("claude-haiku"),
                input_tokens: 5,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 1,
                output_tokens: 2,
                path_refs: Vec::new(),
            },
        )?;
        refresh_chunk_fixture_aggregates(conn, chunk_a_id)?;
        seed_action(
            conn,
            SeedAction {
                project_id: project_b_id,
                project_root: &project_b_root,
                import_chunk_id: chunk_b_id,
                category: Some("documentation writing"),
                normalized_action: Some("document write"),
                command_family: None,
                base_command: None,
                classification_state: "classified",
                timestamp_utc: "2026-03-25T18:00:00Z",
                model_name: Some("claude-opus"),
                input_tokens: 1,
                cache_creation_input_tokens: 1,
                cache_read_input_tokens: 0,
                output_tokens: 4,
                path_refs: vec![project_b_root.join("docs").join("README.md")],
            },
        )?;
        refresh_chunk_fixture_aggregates(conn, chunk_b_id)?;
        seed_action(
            conn,
            SeedAction {
                project_id: project_b_id,
                project_root: &project_b_root,
                import_chunk_id: chunk_b_id,
                category: None,
                normalized_action: None,
                command_family: None,
                base_command: None,
                classification_state: "mixed",
                timestamp_utc: "2026-03-18T10:00:00Z",
                model_name: Some("claude-haiku"),
                input_tokens: 4,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                output_tokens: 0,
                path_refs: Vec::new(),
            },
        )?;
        refresh_chunk_fixture_aggregates(conn, chunk_b_id)?;

        Ok(Fixture { project_a_id })
    }

    fn insert_project(conn: &Connection, display_name: &str, root_path: &Path) -> Result<i64> {
        let root_path = root_path.to_string_lossy().to_string();
        Ok(conn.query_row(
            "
            INSERT INTO project (
                identity_kind,
                canonical_key,
                display_name,
                root_path,
                git_root_path,
                git_origin,
                identity_reason
            )
            VALUES ('path', ?1, ?2, ?3, NULL, NULL, 'fixture')
            RETURNING id
            ",
            params![format!("path:{root_path}"), display_name, root_path],
            |row| row.get(0),
        )?)
    }

    fn insert_import_chunk(
        conn: &Connection,
        project_id: i64,
        chunk_day_local: &str,
        state: &str,
        publish_seq: Option<i64>,
        completed_at_utc: Option<&str>,
    ) -> Result<i64> {
        Ok(conn.query_row(
            "
            INSERT INTO import_chunk (
                project_id,
                chunk_day_local,
                state,
                publish_seq,
                started_at_utc,
                completed_at_utc
            )
            VALUES (?1, ?2, ?3, ?4, COALESCE(?5, CURRENT_TIMESTAMP), ?5)
            RETURNING id
            ",
            params![
                project_id,
                chunk_day_local,
                state,
                publish_seq,
                completed_at_utc
            ],
            |row| row.get(0),
        )?)
    }

    struct SeedAction<'a> {
        project_id: i64,
        project_root: &'a Path,
        import_chunk_id: i64,
        category: Option<&'a str>,
        normalized_action: Option<&'a str>,
        command_family: Option<&'a str>,
        base_command: Option<&'a str>,
        classification_state: &'a str,
        timestamp_utc: &'a str,
        model_name: Option<&'a str>,
        input_tokens: i64,
        cache_creation_input_tokens: i64,
        cache_read_input_tokens: i64,
        output_tokens: i64,
        path_refs: Vec<PathBuf>,
    }

    fn seed_action(conn: &Connection, spec: SeedAction<'_>) -> Result<()> {
        let source_file_id: i64 = conn.query_row(
            "
            INSERT INTO source_file (
                project_id,
                relative_path,
                modified_at_utc,
                size_bytes,
                scan_warnings_json
            )
            VALUES (?1, ?2, ?3, 1, '[]')
            RETURNING id
            ",
            params![
                spec.project_id,
                format!("fixtures/{}.jsonl", spec.timestamp_utc),
                spec.timestamp_utc
            ],
            |row| row.get(0),
        )?;

        let conversation_id: i64 = conn.query_row(
            "
            INSERT INTO conversation (
                project_id,
                source_file_id,
                external_id,
                started_at_utc,
                ended_at_utc
            )
            VALUES (?1, ?2, ?3, ?4, ?4)
            RETURNING id
            ",
            params![
                spec.project_id,
                source_file_id,
                format!("conversation-{}", spec.timestamp_utc),
                spec.timestamp_utc
            ],
            |row| row.get(0),
        )?;

        let stream_id: i64 = conn.query_row(
            "
            INSERT INTO stream (
                conversation_id,
                import_chunk_id,
                external_id,
                stream_kind,
                sequence_no,
                opened_at_utc,
                closed_at_utc
            )
            VALUES (?1, ?2, ?3, 'primary', 0, ?4, ?4)
            RETURNING id
            ",
            params![
                conversation_id,
                spec.import_chunk_id,
                format!("stream-{}", spec.timestamp_utc),
                spec.timestamp_utc
            ],
            |row| row.get(0),
        )?;

        let message_id: i64 = conn.query_row(
            "
            INSERT INTO message (
                stream_id,
                conversation_id,
                import_chunk_id,
                external_id,
                role,
                message_kind,
                sequence_no,
                created_at_utc,
                completed_at_utc,
                input_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                output_tokens,
                model_name,
                usage_source
            )
            VALUES (?1, ?2, ?3, ?4, 'assistant', 'assistant', 0, ?5, ?5, ?6, ?7, ?8, ?9, ?10, 'fixture')
            RETURNING id
            ",
            params![
                stream_id,
                conversation_id,
                spec.import_chunk_id,
                format!("message-{}", spec.timestamp_utc),
                spec.timestamp_utc,
                spec.input_tokens,
                spec.cache_creation_input_tokens,
                spec.cache_read_input_tokens,
                spec.output_tokens,
                spec.model_name,
            ],
            |row| row.get(0),
        )?;

        let turn_id: i64 = conn.query_row(
            "
            INSERT INTO turn (
                stream_id,
                conversation_id,
                import_chunk_id,
                root_message_id,
                sequence_no,
                started_at_utc,
                ended_at_utc,
                input_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                output_tokens
            )
            VALUES (?1, ?2, ?3, ?4, 0, ?5, ?5, ?6, ?7, ?8, ?9)
            RETURNING id
            ",
            params![
                stream_id,
                conversation_id,
                spec.import_chunk_id,
                message_id,
                spec.timestamp_utc,
                spec.input_tokens,
                spec.cache_creation_input_tokens,
                spec.cache_read_input_tokens,
                spec.output_tokens,
            ],
            |row| row.get(0),
        )?;

        conn.execute(
            "UPDATE message SET turn_id = ?1 WHERE id = ?2",
            params![turn_id, message_id],
        )?;

        let action_id: i64 = conn.query_row(
            "
            INSERT INTO action (
                turn_id,
                import_chunk_id,
                sequence_no,
                category,
                normalized_action,
                command_family,
                base_command,
                classification_state,
                started_at_utc,
                ended_at_utc,
                input_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                output_tokens,
                message_count
            )
            VALUES (?1, ?2, 0, ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?9, ?10, ?11, ?12, 1)
            RETURNING id
            ",
            params![
                turn_id,
                spec.import_chunk_id,
                spec.category,
                spec.normalized_action,
                spec.command_family,
                spec.base_command,
                spec.classification_state,
                spec.timestamp_utc,
                spec.input_tokens,
                spec.cache_creation_input_tokens,
                spec.cache_read_input_tokens,
                spec.output_tokens,
            ],
            |row| row.get(0),
        )?;

        conn.execute(
            "UPDATE message SET action_id = ?1 WHERE id = ?2",
            params![action_id, message_id],
        )?;

        for (ordinal, path_ref) in spec.path_refs.iter().enumerate() {
            let path_node_id =
                ensure_path_node_chain(conn, spec.project_id, spec.project_root, path_ref)?;
            conn.execute(
                "
                INSERT INTO message_path_ref (
                    message_id,
                    path_node_id,
                    ref_kind,
                    ordinal
                )
                VALUES (?1, ?2, 'read', ?3)
                ",
                params![message_id, path_node_id, ordinal as i64],
            )?;
        }

        Ok(())
    }

    fn refresh_chunk_fixture_aggregates(conn: &Connection, import_chunk_id: i64) -> Result<()> {
        conn.execute(
            "
            UPDATE import_chunk
            SET
                imported_record_count = 0,
                imported_message_count = (
                    SELECT COUNT(*)
                    FROM message
                    WHERE import_chunk_id = ?1
                ),
                imported_action_count = (
                    SELECT COUNT(*)
                    FROM action
                    WHERE import_chunk_id = ?1
                ),
                imported_conversation_count = (
                    SELECT COUNT(DISTINCT conversation_id)
                    FROM stream
                    WHERE import_chunk_id = ?1
                ),
                imported_turn_count = (
                    SELECT COUNT(*)
                    FROM turn
                    WHERE import_chunk_id = ?1
                )
            WHERE id = ?1
            ",
            [import_chunk_id],
        )?;
        crate::rollup::rebuild_chunk_action_rollups(conn, import_chunk_id, None)?;
        crate::rollup::rebuild_chunk_path_rollups(conn, import_chunk_id, None)?;
        Ok(())
    }

    fn ensure_path_node_chain(
        conn: &Connection,
        project_id: i64,
        project_root: &Path,
        full_path: &Path,
    ) -> Result<i64> {
        let root_full_path = project_root.to_string_lossy().to_string();
        let root_name = project_root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(root_full_path.as_str());
        let root_id = ensure_path_node(
            conn,
            project_id,
            None,
            root_name,
            &root_full_path,
            "root",
            0,
        )?;

        let relative_path = full_path.strip_prefix(project_root)?;
        let mut current_full_path = project_root.to_path_buf();
        let mut parent_id = root_id;
        let components: Vec<_> = relative_path.components().collect();

        for (depth, component) in components.iter().enumerate() {
            current_full_path.push(component.as_os_str());
            let node_kind = if depth + 1 == components.len() {
                "file"
            } else {
                "dir"
            };
            let name = component.as_os_str().to_string_lossy().to_string();
            parent_id = ensure_path_node(
                conn,
                project_id,
                Some(parent_id),
                &name,
                &current_full_path.to_string_lossy(),
                node_kind,
                (depth + 1) as i64,
            )?;
        }

        Ok(parent_id)
    }

    fn ensure_path_node(
        conn: &Connection,
        project_id: i64,
        parent_id: Option<i64>,
        name: &str,
        full_path: &str,
        node_kind: &str,
        depth: i64,
    ) -> Result<i64> {
        if let Some(existing_id) = conn
            .query_row(
                "
                SELECT id
                FROM path_node
                WHERE project_id = ?1 AND full_path = ?2
                ",
                params![project_id, full_path],
                |row| row.get(0),
            )
            .optional()?
        {
            return Ok(existing_id);
        }

        Ok(conn.query_row(
            "
            INSERT INTO path_node (
                project_id,
                parent_id,
                name,
                full_path,
                node_kind,
                depth
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            RETURNING id
            ",
            params![project_id, parent_id, name, full_path, node_kind, depth],
            |row| row.get(0),
        )?)
    }

    #[test]
    fn build_opportunities_rows_detects_history_drag() {
        // Simulate a conversation with growing input across 6 turns — enough to
        // trigger the history drag detector (min 4 turns, growth ratio > 1.35).
        let turn_rows: Vec<ConversationTurnRow> = (0..6)
            .map(|i| ConversationTurnRow {
                conversation_id: 1,
                project_id: 10,
                project_name: "test-project".to_string(),
                conversation_title: Some("growing session".to_string()),
                _sequence_no: i,
                uncached_input: 20 + i * 30, // 20, 50, 80, 110, 140, 170
                cached_input: 5,
                output_tokens: 10,
            })
            .collect();

        let rows = build_opportunities_rows(&turn_rows, false);

        // With this growth pattern the detector should fire.
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.conversation_id, 1);
        assert_eq!(row.project_id, 10);
        assert_eq!(row.turn_count, 6);
        assert!(!row.opportunities.is_empty());
        assert_eq!(
            row.opportunities.top_category,
            Some(crate::opportunity::OpportunityCategory::HistoryDrag)
        );
    }

    #[test]
    fn build_opportunities_rows_excludes_empty_by_default() {
        // 2 turns — below the history drag minimum of 4.
        let turn_rows = vec![
            ConversationTurnRow {
                conversation_id: 1,
                project_id: 10,
                project_name: "test-project".to_string(),
                conversation_title: None,
                _sequence_no: 0,
                uncached_input: 100,
                cached_input: 10,
                output_tokens: 50,
            },
            ConversationTurnRow {
                conversation_id: 1,
                project_id: 10,
                project_name: "test-project".to_string(),
                conversation_title: None,
                _sequence_no: 1,
                uncached_input: 100,
                cached_input: 10,
                output_tokens: 50,
            },
        ];

        let rows = build_opportunities_rows(&turn_rows, false);
        assert!(rows.is_empty());

        let rows_with_empty = build_opportunities_rows(&turn_rows, true);
        assert_eq!(rows_with_empty.len(), 1);
        assert!(rows_with_empty[0].opportunities.is_empty());
    }

    #[test]
    fn build_opportunities_rows_groups_by_conversation() {
        // Two conversations: one with drag, one without.
        let mut turn_rows = Vec::new();

        // Conversation 1: 6 turns with strong growth.
        for i in 0..6 {
            turn_rows.push(ConversationTurnRow {
                conversation_id: 1,
                project_id: 10,
                project_name: "proj".to_string(),
                conversation_title: Some("has drag".to_string()),
                _sequence_no: i,
                uncached_input: 20 + i * 30,
                cached_input: 5,
                output_tokens: 10,
            });
        }

        // Conversation 2: 2 turns — too few.
        for i in 0..2 {
            turn_rows.push(ConversationTurnRow {
                conversation_id: 2,
                project_id: 10,
                project_name: "proj".to_string(),
                conversation_title: Some("no drag".to_string()),
                _sequence_no: i,
                uncached_input: 100,
                cached_input: 10,
                output_tokens: 50,
            });
        }

        let rows = build_opportunities_rows(&turn_rows, false);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].conversation_id, 1);
    }

    #[test]
    fn snapshot_bounds_bootstrap_has_zero_publish_seq() {
        let bounds = SnapshotBounds::bootstrap();
        assert_eq!(bounds.max_publish_seq, 0);
        assert_eq!(bounds.published_chunk_count, 0);
        assert!(bounds.upper_bound_utc.is_none());
        assert!(bounds.is_bootstrap());
    }

    #[test]
    fn snapshot_bounds_parse_sqlite_timestamp_upper_bound() -> Result<()> {
        let bounds = SnapshotBounds {
            max_publish_seq: 1,
            published_chunk_count: 1,
            upper_bound_utc: Some("2026-03-27 18:28:38".to_string()),
        };

        let timestamp = bounds
            .upper_bound_timestamp()?
            .context("expected parsed upper bound")?;

        assert_eq!(timestamp.to_string(), "2026-03-27T18:28:38Z");
        Ok(())
    }

    #[test]
    fn browse_batch_multi_parent_grouped_matches_individual() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let batch_request = BatchBrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            paths: vec![
                BrowsePath::Project {
                    project_id: fixture.project_a_id,
                },
                BrowsePath::Root,
            ],
        };

        let batch_response = engine.browse_batch(&batch_request)?;
        assert_eq!(batch_response.results.len(), 2);

        for result in &batch_response.results {
            let individual = engine.browse(&BrowseRequest {
                snapshot: snapshot.clone(),
                root: RootView::ProjectHierarchy,
                lens: MetricLens::UncachedInput,
                filters: BrowseFilters::default(),
                path: result.path.clone(),
            })?;
            assert_eq!(
                *result,
                BatchBrowseResult {
                    path: result.path.clone(),
                    rows: individual,
                }
            );
        }

        Ok(())
    }

    #[test]
    fn browse_batch_single_element_matches_browse() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let path = BrowsePath::Project {
            project_id: fixture.project_a_id,
        };
        let batch_response = engine.browse_batch(&BatchBrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            paths: vec![path.clone()],
        })?;

        let individual = engine.browse(&BrowseRequest {
            snapshot,
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: path.clone(),
        })?;

        assert_eq!(batch_response.results.len(), 1);
        assert_eq!(
            batch_response.results[0],
            BatchBrowseResult {
                path,
                rows: individual,
            }
        );

        Ok(())
    }

    #[test]
    fn browse_batch_empty_paths_returns_empty_results() -> Result<()> {
        let temp = tempdir()?;
        let db = Database::open(temp.path().join("usage.sqlite3"))?;
        let engine = QueryEngine::new(db.connection());
        let snapshot = SnapshotBounds::bootstrap();

        let response = engine.browse_batch(&BatchBrowseRequest {
            snapshot,
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            paths: vec![],
        })?;

        assert!(response.results.is_empty());
        Ok(())
    }

    #[test]
    fn browse_batch_path_browse_falls_back_to_individual() -> Result<()> {
        let temp = tempdir()?;
        let mut db = Database::open(temp.path().join("usage.sqlite3"))?;
        let conn = db.connection_mut();
        let fixture = seed_query_fixture(conn, temp.path())?;
        let engine = QueryEngine::new(conn);
        let snapshot = engine.latest_snapshot_bounds()?;

        let editing_action = ActionKey {
            classification_state: ClassificationState::Classified,
            normalized_action: Some("read file".to_string()),
            command_family: None,
            base_command: None,
        };
        let path_browse = BrowsePath::ProjectAction {
            project_id: fixture.project_a_id,
            category: "editing".to_string(),
            action: editing_action,
            parent_path: None,
        };

        let batch_response = engine.browse_batch(&BatchBrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            paths: vec![path_browse.clone()],
        })?;

        let individual = engine.browse(&BrowseRequest {
            snapshot,
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: path_browse.clone(),
        })?;

        assert_eq!(batch_response.results.len(), 1);
        assert_eq!(
            batch_response.results[0],
            BatchBrowseResult {
                path: path_browse,
                rows: individual,
            }
        );

        Ok(())
    }

    #[test]
    fn batch_browse_request_to_individual_requests_decomposes_correctly() {
        let batch = BatchBrowseRequest {
            snapshot: SnapshotBounds::bootstrap(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::Total,
            filters: BrowseFilters::default(),
            paths: vec![
                BrowsePath::Root,
                BrowsePath::Project { project_id: 1 },
                BrowsePath::Category {
                    category: "editing".to_string(),
                },
            ],
        };

        let individual = batch.to_individual_requests();
        assert_eq!(individual.len(), 3);
        for (request, path) in individual.iter().zip(batch.paths.iter()) {
            assert_eq!(&request.path, path);
            assert_eq!(request.snapshot, batch.snapshot);
            assert_eq!(request.root, batch.root);
            assert_eq!(request.lens, batch.lens);
            assert_eq!(request.filters, batch.filters);
        }
    }
}
