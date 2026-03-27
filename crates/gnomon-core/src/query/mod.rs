use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use jiff::{Timestamp, ToSpan};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowseRequest {
    pub snapshot: SnapshotBounds,
    pub root: RootView,
    pub lens: MetricLens,
    pub filters: BrowseFilters,
    pub path: BrowsePath,
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
    fn label(&self) -> String {
        match self.classification_state {
            ClassificationState::Classified => self
                .normalized_action
                .clone()
                .or_else(|| self.base_command.clone())
                .or_else(|| self.command_family.clone())
                .unwrap_or_else(|| "classified".to_string()),
            ClassificationState::Mixed => "mixed".to_string(),
            ClassificationState::Unclassified => "unclassified".to_string(),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollupRowKind {
    Project,
    ActionCategory,
    Action,
    Directory,
    File,
}

#[derive(Debug, Clone, PartialEq)]
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricIndicators {
    pub selected_lens_last_5_hours: f64,
    pub selected_lens_last_week: f64,
    pub uncached_input_reference: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RollupRow {
    pub kind: RollupRowKind,
    pub key: String,
    pub label: String,
    pub metrics: MetricTotals,
    pub indicators: MetricIndicators,
    pub item_count: u64,
    pub project_id: Option<i64>,
    pub category: Option<String>,
    pub action: Option<ActionKey>,
    pub full_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectFilterOption {
    pub id: i64,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionFilterOption {
    pub category: String,
    pub action: ActionKey,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterOptions {
    pub projects: Vec<ProjectFilterOption>,
    pub models: Vec<String>,
    pub categories: Vec<String>,
    pub actions: Vec<ActionFilterOption>,
}

pub struct QueryEngine<'conn> {
    conn: &'conn Connection,
}

impl<'conn> QueryEngine<'conn> {
    pub fn new(conn: &'conn Connection) -> Self {
        Self { conn }
    }

    pub fn latest_snapshot_bounds(&self) -> Result<SnapshotBounds> {
        let row = self
            .conn
            .query_row(
                "
                SELECT
                    MAX(publish_seq),
                    COUNT(*),
                    MAX(completed_at_utc)
                FROM import_chunk
                WHERE state = 'complete' AND publish_seq IS NOT NULL
                ",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .context("unable to compute the latest published snapshot bounds")?;

        let Some(max_publish_seq) = row.0 else {
            return Ok(SnapshotBounds::bootstrap());
        };

        Ok(SnapshotBounds {
            max_publish_seq: u64::try_from(max_publish_seq)
                .context("latest publish_seq was negative")?,
            published_chunk_count: usize::try_from(row.1)
                .context("published chunk count overflowed usize")?,
            upper_bound_utc: row.2,
        })
    }

    pub fn has_newer_snapshot(&self, snapshot: &SnapshotBounds) -> Result<bool> {
        Ok(self.latest_snapshot_bounds()?.max_publish_seq > snapshot.max_publish_seq)
    }

    pub fn filter_options(&self, snapshot: &SnapshotBounds) -> Result<FilterOptions> {
        let action_facts = self.load_action_facts(snapshot)?;
        let mut projects = BTreeMap::new();
        let mut models = BTreeSet::new();
        let mut categories = BTreeSet::new();
        let mut actions = BTreeMap::new();

        for fact in &action_facts {
            projects
                .entry(fact.project_id)
                .or_insert_with(|| fact.project_display_name.clone());
            categories.insert(fact.category.clone());
            actions
                .entry((fact.category.clone(), fact.action.clone()))
                .or_insert_with(|| fact.action.label());

            for model in &fact.model_names {
                models.insert(model.clone());
            }
        }

        Ok(FilterOptions {
            projects: projects
                .into_iter()
                .map(|(id, display_name)| ProjectFilterOption { id, display_name })
                .collect(),
            models: models.into_iter().collect(),
            categories: categories.into_iter().collect(),
            actions: actions
                .into_iter()
                .map(|((category, action), label)| ActionFilterOption {
                    category,
                    action,
                    label,
                })
                .collect(),
        })
    }

    pub fn browse(&self, request: &BrowseRequest) -> Result<Vec<RollupRow>> {
        let action_facts = self.load_action_facts(&request.snapshot)?;
        let path_facts = self.load_path_facts(&request.snapshot)?;
        let compiled_filters = CompiledFilters::compile(&request.filters)?;
        let windows = Windows::from_snapshot(&request.snapshot)?;

        let mut rows = match (&request.root, &request.path) {
            (RootView::ProjectHierarchy, BrowsePath::Root) => aggregate_projects(
                &action_facts,
                &compiled_filters,
                &windows,
                request.lens,
                None,
                None,
                None,
            ),
            (RootView::ProjectHierarchy, BrowsePath::Project { project_id }) => {
                aggregate_categories(
                    &action_facts,
                    &compiled_filters,
                    &windows,
                    request.lens,
                    Some(*project_id),
                    None,
                    None,
                )
            }
            (
                RootView::ProjectHierarchy,
                BrowsePath::ProjectCategory {
                    project_id,
                    category,
                },
            ) => aggregate_actions(
                &action_facts,
                &compiled_filters,
                &windows,
                request.lens,
                Some(*project_id),
                Some(category),
                None,
            ),
            (
                RootView::ProjectHierarchy,
                BrowsePath::ProjectAction {
                    project_id,
                    category,
                    action,
                    parent_path,
                },
            ) => aggregate_paths(
                &path_facts,
                &compiled_filters,
                &windows,
                request.lens,
                PathBrowseScope {
                    project_id: *project_id,
                    category,
                    action,
                    parent_path: parent_path.as_deref(),
                },
            ),
            (RootView::CategoryHierarchy, BrowsePath::Root) => aggregate_categories(
                &action_facts,
                &compiled_filters,
                &windows,
                request.lens,
                None,
                None,
                None,
            ),
            (RootView::CategoryHierarchy, BrowsePath::Category { category }) => aggregate_actions(
                &action_facts,
                &compiled_filters,
                &windows,
                request.lens,
                None,
                Some(category),
                None,
            ),
            (RootView::CategoryHierarchy, BrowsePath::CategoryAction { category, action }) => {
                aggregate_projects(
                    &action_facts,
                    &compiled_filters,
                    &windows,
                    request.lens,
                    None,
                    Some(category),
                    Some(action),
                )
            }
            (
                RootView::CategoryHierarchy,
                BrowsePath::CategoryActionProject {
                    category,
                    action,
                    project_id,
                    parent_path,
                },
            ) => aggregate_paths(
                &path_facts,
                &compiled_filters,
                &windows,
                request.lens,
                PathBrowseScope {
                    project_id: *project_id,
                    category,
                    action,
                    parent_path: parent_path.as_deref(),
                },
            ),
            _ => bail!(
                "browse path {:?} is incompatible with {:?}",
                request.path,
                request.root
            ),
        };

        rows.sort_by(|left, right| {
            right
                .metrics
                .lens_value(request.lens)
                .total_cmp(&left.metrics.lens_value(request.lens))
                .then_with(|| left.label.cmp(&right.label))
        });

        Ok(rows)
    }

    fn load_action_facts(&self, snapshot: &SnapshotBounds) -> Result<Vec<ActionFact>> {
        if snapshot.max_publish_seq == 0 {
            return Ok(Vec::new());
        }

        let mut stmt = self.conn.prepare(
            "
            SELECT
                p.id,
                p.display_name,
                p.root_path,
                a.category,
                a.normalized_action,
                a.command_family,
                a.base_command,
                a.classification_state,
                COALESCE(a.ended_at_utc, a.started_at_utc),
                COALESCE(a.input_tokens, 0),
                COALESCE(a.cache_creation_input_tokens, 0),
                COALESCE(a.cache_read_input_tokens, 0),
                COALESCE(a.output_tokens, 0),
                GROUP_CONCAT(DISTINCT m.model_name)
            FROM action a
            JOIN import_chunk ic ON ic.id = a.import_chunk_id
            JOIN project p ON p.id = ic.project_id
            LEFT JOIN action_message am ON am.action_id = a.id
            LEFT JOIN message m ON m.id = am.message_id
            WHERE ic.state = 'complete'
              AND ic.publish_seq IS NOT NULL
              AND ic.publish_seq <= ?1
            GROUP BY
                a.id,
                p.id,
                p.display_name,
                p.root_path,
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
                a.output_tokens
            ",
        )?;

        let rows = stmt.query_map([snapshot.max_publish_seq as i64], |row| {
            Ok(LoadedActionFact {
                project_id: row.get(0)?,
                project_display_name: row.get(1)?,
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
                model_names_csv: row.get(13)?,
            })
        })?;

        rows.map(|row| {
            let row = row.context("unable to read an action fact row")?;
            Ok(ActionFact {
                project_id: row.project_id,
                project_display_name: row.project_display_name,
                project_root: row.project_root,
                category: display_category(row.category.as_deref(), &row.classification_state)?,
                action: ActionKey {
                    classification_state: parse_classification_state(&row.classification_state)?,
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
            })
        })
        .collect()
    }

    fn load_path_facts(&self, snapshot: &SnapshotBounds) -> Result<Vec<PathFact>> {
        if snapshot.max_publish_seq == 0 {
            return Ok(Vec::new());
        }

        let mut stmt = self.conn.prepare(
            "
            WITH message_ref_counts AS (
                SELECT message_id, COUNT(*) AS ref_count
                FROM message_path_ref
                GROUP BY message_id
            )
            SELECT
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
                rc.ref_count
            FROM action a
            JOIN import_chunk ic ON ic.id = a.import_chunk_id
            JOIN project p ON p.id = ic.project_id
            JOIN action_message am ON am.action_id = a.id
            JOIN message m ON m.id = am.message_id
            JOIN message_path_ref mpr ON mpr.message_id = m.id
            JOIN path_node pn ON pn.id = mpr.path_node_id
            JOIN message_ref_counts rc ON rc.message_id = m.id
            WHERE ic.state = 'complete'
              AND ic.publish_seq IS NOT NULL
              AND ic.publish_seq <= ?1
              AND pn.node_kind = 'file'
            ",
        )?;

        let rows = stmt.query_map([snapshot.max_publish_seq as i64], |row| {
            Ok(LoadedPathFact {
                project_id: row.get(0)?,
                project_root: row.get(1)?,
                category: row.get(2)?,
                normalized_action: row.get(3)?,
                command_family: row.get(4)?,
                base_command: row.get(5)?,
                classification_state: row.get(6)?,
                timestamp: row.get(7)?,
                input_tokens: row.get(8)?,
                cache_creation_input_tokens: row.get(9)?,
                cache_read_input_tokens: row.get(10)?,
                output_tokens: row.get(11)?,
                model_name: row.get(12)?,
                file_path: row.get(13)?,
                ref_count: row.get(14)?,
            })
        })?;

        rows.map(|row| {
            let row = row.context("unable to read a path attribution fact row")?;
            let metrics = MetricTotals::from_usage(
                row.input_tokens,
                row.cache_creation_input_tokens,
                row.cache_read_input_tokens,
                row.output_tokens,
            )
            .divided_by(row.ref_count as f64);

            Ok(PathFact {
                project_id: row.project_id,
                project_root: row.project_root,
                category: display_category(row.category.as_deref(), &row.classification_state)?,
                action: ActionKey {
                    classification_state: parse_classification_state(&row.classification_state)?,
                    normalized_action: row.normalized_action,
                    command_family: row.command_family,
                    base_command: row.base_command,
                },
                timestamp: parse_timestamp(row.timestamp.as_deref())?,
                model_name: row.model_name,
                file_path: row.file_path,
                metrics,
            })
        })
        .collect()
    }
}

#[derive(Debug)]
struct LoadedActionFact {
    project_id: i64,
    project_display_name: String,
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
    model_names_csv: Option<String>,
}

#[derive(Debug)]
struct ActionFact {
    project_id: i64,
    project_display_name: String,
    project_root: String,
    category: String,
    action: ActionKey,
    timestamp: Option<Timestamp>,
    metrics: MetricTotals,
    model_names: BTreeSet<String>,
}

#[derive(Debug)]
struct LoadedPathFact {
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
}

#[derive(Debug)]
struct PathFact {
    project_id: i64,
    project_root: String,
    category: String,
    action: ActionKey,
    timestamp: Option<Timestamp>,
    model_name: Option<String>,
    file_path: String,
    metrics: MetricTotals,
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
        let Some(upper_bound) = snapshot.upper_bound_utc.as_deref() else {
            return Ok(Self {
                last_5_hours_start: None,
                last_week_start: None,
            });
        };

        let upper_bound = upper_bound
            .parse::<Timestamp>()
            .with_context(|| format!("unable to parse snapshot upper bound {upper_bound}"))?;

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
    project_id: Option<i64>,
    category: Option<String>,
    action: Option<ActionKey>,
    full_path: Option<String>,
    metrics: MetricTotals,
    selected_lens_last_5_hours: f64,
    selected_lens_last_week: f64,
    uncached_input_reference: f64,
    item_count: u64,
}

impl RollupBuilder {
    fn new(
        kind: RollupRowKind,
        key: impl Into<String>,
        label: impl Into<String>,
        project_id: Option<i64>,
        category: Option<String>,
        action: Option<ActionKey>,
        full_path: Option<String>,
    ) -> Self {
        Self {
            kind,
            key: key.into(),
            label: label.into(),
            project_id,
            category,
            action,
            full_path,
            metrics: MetricTotals::zero(),
            selected_lens_last_5_hours: 0.0,
            selected_lens_last_week: 0.0,
            uncached_input_reference: 0.0,
            item_count: 0,
        }
    }

    fn add_metrics(
        &mut self,
        metrics: &MetricTotals,
        lens: MetricLens,
        timestamp: Option<Timestamp>,
        windows: &Windows,
    ) {
        self.metrics.add_assign(metrics);
        self.item_count += 1;
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
            project_id: self.project_id,
            category: self.category,
            action: self.action,
            full_path: self.full_path,
        }
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
                    Some(fact.project_id),
                    None,
                    None,
                    Some(fact.project_root.clone()),
                )
            })
            .add_metrics(&fact.metrics, lens, fact.timestamp, windows);
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
            .add_metrics(&fact.metrics, lens, fact.timestamp, windows);
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
    let mut builders = BTreeMap::<ActionKey, RollupBuilder>::new();

    for fact in facts
        .iter()
        .filter(|fact| filters.matches_action(fact, project_id, category, action))
    {
        builders
            .entry(fact.action.clone())
            .or_insert_with(|| {
                RollupBuilder::new(
                    RollupRowKind::Action,
                    format!("action:{}", fact.action.stable_key()),
                    fact.action.label(),
                    project_id,
                    Some(fact.category.clone()),
                    Some(fact.action.clone()),
                    None,
                )
            })
            .add_metrics(&fact.metrics, lens, fact.timestamp, windows);
    }

    builders.into_values().map(RollupBuilder::build).collect()
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
    let mut builders = BTreeMap::<String, (RollupBuilder, BTreeSet<String>)>::new();

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
                        Some(scope.project_id),
                        Some(scope.category.to_string()),
                        Some(scope.action.clone()),
                        Some(child_path_string.clone()),
                    ),
                    BTreeSet::new(),
                )
            });

        entry
            .0
            .add_metrics(&fact.metrics, lens, fact.timestamp, windows);
        entry.1.insert(fact.file_path.clone());
    }

    builders
        .into_values()
        .map(|(mut builder, leaf_paths)| {
            builder.item_count = leaf_paths.len() as u64;
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
    raw.map(|raw| {
        raw.parse::<Timestamp>()
            .with_context(|| format!("unable to parse timestamp {raw}"))
    })
    .transpose()
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
        ClassificationState::Mixed => Ok("mixed".to_string()),
        ClassificationState::Unclassified => Ok("unclassified".to_string()),
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

    use anyhow::Result;
    use rusqlite::{Connection, OptionalExtension, params};
    use tempfile::tempdir;

    use crate::db::Database;

    use super::{
        ActionKey, BrowseFilters, BrowsePath, BrowseRequest, ClassificationState, FilterOptions,
        MetricLens, QueryEngine, RootView, SnapshotBounds, TimeWindowFilter,
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
                category: Some("Editing"),
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
        assert_eq!(labels, vec!["Editing", "test/build/run"]);

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
        assert_eq!(category_rows[0].label, "Editing");
        assert_eq!(category_rows[0].metrics.uncached_input, 5.0);

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
                "Editing",
                "test/build/run",
                "mixed",
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
                category: "Editing".to_string(),
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
                category: "Editing".to_string(),
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
                category: "Editing".to_string(),
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
                category: "Editing".to_string(),
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
                    "Editing".to_string(),
                    "documentation writing".to_string(),
                    "mixed".to_string(),
                    "test/build/run".to_string(),
                ],
                actions: vec![
                    super::ActionFilterOption {
                        category: "Editing".to_string(),
                        action: ActionKey {
                            classification_state: ClassificationState::Classified,
                            normalized_action: Some("read file".to_string()),
                            command_family: None,
                            base_command: None,
                        },
                        label: "read file".to_string(),
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
                        category: "mixed".to_string(),
                        action: ActionKey {
                            classification_state: ClassificationState::Mixed,
                            normalized_action: None,
                            command_family: None,
                            base_command: None,
                        },
                        label: "mixed".to_string(),
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
                category: Some("Editing"),
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
            "
            INSERT INTO turn_message (turn_id, message_id, ordinal_in_turn)
            VALUES (?1, ?2, 0)
            ",
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
            "
            INSERT INTO action_message (action_id, message_id, ordinal_in_action)
            VALUES (?1, ?2, 0)
            ",
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
}
