use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::db::Database;
use crate::query::{
    ActionKey, BrowseFilters, BrowsePath, BrowseRequest, MetricLens, QueryEngine, RollupRowKind,
    RootView, SnapshotBounds,
};

const DEFAULT_ITERATIONS: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryBenchmarkOptions {
    pub iterations: usize,
}

impl Default for QueryBenchmarkOptions {
    fn default() -> Self {
        Self {
            iterations: DEFAULT_ITERATIONS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryBenchmarkSelection {
    pub project_id: i64,
    pub project_label: String,
    pub category: String,
    pub action_label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPlanReport {
    pub name: String,
    pub used_by_scenarios: Vec<String>,
    pub detail: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryBenchmarkScenario {
    pub name: String,
    pub iterations: usize,
    pub result_count: usize,
    pub samples_ms: Vec<u128>,
    pub min_ms: u128,
    pub median_ms: u128,
    pub max_ms: u128,
    pub total_ms: u128,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowseFootprintBreakdown {
    pub level: String,
    pub request_count: usize,
    pub row_count: usize,
    pub payload_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowseFootprintScenario {
    pub name: String,
    pub request_count: usize,
    pub row_count: usize,
    pub payload_bytes: u64,
    pub by_level: Vec<BrowseFootprintBreakdown>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowseFootprintRecommendations {
    pub estimated_budget_bytes: u64,
    pub snapshot_retention_count: usize,
    pub recursion_depth_limit: usize,
    pub recursion_breadth_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowseFootprintReport {
    pub snapshot_max_publish_seq: u64,
    pub scenarios: Vec<BrowseFootprintScenario>,
    pub recommendations: BrowseFootprintRecommendations,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryBenchmarkReport {
    pub db_path: PathBuf,
    pub snapshot: SnapshotBounds,
    pub iterations: usize,
    pub selection: QueryBenchmarkSelection,
    pub scenarios: Vec<QueryBenchmarkScenario>,
    pub query_plans: Vec<QueryPlanReport>,
    pub browse_footprint: BrowseFootprintReport,
}

pub fn run_query_benchmark(
    db_path: &Path,
    options: QueryBenchmarkOptions,
) -> Result<QueryBenchmarkReport> {
    if options.iterations == 0 {
        bail!("query benchmark requires at least one iteration");
    }

    let database = Database::open_read_only(db_path)?;
    let engine = QueryEngine::new(database.connection());
    let snapshot = engine.latest_snapshot_bounds()?;
    if snapshot.max_publish_seq == 0 {
        bail!(
            "query benchmark requires at least one published chunk in {}",
            db_path.display()
        );
    }

    let selection = select_benchmark_selection(&engine, &snapshot)?;
    let path_browse_request = BrowseRequest {
        snapshot: snapshot.clone(),
        root: RootView::ProjectHierarchy,
        lens: MetricLens::UncachedInput,
        filters: BrowseFilters::default(),
        path: BrowsePath::ProjectAction {
            project_id: selection.project_id,
            category: selection.category.clone(),
            action: selection.action_key(),
            parent_path: None,
        },
    };
    let filtered_root_request = BrowseRequest {
        snapshot: snapshot.clone(),
        root: RootView::ProjectHierarchy,
        lens: MetricLens::UncachedInput,
        filters: BrowseFilters {
            project_id: Some(selection.project_id),
            ..BrowseFilters::default()
        },
        path: BrowsePath::Root,
    };
    let scoped_action_browse_request = BrowseRequest {
        snapshot: snapshot.clone(),
        root: RootView::ProjectHierarchy,
        lens: MetricLens::UncachedInput,
        filters: BrowseFilters {
            model: Some(selection.model_name.clone()),
            ..BrowseFilters::default()
        },
        path: BrowsePath::ProjectCategory {
            project_id: selection.project_id,
            category: selection.category.clone(),
        },
    };
    let non_path_prefetch_requests =
        collect_non_path_prefetch_requests(&engine, &snapshot, &selection)?;
    let path_prefetch_requests = collect_path_prefetch_requests(&engine, &path_browse_request)?;

    let scenarios = vec![
        measure_scenario("refresh_snapshot_status", options.iterations, || {
            let _ = engine.latest_snapshot_bounds()?;
            Ok(0)
        })?,
        measure_scenario("project_root_browse", options.iterations, || {
            Ok(run_browse(&engine, project_root_request(&snapshot))?.len())
        })?,
        measure_scenario("category_root_browse", options.iterations, || {
            Ok(run_browse(&engine, category_root_request(&snapshot))?.len())
        })?,
        measure_scenario("path_drill_browse", options.iterations, || {
            Ok(run_browse(&engine, path_browse_request.clone())?.len())
        })?,
        measure_scenario("project_root_refresh", options.iterations, || {
            let latest_snapshot = engine.latest_snapshot_bounds()?;
            let _ = engine.filter_options(&latest_snapshot)?;
            Ok(run_browse(&engine, project_root_request(&latest_snapshot))?.len())
        })?,
        measure_scenario("project_root_filter_change", options.iterations, || {
            let _ = engine.filter_options(&snapshot)?;
            Ok(run_browse(&engine, filtered_root_request.clone())?.len())
        })?
        .with_notes(format!(
            "applies --project-id style filtering with project {} ({})",
            selection.project_id, selection.project_label
        )),
        measure_scenario(
            "project_category_model_filter_browse",
            options.iterations,
            || Ok(run_browse(&engine, scoped_action_browse_request.clone())?.len()),
        )?
        .with_notes(format!(
            "applies model filtering with {} inside project {} ({}) category {}",
            selection.model_name, selection.project_id, selection.project_label, selection.category
        )),
        measure_scenario("jump_target_build", options.iterations, || {
            build_jump_target_count(&engine, &snapshot)
        })?,
        measure_scenario("non_path_prefetch_individual", options.iterations, || {
            browse_many_individually(&engine, &non_path_prefetch_requests)
        })?
        .with_notes("warms multiple non-path sibling sets one request at a time".to_string()),
        measure_scenario("non_path_prefetch_batched", options.iterations, || {
            browse_many_batched(&engine, &non_path_prefetch_requests)
        })?
        .with_notes(
            "warms the same non-path sibling sets through QueryEngine::browse_many".to_string(),
        ),
        measure_scenario("path_prefetch_individual", options.iterations, || {
            browse_many_individually(&engine, &path_prefetch_requests)
        })?
        .with_notes("warms recursive path drill parents one request at a time".to_string()),
        measure_scenario("path_prefetch_batched", options.iterations, || {
            browse_many_batched(&engine, &path_prefetch_requests)
        })?
        .with_notes(
            "warms the same recursive path drill parents through QueryEngine::browse_many"
                .to_string(),
        ),
    ];

    let query_plans = vec![
        QueryPlanReport {
            name: "latest_snapshot_bounds".to_string(),
            used_by_scenarios: vec![
                "refresh_snapshot_status".to_string(),
                "project_root_refresh".to_string(),
            ],
            detail: engine.latest_snapshot_bounds_query_plan()?,
        },
        QueryPlanReport {
            name: "grouped_action_rollup_browse".to_string(),
            used_by_scenarios: vec![
                "project_root_browse".to_string(),
                "category_root_browse".to_string(),
                "project_root_refresh".to_string(),
                "project_root_filter_change".to_string(),
                "jump_target_build".to_string(),
            ],
            detail: engine
                .grouped_action_rollup_browse_query_plan(&project_root_request(&snapshot))?,
        },
        QueryPlanReport {
            name: "load_recent_action_facts".to_string(),
            used_by_scenarios: vec![
                "project_root_browse".to_string(),
                "category_root_browse".to_string(),
                "project_root_refresh".to_string(),
                "project_root_filter_change".to_string(),
                "jump_target_build".to_string(),
            ],
            detail: engine.recent_action_facts_query_plan(&snapshot)?,
        },
        QueryPlanReport {
            name: "path_browse_scope".to_string(),
            used_by_scenarios: vec!["path_drill_browse".to_string()],
            detail: engine.path_browse_query_plan(&path_browse_request)?,
        },
        QueryPlanReport {
            name: "batched_non_path_browse".to_string(),
            used_by_scenarios: vec!["non_path_prefetch_batched".to_string()],
            detail: engine.batched_non_path_browse_query_plan(&non_path_prefetch_requests)?,
        },
        QueryPlanReport {
            name: "batched_path_browse".to_string(),
            used_by_scenarios: vec!["path_prefetch_batched".to_string()],
            detail: engine.batched_path_browse_query_plan(&path_prefetch_requests)?,
        },
        QueryPlanReport {
            name: "action_browse_scope".to_string(),
            used_by_scenarios: vec!["project_category_model_filter_browse".to_string()],
            detail: engine.action_browse_query_plan(&scoped_action_browse_request)?,
        },
    ];
    let browse_footprint = build_browse_footprint_report(
        &engine,
        &snapshot,
        &non_path_prefetch_requests,
        &path_prefetch_requests,
    )?;

    Ok(QueryBenchmarkReport {
        db_path: db_path.to_path_buf(),
        snapshot,
        iterations: options.iterations,
        selection: QueryBenchmarkSelection {
            project_id: selection.project_id,
            project_label: selection.project_label,
            category: selection.category,
            action_label: selection.action_label,
        },
        scenarios,
        query_plans,
        browse_footprint,
    })
}

fn measure_scenario<F>(
    name: &str,
    iterations: usize,
    mut operation: F,
) -> Result<QueryBenchmarkScenario>
where
    F: FnMut() -> Result<usize>,
{
    let mut samples_ms = Vec::with_capacity(iterations);
    let mut result_count = 0usize;

    for _ in 0..iterations {
        let started_at = Instant::now();
        result_count = operation()?;
        samples_ms.push(started_at.elapsed().as_millis());
    }

    let mut ordered = samples_ms.clone();
    ordered.sort_unstable();
    let median_ms = ordered[ordered.len() / 2];

    Ok(QueryBenchmarkScenario {
        name: name.to_string(),
        iterations,
        result_count,
        min_ms: *ordered.first().unwrap_or(&0),
        median_ms,
        max_ms: *ordered.last().unwrap_or(&0),
        total_ms: samples_ms.iter().sum(),
        samples_ms,
        notes: None,
    })
}

fn run_browse(
    engine: &QueryEngine<'_>,
    request: BrowseRequest,
) -> Result<Vec<crate::query::RollupRow>> {
    engine.browse(&request)
}

fn browse_many_individually(engine: &QueryEngine<'_>, requests: &[BrowseRequest]) -> Result<usize> {
    let mut total_rows = 0usize;
    for request in requests {
        total_rows += run_browse(engine, request.clone())?.len();
    }
    Ok(total_rows)
}

fn browse_many_batched(engine: &QueryEngine<'_>, requests: &[BrowseRequest]) -> Result<usize> {
    Ok(engine
        .browse_many(requests)?
        .into_iter()
        .map(|rows| rows.len())
        .sum())
}

fn project_root_request(snapshot: &SnapshotBounds) -> BrowseRequest {
    BrowseRequest {
        snapshot: snapshot.clone(),
        root: RootView::ProjectHierarchy,
        lens: MetricLens::UncachedInput,
        filters: BrowseFilters::default(),
        path: BrowsePath::Root,
    }
}

fn category_root_request(snapshot: &SnapshotBounds) -> BrowseRequest {
    BrowseRequest {
        snapshot: snapshot.clone(),
        root: RootView::CategoryHierarchy,
        lens: MetricLens::UncachedInput,
        filters: BrowseFilters::default(),
        path: BrowsePath::Root,
    }
}

fn collect_non_path_prefetch_requests(
    engine: &QueryEngine<'_>,
    snapshot: &SnapshotBounds,
    selection: &BenchmarkSelectionInternal,
) -> Result<Vec<BrowseRequest>> {
    let categories = run_browse(
        engine,
        BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::Project {
                project_id: selection.project_id,
            },
        },
    )?;

    let mut requests = categories
        .iter()
        .take(4)
        .filter_map(|row| row.category.clone())
        .map(|category| BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::ProjectCategory {
                project_id: selection.project_id,
                category,
            },
        })
        .collect::<Vec<_>>();

    if requests.is_empty() {
        requests.push(BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::ProjectCategory {
                project_id: selection.project_id,
                category: selection.category.clone(),
            },
        });
    }

    Ok(requests)
}

fn collect_path_prefetch_requests(
    engine: &QueryEngine<'_>,
    path_browse_request: &BrowseRequest,
) -> Result<Vec<BrowseRequest>> {
    let mut requests = vec![path_browse_request.clone()];
    let root_rows = run_browse(engine, path_browse_request.clone())?;
    for row in root_rows
        .iter()
        .filter(|row| row.kind == RollupRowKind::Directory)
        .take(4)
    {
        if let Some(parent_path) = row.full_path.clone() {
            let mut request = path_browse_request.clone();
            match &mut request.path {
                BrowsePath::ProjectAction {
                    parent_path: path, ..
                }
                | BrowsePath::CategoryActionProject {
                    parent_path: path, ..
                } => *path = Some(parent_path),
                _ => {}
            }
            requests.push(request);
        }
    }
    Ok(requests)
}

fn build_browse_footprint_report(
    engine: &QueryEngine<'_>,
    snapshot: &SnapshotBounds,
    non_path_prefetch_requests: &[BrowseRequest],
    path_prefetch_requests: &[BrowseRequest],
) -> Result<BrowseFootprintReport> {
    let non_path_shallow_requests = non_path_prefetch_requests
        .iter()
        .take(2)
        .cloned()
        .collect::<Vec<_>>();
    let path_shallow_requests = path_prefetch_requests
        .iter()
        .take(1)
        .cloned()
        .collect::<Vec<_>>();
    let scenarios = vec![
        measure_footprint_scenario(
            engine,
            "non_path_shallow_prefetch",
            &non_path_shallow_requests,
        )?,
        measure_footprint_scenario(engine, "non_path_deep_prefetch", non_path_prefetch_requests)?,
        measure_footprint_scenario(engine, "path_shallow_prefetch", &path_shallow_requests)?,
        measure_footprint_scenario(engine, "path_deep_prefetch", path_prefetch_requests)?,
    ];
    let max_payload = scenarios
        .iter()
        .map(|scenario| scenario.payload_bytes)
        .max()
        .unwrap_or(0);
    Ok(BrowseFootprintReport {
        snapshot_max_publish_seq: snapshot.max_publish_seq,
        scenarios,
        recommendations: BrowseFootprintRecommendations {
            estimated_budget_bytes: max_payload.saturating_mul(4).max(4 * 1024 * 1024),
            snapshot_retention_count: 2,
            recursion_depth_limit: 3,
            recursion_breadth_limit: 4,
        },
    })
}

fn measure_footprint_scenario(
    engine: &QueryEngine<'_>,
    name: &str,
    requests: &[BrowseRequest],
) -> Result<BrowseFootprintScenario> {
    let row_sets = engine.browse_many(requests)?;
    let mut by_level = std::collections::BTreeMap::<String, BrowseFootprintBreakdown>::new();
    let mut total_rows = 0usize;
    let mut total_payload_bytes = 0u64;

    for (request, rows) in requests.iter().zip(row_sets.iter()) {
        let payload_bytes = u64::try_from(
            serde_json::to_vec(rows)
                .context("unable to serialize browse rows for footprint measurement")?
                .len(),
        )
        .context("browse footprint payload size overflowed u64")?;
        let level = browse_level_label(&request.path).to_string();
        let breakdown = by_level
            .entry(level.clone())
            .or_insert(BrowseFootprintBreakdown {
                level,
                request_count: 0,
                row_count: 0,
                payload_bytes: 0,
            });
        breakdown.request_count += 1;
        breakdown.row_count += rows.len();
        breakdown.payload_bytes += payload_bytes;
        total_rows += rows.len();
        total_payload_bytes += payload_bytes;
    }

    Ok(BrowseFootprintScenario {
        name: name.to_string(),
        request_count: requests.len(),
        row_count: total_rows,
        payload_bytes: total_payload_bytes,
        by_level: by_level.into_values().collect(),
    })
}

fn browse_level_label(path: &BrowsePath) -> &'static str {
    match path {
        BrowsePath::Root => "root",
        BrowsePath::Project { .. } => "project",
        BrowsePath::ProjectCategory { .. } => "project-category",
        BrowsePath::ProjectAction {
            parent_path: None, ..
        } => "project-action-root-path",
        BrowsePath::ProjectAction {
            parent_path: Some(_),
            ..
        } => "project-action-child-path",
        BrowsePath::Category { .. } => "category",
        BrowsePath::CategoryAction { .. } => "category-action",
        BrowsePath::CategoryActionProject {
            parent_path: None, ..
        } => "category-action-project-root-path",
        BrowsePath::CategoryActionProject {
            parent_path: Some(_),
            ..
        } => "category-action-project-child-path",
    }
}

// Mirrors the TUI jump-to navigator's work: with no filters the target list
// is a pure function of the (project, category, action) tuple set, pulled
// in one query via `action_rollup_tuples`. Replacing the old O(projects ×
// categories) cascading-browse walker here keeps the benchmark aligned with
// what the TUI actually runs.
fn build_jump_target_count(engine: &QueryEngine<'_>, snapshot: &SnapshotBounds) -> Result<usize> {
    let tuples = engine.action_rollup_tuples(snapshot)?;

    let mut projects = std::collections::BTreeSet::<i64>::new();
    let mut project_categories = std::collections::BTreeSet::<(i64, String)>::new();
    let mut categories = std::collections::BTreeSet::<String>::new();
    let mut category_actions = std::collections::BTreeSet::<(String, ActionKey)>::new();
    let mut category_action_projects =
        std::collections::BTreeSet::<(String, ActionKey, i64)>::new();

    let mut project_action_total = 0usize;
    for tuple in &tuples {
        projects.insert(tuple.project_id);
        project_categories.insert((tuple.project_id, tuple.category.clone()));
        categories.insert(tuple.category.clone());
        category_actions.insert((tuple.category.clone(), tuple.action.clone()));
        category_action_projects.insert((
            tuple.category.clone(),
            tuple.action.clone(),
            tuple.project_id,
        ));
        project_action_total += 1;
    }

    Ok(projects.len()
        + project_categories.len()
        + project_action_total
        + categories.len()
        + category_actions.len()
        + category_action_projects.len())
}

#[derive(Debug, Clone)]
struct BenchmarkSelectionInternal {
    project_id: i64,
    project_label: String,
    category: String,
    action_key: ActionKey,
    action_label: String,
    model_name: String,
}

impl BenchmarkSelectionInternal {
    fn action_key(&self) -> ActionKey {
        self.action_key.clone()
    }
}

fn select_benchmark_selection(
    engine: &QueryEngine<'_>,
    snapshot: &SnapshotBounds,
) -> Result<BenchmarkSelectionInternal> {
    let filter_options = engine.filter_options(snapshot)?;
    let project = filter_options
        .projects
        .first()
        .context("query benchmark requires at least one visible project")?;
    let categories = run_browse(
        engine,
        BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::Project {
                project_id: project.id,
            },
        },
    )?;
    let category = categories
        .first()
        .and_then(|row| row.category.clone())
        .context("query benchmark could not derive a project category drill target")?;
    let actions = run_browse(
        engine,
        BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: BrowseFilters::default(),
            path: BrowsePath::ProjectCategory {
                project_id: project.id,
                category: category.clone(),
            },
        },
    )?;
    let action = actions
        .first()
        .context("query benchmark could not derive an action drill target")?;
    let action_key = action
        .action
        .clone()
        .context("query benchmark action drill target was missing an action key")?;
    let model_name = select_benchmark_model(engine, snapshot, project.id, &filter_options.models)?;

    Ok(BenchmarkSelectionInternal {
        project_id: project.id,
        project_label: project.display_name.clone(),
        category,
        action_key,
        action_label: action.label.clone(),
        model_name,
    })
}

fn select_benchmark_model(
    engine: &QueryEngine<'_>,
    snapshot: &SnapshotBounds,
    project_id: i64,
    models: &[String],
) -> Result<String> {
    let fallback = models
        .first()
        .cloned()
        .context("query benchmark requires at least one visible model")?;

    for model in models {
        let rows = run_browse(
            engine,
            BrowseRequest {
                snapshot: snapshot.clone(),
                root: RootView::ProjectHierarchy,
                lens: MetricLens::UncachedInput,
                filters: BrowseFilters {
                    model: Some(model.clone()),
                    ..BrowseFilters::default()
                },
                path: BrowsePath::Project { project_id },
            },
        )?;
        if !rows.is_empty() {
            return Ok(model.clone());
        }
    }

    Ok(fallback)
}

impl QueryBenchmarkScenario {
    fn with_notes(mut self, notes: String) -> Self {
        self.notes = Some(notes);
        self
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tempfile::tempdir;

    use super::{QueryBenchmarkOptions, run_query_benchmark};
    use crate::validation::{ScaleValidationSpec, run_scale_validation};

    #[test]
    fn query_benchmark_rejects_zero_iterations() {
        let temp = tempdir().expect("tempdir");
        let err = run_query_benchmark(
            temp.path().join("missing.sqlite3").as_path(),
            QueryBenchmarkOptions { iterations: 0 },
        )
        .expect_err("zero-iteration benchmark should fail");

        assert!(err.to_string().contains("at least one iteration"));
    }

    #[test]
    fn query_benchmark_reports_hot_scenarios_and_query_plans() -> Result<()> {
        let temp = tempdir()?;
        let validation = run_scale_validation(
            temp.path(),
            ScaleValidationSpec {
                project_count: 1,
                day_count: 2,
                sessions_per_day: 1,
            },
        )?;

        let report =
            run_query_benchmark(&validation.db_path, QueryBenchmarkOptions { iterations: 2 })?;

        assert_eq!(report.iterations, 2);
        assert_eq!(report.query_plans.len(), 7);
        assert!(
            report
                .query_plans
                .iter()
                .any(|plan| plan.name == "grouped_action_rollup_browse")
        );
        assert!(
            report
                .query_plans
                .iter()
                .any(|plan| plan.name == "action_browse_scope")
        );
        assert!(
            report
                .query_plans
                .iter()
                .any(|plan| plan.name == "batched_non_path_browse")
        );
        assert!(
            report
                .query_plans
                .iter()
                .any(|plan| plan.name == "batched_path_browse")
        );
        assert!(
            report
                .scenarios
                .iter()
                .any(|scenario| scenario.name == "jump_target_build" && scenario.result_count > 0)
        );
        assert!(
            report
                .scenarios
                .iter()
                .any(|scenario| scenario.name == "path_drill_browse")
        );
        assert!(
            report
                .scenarios
                .iter()
                .any(|scenario| scenario.name == "project_category_model_filter_browse")
        );
        assert!(
            report
                .scenarios
                .iter()
                .any(|scenario| scenario.name == "non_path_prefetch_batched")
        );
        assert!(
            report
                .scenarios
                .iter()
                .any(|scenario| scenario.name == "path_prefetch_batched")
        );
        assert_eq!(
            report.browse_footprint.snapshot_max_publish_seq,
            report.snapshot.max_publish_seq
        );
        assert!(
            report
                .browse_footprint
                .scenarios
                .iter()
                .any(|scenario| scenario.name == "path_deep_prefetch")
        );
        assert!(
            report
                .browse_footprint
                .recommendations
                .estimated_budget_bytes
                > 0
        );

        Ok(())
    }
}
