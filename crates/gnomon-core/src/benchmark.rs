use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::db::Database;
use crate::query::{
    ActionKey, BrowseFilters, BrowsePath, BrowseRequest, MetricLens, QueryEngine, RootView,
    SnapshotBounds,
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
pub struct QueryBenchmarkReport {
    pub db_path: PathBuf,
    pub snapshot: SnapshotBounds,
    pub iterations: usize,
    pub selection: QueryBenchmarkSelection,
    pub scenarios: Vec<QueryBenchmarkScenario>,
    pub query_plans: Vec<QueryPlanReport>,
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
        measure_scenario("jump_target_build", options.iterations, || {
            build_jump_target_count(&engine, &snapshot)
        })?,
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
            name: "load_action_facts".to_string(),
            used_by_scenarios: vec![
                "project_root_browse".to_string(),
                "category_root_browse".to_string(),
                "project_root_refresh".to_string(),
                "project_root_filter_change".to_string(),
                "jump_target_build".to_string(),
            ],
            detail: engine.action_facts_query_plan(&snapshot)?,
        },
        QueryPlanReport {
            name: "load_path_facts".to_string(),
            used_by_scenarios: vec!["path_drill_browse".to_string()],
            detail: engine.path_facts_query_plan(&snapshot)?,
        },
    ];

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

fn build_jump_target_count(engine: &QueryEngine<'_>, snapshot: &SnapshotBounds) -> Result<usize> {
    let filters = BrowseFilters::default();
    let mut target_count = 0usize;

    let project_root_rows = run_browse(
        engine,
        BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: MetricLens::UncachedInput,
            filters: filters.clone(),
            path: BrowsePath::Root,
        },
    )?;

    for project in &project_root_rows {
        let Some(project_id) = project.project_id else {
            continue;
        };
        target_count += 1;

        let categories = run_browse(
            engine,
            BrowseRequest {
                snapshot: snapshot.clone(),
                root: RootView::ProjectHierarchy,
                lens: MetricLens::UncachedInput,
                filters: filters.clone(),
                path: BrowsePath::Project { project_id },
            },
        )?;

        for category in &categories {
            let Some(category_name) = category.category.clone() else {
                continue;
            };
            target_count += 1;

            let actions = run_browse(
                engine,
                BrowseRequest {
                    snapshot: snapshot.clone(),
                    root: RootView::ProjectHierarchy,
                    lens: MetricLens::UncachedInput,
                    filters: filters.clone(),
                    path: BrowsePath::ProjectCategory {
                        project_id,
                        category: category_name,
                    },
                },
            )?;

            target_count += actions.len();
        }
    }

    let category_root_rows = run_browse(
        engine,
        BrowseRequest {
            snapshot: snapshot.clone(),
            root: RootView::CategoryHierarchy,
            lens: MetricLens::UncachedInput,
            filters,
            path: BrowsePath::Root,
        },
    )?;

    for category in &category_root_rows {
        let Some(category_name) = category.category.clone() else {
            continue;
        };
        target_count += 1;

        let actions = run_browse(
            engine,
            BrowseRequest {
                snapshot: snapshot.clone(),
                root: RootView::CategoryHierarchy,
                lens: MetricLens::UncachedInput,
                filters: BrowseFilters::default(),
                path: BrowsePath::Category {
                    category: category_name.clone(),
                },
            },
        )?;

        for action in &actions {
            let Some(action_key) = action.action.clone() else {
                continue;
            };
            target_count += 1;

            let projects = run_browse(
                engine,
                BrowseRequest {
                    snapshot: snapshot.clone(),
                    root: RootView::CategoryHierarchy,
                    lens: MetricLens::UncachedInput,
                    filters: BrowseFilters::default(),
                    path: BrowsePath::CategoryAction {
                        category: category_name.clone(),
                        action: action_key,
                    },
                },
            )?;

            target_count += projects.len();
        }
    }

    Ok(target_count)
}

#[derive(Debug, Clone)]
struct BenchmarkSelectionInternal {
    project_id: i64,
    project_label: String,
    category: String,
    action_key: ActionKey,
    action_label: String,
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

    Ok(BenchmarkSelectionInternal {
        project_id: project.id,
        project_label: project.display_name.clone(),
        category,
        action_key,
        action_label: action.label.clone(),
    })
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
        assert_eq!(report.query_plans.len(), 3);
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

        Ok(())
    }
}
