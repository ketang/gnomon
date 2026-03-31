use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use tempfile::tempdir;

use super::*;
use gnomon_core::validation::{ScaleValidationReport, ScaleValidationSpec, run_scale_validation};

#[derive(Debug, Serialize)]
struct SelectionLagMeasurementReport {
    fixture: SelectionLagFixture,
    summary: SelectionLagSummary,
    slow_samples: Vec<SelectionLagSample>,
}

#[derive(Debug, Serialize)]
struct SelectionLagFixture {
    project_count: usize,
    day_count: usize,
    sessions_per_day: usize,
    navigation_steps: usize,
    slow_threshold_ms: f64,
}

#[derive(Debug, Serialize)]
struct SelectionLagSummary {
    selection_change_count: usize,
    selection_change_cache_hit_count: usize,
    selection_change_cache_miss_count: usize,
    selection_change_max_ms: f64,
    selection_context_load_count: usize,
    selection_context_load_max_ms: f64,
    selection_context_load_browse_request_count: usize,
    selection_context_load_distinct_browse_request_count: usize,
    selection_context_load_duplicate_browse_request_count: usize,
    selection_context_load_cache_memory_hit_count: usize,
    selection_context_load_cache_persisted_hit_count: usize,
    selection_context_load_cache_live_query_count: usize,
    prefetch_batch_count: usize,
    prefetch_batch_max_ms: f64,
    prefetch_queue_wait_max_ms: f64,
    prefetch_queue_wait_total_ms: f64,
}

#[derive(Debug, Serialize)]
struct SelectionLagSample {
    operation: String,
    duration_ms: f64,
    selection_context_cache_hit: Option<bool>,
    selected_tree_row_key: Option<Value>,
    active_path: Option<Value>,
    browse_request_count: Option<u64>,
    distinct_browse_request_count: Option<u64>,
    duplicate_browse_request_count: Option<u64>,
    cache_memory_hit_count: Option<u64>,
    cache_persisted_hit_count: Option<u64>,
    cache_live_query_count: Option<u64>,
    queue_wait_ms: Option<f64>,
}

#[test]
fn selection_move_measurement_trace_is_reproducible() -> Result<()> {
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
            db_path: validation.db_path.clone(),
            source_root: validation.source_root.clone(),
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

    let baseline_line_count = std::fs::read_to_string(&log_path)?.lines().count();
    let trace = [
        KeyCode::Down,
        KeyCode::Down,
        KeyCode::Up,
        KeyCode::Down,
        KeyCode::Up,
    ];
    for key_code in trace {
        app.handle_normal_key(KeyEvent::from(key_code))?;
    }

    drop(app);

    let payloads = std::fs::read_to_string(log_path)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .skip(baseline_line_count)
        .collect::<Vec<_>>();

    let report = selection_lag_measurement_report(&payloads, trace.len(), &validation);
    println!("{}", serde_json::to_string_pretty(&report)?);

    assert!(
        report.summary.selection_change_count > 0,
        "selection changes should emit slow perf events"
    );
    assert!(
        report.summary.selection_context_load_count > 0,
        "selection context loads should emit slow perf events"
    );
    assert!(
        report.summary.prefetch_batch_count > 0,
        "prefetch batches should emit slow perf events"
    );

    Ok(())
}

fn selection_lag_measurement_report(
    payloads: &[Value],
    navigation_steps: usize,
    validation: &ScaleValidationReport,
) -> SelectionLagMeasurementReport {
    let mut selection_change_count = 0usize;
    let mut selection_change_cache_hit_count = 0usize;
    let mut selection_change_cache_miss_count = 0usize;
    let mut selection_change_max_ms = 0.0f64;
    let mut selection_context_load_count = 0usize;
    let mut selection_context_load_max_ms = 0.0f64;
    let mut selection_context_load_browse_request_count = 0usize;
    let mut selection_context_load_distinct_browse_request_count = 0usize;
    let mut selection_context_load_duplicate_browse_request_count = 0usize;
    let mut selection_context_load_cache_memory_hit_count = 0usize;
    let mut selection_context_load_cache_persisted_hit_count = 0usize;
    let mut selection_context_load_cache_live_query_count = 0usize;
    let mut prefetch_batch_count = 0usize;
    let mut prefetch_batch_max_ms = 0.0f64;
    let mut prefetch_queue_wait_max_ms = 0.0f64;
    let mut prefetch_queue_wait_total_ms = 0.0f64;
    let mut slow_samples = Vec::new();
    let slow_threshold_ms = SELECTION_SLOW_LOG_THRESHOLD.as_secs_f64() * 1000.0;

    for payload in payloads {
        let Some(operation) = payload.get("operation").and_then(Value::as_str) else {
            continue;
        };
        let duration_ms = payload
            .get("duration_ms")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);

        if duration_ms >= slow_threshold_ms
            && matches!(
                operation,
                "tui.selection_change" | "tui.selection_context_load" | "tui.prefetch_batch"
            )
        {
            slow_samples.push(SelectionLagSample {
                operation: operation.to_string(),
                duration_ms,
                selection_context_cache_hit: payload
                    .get("selection_context_cache_hit")
                    .and_then(Value::as_bool),
                selected_tree_row_key: payload.get("selected_tree_row_key").cloned(),
                active_path: payload.get("active_path").cloned(),
                browse_request_count: payload.get("browse_request_count").and_then(Value::as_u64),
                distinct_browse_request_count: payload
                    .get("distinct_browse_request_count")
                    .and_then(Value::as_u64),
                duplicate_browse_request_count: payload
                    .get("duplicate_browse_request_count")
                    .and_then(Value::as_u64),
                cache_memory_hit_count: payload
                    .get("cache_memory_hit_count")
                    .and_then(Value::as_u64),
                cache_persisted_hit_count: payload
                    .get("cache_persisted_hit_count")
                    .and_then(Value::as_u64),
                cache_live_query_count: payload
                    .get("cache_live_query_count")
                    .and_then(Value::as_u64),
                queue_wait_ms: payload.get("queue_wait_ms").and_then(Value::as_f64),
            });
        }

        match operation {
            "tui.selection_change" => {
                selection_change_count += 1;
                selection_change_max_ms = selection_change_max_ms.max(duration_ms);
                match payload
                    .get("selection_context_cache_hit")
                    .and_then(Value::as_bool)
                {
                    Some(true) => selection_change_cache_hit_count += 1,
                    Some(false) => selection_change_cache_miss_count += 1,
                    None => {}
                }
            }
            "tui.selection_context_load" => {
                selection_context_load_count += 1;
                selection_context_load_max_ms = selection_context_load_max_ms.max(duration_ms);
                selection_context_load_browse_request_count += payload
                    .get("browse_request_count")
                    .and_then(Value::as_u64)
                    .map(|count| count as usize)
                    .unwrap_or(0);
                selection_context_load_distinct_browse_request_count += payload
                    .get("distinct_browse_request_count")
                    .and_then(Value::as_u64)
                    .map(|count| count as usize)
                    .unwrap_or(0);
                selection_context_load_duplicate_browse_request_count += payload
                    .get("duplicate_browse_request_count")
                    .and_then(Value::as_u64)
                    .map(|count| count as usize)
                    .unwrap_or(0);
                selection_context_load_cache_memory_hit_count += payload
                    .get("cache_memory_hit_count")
                    .and_then(Value::as_u64)
                    .map(|count| count as usize)
                    .unwrap_or(0);
                selection_context_load_cache_persisted_hit_count += payload
                    .get("cache_persisted_hit_count")
                    .and_then(Value::as_u64)
                    .map(|count| count as usize)
                    .unwrap_or(0);
                selection_context_load_cache_live_query_count += payload
                    .get("cache_live_query_count")
                    .and_then(Value::as_u64)
                    .map(|count| count as usize)
                    .unwrap_or(0);
            }
            "tui.prefetch_batch" => {
                prefetch_batch_count += 1;
                prefetch_batch_max_ms = prefetch_batch_max_ms.max(duration_ms);
                if let Some(queue_wait_ms) = payload.get("queue_wait_ms").and_then(Value::as_f64) {
                    prefetch_queue_wait_max_ms = prefetch_queue_wait_max_ms.max(queue_wait_ms);
                    prefetch_queue_wait_total_ms += queue_wait_ms;
                }
            }
            _ => {}
        }
    }

    SelectionLagMeasurementReport {
        fixture: SelectionLagFixture {
            project_count: validation.spec.project_count,
            day_count: validation.spec.day_count,
            sessions_per_day: validation.spec.sessions_per_day,
            navigation_steps,
            slow_threshold_ms,
        },
        summary: SelectionLagSummary {
            selection_change_count,
            selection_change_cache_hit_count,
            selection_change_cache_miss_count,
            selection_change_max_ms,
            selection_context_load_count,
            selection_context_load_max_ms,
            selection_context_load_browse_request_count,
            selection_context_load_distinct_browse_request_count,
            selection_context_load_duplicate_browse_request_count,
            selection_context_load_cache_memory_hit_count,
            selection_context_load_cache_persisted_hit_count,
            selection_context_load_cache_live_query_count,
            prefetch_batch_count,
            prefetch_batch_max_ms,
            prefetch_queue_wait_max_ms,
            prefetch_queue_wait_total_ms,
        },
        slow_samples,
    }
}
