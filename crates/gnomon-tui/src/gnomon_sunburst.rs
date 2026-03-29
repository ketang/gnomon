use gnomon_core::query::{
    ClassificationState, FilterOptions, MetricLens, RollupRow, RollupRowKind,
};

use crate::app::{
    cached_ratio, describe_browse_path, format_metric, metric_lens_label, row_kind_label,
};
use crate::sunburst::{
    SunburstBucket, SunburstCenter, SunburstLayer, SunburstModel, SunburstSegment, SunburstSpan,
};

pub(crate) fn build_sunburst_model(
    ancestor_layers: Vec<SunburstLayer>,
    current_span: SunburstSpan,
    visible_rows: &[RollupRow],
    selected_row: Option<&RollupRow>,
    scope_label: String,
    lens: MetricLens,
) -> SunburstModel {
    let mut layers = ancestor_layers;
    if !visible_rows.is_empty() {
        layers.push(build_sunburst_layer(
            visible_rows,
            lens,
            selected_row.map(|row| row.key.as_str()),
            current_span,
        ));
    }

    let selection_label = selected_row
        .map(|row| {
            format!(
                "selected: {} ({}, {} {})",
                row.label,
                row_kind_label(row.kind),
                metric_lens_label(lens),
                format_metric(row.metrics.lens_value(lens))
            )
        })
        .unwrap_or_else(|| "selected: none".to_string());

    SunburstModel {
        center: SunburstCenter {
            scope_label,
            lens_label: metric_lens_label(lens).to_string(),
            selection_label,
        },
        layers,
    }
}

pub(crate) fn build_sunburst_scope_label(
    root: &gnomon_core::query::RootView,
    path: &gnomon_core::query::BrowsePath,
    filter_options: &FilterOptions,
) -> String {
    describe_browse_path(root, path, filter_options)
}

pub(crate) fn build_sunburst_layer(
    rows: &[RollupRow],
    lens: MetricLens,
    selected_key: Option<&str>,
    span: SunburstSpan,
) -> SunburstLayer {
    let segments = rows
        .iter()
        .map(|row| SunburstSegment {
            value: row.metrics.lens_value(lens),
            cached_ratio: cached_ratio(row),
            bucket: sunburst_bucket(row),
            is_selected: selected_key.is_some_and(|key| key == row.key),
        })
        .collect::<Vec<_>>();
    let total_value = segments.iter().map(|segment| segment.value.max(0.0)).sum();

    SunburstLayer {
        span,
        segments,
        total_value,
    }
}

fn sunburst_bucket(row: &RollupRow) -> SunburstBucket {
    match row
        .action
        .as_ref()
        .map(|action| action.classification_state)
    {
        Some(ClassificationState::Classified) => SunburstBucket::Classified,
        Some(ClassificationState::Mixed) => SunburstBucket::Mixed,
        Some(ClassificationState::Unclassified) => SunburstBucket::Unclassified,
        None => match row.kind {
            RollupRowKind::Project => SunburstBucket::Project,
            RollupRowKind::ActionCategory => SunburstBucket::Category,
            RollupRowKind::Action => SunburstBucket::Unclassified,
            RollupRowKind::Directory | RollupRowKind::File => SunburstBucket::Project,
        },
    }
}
