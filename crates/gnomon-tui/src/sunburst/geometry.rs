use super::model::{SunburstDistortionPolicy, SunburstLayer, SunburstSegment, SunburstSpan};

pub(crate) fn sunburst_segment_at_angle(
    layer: &SunburstLayer,
    angle: f64,
    policy: SunburstDistortionPolicy,
) -> Option<&SunburstSegment> {
    let local_angle = layer.span.local_angle(angle)?;
    sunburst_segment_at_local_angle(layer, local_angle, policy)
}

pub(crate) fn sunburst_selected_child_span(
    layer: &SunburstLayer,
    policy: SunburstDistortionPolicy,
) -> SunburstSpan {
    if layer.segments.is_empty() {
        return layer.span;
    }

    for (segment, span) in layer
        .segments
        .iter()
        .zip(display_segment_spans(layer, policy))
    {
        if segment.is_selected {
            return span;
        }
    }

    layer.span
}

fn sunburst_segment_at_local_angle(
    layer: &SunburstLayer,
    local_angle: f64,
    policy: SunburstDistortionPolicy,
) -> Option<&SunburstSegment> {
    if layer.segments.is_empty() {
        return None;
    }

    for (index, (segment, span)) in layer
        .segments
        .iter()
        .zip(display_segment_spans(layer, policy))
        .enumerate()
    {
        let end = (span.start - layer.span.start).rem_euclid(std::f64::consts::TAU) + span.sweep;
        if local_angle < end || index + 1 == layer.segments.len() {
            return Some(segment);
        }
    }

    None
}

fn display_segment_spans(
    layer: &SunburstLayer,
    policy: SunburstDistortionPolicy,
) -> Vec<SunburstSpan> {
    let base_sweeps = base_segment_sweeps(layer);
    let Some(selected_index) = layer
        .segments
        .iter()
        .position(|segment| segment.is_selected)
    else {
        return segment_spans_from_sweeps(layer.span, &base_sweeps);
    };

    let selected_base = base_sweeps[selected_index];
    let selected_threshold = layer.span.sweep * policy.focus_zoom_threshold_ratio;
    if selected_base >= selected_threshold && selected_base >= policy.minimum_visible_sweep {
        return segment_spans_from_sweeps(layer.span, &base_sweeps);
    }

    let selected_target = (selected_base * policy.focus_zoom_multiplier)
        .max(policy.minimum_visible_sweep)
        .min(layer.span.sweep * policy.maximum_selected_share)
        .min(layer.span.sweep);

    let other_total = layer.span.sweep - selected_base;
    if other_total <= 0.0 {
        return vec![layer.span];
    }

    let remainder = (layer.span.sweep - selected_target).max(0.0);
    let scale = remainder / other_total;
    let adjusted = base_sweeps
        .iter()
        .enumerate()
        .map(|(index, sweep)| {
            if index == selected_index {
                selected_target
            } else {
                sweep * scale
            }
        })
        .collect::<Vec<_>>();

    segment_spans_from_sweeps(layer.span, &adjusted)
}

fn base_segment_sweeps(layer: &SunburstLayer) -> Vec<f64> {
    let total_weight = if layer.total_value > 0.0 {
        layer.total_value
    } else {
        layer.segments.len() as f64
    };

    layer
        .segments
        .iter()
        .map(|segment| {
            let weight = if layer.total_value > 0.0 {
                segment.value.max(0.0)
            } else {
                1.0
            };
            (weight / total_weight) * layer.span.sweep
        })
        .collect()
}

fn segment_spans_from_sweeps(layer_span: SunburstSpan, sweeps: &[f64]) -> Vec<SunburstSpan> {
    let mut cursor = 0.0;
    sweeps
        .iter()
        .map(|sweep| {
            let span = layer_span.child_span(cursor, *sweep);
            cursor += *sweep;
            span
        })
        .collect()
}
