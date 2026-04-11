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
    _policy: SunburstDistortionPolicy,
) -> SunburstSpan {
    if layer.segments.is_empty() {
        return layer.span;
    }

    // Use base (undistorted) spans so the descendant ring occupies a fixed
    // angular range that depends only on data proportions, not which segment
    // is selected.  Distortion is still applied during rendering of the
    // ancestor ring, but the child span must be stable across selections
    // to prevent overlapping arcs.
    let base_sweeps = base_segment_sweeps(layer);
    for (segment, span) in layer
        .segments
        .iter()
        .zip(segment_spans_from_sweeps(layer.span, &base_sweeps))
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
    _policy: SunburstDistortionPolicy,
) -> Vec<SunburstSpan> {
    segment_spans_from_sweeps(layer.span, &base_segment_sweeps(layer))
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
