use std::f64::consts::{FRAC_PI_2, TAU};

use ratatui::buffer::Buffer;
use ratatui::style::{Color, Modifier, Style};

use super::geometry::sunburst_segment_at_angle;
use super::model::{
    SunburstBucket, SunburstLayer, SunburstModel, SunburstRenderConfig, SunburstRenderMode,
    SunburstSegment,
};

const SUPPORTED_RENDER_MODES: [SunburstRenderMode; 2] =
    [SunburstRenderMode::Coarse, SunburstRenderMode::Braille];

/// Uniform fill glyph for coarse mode — geometry carries the signal, not texture.
const COARSE_FILL: char = '█';

const BRAILLE_DOT_SAMPLES: [(f64, f64, u32); 8] = [
    (0.25, 0.125, 0x01),
    (0.25, 0.375, 0x02),
    (0.25, 0.625, 0x04),
    (0.75, 0.125, 0x08),
    (0.75, 0.375, 0x10),
    (0.75, 0.625, 0x20),
    (0.25, 0.875, 0x40),
    (0.75, 0.875, 0x80),
];

#[derive(Debug, Clone)]
struct QuantizedLayer {
    slot_owners: Vec<Option<usize>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SampleHit {
    layer_index: usize,
    segment_index: usize,
    dot_bit: u32,
}

struct BrailleRasterContext<'a> {
    center_x: f64,
    center_y: f64,
    radius_x: f64,
    radius_y: f64,
    ring_band: f64,
    model: &'a SunburstModel,
    quantized_layers: &'a [QuantizedLayer],
    config: SunburstRenderConfig,
}

pub(crate) fn rasterize_sunburst(
    buf: &mut Buffer,
    inner: ratatui::layout::Rect,
    model: &SunburstModel,
    config: SunburstRenderConfig,
) {
    debug_assert!(SUPPORTED_RENDER_MODES.contains(&config.mode));
    match config.mode {
        SunburstRenderMode::Coarse => rasterize_coarse(buf, inner, model, config),
        SunburstRenderMode::Braille => rasterize_braille(buf, inner, model, config),
    }
}

fn rasterize_coarse(
    buf: &mut Buffer,
    inner: ratatui::layout::Rect,
    model: &SunburstModel,
    config: SunburstRenderConfig,
) {
    let layer_count = model.layers.len();
    if layer_count == 0 {
        return;
    }
    let quantized_layers = quantize_layers(inner, model, config);

    let center_x = f64::from(inner.x) + f64::from(inner.width) / 2.0;
    let center_y = f64::from(inner.y) + f64::from(inner.height) / 2.0;
    let radius_x = (f64::from(inner.width) / 2.0).max(1.0);
    let radius_y = (f64::from(inner.height) / 2.0).max(1.0);
    let ring_band = (config.outer_radius - config.center_radius) / layer_count as f64;

    for y in inner.y..inner.y + inner.height {
        for x in inner.x..inner.x + inner.width {
            let normalized_x = (f64::from(x) + 0.5 - center_x) / radius_x;
            let normalized_y = (f64::from(y) + 0.5 - center_y) / radius_y;
            let radius = (normalized_x.powi(2) + normalized_y.powi(2)).sqrt();

            if !(config.center_radius..=config.outer_radius).contains(&radius) {
                continue;
            }

            let layer_index = ((radius - config.center_radius) / ring_band)
                .floor()
                .clamp(0.0, (layer_count - 1) as f64) as usize;
            let Some(layer) = model.layers.get(layer_index) else {
                continue;
            };
            let Some(quantized_layer) = quantized_layers.get(layer_index) else {
                continue;
            };
            let angle = (normalized_y.atan2(normalized_x) + FRAC_PI_2).rem_euclid(TAU);
            let Some(segment_index) =
                quantized_segment_index(layer, quantized_layer, angle, config)
            else {
                continue;
            };
            let Some(segment) = layer.segments.get(segment_index) else {
                continue;
            };

            if let Some(cell) = buf.cell_mut((x, y)) {
                cell.set_style(sunburst_segment_style(segment));
                cell.set_char(COARSE_FILL);
            }
        }
    }
}

fn rasterize_braille(
    buf: &mut Buffer,
    inner: ratatui::layout::Rect,
    model: &SunburstModel,
    config: SunburstRenderConfig,
) {
    let layer_count = model.layers.len();
    if layer_count == 0 {
        return;
    }
    let quantized_layers = quantize_layers(inner, model, config);

    let center_x = f64::from(inner.x) + f64::from(inner.width) / 2.0;
    let center_y = f64::from(inner.y) + f64::from(inner.height) / 2.0;
    let radius_x = (f64::from(inner.width) / 2.0).max(1.0);
    let radius_y = (f64::from(inner.height) / 2.0).max(1.0);
    let ring_band = (config.outer_radius - config.center_radius) / layer_count as f64;
    let raster_context = BrailleRasterContext {
        center_x,
        center_y,
        radius_x,
        radius_y,
        ring_band,
        model,
        quantized_layers: &quantized_layers,
        config,
    };

    for y in inner.y..inner.y + inner.height {
        for x in inner.x..inner.x + inner.width {
            let hits = braille_cell_hits(x, y, &raster_context);

            let Some(owner) = winning_braille_segment(&hits, &model.layers) else {
                continue;
            };
            let dots = hits
                .iter()
                .filter(|hit| {
                    hit.layer_index == owner.layer_index && hit.segment_index == owner.segment_index
                })
                .fold(0_u32, |bits, hit| bits | hit.dot_bit);
            if dots == 0 {
                continue;
            }
            let Some(symbol) = char::from_u32(0x2800 + dots) else {
                continue;
            };
            let Some(segment) = model
                .layers
                .get(owner.layer_index)
                .and_then(|layer| layer.segments.get(owner.segment_index))
            else {
                continue;
            };

            if let Some(cell) = buf.cell_mut((x, y)) {
                cell.set_style(sunburst_braille_style(segment));
                cell.set_char(symbol);
            }
        }
    }
}

fn sunburst_segment_style(segment: &SunburstSegment) -> Style {
    let mut style = Style::default()
        .bg(sunburst_bucket_color(segment.bucket))
        .fg(Color::Black);

    if segment.is_selected {
        style = style.fg(Color::White).add_modifier(Modifier::BOLD);
    }

    style
}

fn braille_cell_hits(x: u16, y: u16, context: &BrailleRasterContext<'_>) -> Vec<SampleHit> {
    let layer_count = context.model.layers.len();
    let mut hits = Vec::with_capacity(BRAILLE_DOT_SAMPLES.len());

    for (sample_x, sample_y, bit) in BRAILLE_DOT_SAMPLES {
        let normalized_x = (f64::from(x) + sample_x - context.center_x) / context.radius_x;
        let normalized_y = (f64::from(y) + sample_y - context.center_y) / context.radius_y;
        let radius = (normalized_x.powi(2) + normalized_y.powi(2)).sqrt();

        if !(context.config.center_radius..=context.config.outer_radius).contains(&radius) {
            continue;
        }

        let layer_index = ((radius - context.config.center_radius) / context.ring_band)
            .floor()
            .clamp(0.0, (layer_count - 1) as f64) as usize;
        let Some(layer) = context.model.layers.get(layer_index) else {
            continue;
        };
        let Some(quantized_layer) = context.quantized_layers.get(layer_index) else {
            continue;
        };
        let angle = (normalized_y.atan2(normalized_x) + FRAC_PI_2).rem_euclid(TAU);
        let Some(segment_index) =
            quantized_segment_index(layer, quantized_layer, angle, context.config)
        else {
            continue;
        };

        hits.push(SampleHit {
            layer_index,
            segment_index,
            dot_bit: bit,
        });
    }

    hits
}

fn quantize_layers(
    inner: ratatui::layout::Rect,
    model: &SunburstModel,
    config: SunburstRenderConfig,
) -> Vec<QuantizedLayer> {
    let layer_count = model.layers.len();
    model
        .layers
        .iter()
        .enumerate()
        .map(|(layer_index, layer)| {
            let slot_count = angular_slot_count(inner, config, layer_index, layer_count);
            QuantizedLayer {
                slot_owners: quantize_layer_owners(layer, config, slot_count),
            }
        })
        .collect()
}

fn angular_slot_count(
    inner: ratatui::layout::Rect,
    config: SunburstRenderConfig,
    layer_index: usize,
    layer_count: usize,
) -> usize {
    if layer_count == 0 {
        return 0;
    }

    let terminal_radius = f64::from(inner.width.min(inner.height)) / 2.0;
    let ring_band = (config.outer_radius - config.center_radius) / layer_count as f64;
    let layer_midpoint = config.center_radius + ring_band * (layer_index as f64 + 0.5);
    let circumference = (TAU * terminal_radius * layer_midpoint).max(1.0);
    let density = match config.mode {
        SunburstRenderMode::Coarse => 1.0,
        SunburstRenderMode::Braille => 2.0,
    };

    (circumference * density).round().max(1.0) as usize
}

fn quantize_layer_owners(
    layer: &SunburstLayer,
    config: SunburstRenderConfig,
    slot_count: usize,
) -> Vec<Option<usize>> {
    if layer.segments.is_empty() || slot_count == 0 || layer.span.sweep <= 0.0 {
        return Vec::new();
    }

    let ideal_sweeps = display_segment_sweeps(layer, config);
    let allocations = quantize_segment_slots(&ideal_sweeps, layer, slot_count);
    allocations
        .into_iter()
        .enumerate()
        .flat_map(|(segment_index, count)| std::iter::repeat_n(Some(segment_index), count))
        .collect()
}

fn display_segment_sweeps(layer: &SunburstLayer, config: SunburstRenderConfig) -> Vec<f64> {
    let base_sweeps = base_segment_sweeps(layer);
    let Some(selected_index) = layer
        .segments
        .iter()
        .position(|segment| segment.is_selected)
    else {
        return base_sweeps;
    };

    let policy = config.distortion_policy;
    let selected_base = base_sweeps[selected_index];
    let selected_threshold = layer.span.sweep * policy.focus_zoom_threshold_ratio;
    if selected_base >= selected_threshold && selected_base >= policy.minimum_visible_sweep {
        return base_sweeps;
    }

    let selected_target = (selected_base * policy.focus_zoom_multiplier)
        .max(policy.minimum_visible_sweep)
        .min(layer.span.sweep * policy.maximum_selected_share)
        .min(layer.span.sweep);
    let other_total = layer.span.sweep - selected_base;
    if other_total <= 0.0 {
        return vec![layer.span.sweep];
    }

    let remainder = (layer.span.sweep - selected_target).max(0.0);
    let scale = remainder / other_total;
    base_sweeps
        .into_iter()
        .enumerate()
        .map(|(index, sweep)| {
            if index == selected_index {
                selected_target
            } else {
                sweep * scale
            }
        })
        .collect()
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

fn quantize_segment_slots(
    ideal_sweeps: &[f64],
    layer: &SunburstLayer,
    slot_count: usize,
) -> Vec<usize> {
    let segment_count = layer.segments.len();
    if segment_count == 0 || slot_count == 0 {
        return vec![0; segment_count];
    }

    let total_sweep = layer.span.sweep.max(f64::EPSILON);
    let desired = ideal_sweeps
        .iter()
        .map(|sweep| ((*sweep / total_sweep) * slot_count as f64).max(0.0))
        .collect::<Vec<_>>();
    let selected_index = layer
        .segments
        .iter()
        .position(|segment| segment.is_selected);
    let visible_count = ideal_sweeps.iter().filter(|sweep| **sweep > 0.0).count();

    if slot_count < visible_count {
        let mut rankings = desired
            .iter()
            .enumerate()
            .filter(|(index, _)| ideal_sweeps[*index] > 0.0)
            .map(|(index, desired)| {
                let selected_rank = usize::from(Some(index) != selected_index);
                (selected_rank, -*desired, index)
            })
            .collect::<Vec<_>>();
        rankings.sort_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| {
                left.1
                    .partial_cmp(&right.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
        });
        let mut allocations = vec![0; segment_count];
        for (_, _, index) in rankings.into_iter().take(slot_count) {
            allocations[index] = 1;
        }
        return allocations;
    }

    let minimums = ideal_sweeps
        .iter()
        .enumerate()
        .map(|(index, sweep)| usize::from(*sweep > 0.0 || Some(index) == selected_index))
        .collect::<Vec<_>>();
    let mut allocations = desired
        .iter()
        .zip(minimums.iter())
        .map(|(desired, minimum)| desired.floor().max(*minimum as f64) as usize)
        .collect::<Vec<_>>();

    rebalance_allocations(
        &mut allocations,
        &desired,
        &minimums,
        selected_index,
        slot_count,
    );
    allocations
}

fn rebalance_allocations(
    allocations: &mut [usize],
    desired: &[f64],
    minimums: &[usize],
    selected_index: Option<usize>,
    slot_count: usize,
) {
    while allocations.iter().sum::<usize>() > slot_count {
        let Some((index, _)) = allocations
            .iter()
            .enumerate()
            .filter(|(index, allocation)| {
                **allocation > minimums[*index] && Some(*index) != selected_index
            })
            .max_by(
                |(left_index, left_allocation), (right_index, right_allocation)| {
                    let left_excess = **left_allocation as f64 - desired[*left_index];
                    let right_excess = **right_allocation as f64 - desired[*right_index];
                    left_excess
                        .partial_cmp(&right_excess)
                        .unwrap_or(std::cmp::Ordering::Equal)
                },
            )
            .or_else(|| {
                allocations
                    .iter()
                    .enumerate()
                    .filter(|(index, allocation)| **allocation > minimums[*index])
                    .max_by_key(|(_, allocation)| **allocation)
            })
        else {
            break;
        };
        allocations[index] -= 1;
    }

    while allocations.iter().sum::<usize>() < slot_count {
        let Some((index, _)) = allocations.iter().enumerate().max_by(
            |(left_index, left_allocation), (right_index, right_allocation)| {
                let left_remainder = desired[*left_index] - **left_allocation as f64;
                let right_remainder = desired[*right_index] - **right_allocation as f64;
                left_remainder
                    .partial_cmp(&right_remainder)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| Some(*left_index).cmp(&Some(*right_index)))
            },
        ) else {
            break;
        };
        allocations[index] += 1;
    }
}

fn quantized_segment_index(
    layer: &SunburstLayer,
    quantized_layer: &QuantizedLayer,
    angle: f64,
    config: SunburstRenderConfig,
) -> Option<usize> {
    let slot_count = quantized_layer.slot_owners.len();
    if slot_count == 0 {
        return sunburst_segment_at_angle(layer, angle, config.distortion_policy).and_then(
            |segment| {
                layer
                    .segments
                    .iter()
                    .position(|candidate| std::ptr::eq(candidate, segment))
            },
        );
    }

    let local_angle = layer.span.local_angle(angle)?;
    let slot_index = ((local_angle / layer.span.sweep) * slot_count as f64)
        .floor()
        .clamp(0.0, (slot_count - 1) as f64) as usize;
    quantized_layer
        .slot_owners
        .get(slot_index)
        .copied()
        .flatten()
}

fn winning_braille_segment(hits: &[SampleHit], layers: &[SunburstLayer]) -> Option<SampleHit> {
    if hits.is_empty() {
        return None;
    }

    let mut counts = std::collections::BTreeMap::<(usize, usize), usize>::new();
    for hit in hits {
        *counts
            .entry((hit.layer_index, hit.segment_index))
            .or_default() += 1;
    }

    counts
        .into_iter()
        .max_by(
            |((left_layer, left_segment), left_count),
             ((right_layer, right_segment), right_count)| {
                let left_selected = layers
                    .get(*left_layer)
                    .and_then(|layer| layer.segments.get(*left_segment))
                    .is_some_and(|segment| segment.is_selected);
                let right_selected = layers
                    .get(*right_layer)
                    .and_then(|layer| layer.segments.get(*right_segment))
                    .is_some_and(|segment| segment.is_selected);
                left_selected
                    .cmp(&right_selected)
                    .then_with(|| left_count.cmp(right_count))
                    .then_with(|| left_layer.cmp(right_layer))
                    .then_with(|| left_segment.cmp(right_segment))
            },
        )
        .map(|((layer_index, segment_index), _)| SampleHit {
            layer_index,
            segment_index,
            dot_bit: 0,
        })
}

fn sunburst_braille_style(segment: &SunburstSegment) -> Style {
    let mut style = Style::default().fg(sunburst_bucket_color(segment.bucket));

    if segment.is_selected {
        style = style.fg(Color::White).add_modifier(Modifier::BOLD);
    }

    style
}

/// Muted ANSI-256 palette — geometry carries the primary signal, color stays
/// secondary.
fn sunburst_bucket_color(bucket: SunburstBucket) -> Color {
    match bucket {
        SunburstBucket::Project => Color::Indexed(67), // steel blue
        SunburstBucket::Category => Color::Indexed(73), // muted teal
        SunburstBucket::Classified => Color::Indexed(107), // sage green
        SunburstBucket::Mixed => Color::Indexed(179),  // muted gold
        SunburstBucket::Unclassified => Color::Indexed(243), // mid gray
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sunburst::SunburstSpan;

    fn make_segment(bucket: SunburstBucket, is_selected: bool) -> SunburstSegment {
        SunburstSegment {
            value: 100.0,
            bucket,
            is_selected,
        }
    }

    fn make_layer(
        values: &[(f64, SunburstBucket, bool)],
        span: SunburstSpan,
        total_value: f64,
    ) -> SunburstLayer {
        SunburstLayer {
            span,
            segments: values
                .iter()
                .map(|(value, bucket, is_selected)| SunburstSegment {
                    value: *value,
                    bucket: *bucket,
                    is_selected: *is_selected,
                })
                .collect(),
            total_value,
        }
    }

    #[test]
    fn bucket_colors_use_muted_indexed_palette() {
        assert_eq!(
            sunburst_bucket_color(SunburstBucket::Project),
            Color::Indexed(67)
        );
        assert_eq!(
            sunburst_bucket_color(SunburstBucket::Category),
            Color::Indexed(73)
        );
        assert_eq!(
            sunburst_bucket_color(SunburstBucket::Classified),
            Color::Indexed(107)
        );
        assert_eq!(
            sunburst_bucket_color(SunburstBucket::Mixed),
            Color::Indexed(179)
        );
        assert_eq!(
            sunburst_bucket_color(SunburstBucket::Unclassified),
            Color::Indexed(243)
        );
    }

    #[test]
    fn coarse_fill_is_full_block() {
        assert_eq!(COARSE_FILL, '█');
    }

    #[test]
    fn segment_style_no_underline_when_selected() {
        let seg = make_segment(SunburstBucket::Project, true);
        let style = sunburst_segment_style(&seg);
        assert!(style.add_modifier.contains(Modifier::BOLD));
        assert!(!style.add_modifier.contains(Modifier::UNDERLINED));
    }

    #[test]
    fn braille_style_no_underline_when_selected() {
        let seg = make_segment(SunburstBucket::Project, true);
        let style = sunburst_braille_style(&seg);
        assert!(style.add_modifier.contains(Modifier::BOLD));
        assert!(!style.add_modifier.contains(Modifier::UNDERLINED));
    }

    #[test]
    fn segment_style_bucket_color_when_focused_and_high_cached_ratio() {
        // cached_ratio no longer affects color — bucket color always shows when focused
        let seg = make_segment(SunburstBucket::Classified, false);
        let style = sunburst_segment_style(&seg);
        assert_eq!(
            style.bg,
            Some(sunburst_bucket_color(SunburstBucket::Classified))
        );
    }

    #[test]
    fn segment_style_preserves_bucket_color_when_unfocused() {
        let seg = make_segment(SunburstBucket::Classified, false);
        let style = sunburst_segment_style(&seg);
        assert_eq!(
            style.bg,
            Some(sunburst_bucket_color(SunburstBucket::Classified))
        );
    }

    #[test]
    fn segment_style_bucket_color_when_focused_and_uncached() {
        let seg = make_segment(SunburstBucket::Classified, false);
        let style = sunburst_segment_style(&seg);
        assert_eq!(
            style.bg,
            Some(sunburst_bucket_color(SunburstBucket::Classified))
        );
    }

    #[test]
    fn braille_style_bucket_color_when_focused_and_high_cached_ratio() {
        // cached_ratio no longer affects color — bucket color always shows when focused
        let seg = make_segment(SunburstBucket::Category, false);
        let style = sunburst_braille_style(&seg);
        assert_eq!(
            style.fg,
            Some(sunburst_bucket_color(SunburstBucket::Category))
        );
    }

    #[test]
    fn braille_style_preserves_bucket_color_when_unfocused() {
        let seg = make_segment(SunburstBucket::Category, false);
        let style = sunburst_braille_style(&seg);
        assert_eq!(
            style.fg,
            Some(sunburst_bucket_color(SunburstBucket::Category))
        );
    }

    #[test]
    fn braille_style_selected_overrides_fg_to_white() {
        let seg = make_segment(SunburstBucket::Mixed, true);
        let style = sunburst_braille_style(&seg);
        assert_eq!(style.fg, Some(Color::White));
    }

    #[test]
    fn segment_style_selected_overrides_fg_to_white() {
        let seg = make_segment(SunburstBucket::Mixed, true);
        let style = sunburst_segment_style(&seg);
        assert_eq!(style.fg, Some(Color::White));
        assert_eq!(style.bg, Some(Color::Indexed(179)));
    }

    #[test]
    fn default_render_mode_is_braille() {
        assert_eq!(
            SunburstRenderConfig::default().mode,
            SunburstRenderMode::Braille
        );
    }

    #[test]
    fn quantization_gives_each_visible_segment_a_disjoint_slot_when_capacity_allows() {
        let layer = make_layer(
            &[
                (96.0, SunburstBucket::Project, false),
                (2.0, SunburstBucket::Category, false),
                (2.0, SunburstBucket::Classified, false),
            ],
            SunburstSpan::full(),
            100.0,
        );

        let owners = quantize_layer_owners(&layer, SunburstRenderConfig::default(), 8);
        let mut seen = std::collections::BTreeSet::new();
        for owner in owners.into_iter().flatten() {
            seen.insert(owner);
        }

        assert_eq!(seen, std::collections::BTreeSet::from([0, 1, 2]));
    }

    #[test]
    fn quantization_keeps_selected_segment_visible_when_slots_are_scarce() {
        let layer = make_layer(
            &[
                (60.0, SunburstBucket::Project, false),
                (30.0, SunburstBucket::Category, false),
                (10.0, SunburstBucket::Mixed, true),
            ],
            SunburstSpan::full(),
            100.0,
        );

        let owners = quantize_layer_owners(&layer, SunburstRenderConfig::default(), 2);
        assert!(owners.contains(&Some(2)));
    }

    #[test]
    fn braille_winner_prefers_selected_segment_when_cell_contains_multiple_segments() {
        let layers = vec![make_layer(
            &[
                (50.0, SunburstBucket::Project, false),
                (50.0, SunburstBucket::Category, true),
            ],
            SunburstSpan::full(),
            100.0,
        )];
        let hits = vec![
            SampleHit {
                layer_index: 0,
                segment_index: 0,
                dot_bit: 0x01,
            },
            SampleHit {
                layer_index: 0,
                segment_index: 0,
                dot_bit: 0x02,
            },
            SampleHit {
                layer_index: 0,
                segment_index: 1,
                dot_bit: 0x04,
            },
        ];

        let owner = winning_braille_segment(&hits, &layers).expect("winner");
        assert_eq!(owner.layer_index, 0);
        assert_eq!(owner.segment_index, 1);
    }

    #[test]
    fn rendered_braille_cell_uses_only_winning_segments_dots_in_narrow_selected_child_span() {
        let root_layer = make_layer(
            &[
                (90.0, SunburstBucket::Project, false),
                (10.0, SunburstBucket::Category, true),
            ],
            SunburstSpan::full(),
            100.0,
        );
        let child_layer = make_layer(
            &[
                (1.0, SunburstBucket::Project, true),
                (1.0, SunburstBucket::Category, false),
                (1.0, SunburstBucket::Classified, false),
                (1.0, SunburstBucket::Mixed, false),
            ],
            SunburstSpan {
                start: 0.0,
                sweep: TAU / 12.0,
            },
            4.0,
        );
        let model = SunburstModel {
            center: Default::default(),
            layers: vec![root_layer, child_layer],
        };
        let config = SunburstRenderConfig {
            mode: SunburstRenderMode::Braille,
            ..SunburstRenderConfig::default()
        };
        let inner = ratatui::layout::Rect::new(0, 0, 28, 14);
        let quantized_layers = quantize_layers(inner, &model, config);
        let center_x = f64::from(inner.x) + f64::from(inner.width) / 2.0;
        let center_y = f64::from(inner.y) + f64::from(inner.height) / 2.0;
        let radius_x = (f64::from(inner.width) / 2.0).max(1.0);
        let radius_y = (f64::from(inner.height) / 2.0).max(1.0);
        let ring_band = (config.outer_radius - config.center_radius) / model.layers.len() as f64;
        let raster_context = BrailleRasterContext {
            center_x,
            center_y,
            radius_x,
            radius_y,
            ring_band,
            model: &model,
            quantized_layers: &quantized_layers,
            config,
        };

        let mixed_cell = (inner.y..inner.y + inner.height)
            .flat_map(|y| (inner.x..inner.x + inner.width).map(move |x| (x, y)))
            .find_map(|(x, y)| {
                let hits = braille_cell_hits(x, y, &raster_context);
                let owners = hits
                    .iter()
                    .map(|hit| (hit.layer_index, hit.segment_index))
                    .collect::<std::collections::BTreeSet<_>>();
                (owners.len() > 1).then_some((x, y, hits))
            })
            .expect("expected at least one braille cell to sample multiple slice owners");

        let owner = winning_braille_segment(&mixed_cell.2, &model.layers).expect("winner");
        let expected_dots = mixed_cell
            .2
            .iter()
            .filter(|hit| {
                hit.layer_index == owner.layer_index && hit.segment_index == owner.segment_index
            })
            .fold(0_u32, |bits, hit| bits | hit.dot_bit);
        let all_dots = mixed_cell
            .2
            .iter()
            .fold(0_u32, |bits, hit| bits | hit.dot_bit);
        assert_ne!(
            expected_dots, all_dots,
            "the mixed cell must include losing-owner dots for this regression to matter"
        );

        let mut buf = Buffer::empty(inner);
        rasterize_sunburst(&mut buf, inner, &model, config);
        let symbol = buf
            .cell((mixed_cell.0, mixed_cell.1))
            .expect("rendered cell")
            .symbol()
            .chars()
            .next()
            .expect("braille symbol");
        let actual_dots = u32::from(symbol).saturating_sub(0x2800);

        assert_eq!(
            actual_dots, expected_dots,
            "the rendered cell should keep only the winning segment's dots"
        );
    }
}
