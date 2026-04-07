use std::f64::consts::{FRAC_PI_2, TAU};

use ratatui::buffer::Buffer;
use ratatui::style::{Color, Modifier, Style};

use super::geometry::sunburst_segment_at_angle;
use super::model::{
    SunburstBucket, SunburstModel, SunburstRenderConfig, SunburstRenderMode, SunburstSegment,
};
#[cfg(test)]
use super::model::{SunburstLayer, SunburstSpan};

const SUPPORTED_RENDER_MODES: [SunburstRenderMode; 2] =
    [SunburstRenderMode::Coarse, SunburstRenderMode::Braille];

/// Uniform fill glyph for coarse mode — geometry carries the signal, not texture.
const COARSE_FILL: char = '█';

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SampleHit {
    layer_index: usize,
    segment_index: usize,
    sample_bit: u32,
}

pub(crate) fn rasterize_sunburst(
    buf: &mut Buffer,
    inner: ratatui::layout::Rect,
    model: &SunburstModel,
    focused: bool,
    config: SunburstRenderConfig,
) {
    debug_assert!(SUPPORTED_RENDER_MODES.contains(&config.mode));
    match config.mode {
        SunburstRenderMode::Coarse => rasterize_coarse(buf, inner, model, focused, config),
        SunburstRenderMode::Braille => rasterize_braille(buf, inner, model, focused, config),
    }
}

fn rasterize_coarse(
    buf: &mut Buffer,
    inner: ratatui::layout::Rect,
    model: &SunburstModel,
    focused: bool,
    config: SunburstRenderConfig,
) {
    let layer_count = model.layers.len();
    if layer_count == 0 {
        return;
    }

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
            let Some(segment) = sunburst_segment_at_angle(
                layer,
                (normalized_y.atan2(normalized_x) + FRAC_PI_2).rem_euclid(TAU),
                config.distortion_policy,
            ) else {
                continue;
            };

            if let Some(cell) = buf.cell_mut((x, y)) {
                cell.set_style(sunburst_segment_style(segment, focused));
                cell.set_char(COARSE_FILL);
            }
        }
    }
}

fn rasterize_braille(
    buf: &mut Buffer,
    inner: ratatui::layout::Rect,
    model: &SunburstModel,
    focused: bool,
    config: SunburstRenderConfig,
) {
    const DOT_SAMPLES: [(f64, f64, u32); 8] = [
        (0.25, 0.125, 0x01),
        (0.25, 0.375, 0x02),
        (0.25, 0.625, 0x04),
        (0.75, 0.125, 0x08),
        (0.75, 0.375, 0x10),
        (0.75, 0.625, 0x20),
        (0.25, 0.875, 0x40),
        (0.75, 0.875, 0x80),
    ];

    let layer_count = model.layers.len();
    if layer_count == 0 {
        return;
    }

    let center_x = f64::from(inner.x) + f64::from(inner.width) / 2.0;
    let center_y = f64::from(inner.y) + f64::from(inner.height) / 2.0;
    let radius_x = (f64::from(inner.width) / 2.0).max(1.0);
    let radius_y = (f64::from(inner.height) / 2.0).max(1.0);
    let ring_band = (config.outer_radius - config.center_radius) / layer_count as f64;

    for y in inner.y..inner.y + inner.height {
        for x in inner.x..inner.x + inner.width {
            let mut hits = Vec::with_capacity(DOT_SAMPLES.len());

            for (sample_x, sample_y, bit) in DOT_SAMPLES {
                let normalized_x = (f64::from(x) + sample_x - center_x) / radius_x;
                let normalized_y = (f64::from(y) + sample_y - center_y) / radius_y;
                let radius = (normalized_x.powi(2) + normalized_y.powi(2)).sqrt();

                if !(config.center_radius..=config.outer_radius).contains(&radius) {
                    continue;
                }

                let layer_index = ((radius - config.center_radius) / ring_band)
                    .floor()
                    .clamp(0.0, (layer_count - 1) as f64)
                    as usize;
                let Some(layer) = model.layers.get(layer_index) else {
                    continue;
                };
                let Some(segment) = sunburst_segment_at_angle(
                    layer,
                    (normalized_y.atan2(normalized_x) + FRAC_PI_2).rem_euclid(TAU),
                    config.distortion_policy,
                ) else {
                    continue;
                };
                let Some(segment_index) = layer
                    .segments
                    .iter()
                    .position(|candidate| std::ptr::eq(candidate, segment))
                else {
                    continue;
                };

                hits.push(SampleHit {
                    layer_index,
                    segment_index,
                    sample_bit: bit,
                });
            }

            let Some(dominant_layer_index) = dominant_layer_index(&hits) else {
                continue;
            };
            let layer_hits = hits
                .iter()
                .copied()
                .filter(|hit| hit.layer_index == dominant_layer_index)
                .collect::<Vec<_>>();
            let Some(owner) = dominant_segment(&layer_hits, model) else {
                continue;
            };
            let dots = layer_hits
                .iter()
                .fold(0_u32, |bits, hit| bits | hit.sample_bit);
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
                cell.set_style(sunburst_braille_style(segment, focused));
                cell.set_char(symbol);
            }
        }
    }
}

/// Color used to de-emphasize cached or unfocused segments instead of the
/// terminal-dependent `DIM` modifier, which can make braille dots invisible.
const CACHE_DEEMPH_COLOR: Color = Color::Indexed(239);

fn sunburst_segment_style(segment: &SunburstSegment, focused: bool) -> Style {
    let mut style = Style::default()
        .bg(sunburst_bucket_color(segment.bucket))
        .fg(Color::Black);

    if !focused {
        style = style.bg(CACHE_DEEMPH_COLOR);
    }

    if segment.is_selected {
        style = style.fg(Color::White).add_modifier(Modifier::BOLD);
    }

    style
}

fn sunburst_braille_style(segment: &SunburstSegment, focused: bool) -> Style {
    let mut style = Style::default().fg(sunburst_bucket_color(segment.bucket));

    if !focused {
        style = style.fg(CACHE_DEEMPH_COLOR);
    }

    if segment.is_selected {
        style = style.fg(Color::White).add_modifier(Modifier::BOLD);
    }

    style
}

fn dominant_segment(hits: &[SampleHit], model: &SunburstModel) -> Option<SampleHit> {
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
                let left_selected = model
                    .layers
                    .get(*left_layer)
                    .and_then(|layer| layer.segments.get(*left_segment))
                    .is_some_and(|segment| segment.is_selected);
                let right_selected = model
                    .layers
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
            sample_bit: 0,
        })
}

fn dominant_layer_index(hits: &[SampleHit]) -> Option<usize> {
    if hits.is_empty() {
        return None;
    }

    let mut counts = std::collections::BTreeMap::<usize, usize>::new();
    for hit in hits {
        *counts.entry(hit.layer_index).or_default() += 1;
    }

    counts
        .into_iter()
        .max_by(|(left_layer, left_count), (right_layer, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| left_layer.cmp(right_layer))
        })
        .map(|(layer_index, _)| layer_index)
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
        let style = sunburst_segment_style(&seg, true);
        assert!(style.add_modifier.contains(Modifier::BOLD));
        assert!(!style.add_modifier.contains(Modifier::UNDERLINED));
    }

    #[test]
    fn braille_style_no_underline_when_selected() {
        let seg = make_segment(SunburstBucket::Project, true);
        let style = sunburst_braille_style(&seg, true);
        assert!(style.add_modifier.contains(Modifier::BOLD));
        assert!(!style.add_modifier.contains(Modifier::UNDERLINED));
    }

    #[test]
    fn segment_style_bucket_color_when_focused_and_high_cached_ratio() {
        // cached_ratio no longer affects color — bucket color always shows when focused
        let seg = make_segment(SunburstBucket::Classified, false);
        let style = sunburst_segment_style(&seg, true);
        assert_eq!(
            style.bg,
            Some(sunburst_bucket_color(SunburstBucket::Classified))
        );
    }

    #[test]
    fn segment_style_deemphasized_when_unfocused() {
        let seg = make_segment(SunburstBucket::Classified, false);
        let style = sunburst_segment_style(&seg, false);
        assert_eq!(style.bg, Some(CACHE_DEEMPH_COLOR));
    }

    #[test]
    fn segment_style_bucket_color_when_focused_and_uncached() {
        let seg = make_segment(SunburstBucket::Classified, false);
        let style = sunburst_segment_style(&seg, true);
        assert_eq!(
            style.bg,
            Some(sunburst_bucket_color(SunburstBucket::Classified))
        );
    }

    #[test]
    fn braille_style_bucket_color_when_focused_and_high_cached_ratio() {
        // cached_ratio no longer affects color — bucket color always shows when focused
        let seg = make_segment(SunburstBucket::Category, false);
        let style = sunburst_braille_style(&seg, true);
        assert_eq!(
            style.fg,
            Some(sunburst_bucket_color(SunburstBucket::Category))
        );
    }

    #[test]
    fn braille_style_deemphasized_when_unfocused() {
        let seg = make_segment(SunburstBucket::Category, false);
        let style = sunburst_braille_style(&seg, false);
        assert_eq!(style.fg, Some(CACHE_DEEMPH_COLOR));
    }

    #[test]
    fn braille_style_selected_overrides_fg_to_white() {
        let seg = make_segment(SunburstBucket::Mixed, true);
        let style = sunburst_braille_style(&seg, true);
        assert_eq!(style.fg, Some(Color::White));
    }

    #[test]
    fn segment_style_selected_overrides_fg_to_white() {
        let seg = make_segment(SunburstBucket::Mixed, true);
        let style = sunburst_segment_style(&seg, true);
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
    fn dominant_layer_prefers_majority_hits_over_inner_selected_layer() {
        let hits = vec![
            SampleHit {
                layer_index: 0,
                segment_index: 0,
                sample_bit: 0x01,
            },
            SampleHit {
                layer_index: 1,
                segment_index: 0,
                sample_bit: 0x02,
            },
            SampleHit {
                layer_index: 1,
                segment_index: 1,
                sample_bit: 0x04,
            },
        ];

        assert_eq!(dominant_layer_index(&hits), Some(1));
    }

    #[test]
    fn dominant_segment_prefers_selected_segment_when_counts_tie_within_layer() {
        let model = SunburstModel {
            center: Default::default(),
            layers: vec![make_layer(
                &[
                    (50.0, SunburstBucket::Project, false),
                    (50.0, SunburstBucket::Category, true),
                ],
                SunburstSpan::full(),
                100.0,
            )],
        };
        let hits = vec![
            SampleHit {
                layer_index: 0,
                segment_index: 0,
                sample_bit: 0x01,
            },
            SampleHit {
                layer_index: 0,
                segment_index: 1,
                sample_bit: 0x02,
            },
        ];

        let owner = dominant_segment(&hits, &model).expect("winner");
        assert_eq!(owner.layer_index, 0);
        assert_eq!(owner.segment_index, 1);
    }

    #[test]
    fn braille_uses_only_dots_from_the_dominant_ring_layer() {
        let model = SunburstModel {
            center: Default::default(),
            layers: vec![
                make_layer(
                    &[(100.0, SunburstBucket::Project, true)],
                    SunburstSpan::full(),
                    100.0,
                ),
                make_layer(
                    &[
                        (50.0, SunburstBucket::Category, false),
                        (50.0, SunburstBucket::Classified, false),
                    ],
                    SunburstSpan {
                        start: 0.0,
                        sweep: TAU / 6.0,
                    },
                    100.0,
                ),
            ],
        };
        let hits = vec![
            SampleHit {
                layer_index: 0,
                segment_index: 0,
                sample_bit: 0x01,
            },
            SampleHit {
                layer_index: 1,
                segment_index: 0,
                sample_bit: 0x02,
            },
            SampleHit {
                layer_index: 1,
                segment_index: 1,
                sample_bit: 0x04,
            },
        ];

        let dominant_layer = dominant_layer_index(&hits).expect("dominant layer");
        let layer_hits = hits
            .iter()
            .copied()
            .filter(|hit| hit.layer_index == dominant_layer)
            .collect::<Vec<_>>();
        let owner = dominant_segment(&layer_hits, &model).expect("dominant owner");
        let dots = layer_hits
            .iter()
            .fold(0_u32, |bits, hit| bits | hit.sample_bit);

        assert_eq!(dominant_layer, 1);
        assert_eq!(owner.layer_index, 1);
        assert_eq!(dots, 0x06);
    }
}
