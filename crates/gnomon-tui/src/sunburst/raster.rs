use std::f64::consts::{FRAC_PI_2, TAU};

use ratatui::buffer::Buffer;
use ratatui::style::{Color, Modifier, Style};

use super::geometry::sunburst_segment_at_angle;
use super::model::{
    SunburstBucket, SunburstModel, SunburstRenderConfig, SunburstRenderMode, SunburstSegment,
};

const SUPPORTED_RENDER_MODES: [SunburstRenderMode; 2] =
    [SunburstRenderMode::Coarse, SunburstRenderMode::Braille];

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
                cell.set_char(sunburst_coarse_glyph(segment));
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
            let mut dots = 0_u32;
            let mut winning_segment: Option<&SunburstSegment> = None;

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

                dots |= bit;
                if winning_segment.is_none_or(|current| !current.is_selected && segment.is_selected)
                {
                    winning_segment = Some(segment);
                }
            }

            if dots == 0 {
                continue;
            }

            let Some(symbol) = char::from_u32(0x2800 + dots) else {
                continue;
            };
            let Some(segment) = winning_segment else {
                continue;
            };

            if let Some(cell) = buf.cell_mut((x, y)) {
                cell.set_style(sunburst_braille_style(segment, focused));
                cell.set_char(symbol);
            }
        }
    }
}

fn sunburst_segment_style(segment: &SunburstSegment, focused: bool) -> Style {
    let mut style = Style::default()
        .bg(sunburst_bucket_color(segment.bucket))
        .fg(Color::Black);

    if !focused || segment.cached_ratio >= 0.7 {
        style = style.add_modifier(Modifier::DIM);
    }

    if segment.is_selected {
        style = style.fg(Color::White).add_modifier(Modifier::BOLD);
        if focused {
            style = style.add_modifier(Modifier::UNDERLINED);
        }
    }

    style
}

fn sunburst_braille_style(segment: &SunburstSegment, focused: bool) -> Style {
    let mut style = Style::default().fg(sunburst_bucket_color(segment.bucket));

    if !focused || segment.cached_ratio >= 0.7 {
        style = style.add_modifier(Modifier::DIM);
    }

    if segment.is_selected {
        style = style.fg(Color::White).add_modifier(Modifier::BOLD);
        if focused {
            style = style.add_modifier(Modifier::UNDERLINED);
        }
    }

    style
}

fn sunburst_coarse_glyph(segment: &SunburstSegment) -> char {
    if segment.is_selected {
        return '#';
    }

    if segment.cached_ratio >= 0.7 {
        '·'
    } else {
        ' '
    }
}

fn sunburst_bucket_color(bucket: SunburstBucket) -> Color {
    match bucket {
        SunburstBucket::Project => Color::LightBlue,
        SunburstBucket::Category => Color::LightCyan,
        SunburstBucket::Classified => Color::LightGreen,
        SunburstBucket::Mixed => Color::LightYellow,
        SunburstBucket::Unclassified => Color::Gray,
    }
}
