use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::widgets::Widget;

use super::model::{
    SunburstBucket, SunburstCenter, SunburstLayer, SunburstModel, SunburstRenderConfig,
    SunburstRenderMode, SunburstSegment, SunburstSpan,
};
use super::render::SunburstPane;

const WIDTH: u16 = 40;
const HEIGHT: u16 = 20;

/// Serialize a rendered Buffer to a string that encodes both characters and
/// foreground color changes.  When the foreground color changes from the
/// previous cell, a tag like `[67]` is emitted before the character.  This
/// keeps snapshots compact while capturing which bucket owns each pixel.
fn buffer_to_color_string(buf: &Buffer, area: Rect) -> String {
    let mut out = String::new();
    let mut prev_fg: Option<Color>;

    for y in area.y..area.y + area.height {
        if y > area.y {
            out.push('\n');
        }
        // Reset color tracking at each line for readability.
        prev_fg = None;
        for x in area.x..area.x + area.width {
            let cell = &buf[(x, y)];
            let fg = cell.fg;

            // Emit a color tag when the foreground changes.
            if Some(fg) != prev_fg {
                match fg {
                    Color::Indexed(idx) => {
                        out.push_str(&format!("[{idx}]"));
                    }
                    Color::Reset => out.push_str("[_]"),
                    Color::Black => out.push_str("[blk]"),
                    Color::White => out.push_str("[wht]"),
                    Color::Gray => out.push_str("[gry]"),
                    _ => out.push_str(&format!("[{fg:?}]")),
                }
                prev_fg = Some(fg);
            }

            out.push_str(cell.symbol());
        }
    }
    out
}

fn render_fixture(
    model: &SunburstModel,
    width: u16,
    height: u16,
    mode: SunburstRenderMode,
) -> String {
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);

    let config = SunburstRenderConfig {
        mode,
        ..SunburstRenderConfig::default()
    };
    let pane = SunburstPane {
        model,
        focused: false,
        config,
    };
    (&pane).render(area, &mut buf);
    buffer_to_color_string(&buf, area)
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn default_center() -> SunburstCenter {
    SunburstCenter {
        scope_label: String::new(),
        lens_label: String::new(),
        selection_label: String::new(),
    }
}

fn make_segment(value: f64, bucket: SunburstBucket, selected: bool) -> SunburstSegment {
    SunburstSegment {
        value,
        bucket,
        is_selected: selected,
    }
}

/// Fixture 1: single layer, no selection.
fn single_layer_no_selection() -> SunburstModel {
    let segments = vec![
        make_segment(60.0, SunburstBucket::Project, false),
        make_segment(30.0, SunburstBucket::Category, false),
        make_segment(10.0, SunburstBucket::Classified, false),
    ];
    let total: f64 = segments.iter().map(|s| s.value).sum();
    SunburstModel {
        center: default_center(),
        layers: vec![SunburstLayer {
            span: SunburstSpan::full(),
            segments,
            total_value: total,
        }],
    }
}

/// Fixture 2: single layer, one segment selected.
fn single_layer_selected() -> SunburstModel {
    let segments = vec![
        make_segment(60.0, SunburstBucket::Project, true),
        make_segment(30.0, SunburstBucket::Category, false),
        make_segment(10.0, SunburstBucket::Classified, false),
    ];
    let total: f64 = segments.iter().map(|s| s.value).sum();
    SunburstModel {
        center: default_center(),
        layers: vec![SunburstLayer {
            span: SunburstSpan::full(),
            segments,
            total_value: total,
        }],
    }
}

/// Fixture 3: two layers, selection in layer 0 — the pattern that triggered
/// the overlap bug (ancestor + descendant).
fn two_layers_selection_in_layer0() -> SunburstModel {
    let layer0_segments = vec![
        make_segment(60.0, SunburstBucket::Project, true),
        make_segment(30.0, SunburstBucket::Category, false),
        make_segment(10.0, SunburstBucket::Classified, false),
    ];
    let layer0_total: f64 = layer0_segments.iter().map(|s| s.value).sum();

    // Layer 1: children of the selected segment in layer 0.
    let selected_ratio = 60.0 / layer0_total;
    let child_span = SunburstSpan::full().child_span(0.0, std::f64::consts::TAU * selected_ratio);
    let layer1_segments = vec![
        make_segment(40.0, SunburstBucket::Mixed, false),
        make_segment(20.0, SunburstBucket::Unclassified, false),
    ];
    let layer1_total: f64 = layer1_segments.iter().map(|s| s.value).sum();

    SunburstModel {
        center: default_center(),
        layers: vec![
            SunburstLayer {
                span: SunburstSpan::full(),
                segments: layer0_segments,
                total_value: layer0_total,
            },
            SunburstLayer {
                span: child_span,
                segments: layer1_segments,
                total_value: layer1_total,
            },
        ],
    }
}

/// Fixture 4: two layers, no selection.
fn two_layers_no_selection() -> SunburstModel {
    let layer0_segments = vec![
        make_segment(50.0, SunburstBucket::Project, false),
        make_segment(50.0, SunburstBucket::Category, false),
    ];

    let child_span = SunburstSpan::full().child_span(0.0, std::f64::consts::TAU * 0.5);
    let layer1_segments = vec![
        make_segment(30.0, SunburstBucket::Classified, false),
        make_segment(20.0, SunburstBucket::Mixed, false),
    ];

    SunburstModel {
        center: default_center(),
        layers: vec![
            SunburstLayer {
                span: SunburstSpan::full(),
                segments: layer0_segments,
                total_value: 100.0,
            },
            SunburstLayer {
                span: child_span,
                segments: layer1_segments,
                total_value: 50.0,
            },
        ],
    }
}

/// Fixture 5: three layers, nested selection.
fn three_layers_nested_selection() -> SunburstModel {
    let layer0_segments = vec![
        make_segment(60.0, SunburstBucket::Project, true),
        make_segment(40.0, SunburstBucket::Category, false),
    ];
    let layer0_total = 100.0;

    let child_span = SunburstSpan::full().child_span(0.0, std::f64::consts::TAU * 0.6);
    let layer1_segments = vec![
        make_segment(35.0, SunburstBucket::Mixed, true),
        make_segment(25.0, SunburstBucket::Classified, false),
    ];
    let layer1_total = 60.0;

    let grandchild_span = child_span.child_span(0.0, child_span.sweep * (35.0 / 60.0));
    let layer2_segments = vec![
        make_segment(20.0, SunburstBucket::Unclassified, false),
        make_segment(15.0, SunburstBucket::Project, false),
    ];
    let layer2_total = 35.0;

    SunburstModel {
        center: default_center(),
        layers: vec![
            SunburstLayer {
                span: SunburstSpan::full(),
                segments: layer0_segments,
                total_value: layer0_total,
            },
            SunburstLayer {
                span: child_span,
                segments: layer1_segments,
                total_value: layer1_total,
            },
            SunburstLayer {
                span: grandchild_span,
                segments: layer2_segments,
                total_value: layer2_total,
            },
        ],
    }
}

// ---------------------------------------------------------------------------
// Snapshot tests
// ---------------------------------------------------------------------------

#[test]
fn single_layer_no_selection_snapshot() {
    let model = single_layer_no_selection();
    let rendered = render_fixture(&model, WIDTH, HEIGHT, SunburstRenderMode::Quadrant);
    insta::assert_snapshot!(rendered);
}

#[test]
fn single_layer_selected_snapshot() {
    let model = single_layer_selected();
    let rendered = render_fixture(&model, WIDTH, HEIGHT, SunburstRenderMode::Quadrant);
    insta::assert_snapshot!(rendered);
}

#[test]
fn two_layers_selection_in_layer0_snapshot() {
    let model = two_layers_selection_in_layer0();
    let rendered = render_fixture(&model, WIDTH, HEIGHT, SunburstRenderMode::Quadrant);
    insta::assert_snapshot!(rendered);
}

#[test]
fn two_layers_no_selection_snapshot() {
    let model = two_layers_no_selection();
    let rendered = render_fixture(&model, WIDTH, HEIGHT, SunburstRenderMode::Quadrant);
    insta::assert_snapshot!(rendered);
}

#[test]
fn three_layers_nested_selection_snapshot() {
    let model = three_layers_nested_selection();
    let rendered = render_fixture(&model, WIDTH, HEIGHT, SunburstRenderMode::Quadrant);
    insta::assert_snapshot!(rendered);
}

// ---------------------------------------------------------------------------
// Overlap regression: the old bug produced 3 layers (with a duplicate current
// layer) while the fix produces 2 layers.  Confirm different snapshots.
// ---------------------------------------------------------------------------

/// Build the OLD buggy model: layer 0 (ancestor), layer 1 (current, selected),
/// layer 2 (duplicate of current with same segments — the bug).
fn overlap_buggy_model() -> SunburstModel {
    let layer0_segments = vec![
        make_segment(60.0, SunburstBucket::Project, true),
        make_segment(40.0, SunburstBucket::Category, false),
    ];

    let child_span = SunburstSpan::full().child_span(0.0, std::f64::consts::TAU * 0.6);
    let current_segments = vec![
        make_segment(35.0, SunburstBucket::Mixed, false),
        make_segment(25.0, SunburstBucket::Classified, false),
    ];

    // The bug: a duplicate of the current layer rendered on top.
    let duplicate_segments = current_segments.clone();

    SunburstModel {
        center: default_center(),
        layers: vec![
            SunburstLayer {
                span: SunburstSpan::full(),
                segments: layer0_segments,
                total_value: 100.0,
            },
            SunburstLayer {
                span: child_span,
                segments: current_segments,
                total_value: 60.0,
            },
            SunburstLayer {
                span: child_span,
                segments: duplicate_segments,
                total_value: 60.0,
            },
        ],
    }
}

/// Build the FIXED model: layer 0 (ancestor), layer 1 (current, no duplicate).
fn overlap_fixed_model() -> SunburstModel {
    let layer0_segments = vec![
        make_segment(60.0, SunburstBucket::Project, true),
        make_segment(40.0, SunburstBucket::Category, false),
    ];

    let child_span = SunburstSpan::full().child_span(0.0, std::f64::consts::TAU * 0.6);
    let current_segments = vec![
        make_segment(35.0, SunburstBucket::Mixed, false),
        make_segment(25.0, SunburstBucket::Classified, false),
    ];

    SunburstModel {
        center: default_center(),
        layers: vec![
            SunburstLayer {
                span: SunburstSpan::full(),
                segments: layer0_segments,
                total_value: 100.0,
            },
            SunburstLayer {
                span: child_span,
                segments: current_segments,
                total_value: 60.0,
            },
        ],
    }
}

#[test]
fn overlap_bug_produces_different_snapshot() {
    let buggy = render_fixture(
        &overlap_buggy_model(),
        WIDTH,
        HEIGHT,
        SunburstRenderMode::Quadrant,
    );
    let fixed = render_fixture(
        &overlap_fixed_model(),
        WIDTH,
        HEIGHT,
        SunburstRenderMode::Quadrant,
    );
    // The two models MUST render differently — if they don't, the snapshot
    // tests would not have caught the original overlap bug.
    assert_ne!(
        buggy, fixed,
        "buggy and fixed models must render differently"
    );

    // Snapshot both for visual inspection.
    insta::assert_snapshot!("overlap_buggy", buggy);
    insta::assert_snapshot!("overlap_fixed", fixed);
}

// ---------------------------------------------------------------------------
// Angular overlap analysis: verify descendant ring never bleeds past the
// selected segment's angular boundary in the ancestor ring.
// ---------------------------------------------------------------------------

use super::geometry::sunburst_selected_child_span;
use super::model::SunburstDistortionPolicy;
use super::raster::rasterize_sunburst;

/// Render directly into a buffer (no pane border) and serialize with color tags.
fn render_direct(model: &SunburstModel, width: u16, height: u16) -> String {
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    let config = SunburstRenderConfig::default();
    rasterize_sunburst(&mut buf, area, model, config);
    buffer_to_color_string(&buf, area)
}

/// Diagnostic: dump cells near the descendant span boundary to understand
/// where teal appears relative to the span edge.
#[test]
fn diagnose_boundary_cells() {
    let (model, child_span) = realistic_two_layer_model(1); // gnomon selected
    let width: u16 = 96;
    let height: u16 = 44;
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    let config = SunburstRenderConfig::default();
    rasterize_sunburst(&mut buf, area, &model, config);

    let center_x = f64::from(width) / 2.0;
    let center_y = f64::from(height) / 2.0;
    let radius_x = center_x;
    let radius_y = center_y;
    let ring_band = (config.outer_radius - config.center_radius) / model.layers.len() as f64;
    let span_end = child_span.start + child_span.sweep;

    let descendant_colors = [Color::Indexed(73), Color::Indexed(107)];

    eprintln!(
        "Descendant span: {:.3}-{:.3} rad ({:.1}°-{:.1}°)",
        child_span.start,
        span_end,
        child_span.start * 180.0 / std::f64::consts::PI,
        span_end * 180.0 / std::f64::consts::PI,
    );

    // Find all teal/green cells in the outer ring and report their angles
    let mut outer_teal_angles: Vec<(u16, u16, f64)> = Vec::new();
    for y in 0..height {
        for x in 0..width {
            let cell = &buf[(x, y)];
            if !descendant_colors.contains(&cell.fg) {
                continue;
            }
            let nx = (f64::from(x) + 0.5 - center_x) / radius_x;
            let ny = (f64::from(y) + 0.5 - center_y) / radius_y;
            let r = (nx * nx + ny * ny).sqrt();
            if r < config.center_radius + ring_band || r > config.outer_radius {
                continue;
            }
            let angle =
                (ny.atan2(nx) + std::f64::consts::FRAC_PI_2).rem_euclid(std::f64::consts::TAU);
            outer_teal_angles.push((x, y, angle));
        }
    }

    let min_angle = outer_teal_angles
        .iter()
        .map(|t| t.2)
        .fold(f64::MAX, f64::min);
    let max_angle = outer_teal_angles
        .iter()
        .map(|t| t.2)
        .fold(f64::MIN, f64::max);
    let outside_span: Vec<_> = outer_teal_angles
        .iter()
        .filter(|t| {
            let offset = (t.2 - child_span.start).rem_euclid(std::f64::consts::TAU);
            offset > child_span.sweep
        })
        .collect();

    eprintln!(
        "Teal in outer ring: {} cells, angle range {:.1}°-{:.1}°",
        outer_teal_angles.len(),
        min_angle * 180.0 / std::f64::consts::PI,
        max_angle * 180.0 / std::f64::consts::PI,
    );
    eprintln!("Teal OUTSIDE descendant span: {} cells", outside_span.len());
    for (cx, cy, angle) in outside_span.iter().take(10) {
        let nx = (f64::from(*cx) + 0.5 - center_x) / radius_x;
        let ny = (f64::from(*cy) + 0.5 - center_y) / radius_y;
        let r = (nx * nx + ny * ny).sqrt();
        let cell = &buf[(*cx, *cy)];
        eprintln!(
            "  ({}, {}) angle={:.1}° r={:.3} symbol='{}' fg={:?}",
            cx,
            cy,
            angle * 180.0 / std::f64::consts::PI,
            r,
            cell.symbol(),
            cell.fg,
        );
    }
    // A small number of boundary cells (< 10) are expected from sub-cell
    // anti-aliasing: the cell center falls outside the span but one of the
    // 4 quadrant samples lands inside.  A large count indicates a real bug.
    assert!(
        outside_span.len() < 10,
        "{} teal cells outside descendant span (threshold 10)",
        outside_span.len()
    );
}

/// Large-resolution snapshot of the gnomon-selected scenario.
/// Visually inspect this to see if the descendant ring (teal/73) extends
/// past the selected segment (white/15) boundary into steel-blue (67) areas.
#[test]
fn large_render_gnomon_selected() {
    let (model, _) = realistic_two_layer_model(1); // gnomon = index 1
    let rendered = render_direct(&model, 96, 44);
    insta::assert_snapshot!(rendered);
}

/// Large-resolution snapshot of the dotfiles-selected scenario.
#[test]
fn large_render_dotfiles_selected() {
    let (model, _) = realistic_two_layer_model(2); // dotfiles = index 2
    let rendered = render_direct(&model, 96, 44);
    insta::assert_snapshot!(rendered);
}

/// Build a realistic 2-layer model matching the screenshot proportions.
/// Returns (model, ancestor_layer_for_span_check).
fn realistic_two_layer_model(selected_index: usize) -> (SunburstModel, SunburstSpan) {
    // Real data proportions: kapow=60M, gnomon=25M, dotfiles=16M
    let project_values = [60_106_766.0, 24_925_990.0, 16_038_008.0];
    let total: f64 = project_values.iter().sum();

    let ancestor_segments: Vec<SunburstSegment> = project_values
        .iter()
        .enumerate()
        .map(|(i, &v)| SunburstSegment {
            value: v,
            bucket: SunburstBucket::Project,
            is_selected: i == selected_index,
        })
        .collect();

    let ancestor_layer = SunburstLayer {
        span: SunburstSpan::full(),
        segments: ancestor_segments,
        total_value: total,
    };

    let child_span =
        sunburst_selected_child_span(&ancestor_layer, SunburstDistortionPolicy::default());

    // Descendant layer: 3 category children within the selected project's span
    let descendant_segments = vec![
        make_segment(40.0, SunburstBucket::Category, false),
        make_segment(30.0, SunburstBucket::Category, false),
        make_segment(20.0, SunburstBucket::Classified, false),
    ];
    let desc_total: f64 = descendant_segments.iter().map(|s| s.value).sum();
    let descendant_layer = SunburstLayer {
        span: child_span,
        segments: descendant_segments,
        total_value: desc_total,
    };

    let model = SunburstModel {
        center: default_center(),
        layers: vec![ancestor_layer, descendant_layer],
    };

    (model, child_span)
}

/// For each cell in the rendered buffer, check that descendant-ring colors
/// (Category=73, Classified=107) never appear at angles where the ancestor
/// ring shows a non-selected project (Project=67).
///
/// The check works by scanning each row: at a given y, we identify cells
/// that are in the outer ring radius band and have descendant colors, then
/// verify that no cell at the SAME y in the inner ring radius band has a
/// non-selected ancestor color at the same approximate angle.
#[test]
fn descendant_ring_does_not_bleed_past_selected_segment() {
    let test_width: u16 = 80;
    let test_height: u16 = 40;
    let config = SunburstRenderConfig::default();

    for selected_index in [1_usize, 2] {
        let (model, _child_span) = realistic_two_layer_model(selected_index);

        let area = Rect::new(0, 0, test_width, test_height);
        let mut buf = Buffer::empty(area);

        // Render directly (skip pane_block border to avoid offset issues)
        rasterize_sunburst(&mut buf, area, &model, config);

        let center_x = f64::from(test_width) / 2.0;
        let center_y = f64::from(test_height) / 2.0;
        let radius_x = center_x.max(1.0);
        let radius_y = center_y.max(1.0);
        let ring_band = (config.outer_radius - config.center_radius) / model.layers.len() as f64;

        let descendant_colors = [Color::Indexed(73), Color::Indexed(107)];
        let non_selected_project = Color::Indexed(67);

        let mut violations = Vec::new();

        for y in 0..test_height {
            for x in 0..test_width {
                let cell = &buf[(x, y)];
                let fg = cell.fg;

                // Only look at cells with descendant colors
                if !descendant_colors.contains(&fg) {
                    continue;
                }

                // Compute this cell's normalized position and radius
                let nx = (f64::from(x) + 0.5 - center_x) / radius_x;
                let ny = (f64::from(y) + 0.5 - center_y) / radius_y;
                let r = (nx * nx + ny * ny).sqrt();

                // Must be in the outer ring radius band
                let outer_start = config.center_radius + ring_band;
                if r < outer_start || r > config.outer_radius {
                    continue;
                }

                // Compute the angle
                let angle =
                    (ny.atan2(nx) + std::f64::consts::FRAC_PI_2).rem_euclid(std::f64::consts::TAU);

                // Now check: at this same angle but in the inner ring, is there
                // a non-selected project color? If so, the descendant ring is
                // bleeding into a different project's angular range.
                //
                // Walk inward at same angle to find an inner-ring cell.
                // The angle convention: angle = atan2(ny, nx) + π/2,
                // so nx = sin(angle), ny = -cos(angle).
                let inner_mid = config.center_radius + ring_band / 2.0;
                let ix = center_x + angle.sin() * inner_mid * radius_x;
                let iy = center_y - angle.cos() * inner_mid * radius_y;
                let ix = (ix.round() as u16).min(test_width - 1);
                let iy = (iy.round() as u16).min(test_height - 1);

                let inner_cell = &buf[(ix, iy)];
                if inner_cell.fg == non_selected_project {
                    violations.push((x, y, ix, iy, angle * 180.0 / std::f64::consts::PI));
                }
            }
        }

        if !violations.is_empty() {
            // Compute the angular range of violations
            let min_angle = violations.iter().map(|v| v.4).fold(f64::MAX, f64::min);
            let max_angle = violations.iter().map(|v| v.4).fold(f64::MIN, f64::max);
            eprintln!(
                "selected_index={}: {} violations at angles {:.1}°-{:.1}°",
                selected_index,
                violations.len(),
                min_angle,
                max_angle,
            );
            eprintln!(
                "  child_span: start={:.3} ({:.1}°) sweep={:.3} ({:.1}°) end={:.1}°",
                _child_span.start,
                _child_span.start * 180.0 / std::f64::consts::PI,
                _child_span.sweep,
                _child_span.sweep * 180.0 / std::f64::consts::PI,
                (_child_span.start + _child_span.sweep) * 180.0 / std::f64::consts::PI,
            );
        }

        // Allow a small number of boundary violations from quantization
        // (typically < 30 cells at the span edges). A large count (100+)
        // indicates a real angular overlap bug.
        assert!(
            violations.len() < 30,
            "selected_index={}: descendant colors appear at {} cells where inner ring shows \
             non-selected project (threshold 30). First 5: {:?}",
            selected_index,
            violations.len(),
            &violations[..violations.len().min(5)],
        );
    }
}
