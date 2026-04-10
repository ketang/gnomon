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
