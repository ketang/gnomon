use std::time::Instant;

use anyhow::{Result, bail};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use serde::Serialize;

use crate::sunburst::{
    SunburstBucket, SunburstCenter, SunburstLayer, SunburstModel, SunburstPane,
    SunburstRenderConfig, SunburstRenderMode, SunburstSegment, SunburstSpan,
};

#[derive(Debug, Clone, Copy)]
pub struct SunburstBenchmarkOptions {
    pub iterations: usize,
}

impl Default for SunburstBenchmarkOptions {
    fn default() -> Self {
        Self { iterations: 20 }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SunburstBenchmarkReport {
    pub assumptions: SunburstBenchmarkAssumptions,
    pub scenarios: Vec<SunburstBenchmarkScenario>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SunburstBenchmarkAssumptions {
    pub iterations: usize,
    pub focus_mode: &'static str,
    pub fixture_shape: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct SunburstBenchmarkScenario {
    pub fixture: String,
    pub mode: String,
    pub terminal: BenchmarkTerminalSize,
    pub layer_count: usize,
    pub max_segment_count: usize,
    pub sample_count: usize,
    pub min_micros: f64,
    pub median_micros: f64,
    pub p95_micros: f64,
    pub max_micros: f64,
    pub mean_micros: f64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct BenchmarkTerminalSize {
    pub width: u16,
    pub height: u16,
}

#[derive(Clone, Copy)]
struct FixtureSpec {
    name: &'static str,
    layer_count: usize,
    max_segment_count: usize,
}

const FIXTURES: [FixtureSpec; 3] = [
    FixtureSpec {
        name: "compact",
        layer_count: 3,
        max_segment_count: 4,
    },
    FixtureSpec {
        name: "browse",
        layer_count: 5,
        max_segment_count: 10,
    },
    FixtureSpec {
        name: "dense",
        layer_count: 6,
        max_segment_count: 20,
    },
];

const TERMINAL_SIZES: [BenchmarkTerminalSize; 3] = [
    BenchmarkTerminalSize {
        width: 24,
        height: 12,
    },
    BenchmarkTerminalSize {
        width: 48,
        height: 18,
    },
    BenchmarkTerminalSize {
        width: 96,
        height: 30,
    },
];

const MODES: [SunburstRenderMode; 2] = [SunburstRenderMode::Coarse, SunburstRenderMode::Braille];

pub fn run_sunburst_benchmark(
    options: SunburstBenchmarkOptions,
) -> Result<SunburstBenchmarkReport> {
    if options.iterations == 0 {
        bail!("iterations must be greater than zero");
    }

    let mut scenarios = Vec::new();
    for fixture in FIXTURES {
        let model = synthetic_model(fixture);
        for terminal in TERMINAL_SIZES {
            for mode in MODES {
                let area = Rect::new(0, 0, terminal.width, terminal.height);
                let mut samples = Vec::with_capacity(options.iterations);
                for _ in 0..options.iterations {
                    let mut buffer = Buffer::empty(area);
                    let pane = SunburstPane {
                        model: &model,
                        focused: true,
                        config: SunburstRenderConfig {
                            mode,
                            ..SunburstRenderConfig::default()
                        },
                    };
                    let started = Instant::now();
                    (&pane).render(area, &mut buffer);
                    samples.push(started.elapsed().as_secs_f64() * 1_000_000.0);
                }

                samples.sort_by(f64::total_cmp);
                scenarios.push(SunburstBenchmarkScenario {
                    fixture: fixture.name.to_string(),
                    mode: render_mode_label(mode).to_string(),
                    terminal,
                    layer_count: fixture.layer_count,
                    max_segment_count: fixture.max_segment_count,
                    sample_count: samples.len(),
                    min_micros: *samples.first().unwrap_or(&0.0),
                    median_micros: percentile(&samples, 0.5),
                    p95_micros: percentile(&samples, 0.95),
                    max_micros: *samples.last().unwrap_or(&0.0),
                    mean_micros: samples.iter().sum::<f64>() / samples.len() as f64,
                });
            }
        }
    }

    Ok(SunburstBenchmarkReport {
        assumptions: SunburstBenchmarkAssumptions {
            iterations: options.iterations,
            focus_mode: "focused selected branch with deterministic synthetic layers",
            fixture_shape: "layered synthetic browse hierarchies with one selected branch per layer",
        },
        scenarios,
    })
}

fn render_mode_label(mode: SunburstRenderMode) -> &'static str {
    match mode {
        SunburstRenderMode::Coarse => "coarse",
        SunburstRenderMode::Braille => "braille",
    }
}

fn percentile(samples: &[f64], ratio: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }

    let index = ((samples.len() - 1) as f64 * ratio).round() as usize;
    samples[index]
}

fn synthetic_model(fixture: FixtureSpec) -> SunburstModel {
    let mut layers = Vec::with_capacity(fixture.layer_count);
    let mut inherited_span = SunburstSpan::full();

    for layer_index in 0..fixture.layer_count {
        let segment_count = (fixture.max_segment_count / 2 + layer_index + 1)
            .min(fixture.max_segment_count)
            .max(2);
        let selected_index = segment_count / 3;
        let segments = (0..segment_count)
            .map(|segment_index| {
                let is_selected = segment_index == selected_index;
                SunburstSegment {
                    value: if is_selected {
                        1.0 + layer_index as f64
                    } else {
                        (segment_count - segment_index) as f64 + 1.0
                    },
                    cached_ratio: if segment_index % 3 == 0 { 0.75 } else { 0.1 },
                    bucket: synthetic_bucket(layer_index, segment_index),
                    is_selected,
                }
            })
            .collect::<Vec<_>>();
        let total_value = segments.iter().map(|segment| segment.value).sum();
        let layer = SunburstLayer {
            span: inherited_span,
            segments,
            total_value,
        };
        inherited_span = layer
            .segments
            .iter()
            .find(|segment| segment.is_selected)
            .map(|selected| {
                let selected_ratio = selected.value / total_value;
                layer.span.child_span(
                    layer.span.sweep * 0.2,
                    (layer.span.sweep * selected_ratio.max(0.15)).min(layer.span.sweep * 0.4),
                )
            })
            .unwrap_or(inherited_span);
        layers.push(layer);
    }

    SunburstModel {
        center: SunburstCenter {
            scope_label: format!("fixture: {}", fixture.name),
            lens_label: "uncached".to_string(),
            selection_label: "selected synthetic branch".to_string(),
        },
        layers,
    }
}

fn synthetic_bucket(layer_index: usize, segment_index: usize) -> SunburstBucket {
    match (layer_index + segment_index) % 5 {
        0 => SunburstBucket::Project,
        1 => SunburstBucket::Category,
        2 => SunburstBucket::Classified,
        3 => SunburstBucket::Mixed,
        _ => SunburstBucket::Unclassified,
    }
}

#[cfg(test)]
mod tests {
    use super::{SunburstBenchmarkOptions, run_sunburst_benchmark};

    #[test]
    fn benchmark_report_covers_all_modes_and_fixture_sizes() {
        let report = run_sunburst_benchmark(SunburstBenchmarkOptions { iterations: 1 })
            .expect("benchmark report");
        assert_eq!(report.assumptions.iterations, 1);
        assert!(
            report
                .scenarios
                .iter()
                .any(|scenario| scenario.mode == "coarse")
        );
        assert!(
            report
                .scenarios
                .iter()
                .any(|scenario| scenario.mode == "braille")
        );
        assert!(
            report
                .scenarios
                .iter()
                .any(|scenario| scenario.fixture == "dense" && scenario.terminal.width == 96)
        );
    }
}
