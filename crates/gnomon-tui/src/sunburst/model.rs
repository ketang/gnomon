use std::f64::consts::TAU;

#[derive(Debug, Clone, Default)]
pub(crate) struct SunburstModel {
    pub(crate) center: SunburstCenter,
    pub(crate) layers: Vec<SunburstLayer>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SunburstCenter {
    pub(crate) scope_label: String,
    pub(crate) lens_label: String,
    pub(crate) selection_label: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SunburstLayer {
    pub(crate) span: SunburstSpan,
    pub(crate) segments: Vec<SunburstSegment>,
    pub(crate) total_value: f64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SunburstSpan {
    pub(crate) start: f64,
    pub(crate) sweep: f64,
}

impl Default for SunburstSpan {
    fn default() -> Self {
        Self::full()
    }
}

impl SunburstSpan {
    pub(crate) fn full() -> Self {
        Self {
            start: 0.0,
            sweep: TAU,
        }
    }

    pub(crate) fn child_span(self, start_offset: f64, sweep: f64) -> Self {
        Self {
            start: (self.start + start_offset).rem_euclid(TAU),
            sweep,
        }
    }

    pub(crate) fn local_angle(self, angle: f64) -> Option<f64> {
        if self.sweep <= 0.0 {
            return None;
        }

        let offset = (angle - self.start).rem_euclid(TAU);
        if offset > self.sweep {
            None
        } else {
            Some(offset)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SunburstSegment {
    pub(crate) value: f64,
    pub(crate) bucket: SunburstBucket,
    pub(crate) is_selected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SunburstBucket {
    Project,
    Category,
    Classified,
    Mixed,
    Unclassified,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SunburstRenderConfig {
    pub(crate) center_radius: f64,
    pub(crate) outer_radius: f64,
    pub(crate) mode: SunburstRenderMode,
    pub(crate) distortion_policy: SunburstDistortionPolicy,
}

impl Default for SunburstRenderConfig {
    fn default() -> Self {
        Self {
            center_radius: 0.24,
            outer_radius: 0.96,
            mode: SunburstRenderMode::Quadrant,
            distortion_policy: SunburstDistortionPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SunburstRenderMode {
    Quadrant,
    Coarse,
    Braille,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SunburstDistortionPolicy {
    pub(crate) minimum_visible_sweep: f64,
    pub(crate) focus_zoom_threshold_ratio: f64,
    pub(crate) focus_zoom_multiplier: f64,
    pub(crate) maximum_selected_share: f64,
}

impl Default for SunburstDistortionPolicy {
    fn default() -> Self {
        Self {
            minimum_visible_sweep: TAU / 48.0,
            focus_zoom_threshold_ratio: 0.25,
            focus_zoom_multiplier: 1.8,
            maximum_selected_share: 0.4,
        }
    }
}
