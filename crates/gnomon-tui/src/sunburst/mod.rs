//! Internal focused sunburst renderer boundary.
//!
//! This module keeps reusable model and rendering concerns separate from
//! `gnomon`'s browse/query mapping. The higher-resolution terminal rendering
//! direction is inspired by `tui-piechart`, but this module does not depend on
//! or reuse code from that project.

mod geometry;
mod model;
mod raster;
mod render;

pub(crate) use geometry::sunburst_selected_child_span;
pub(crate) use model::{
    SunburstBucket, SunburstCenter, SunburstDistortionPolicy, SunburstLayer, SunburstModel,
    SunburstRenderConfig, SunburstRenderMode, SunburstSegment, SunburstSpan,
};
pub(crate) use render::SunburstPane;

#[cfg(test)]
pub(crate) use geometry::sunburst_segment_at_angle;

#[cfg(test)]
pub(crate) use render::{sunburst_center_label_area, sunburst_center_label_style};
