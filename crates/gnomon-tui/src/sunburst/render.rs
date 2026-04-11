use ratatui::buffer::Buffer;
use ratatui::layout::{Alignment, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Text};
use ratatui::widgets::{Paragraph, Widget};

use crate::app::pane_block;

use super::model::{SunburstCenter, SunburstModel, SunburstRenderConfig};
use super::raster::rasterize_sunburst;

pub(crate) struct SunburstPane<'a> {
    pub(crate) model: &'a SunburstModel,
    pub(crate) focused: bool,
    pub(crate) config: SunburstRenderConfig,
}

impl Widget for &SunburstPane<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = pane_block("Map", self.focused);
        let inner = block.inner(area);
        block.render(area, buf);

        if inner.width < 12 || inner.height < 8 {
            return;
        }

        rasterize_sunburst(buf, inner, self.model, self.config);

        let center_area = sunburst_center_label_area(inner, self.config);

        // Clear the center area so sunburst quadrant glyphs don't bleed
        // through the centered-text padding.
        let clear_style = sunburst_center_label_style(self.focused);
        for y in center_area.y..center_area.y + center_area.height {
            for x in center_area.x..center_area.x + center_area.width {
                if let Some(cell) = buf.cell_mut((x, y)) {
                    cell.reset();
                    cell.set_style(clear_style);
                }
            }
        }

        let center_lines =
            sunburst_center_label_lines(&self.model.center, center_area.width, center_area.height);
        Paragraph::new(Text::from(center_lines))
            .style(clear_style)
            .alignment(Alignment::Center)
            .render(center_area, buf);
    }
}

pub(crate) fn sunburst_center_label_area(inner: Rect, config: SunburstRenderConfig) -> Rect {
    let center_width = sunburst_center_extent(inner.width, config);
    let center_height = sunburst_center_extent(inner.height, config).min(3);
    Rect::new(
        inner.x + inner.width.saturating_sub(center_width) / 2,
        inner.y + inner.height.saturating_sub(center_height) / 2,
        center_width,
        center_height,
    )
}

fn sunburst_center_extent(dimension: u16, config: SunburstRenderConfig) -> u16 {
    if dimension == 0 {
        return 0;
    }

    let extent = (f64::from(dimension) * config.center_radius).floor() as u16;
    extent.clamp(1, dimension)
}

fn sunburst_center_label_lines(
    center: &SunburstCenter,
    width: u16,
    height: u16,
) -> Vec<Line<'static>> {
    let max_width = usize::from(width);
    let line_count = usize::from(height).min(3);
    let lens_line = format!("lens: {}  |  cache hint: subtle dot", center.lens_label);
    let lines = [
        center.scope_label.as_str(),
        center.selection_label.as_str(),
        lens_line.as_str(),
    ];

    lines
        .into_iter()
        .map(|line| Line::from(truncate_center_label(line, max_width)))
        .take(line_count)
        .collect()
}

fn truncate_center_label(text: &str, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }

    text.chars().take(max_width).collect()
}

pub(crate) fn sunburst_center_label_style(focused: bool) -> Style {
    let mut style = Style::default().fg(Color::Gray);
    if focused {
        style = style.fg(Color::White).add_modifier(Modifier::BOLD);
    }
    style
}
