use std::f64::consts::{FRAC_PI_2, TAU};
use std::fs;
use std::io::{self, Stdout};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::{Duration, Instant};

use crate::StartupBrowseState;
use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use gnomon_core::config::RuntimeConfig;
use gnomon_core::db::Database;
use gnomon_core::import::{StartupOpenReason, StartupWorkerEvent};
use gnomon_core::query::{
    ActionKey, BrowseFilters, BrowsePath, BrowseRequest, ClassificationState, FilterOptions,
    MetricLens, QueryEngine, RollupRow, RollupRowKind, RootView, SnapshotBounds,
    SnapshotCoverageSummary, TimeWindowFilter,
};
use jiff::ToSpan;
use nucleo_matcher::pattern::{CaseMatching, Normalization, Pattern};
use nucleo_matcher::{Config as MatcherConfig, Matcher};
use ratatui::backend::CrosstermBackend;
use ratatui::buffer::Buffer;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Table, TableState, Widget, Wrap,
};
use ratatui::{Frame, Terminal};
use serde::{Deserialize, Serialize};

const UI_STATE_FILENAME: &str = "tui-state.json";
const REFRESH_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const WIDE_LAYOUT_WIDTH: u16 = 120;
const JUMP_MATCH_LIMIT: usize = 8;
const RADIAL_CENTER_RADIUS: f64 = 0.24;
pub struct App {
    database: Database,
    ui_state_path: PathBuf,
    ui_state: PersistedUiState,
    snapshot: SnapshotBounds,
    snapshot_coverage: SnapshotCoverageSummary,
    latest_snapshot: SnapshotBounds,
    startup_open_reason: StartupOpenReason,
    has_newer_snapshot: bool,
    filter_options: FilterOptions,
    raw_rows: Vec<RollupRow>,
    visible_rows: Vec<RollupRow>,
    table_state: TableState,
    input_mode: InputMode,
    focused_pane: PaneFocus,
    breadcrumb_targets: Vec<BreadcrumbTarget>,
    breadcrumb_picker: BreadcrumbPickerState,
    radial_context: RadialContext,
    radial_model: RadialModel,
    jump_state: JumpState,
    status_message: Option<StatusMessage>,
    status_updates: Option<Receiver<StartupWorkerEvent>>,
    last_refresh_check: Instant,
}

impl App {
    pub fn new(
        config: RuntimeConfig,
        snapshot: SnapshotBounds,
        startup_open_reason: StartupOpenReason,
        startup_status_message: Option<String>,
        startup_browse_state: Option<StartupBrowseState>,
        status_updates: Option<Receiver<StartupWorkerEvent>>,
    ) -> Result<Self> {
        let ui_state_path = config.state_dir.join(UI_STATE_FILENAME);
        let (ui_state, mut status_message) = match PersistedUiState::load(&ui_state_path) {
            Ok(Some(state)) => (state, None),
            Ok(None) => (PersistedUiState::default(), None),
            Err(error) => (
                PersistedUiState::default(),
                Some(StatusMessage::error(compact_status_text(format!(
                    "Unable to load saved TUI state: {error:#}"
                )))),
            ),
        };
        if let Some(startup_status_message) = startup_status_message {
            status_message = Some(match status_message {
                Some(existing) => StatusMessage::error(format!(
                    "{} | {}",
                    compact_status_text(&startup_status_message),
                    compact_status_text(&existing.text)
                )),
                None => StatusMessage::error(compact_status_text(&startup_status_message)),
            });
        }
        let mut ui_state = ui_state;
        ui_state.apply_startup_browse_state(startup_browse_state);
        let focused_pane = PaneFocus::from_pane_mode(ui_state.pane_mode);

        let database = Database::open(&config.db_path)?;
        let mut app = Self {
            database,
            ui_state_path,
            ui_state,
            latest_snapshot: snapshot.clone(),
            snapshot,
            snapshot_coverage: SnapshotCoverageSummary::default(),
            startup_open_reason,
            has_newer_snapshot: false,
            filter_options: FilterOptions {
                projects: Vec::new(),
                models: Vec::new(),
                categories: Vec::new(),
                actions: Vec::new(),
            },
            raw_rows: Vec::new(),
            visible_rows: Vec::new(),
            table_state: TableState::default(),
            input_mode: InputMode::Normal,
            focused_pane,
            breadcrumb_targets: Vec::new(),
            breadcrumb_picker: BreadcrumbPickerState::default(),
            radial_context: RadialContext::default(),
            radial_model: RadialModel::default(),
            jump_state: JumpState::default(),
            status_message,
            status_updates,
            last_refresh_check: Instant::now(),
        };

        app.reload_view()?;
        app.refresh_snapshot_coverage()?;
        app.refresh_snapshot_status()?;
        Ok(app)
    }

    pub fn run(mut self) -> Result<()> {
        let mut terminal = TerminalGuard::enter()?;

        loop {
            self.drain_status_updates();
            if self.last_refresh_check.elapsed() >= REFRESH_CHECK_INTERVAL {
                self.refresh_snapshot_status()?;
                self.last_refresh_check = Instant::now();
            }

            terminal.terminal.draw(|frame| self.render(frame))?;

            if !event::poll(Duration::from_millis(250))? {
                continue;
            }

            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => {
                    if self.handle_key(key)? {
                        break;
                    }
                }
                Event::Resize(_, _) => {}
                _ => {}
            }
        }

        Ok(())
    }

    fn drain_status_updates(&mut self) {
        loop {
            let mut disconnected = false;
            let update = {
                let Some(receiver) = self.status_updates.as_ref() else {
                    return;
                };

                match receiver.try_recv() {
                    Ok(update) => Some(update),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => {
                        disconnected = true;
                        None
                    }
                }
            };

            if disconnected {
                self.status_updates = None;
            }

            let Some(update) = update else {
                break;
            };

            self.apply_status_update(update);
        }
    }

    fn apply_status_update(&mut self, update: StartupWorkerEvent) {
        match update {
            StartupWorkerEvent::StartupSettled {
                startup_status_message,
            } => {
                if let Some(message) = startup_status_message {
                    self.push_status_message(message);
                }
            }
            StartupWorkerEvent::DeferredFailures {
                deferred_status_message,
            } => {
                if let Some(message) = deferred_status_message {
                    self.push_status_message(message);
                }
            }
        }
    }

    fn push_status_message(&mut self, message: impl Into<String>) {
        let message = compact_status_text(message.into());
        self.status_message = Some(match self.status_message.take() {
            Some(existing) => StatusMessage::error(format!(
                "{message} | {}",
                compact_status_text(&existing.text)
            )),
            None => StatusMessage::error(message),
        });
    }

    fn render(&mut self, frame: &mut Frame<'_>) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(7),
                Constraint::Min(10),
                Constraint::Length(4),
            ])
            .split(frame.area());

        self.render_header(frame, layout[0]);
        self.render_body(frame, layout[1]);
        self.render_footer(frame, layout[2]);

        match self.input_mode {
            InputMode::JumpInput => self.render_jump_overlay(frame),
            InputMode::BreadcrumbPicker => self.render_breadcrumb_overlay(frame),
            InputMode::ColumnChooser => self.render_columns_overlay(frame),
            InputMode::Normal | InputMode::FilterInput => {}
        }
    }

    fn render_header(&self, frame: &mut Frame<'_>, area: Rect) {
        let mut lines = vec![
            Line::from(vec![
                Span::styled("gnomon", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw("  radial + table explorer"),
            ]),
            view_line(&self.breadcrumb_targets),
            Line::from(format!(
                "{}  |  lens: {}  |  focus: {}",
                snapshot_summary_text(&self.snapshot, self.has_newer_snapshot),
                metric_lens_label(self.ui_state.lens),
                self.focused_pane.label()
            )),
            Line::from(snapshot_refresh_text(
                &self.snapshot,
                &self.latest_snapshot,
                self.startup_open_reason,
                self.has_newer_snapshot,
            )),
        ];

        if let Some(message) = &self.status_message {
            let style = match message.tone {
                StatusTone::Info => Style::default().fg(Color::Cyan),
                StatusTone::Error => Style::default().fg(Color::Red),
            };
            lines.push(Line::from(vec![
                Span::styled("status: ", Style::default().add_modifier(Modifier::BOLD)),
                Span::styled(&message.text, style),
            ]));
        } else {
            lines.push(Line::from(format!("filters: {}", self.filter_summary())));
        }

        let header = Paragraph::new(Text::from(lines))
            .block(Block::default().borders(Borders::ALL).title("Status"))
            .wrap(Wrap { trim: true });
        frame.render_widget(header, area);
    }

    fn render_body(&mut self, frame: &mut Frame<'_>, area: Rect) {
        if area.width >= WIDE_LAYOUT_WIDTH {
            let panes = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(42), Constraint::Percentage(58)])
                .split(area);
            self.render_radial(frame, panes[0]);
            self.render_table(frame, panes[1]);
            return;
        }

        match self.ui_state.pane_mode {
            PaneMode::Table => self.render_table(frame, area),
            PaneMode::Radial => self.render_radial(frame, area),
        }
    }

    fn render_table(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let visible_columns = active_columns(
            area.width,
            self.ui_state.lens,
            &self.ui_state.enabled_columns,
        );

        let header = Row::new(
            visible_columns
                .iter()
                .map(|column| Cell::from(column.title.clone()))
                .collect::<Vec<_>>(),
        )
        .style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        );

        let rows = if self.visible_rows.is_empty() {
            vec![Row::new(vec![Cell::from(if self.raw_rows.is_empty() {
                "No rows in the current path and filter set."
            } else {
                "The current-view filter hides all rows."
            })])]
        } else {
            self.visible_rows
                .iter()
                .map(|row| {
                    Row::new(
                        visible_columns
                            .iter()
                            .map(|column| {
                                Cell::from(render_column_value(
                                    column.key,
                                    row,
                                    self.ui_state.lens,
                                    &self.ui_state.root,
                                    &self.ui_state.path,
                                ))
                            })
                            .collect::<Vec<_>>(),
                    )
                })
                .collect()
        };

        let widths = visible_columns
            .iter()
            .map(|column| column.constraint)
            .collect::<Vec<_>>();

        let table = Table::new(rows, widths)
            .header(header)
            .block(pane_block("Table", self.focused_pane == PaneFocus::Table))
            .row_highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol(">> ");
        frame.render_stateful_widget(table, area, &mut self.table_state);
    }

    fn render_radial(&self, frame: &mut Frame<'_>, area: Rect) {
        frame.render_widget(
            &RadialPane {
                model: &self.radial_model,
                focused: self.focused_pane == PaneFocus::Radial,
            },
            area,
        );
    }

    fn render_footer(&self, frame: &mut Frame<'_>, area: Rect) {
        let lines = match self.input_mode {
            InputMode::Normal => vec![
                Line::from(
                    "Enter drill  Backspace up  b breadcrumbs  1/2 hierarchy  l lens  Tab focus/pane  o columns  q quit",
                ),
                Line::from(
                    "table focus: up/down rows. radial focus: left/right siblings. t/m/p/c/a filters  0 clear  / row filter  g jump  r refresh",
                ),
                Line::from(format!(
                    "{}  |  {}",
                    snapshot_coverage_footer_text(&self.snapshot_coverage),
                    self.selection_footer_text()
                )),
            ],
            InputMode::FilterInput => vec![
                Line::from("Editing current-view filter. Type to filter rows immediately."),
                Line::from("Enter or Esc returns to navigation mode."),
                Line::from(format!("filter> {}", self.ui_state.row_filter)),
            ],
            InputMode::JumpInput => vec![
                Line::from("Global jump is open. Type to fuzzy-match major navigation nodes."),
                Line::from("Up/down to change selection. Enter jumps. Esc closes."),
                Line::from(format!("jump> {}", self.jump_state.query)),
            ],
            InputMode::BreadcrumbPicker => vec![
                Line::from("Breadcrumb navigation is open. Choose an ancestor scope to jump to."),
                Line::from("Up/down or j/k to change selection. Enter jumps. Esc closes."),
                Line::from(format!(
                    "breadcrumb> {}",
                    self.breadcrumb_targets
                        .get(self.breadcrumb_picker.selected)
                        .map(|target| target.display.as_str())
                        .unwrap_or("none")
                )),
            ],
            InputMode::ColumnChooser => vec![
                Line::from(
                    "Column chooser: toggle with k kind, g gross, o output, t total, f 5h, w 1w, u ref, i items.",
                ),
                Line::from("Esc closes the chooser."),
                Line::from(format!(
                    "enabled columns: {}",
                    enabled_column_summary(&self.ui_state.enabled_columns)
                )),
            ],
        };

        let footer = Paragraph::new(Text::from(lines))
            .block(Block::default().borders(Borders::ALL).title("Keys"))
            .wrap(Wrap { trim: true });
        frame.render_widget(footer, area);
    }

    fn render_jump_overlay(&self, frame: &mut Frame<'_>) {
        let area = centered_rect(frame.area(), 72, 18);
        frame.render_widget(Clear, area);

        let mut items = if self.jump_state.matches.is_empty() {
            vec![ListItem::new("No matches for the current jump query.")]
        } else {
            self.jump_state
                .matches
                .iter()
                .enumerate()
                .map(|(index, target)| {
                    let prefix = if index == self.jump_state.selected {
                        ">> "
                    } else {
                        "   "
                    };
                    ListItem::new(format!("{prefix}{}  |  {}", target.label, target.detail))
                })
                .collect::<Vec<_>>()
        };

        items.insert(0, ListItem::new(format!("jump> {}", self.jump_state.query)));
        let list = List::new(items)
            .block(Block::default().borders(Borders::ALL).title("Global Jump"))
            .highlight_style(Style::default().add_modifier(Modifier::BOLD));
        frame.render_widget(list, area);
    }

    fn render_breadcrumb_overlay(&self, frame: &mut Frame<'_>) {
        let area = centered_rect(frame.area(), 80, 18);
        frame.render_widget(Clear, area);

        let mut items = if self.breadcrumb_targets.is_empty() {
            vec![ListItem::new("No breadcrumb targets available.")]
        } else {
            self.breadcrumb_targets
                .iter()
                .enumerate()
                .map(|(index, target)| {
                    let prefix = if index == self.breadcrumb_picker.selected {
                        ">> "
                    } else {
                        "   "
                    };
                    ListItem::new(format!("{prefix}{}", target.display))
                })
                .collect::<Vec<_>>()
        };

        items.insert(
            0,
            ListItem::new("Breadcrumb target picker. Enter jumps. Esc closes."),
        );
        let list = List::new(items)
            .block(Block::default().borders(Borders::ALL).title("Breadcrumbs"))
            .highlight_style(Style::default().add_modifier(Modifier::BOLD));
        frame.render_widget(list, area);
    }

    fn render_columns_overlay(&self, frame: &mut Frame<'_>) {
        let area = centered_rect(frame.area(), 60, 12);
        frame.render_widget(Clear, area);

        let lines = vec![
            Line::from("Toggle optional columns:"),
            Line::from(format!(
                "k kind [{}]   g gross [{}]   o output [{}]   t total [{}]",
                toggle_mark(&self.ui_state.enabled_columns, OptionalColumn::Kind),
                toggle_mark(&self.ui_state.enabled_columns, OptionalColumn::GrossInput),
                toggle_mark(&self.ui_state.enabled_columns, OptionalColumn::Output),
                toggle_mark(&self.ui_state.enabled_columns, OptionalColumn::Total),
            )),
            Line::from(format!(
                "f 5h [{}]     w 1w [{}]      u ref [{}]      i items [{}]",
                toggle_mark(&self.ui_state.enabled_columns, OptionalColumn::Last5Hours),
                toggle_mark(&self.ui_state.enabled_columns, OptionalColumn::LastWeek),
                toggle_mark(
                    &self.ui_state.enabled_columns,
                    OptionalColumn::UncachedReference
                ),
                toggle_mark(&self.ui_state.enabled_columns, OptionalColumn::Items),
            )),
            Line::from("The label and selected-lens columns are always visible."),
            Line::from("Narrow terminals automatically hide lower-priority enabled columns."),
            Line::from("Esc closes the chooser."),
        ];

        let popup = Paragraph::new(Text::from(lines))
            .block(Block::default().borders(Borders::ALL).title("Columns"))
            .wrap(Wrap { trim: true });
        frame.render_widget(popup, area);
    }

    fn handle_key(&mut self, key: KeyEvent) -> Result<bool> {
        match self.input_mode {
            InputMode::Normal => self.handle_normal_key(key),
            InputMode::FilterInput => self.handle_filter_input(key),
            InputMode::JumpInput => self.handle_jump_input(key),
            InputMode::BreadcrumbPicker => self.handle_breadcrumb_picker(key),
            InputMode::ColumnChooser => self.handle_column_key(key),
        }
    }

    fn handle_normal_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => Ok(true),
            KeyCode::Enter => {
                self.descend_into_selection()?;
                Ok(false)
            }
            KeyCode::Backspace => {
                self.navigate_up()?;
                Ok(false)
            }
            KeyCode::Char('1') => {
                self.switch_root(RootView::ProjectHierarchy)?;
                Ok(false)
            }
            KeyCode::Char('2') => {
                self.switch_root(RootView::CategoryHierarchy)?;
                Ok(false)
            }
            KeyCode::Char('l') => {
                self.ui_state.lens = next_metric_lens(self.ui_state.lens);
                self.reload_view()?;
                Ok(false)
            }
            KeyCode::Tab => {
                self.focused_pane = self.focused_pane.toggle();
                self.ui_state.pane_mode = PaneMode::from_focus(self.focused_pane);
                self.save_state();
                Ok(false)
            }
            KeyCode::Char('t') => {
                self.ui_state.time_window = self.ui_state.time_window.next();
                self.reload_view()?;
                Ok(false)
            }
            KeyCode::Char('m') => {
                self.ui_state.model =
                    cycle_option(self.ui_state.model.clone(), &self.filter_options.models);
                self.reload_view()?;
                Ok(false)
            }
            KeyCode::Char('p') => {
                self.ui_state.project_id =
                    cycle_project(self.ui_state.project_id, &self.filter_options.projects);
                self.reload_view()?;
                Ok(false)
            }
            KeyCode::Char('c') => {
                self.ui_state.action_category = cycle_option(
                    self.ui_state.action_category.clone(),
                    &self.filter_options.categories,
                );
                self.reload_view()?;
                Ok(false)
            }
            KeyCode::Char('a') => {
                self.ui_state.action = cycle_action(
                    self.ui_state.action.clone(),
                    &self.filter_options.actions,
                    self.ui_state.action_category.as_deref(),
                );
                self.reload_view()?;
                Ok(false)
            }
            KeyCode::Char('0') => {
                self.ui_state.clear_filters();
                self.reload_view()?;
                Ok(false)
            }
            KeyCode::Char('/') => {
                self.input_mode = InputMode::FilterInput;
                Ok(false)
            }
            KeyCode::Char('g') => {
                self.input_mode = InputMode::JumpInput;
                self.jump_state.query.clear();
                self.update_jump_matches()?;
                Ok(false)
            }
            KeyCode::Char('b') => {
                self.open_breadcrumb_picker();
                Ok(false)
            }
            KeyCode::Char('r') => {
                self.refresh_snapshot()?;
                Ok(false)
            }
            KeyCode::Char('o') => {
                self.input_mode = InputMode::ColumnChooser;
                Ok(false)
            }
            _ if self.focused_pane == PaneFocus::Table => self.handle_table_navigation_key(key),
            _ => self.handle_radial_navigation_key(key),
        }
    }

    fn handle_table_navigation_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::PageUp => self.move_selection(-10),
            KeyCode::PageDown => self.move_selection(10),
            KeyCode::Home => self.select_first(),
            KeyCode::End => self.select_last(),
            KeyCode::Right => self.descend_into_selection()?,
            KeyCode::Left => self.navigate_up()?,
            _ => {}
        }

        Ok(false)
    }

    fn handle_radial_navigation_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Left | KeyCode::Up | KeyCode::Char('h') | KeyCode::Char('k') => {
                self.move_selection(-1)
            }
            KeyCode::Right | KeyCode::Down | KeyCode::Char('l') | KeyCode::Char('j') => {
                self.move_selection(1)
            }
            KeyCode::Home => self.select_first(),
            KeyCode::End => self.select_last(),
            _ => {}
        }

        Ok(false)
    }

    fn handle_filter_input(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => {
                self.input_mode = InputMode::Normal;
                self.save_state();
            }
            KeyCode::Backspace => {
                self.ui_state.row_filter.pop();
                self.apply_row_filter();
                self.save_state();
            }
            KeyCode::Char(ch) => {
                self.ui_state.row_filter.push(ch);
                self.apply_row_filter();
                self.save_state();
            }
            _ => {}
        }

        Ok(false)
    }

    fn handle_jump_input(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
            }
            KeyCode::Backspace => {
                self.jump_state.query.pop();
                self.update_jump_matches()?;
            }
            KeyCode::Up => {
                if self.jump_state.selected > 0 {
                    self.jump_state.selected -= 1;
                }
            }
            KeyCode::Down => {
                if self.jump_state.selected + 1 < self.jump_state.matches.len() {
                    self.jump_state.selected += 1;
                }
            }
            KeyCode::Enter => {
                if let Some(target) = self
                    .jump_state
                    .matches
                    .get(self.jump_state.selected)
                    .cloned()
                {
                    self.ui_state.root = target.root;
                    self.ui_state.path = target.path;
                    self.input_mode = InputMode::Normal;
                    self.reload_view()?;
                }
            }
            KeyCode::Char(ch) => {
                self.jump_state.query.push(ch);
                self.update_jump_matches()?;
            }
            _ => {}
        }

        Ok(false)
    }

    fn handle_breadcrumb_picker(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc => {
                self.input_mode = InputMode::Normal;
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.move_breadcrumb_selection(-1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.move_breadcrumb_selection(1);
            }
            KeyCode::Left | KeyCode::Char('h') => {
                self.move_breadcrumb_selection(-1);
            }
            KeyCode::Right | KeyCode::Char('l') => {
                self.move_breadcrumb_selection(1);
            }
            KeyCode::Home => {
                if !self.breadcrumb_targets.is_empty() {
                    self.breadcrumb_picker.selected = 0;
                }
            }
            KeyCode::End => {
                if !self.breadcrumb_targets.is_empty() {
                    self.breadcrumb_picker.selected =
                        self.breadcrumb_targets.len().saturating_sub(1);
                }
            }
            KeyCode::Enter => {
                if let Some(target) = self.breadcrumb_targets.get(self.breadcrumb_picker.selected) {
                    self.ui_state.root = target.root;
                    self.ui_state.path = target.path.clone();
                    self.input_mode = InputMode::Normal;
                    self.reload_view()?;
                }
            }
            _ => {}
        }

        Ok(false)
    }

    fn handle_column_key(&mut self, key: KeyEvent) -> Result<bool> {
        let toggled = match key.code {
            KeyCode::Esc | KeyCode::Enter => {
                self.input_mode = InputMode::Normal;
                return Ok(false);
            }
            KeyCode::Char('k') => Some(OptionalColumn::Kind),
            KeyCode::Char('g') => Some(OptionalColumn::GrossInput),
            KeyCode::Char('o') => Some(OptionalColumn::Output),
            KeyCode::Char('t') => Some(OptionalColumn::Total),
            KeyCode::Char('f') => Some(OptionalColumn::Last5Hours),
            KeyCode::Char('w') => Some(OptionalColumn::LastWeek),
            KeyCode::Char('u') => Some(OptionalColumn::UncachedReference),
            KeyCode::Char('i') => Some(OptionalColumn::Items),
            _ => None,
        };

        if let Some(column) = toggled {
            toggle_column(&mut self.ui_state.enabled_columns, column);
            self.save_state();
        }

        Ok(false)
    }

    fn descend_into_selection(&mut self) -> Result<()> {
        let Some(row) = self.selected_row().cloned() else {
            return Ok(());
        };

        if let Some(next_path) = next_browse_path(&self.ui_state.root, &self.ui_state.path, &row) {
            self.ui_state.path = next_path;
            self.reload_view()?;
        } else {
            self.status_message = Some(StatusMessage::info("Reached the current leaf row."));
        }

        Ok(())
    }

    fn open_breadcrumb_picker(&mut self) {
        self.breadcrumb_picker.selected = self.breadcrumb_targets.len().saturating_sub(1);
        self.input_mode = InputMode::BreadcrumbPicker;
    }

    fn move_breadcrumb_selection(&mut self, delta: isize) {
        if self.breadcrumb_targets.is_empty() {
            self.breadcrumb_picker.selected = 0;
            return;
        }

        let current = self.breadcrumb_picker.selected as isize;
        let max_index = self.breadcrumb_targets.len().saturating_sub(1) as isize;
        self.breadcrumb_picker.selected = (current + delta).clamp(0, max_index) as usize;
    }

    fn navigate_up(&mut self) -> Result<()> {
        let project_root = match project_id_from_path(&self.ui_state.path) {
            Some(project_id) => self.project_root_for(project_id)?,
            None => None,
        };

        self.ui_state.path = parent_browse_path(&self.ui_state.path, project_root.as_deref());
        self.reload_view()?;
        Ok(())
    }

    fn switch_root(&mut self, root: RootView) -> Result<()> {
        self.ui_state.root = root;
        self.ui_state.path = BrowsePath::Root;
        self.reload_view()?;
        Ok(())
    }

    fn refresh_snapshot(&mut self) -> Result<()> {
        if !self.has_newer_snapshot {
            self.status_message = Some(StatusMessage::info(
                "No newer published snapshot is available.",
            ));
            return Ok(());
        }

        self.snapshot = self.query_engine().latest_snapshot_bounds()?;
        self.reload_view()?;
        self.refresh_snapshot_coverage()?;
        self.refresh_snapshot_status()?;
        self.status_message = Some(StatusMessage::info(format!(
            "Switched to the newest imported snapshot {}.",
            snapshot_coverage_tail(&self.snapshot)
        )));
        Ok(())
    }

    fn reload_view(&mut self) -> Result<()> {
        let selected_key = self.selected_row().map(|row| row.key.clone());
        self.filter_options = self.query_engine().filter_options(&self.snapshot)?;
        self.sanitize_ui_state();

        let filters = self.current_query_filters()?;
        let mut path = self.ui_state.path.clone();

        loop {
            let rows = self.query_engine().browse(&BrowseRequest {
                snapshot: self.snapshot.clone(),
                root: self.ui_state.root,
                lens: self.ui_state.lens,
                filters: filters.clone(),
                path: path.clone(),
            })?;

            if !rows.is_empty() || matches!(path, BrowsePath::Root) {
                self.ui_state.path = path;
                self.raw_rows = rows;
                break;
            }

            let project_root = match project_id_from_path(&path) {
                Some(project_id) => self.project_root_for(project_id)?,
                None => None,
            };
            let parent = parent_browse_path(&path, project_root.as_deref());
            if parent == path {
                self.raw_rows = rows;
                break;
            }
            path = parent;
        }

        let project_root = match project_id_from_path(&self.ui_state.path) {
            Some(project_id) => self.project_root_for(project_id)?,
            None => None,
        };
        self.breadcrumb_targets = build_breadcrumb_targets(
            &self.ui_state.root,
            &self.ui_state.path,
            &self.filter_options,
            project_root.as_deref(),
        );
        self.breadcrumb_picker.selected = self.breadcrumb_targets.len().saturating_sub(1);
        self.radial_context = self.build_radial_context(&filters)?;
        self.apply_row_filter();
        self.restore_selection(selected_key);
        self.save_state();
        Ok(())
    }

    fn refresh_snapshot_status(&mut self) -> Result<()> {
        self.latest_snapshot = self.query_engine().latest_snapshot_bounds()?;
        self.has_newer_snapshot =
            self.latest_snapshot.max_publish_seq > self.snapshot.max_publish_seq;
        Ok(())
    }

    fn refresh_snapshot_coverage(&mut self) -> Result<()> {
        self.snapshot_coverage = self
            .query_engine()
            .snapshot_coverage_summary(&self.snapshot)?;
        Ok(())
    }

    fn current_query_filters(&self) -> Result<BrowseFilters> {
        Ok(BrowseFilters {
            time_window: self.ui_state.time_window.to_filter(&self.snapshot)?,
            model: self.ui_state.model.clone(),
            project_id: self.ui_state.project_id,
            action_category: self.ui_state.action_category.clone(),
            action: self.ui_state.action.clone(),
        })
    }

    fn sanitize_ui_state(&mut self) {
        if !matches_path_root(&self.ui_state.root, &self.ui_state.path) {
            self.ui_state.path = BrowsePath::Root;
        }

        if self.ui_state.model.as_ref().is_some_and(|model| {
            !self
                .filter_options
                .models
                .iter()
                .any(|candidate| candidate == model)
        }) {
            self.ui_state.model = None;
        }

        if self.ui_state.project_id.is_some_and(|project_id| {
            !self
                .filter_options
                .projects
                .iter()
                .any(|project| project.id == project_id)
        }) {
            self.ui_state.project_id = None;
        }

        if self
            .ui_state
            .action_category
            .as_ref()
            .is_some_and(|category| {
                !self
                    .filter_options
                    .categories
                    .iter()
                    .any(|candidate| candidate == category)
            })
        {
            self.ui_state.action_category = None;
        }

        if self.ui_state.action.as_ref().is_some_and(|action| {
            !self.filter_options.actions.iter().any(|option| {
                option.action == *action
                    && self
                        .ui_state
                        .action_category
                        .as_ref()
                        .is_none_or(|category| option.category == *category)
            })
        }) {
            self.ui_state.action = None;
        }

        normalize_columns(&mut self.ui_state.enabled_columns);
    }

    fn apply_row_filter(&mut self) {
        let selected_key = self.selected_row().map(|row| row.key.clone());
        self.visible_rows = filter_rows(&self.raw_rows, &self.ui_state.row_filter);
        self.restore_selection(selected_key);
    }

    fn restore_selection(&mut self, preferred_key: Option<String>) {
        if self.visible_rows.is_empty() {
            self.table_state.select(None);
            self.rebuild_radial_model();
            return;
        }

        if let Some(preferred_key) = preferred_key
            && let Some(index) = self
                .visible_rows
                .iter()
                .position(|row| row.key == preferred_key)
        {
            self.table_state.select(Some(index));
            self.rebuild_radial_model();
            return;
        }

        let selected = self
            .table_state
            .selected()
            .unwrap_or(0)
            .min(self.visible_rows.len().saturating_sub(1));
        self.table_state.select(Some(selected));
        self.rebuild_radial_model();
    }

    fn selected_row(&self) -> Option<&RollupRow> {
        self.table_state
            .selected()
            .and_then(|index| self.visible_rows.get(index))
    }

    fn select_first(&mut self) {
        if self.visible_rows.is_empty() {
            self.table_state.select(None);
        } else {
            self.table_state.select(Some(0));
        }
        self.rebuild_radial_model();
    }

    fn select_last(&mut self) {
        if self.visible_rows.is_empty() {
            self.table_state.select(None);
        } else {
            self.table_state
                .select(Some(self.visible_rows.len().saturating_sub(1)));
        }
        self.rebuild_radial_model();
    }

    fn move_selection(&mut self, delta: isize) {
        if self.visible_rows.is_empty() {
            self.table_state.select(None);
            self.rebuild_radial_model();
            return;
        }

        let current = self.table_state.selected().unwrap_or(0) as isize;
        let max_index = self.visible_rows.len().saturating_sub(1) as isize;
        let next = (current + delta).clamp(0, max_index) as usize;
        self.table_state.select(Some(next));
        self.rebuild_radial_model();
    }

    fn update_jump_matches(&mut self) -> Result<()> {
        let targets = self.build_jump_targets()?;
        self.jump_state.matches = build_jump_matches(&self.jump_state.query, targets);
        self.jump_state.selected = 0;
        Ok(())
    }

    fn build_jump_targets(&self) -> Result<Vec<JumpTarget>> {
        let filters = self.current_query_filters()?;
        let mut targets = Vec::new();

        let project_root_rows = self.query_engine().browse(&BrowseRequest {
            snapshot: self.snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: self.ui_state.lens,
            filters: filters.clone(),
            path: BrowsePath::Root,
        })?;

        for project in &project_root_rows {
            let Some(project_id) = project.project_id else {
                continue;
            };
            targets.push(JumpTarget {
                label: project.label.clone(),
                detail: "project".to_string(),
                root: RootView::ProjectHierarchy,
                path: BrowsePath::Project { project_id },
            });

            let categories = self.query_engine().browse(&BrowseRequest {
                snapshot: self.snapshot.clone(),
                root: RootView::ProjectHierarchy,
                lens: self.ui_state.lens,
                filters: filters.clone(),
                path: BrowsePath::Project { project_id },
            })?;

            for category in &categories {
                let Some(category_name) = category.category.clone() else {
                    continue;
                };
                targets.push(JumpTarget {
                    label: format!("{} / {}", project.label, category_name),
                    detail: "project category".to_string(),
                    root: RootView::ProjectHierarchy,
                    path: BrowsePath::ProjectCategory {
                        project_id,
                        category: category_name.clone(),
                    },
                });

                let actions = self.query_engine().browse(&BrowseRequest {
                    snapshot: self.snapshot.clone(),
                    root: RootView::ProjectHierarchy,
                    lens: self.ui_state.lens,
                    filters: filters.clone(),
                    path: BrowsePath::ProjectCategory {
                        project_id,
                        category: category_name.clone(),
                    },
                })?;

                for action in actions {
                    let Some(action_key) = action.action.clone() else {
                        continue;
                    };
                    targets.push(JumpTarget {
                        label: format!("{} / {} / {}", project.label, category_name, action.label),
                        detail: "project action".to_string(),
                        root: RootView::ProjectHierarchy,
                        path: BrowsePath::ProjectAction {
                            project_id,
                            category: category_name.clone(),
                            action: action_key,
                            parent_path: None,
                        },
                    });
                }
            }
        }

        let category_root_rows = self.query_engine().browse(&BrowseRequest {
            snapshot: self.snapshot.clone(),
            root: RootView::CategoryHierarchy,
            lens: self.ui_state.lens,
            filters,
            path: BrowsePath::Root,
        })?;

        for category in &category_root_rows {
            let Some(category_name) = category.category.clone() else {
                continue;
            };
            targets.push(JumpTarget {
                label: category_name.clone(),
                detail: "category".to_string(),
                root: RootView::CategoryHierarchy,
                path: BrowsePath::Category {
                    category: category_name.clone(),
                },
            });

            let actions = self.query_engine().browse(&BrowseRequest {
                snapshot: self.snapshot.clone(),
                root: RootView::CategoryHierarchy,
                lens: self.ui_state.lens,
                filters: self.current_query_filters()?,
                path: BrowsePath::Category {
                    category: category_name.clone(),
                },
            })?;

            for action in &actions {
                let Some(action_key) = action.action.clone() else {
                    continue;
                };
                targets.push(JumpTarget {
                    label: format!("{} / {}", category_name, action.label),
                    detail: "category action".to_string(),
                    root: RootView::CategoryHierarchy,
                    path: BrowsePath::CategoryAction {
                        category: category_name.clone(),
                        action: action_key.clone(),
                    },
                });

                let projects = self.query_engine().browse(&BrowseRequest {
                    snapshot: self.snapshot.clone(),
                    root: RootView::CategoryHierarchy,
                    lens: self.ui_state.lens,
                    filters: self.current_query_filters()?,
                    path: BrowsePath::CategoryAction {
                        category: category_name.clone(),
                        action: action_key,
                    },
                })?;

                for project in projects {
                    let Some(project_id) = project.project_id else {
                        continue;
                    };
                    targets.push(JumpTarget {
                        label: format!("{} / {} / {}", category_name, action.label, project.label),
                        detail: "category project".to_string(),
                        root: RootView::CategoryHierarchy,
                        path: BrowsePath::CategoryActionProject {
                            category: category_name.clone(),
                            action: project
                                .action
                                .clone()
                                .unwrap_or_else(|| action_key_from_row(&project)),
                            project_id,
                            parent_path: None,
                        },
                    });
                }
            }
        }

        Ok(targets)
    }

    fn project_root_for(&self, project_id: i64) -> Result<Option<String>> {
        let filters = self.current_query_filters()?;
        let relaxed_filters = BrowseFilters {
            time_window: filters.time_window,
            model: filters.model,
            project_id: None,
            action_category: None,
            action: None,
        };
        let root_rows = self.query_engine().browse(&BrowseRequest {
            snapshot: self.snapshot.clone(),
            root: RootView::ProjectHierarchy,
            lens: self.ui_state.lens,
            filters: relaxed_filters,
            path: BrowsePath::Root,
        })?;

        Ok(root_rows
            .into_iter()
            .find(|row| row.project_id == Some(project_id))
            .and_then(|row| row.full_path))
    }

    fn filter_summary(&self) -> String {
        let mut parts = vec![format!("time {}", self.ui_state.time_window.label())];
        if let Some(model) = &self.ui_state.model {
            parts.push(format!("model {model}"));
        }
        if let Some(project_id) = self.ui_state.project_id {
            let name = self
                .filter_options
                .projects
                .iter()
                .find(|project| project.id == project_id)
                .map(|project| project.display_name.clone())
                .unwrap_or_else(|| format!("#{project_id}"));
            parts.push(format!("project {name}"));
        }
        if let Some(category) = &self.ui_state.action_category {
            parts.push(format!("category {category}"));
        }
        if let Some(action) = &self.ui_state.action {
            parts.push(format!("action {}", action_label(action)));
        }
        if !self.ui_state.row_filter.is_empty() {
            parts.push(format!("row {}", self.ui_state.row_filter));
        }
        parts.join("  |  ")
    }

    fn selection_footer_text(&self) -> String {
        let filter = if self.ui_state.row_filter.is_empty() {
            "(none)".to_string()
        } else {
            self.ui_state.row_filter.clone()
        };
        let focus = self.focused_pane.label();

        let selected = self
            .selected_row()
            .map(|row| {
                format!(
                    "{} ({}, {} {})",
                    row.label,
                    row_kind_label(row.kind),
                    metric_lens_label(self.ui_state.lens),
                    format_metric(row.metrics.lens_value(self.ui_state.lens))
                )
            })
            .unwrap_or_else(|| "none".to_string());

        format!("focus: {focus}  |  selected: {selected}  |  row filter: {filter}")
    }

    fn build_radial_context(&self, filters: &BrowseFilters) -> Result<RadialContext> {
        let project_root = match project_id_from_path(&self.ui_state.path) {
            Some(project_id) => self.project_root_for(project_id)?,
            None => None,
        };
        let steps = radial_ancestor_steps(
            &self.ui_state.root,
            &self.ui_state.path,
            project_root.as_deref(),
        );
        let mut ancestor_layers = Vec::new();
        let mut current_span = RadialSpan::full();

        for step in steps {
            let rows = self.query_engine().browse(&BrowseRequest {
                snapshot: self.snapshot.clone(),
                root: self.ui_state.root,
                lens: self.ui_state.lens,
                filters: filters.clone(),
                path: step.query_path,
            })?;
            let layer = build_radial_layer(
                &rows,
                self.ui_state.lens,
                Some(&step.selected_child),
                current_span,
            );
            current_span = radial_selected_child_span(&layer);
            ancestor_layers.push(layer);
        }

        Ok(RadialContext {
            ancestor_layers,
            current_span,
        })
    }

    fn rebuild_radial_model(&mut self) {
        self.radial_model = build_radial_model(
            &self.radial_context,
            &self.visible_rows,
            self.selected_row(),
            &self.ui_state.root,
            &self.ui_state.path,
            &self.filter_options,
            self.ui_state.lens,
        );
    }

    fn save_state(&mut self) {
        if let Err(error) = self.ui_state.save(&self.ui_state_path) {
            self.status_message = Some(StatusMessage::error(format!(
                "Unable to save TUI state: {error:#}"
            )));
        }
    }

    fn query_engine(&self) -> QueryEngine<'_> {
        QueryEngine::new(self.database.connection())
    }
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;

        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;

        Ok(Self { terminal })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedUiState {
    root: RootView,
    path: BrowsePath,
    lens: MetricLens,
    pane_mode: PaneMode,
    time_window: TimeWindowPreset,
    model: Option<String>,
    project_id: Option<i64>,
    action_category: Option<String>,
    action: Option<ActionKey>,
    row_filter: String,
    enabled_columns: Vec<OptionalColumn>,
}

impl Default for PersistedUiState {
    fn default() -> Self {
        Self {
            root: RootView::ProjectHierarchy,
            path: BrowsePath::Root,
            lens: MetricLens::UncachedInput,
            pane_mode: PaneMode::Table,
            time_window: TimeWindowPreset::All,
            model: None,
            project_id: None,
            action_category: None,
            action: None,
            row_filter: String::new(),
            enabled_columns: default_enabled_columns(),
        }
    }
}

impl PersistedUiState {
    fn load(path: &Path) -> Result<Option<Self>> {
        match fs::read_to_string(path) {
            Ok(raw) => {
                let mut state: Self = serde_json::from_str(&raw)
                    .with_context(|| format!("unable to parse {}", path.display()))?;
                normalize_columns(&mut state.enabled_columns);
                Ok(Some(state))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).with_context(|| format!("unable to read {}", path.display())),
        }
    }

    fn save(&self, path: &Path) -> Result<()> {
        let serialized = serde_json::to_string_pretty(self)
            .context("unable to serialize persisted TUI state")?;
        fs::write(path, serialized).with_context(|| format!("unable to write {}", path.display()))
    }

    fn apply_startup_browse_state(&mut self, startup_browse_state: Option<StartupBrowseState>) {
        match startup_browse_state {
            Some(startup_browse_state) => {
                self.root = startup_browse_state.root;
                self.path = startup_browse_state.path;
            }
            None => {
                self.path = BrowsePath::Root;
            }
        }
    }

    fn clear_filters(&mut self) {
        self.time_window = TimeWindowPreset::All;
        self.model = None;
        self.project_id = None;
        self.action_category = None;
        self.action = None;
        self.row_filter.clear();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum PaneMode {
    Table,
    #[serde(alias = "Details")]
    Radial,
}

impl PaneMode {
    fn from_focus(focus: PaneFocus) -> Self {
        match focus {
            PaneFocus::Table => Self::Table,
            PaneFocus::Radial => Self::Radial,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PaneFocus {
    Table,
    Radial,
}

impl PaneFocus {
    fn toggle(self) -> Self {
        match self {
            Self::Table => Self::Radial,
            Self::Radial => Self::Table,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Radial => "radial",
        }
    }

    fn from_pane_mode(mode: PaneMode) -> Self {
        match mode {
            PaneMode::Table => Self::Table,
            PaneMode::Radial => Self::Radial,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RadialContext {
    ancestor_layers: Vec<RadialLayer>,
    current_span: RadialSpan,
}

#[derive(Debug, Clone, Default)]
struct RadialModel {
    center: RadialCenter,
    layers: Vec<RadialLayer>,
}

#[derive(Debug, Clone, Default)]
struct RadialCenter {
    scope_label: String,
    lens_label: String,
    selection_label: String,
}

#[derive(Debug, Clone, Default)]
struct RadialLayer {
    span: RadialSpan,
    segments: Vec<RadialSegment>,
    total_value: f64,
}

#[derive(Debug, Clone, Copy)]
struct RadialSpan {
    start: f64,
    sweep: f64,
}

impl Default for RadialSpan {
    fn default() -> Self {
        Self::full()
    }
}

impl RadialSpan {
    fn full() -> Self {
        Self {
            start: 0.0,
            sweep: TAU,
        }
    }

    fn child_span(self, start_offset: f64, sweep: f64) -> Self {
        Self {
            start: (self.start + start_offset).rem_euclid(TAU),
            sweep,
        }
    }

    fn local_angle(self, angle: f64) -> Option<f64> {
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
struct RadialSegment {
    value: f64,
    cached_ratio: f64,
    bucket: RadialBucket,
    is_selected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RadialBucket {
    Project,
    Category,
    Classified,
    Mixed,
    Unclassified,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RadialAncestorStep {
    query_path: BrowsePath,
    selected_child: String,
}

struct RadialPane<'a> {
    model: &'a RadialModel,
    focused: bool,
}

impl Widget for &RadialPane<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = pane_block("Radial", self.focused);
        let inner = block.inner(area);
        block.render(area, buf);

        if inner.width < 12 || inner.height < 8 {
            return;
        }

        let layer_count = self.model.layers.len();
        if layer_count > 0 {
            let center_x = f64::from(inner.x) + f64::from(inner.width) / 2.0;
            let center_y = f64::from(inner.y) + f64::from(inner.height) / 2.0;
            let radius_x = (f64::from(inner.width) / 2.0).max(1.0);
            let radius_y = (f64::from(inner.height) / 2.0).max(1.0);
            let ring_band = (0.96 - RADIAL_CENTER_RADIUS) / layer_count as f64;

            for y in inner.y..inner.y + inner.height {
                for x in inner.x..inner.x + inner.width {
                    let normalized_x = (f64::from(x) + 0.5 - center_x) / radius_x;
                    let normalized_y = (f64::from(y) + 0.5 - center_y) / radius_y;
                    let radius = (normalized_x.powi(2) + normalized_y.powi(2)).sqrt();

                    if !(RADIAL_CENTER_RADIUS..=0.96).contains(&radius) {
                        continue;
                    }

                    let layer_index = ((radius - RADIAL_CENTER_RADIUS) / ring_band)
                        .floor()
                        .clamp(0.0, (layer_count - 1) as f64)
                        as usize;
                    let Some(layer) = self.model.layers.get(layer_index) else {
                        continue;
                    };
                    let Some(segment) = radial_segment_at_angle(
                        layer,
                        (normalized_y.atan2(normalized_x) + FRAC_PI_2).rem_euclid(TAU),
                    ) else {
                        continue;
                    };

                    if let Some(cell) = buf.cell_mut((x, y)) {
                        cell.set_style(radial_segment_style(segment, self.focused));
                        cell.set_char(radial_texture(segment));
                    }
                }
            }
        }

        let center_area = radial_center_label_area(inner);
        let center_lines =
            radial_center_label_lines(&self.model.center, center_area.width, center_area.height);
        Paragraph::new(Text::from(center_lines))
            .alignment(Alignment::Center)
            .render(center_area, buf);
    }
}

fn radial_center_label_area(inner: Rect) -> Rect {
    let center_width = radial_center_extent(inner.width);
    let center_height = radial_center_extent(inner.height).min(3);
    Rect::new(
        inner.x + inner.width.saturating_sub(center_width) / 2,
        inner.y + inner.height.saturating_sub(center_height) / 2,
        center_width,
        center_height,
    )
}

fn radial_center_extent(dimension: u16) -> u16 {
    if dimension == 0 {
        return 0;
    }

    let extent = (f64::from(dimension) * RADIAL_CENTER_RADIUS).floor() as u16;
    extent.clamp(1, dimension)
}

fn radial_center_label_lines(center: &RadialCenter, width: u16, height: u16) -> Vec<Line<'static>> {
    let max_width = usize::from(width);
    let line_count = usize::from(height).min(3);
    let lens_line = format!("lens: {}  |  texture: uncached->cached", center.lens_label);
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

    let char_count = text.chars().count();
    if char_count <= max_width {
        return text.to_string();
    }

    if max_width <= 3 {
        return ".".repeat(max_width);
    }

    let mut truncated = text.chars().take(max_width - 3).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn pane_block(title: &str, focused: bool) -> Block<'static> {
    let mut block = Block::default()
        .borders(Borders::ALL)
        .title(title.to_string());
    if focused {
        block = block.border_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    }

    block
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum TimeWindowPreset {
    All,
    Last5Hours,
    Last24Hours,
    LastWeek,
}

impl TimeWindowPreset {
    fn next(self) -> Self {
        match self {
            Self::All => Self::Last5Hours,
            Self::Last5Hours => Self::Last24Hours,
            Self::Last24Hours => Self::LastWeek,
            Self::LastWeek => Self::All,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Last5Hours => "last 5h",
            Self::Last24Hours => "last 24h",
            Self::LastWeek => "last 1w",
        }
    }

    fn to_filter(self, snapshot: &SnapshotBounds) -> Result<Option<TimeWindowFilter>> {
        let Some(upper_bound) = snapshot.upper_bound_utc.as_deref() else {
            return Ok(None);
        };
        let upper_bound = snapshot
            .upper_bound_timestamp()?
            .with_context(|| format!("unable to parse snapshot upper bound {upper_bound}"))?;

        let start = match self {
            Self::All => return Ok(None),
            Self::Last5Hours => upper_bound.checked_sub(5.hours()).ok(),
            Self::Last24Hours => upper_bound.checked_sub(24.hours()).ok(),
            Self::LastWeek => upper_bound.checked_sub(168.hours()).ok(),
        };

        Ok(start.map(|start| TimeWindowFilter {
            start_at_utc: Some(start.to_string()),
            end_at_utc: None,
        }))
    }
}

fn snapshot_summary_text(snapshot: &SnapshotBounds, has_newer_snapshot: bool) -> String {
    if snapshot.is_bootstrap() {
        return if has_newer_snapshot {
            "snapshot: no imported data is visible yet (newer data is ready)".to_string()
        } else {
            "snapshot: no imported data is visible yet".to_string()
        };
    }

    if has_newer_snapshot {
        format!(
            "snapshot: showing imported data {} (newer data is ready)",
            snapshot_coverage_tail(snapshot)
        )
    } else {
        format!(
            "snapshot: showing imported data {}",
            snapshot_coverage_tail(snapshot)
        )
    }
}

fn view_line(targets: &[BreadcrumbTarget]) -> Line<'static> {
    if targets.is_empty() {
        return Line::from("view: none");
    }

    let mut spans = vec![Span::styled(
        "view: ",
        Style::default().add_modifier(Modifier::BOLD),
    )];
    for (index, target) in targets.iter().enumerate() {
        if index > 0 {
            spans.push(Span::styled(" > ", Style::default().fg(Color::DarkGray)));
        }

        let style = if index + 1 == targets.len() {
            Style::default().add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        spans.push(Span::styled(target.label.clone(), style));
    }

    Line::from(spans)
}

fn snapshot_coverage_tail(snapshot: &SnapshotBounds) -> String {
    match snapshot.upper_bound_timestamp().ok().flatten() {
        Some(timestamp) => format!("through {timestamp}"),
        None => snapshot
            .upper_bound_utc
            .as_deref()
            .map(|upper_bound| format!("through {upper_bound}"))
            .unwrap_or_else(|| "with an unknown upper bound".to_string()),
    }
}

fn snapshot_refresh_text(
    snapshot: &SnapshotBounds,
    _latest_snapshot: &SnapshotBounds,
    startup_open_reason: StartupOpenReason,
    has_newer_snapshot: bool,
) -> String {
    if has_newer_snapshot {
        return if snapshot.is_bootstrap() {
            "refresh: imported data is ready. Press r to load it; this empty view stays in place until you refresh."
                .to_string()
        } else {
            "refresh: newer imported data is ready. Press r to switch to it; this view stays pinned until you refresh."
                .to_string()
        };
    }

    match startup_open_reason {
        StartupOpenReason::Last24hReady => {
            "refresh: manual only. Background import never changes the visible view until you press r."
                .to_string()
        }
        StartupOpenReason::TimedOut => {
            "refresh: manual only. Startup opened before the last 24 hours finished importing, so this snapshot may still be partial."
                .to_string()
        }
    }
}

fn snapshot_coverage_footer_text(summary: &SnapshotCoverageSummary) -> String {
    if summary == &SnapshotCoverageSummary::default() {
        return "coverage: no imported data is visible yet".to_string();
    }

    format!(
        "coverage: {}, {} across {}, {}, {}",
        count_label(summary.project_count, "project"),
        count_label(summary.project_day_count, "project-day"),
        count_label(summary.day_count, "day"),
        count_label(summary.session_count, "session"),
        count_label(summary.turn_count, "turn"),
    )
}

fn count_label(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("1 {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputMode {
    Normal,
    FilterInput,
    JumpInput,
    BreadcrumbPicker,
    ColumnChooser,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum OptionalColumn {
    Kind,
    GrossInput,
    Output,
    Total,
    Last5Hours,
    LastWeek,
    UncachedReference,
    Items,
}

impl OptionalColumn {
    fn priority(self) -> usize {
        match self {
            Self::Kind => 0,
            Self::Last5Hours => 1,
            Self::LastWeek => 2,
            Self::UncachedReference => 3,
            Self::GrossInput => 4,
            Self::Output => 5,
            Self::Total => 6,
            Self::Items => 7,
        }
    }

    fn short_label(self) -> &'static str {
        match self {
            Self::Kind => "kind",
            Self::GrossInput => "gross",
            Self::Output => "output",
            Self::Total => "total",
            Self::Last5Hours => "5h",
            Self::LastWeek => "1w",
            Self::UncachedReference => "ref",
            Self::Items => "items",
        }
    }
}

#[derive(Debug, Default)]
struct JumpState {
    query: String,
    matches: Vec<JumpTarget>,
    selected: usize,
}

#[derive(Debug, Clone)]
struct JumpTarget {
    label: String,
    detail: String,
    root: RootView,
    path: BrowsePath,
}

#[derive(Debug, Clone)]
struct BreadcrumbTarget {
    label: String,
    display: String,
    root: RootView,
    path: BrowsePath,
}

#[derive(Debug, Default)]
struct BreadcrumbPickerState {
    selected: usize,
}

#[derive(Debug, Clone)]
struct StatusMessage {
    text: String,
    tone: StatusTone,
}

impl StatusMessage {
    fn info(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            tone: StatusTone::Info,
        }
    }

    fn error(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            tone: StatusTone::Error,
        }
    }
}

fn compact_status_text(text: impl AsRef<str>) -> String {
    text.as_ref()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Debug, Clone, Copy)]
enum StatusTone {
    Info,
    Error,
}

#[derive(Debug, Clone)]
struct ColumnSpec {
    key: ColumnKey,
    title: String,
    constraint: Constraint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnKey {
    Label,
    SelectedLens,
    Optional(OptionalColumn),
}

fn default_enabled_columns() -> Vec<OptionalColumn> {
    vec![
        OptionalColumn::Kind,
        OptionalColumn::Last5Hours,
        OptionalColumn::LastWeek,
        OptionalColumn::UncachedReference,
        OptionalColumn::GrossInput,
        OptionalColumn::Output,
        OptionalColumn::Items,
    ]
}

fn normalize_columns(columns: &mut Vec<OptionalColumn>) {
    columns.sort_by_key(|column| column.priority());
    columns.dedup();
}

fn toggle_column(columns: &mut Vec<OptionalColumn>, column: OptionalColumn) {
    if let Some(index) = columns.iter().position(|candidate| *candidate == column) {
        columns.remove(index);
    } else {
        columns.push(column);
        normalize_columns(columns);
    }
}

fn toggle_mark(columns: &[OptionalColumn], column: OptionalColumn) -> &'static str {
    if columns.contains(&column) { "x" } else { " " }
}

fn enabled_column_summary(columns: &[OptionalColumn]) -> String {
    if columns.is_empty() {
        return "(none)".to_string();
    }

    columns
        .iter()
        .map(|column| column.short_label())
        .collect::<Vec<_>>()
        .join(", ")
}

fn active_columns(
    width: u16,
    lens: MetricLens,
    enabled_columns: &[OptionalColumn],
) -> Vec<ColumnSpec> {
    let mut columns = vec![
        ColumnSpec {
            key: ColumnKey::Label,
            title: "label".to_string(),
            constraint: Constraint::Min(24),
        },
        ColumnSpec {
            key: ColumnKey::SelectedLens,
            title: metric_lens_label(lens).to_string(),
            constraint: Constraint::Length(11),
        },
    ];

    let mut used_width = 35;
    let optional_specs = enabled_columns
        .iter()
        .map(optional_column_spec)
        .collect::<Vec<_>>();

    for spec in optional_specs {
        let column_width = match spec.constraint {
            Constraint::Length(length) => length,
            _ => 10,
        };
        if used_width + column_width + 1 > width {
            continue;
        }
        used_width += column_width + 1;
        columns.push(spec);
    }

    columns
}

fn optional_column_spec(column: &OptionalColumn) -> ColumnSpec {
    match column {
        OptionalColumn::Kind => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::Kind),
            title: "kind".to_string(),
            constraint: Constraint::Length(9),
        },
        OptionalColumn::GrossInput => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::GrossInput),
            title: "gross".to_string(),
            constraint: Constraint::Length(10),
        },
        OptionalColumn::Output => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::Output),
            title: "output".to_string(),
            constraint: Constraint::Length(10),
        },
        OptionalColumn::Total => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::Total),
            title: "total".to_string(),
            constraint: Constraint::Length(10),
        },
        OptionalColumn::Last5Hours => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::Last5Hours),
            title: "5h".to_string(),
            constraint: Constraint::Length(8),
        },
        OptionalColumn::LastWeek => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::LastWeek),
            title: "1w".to_string(),
            constraint: Constraint::Length(8),
        },
        OptionalColumn::UncachedReference => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::UncachedReference),
            title: "ref".to_string(),
            constraint: Constraint::Length(10),
        },
        OptionalColumn::Items => ColumnSpec {
            key: ColumnKey::Optional(OptionalColumn::Items),
            title: "items".to_string(),
            constraint: Constraint::Length(7),
        },
    }
}

fn render_column_value(
    column: ColumnKey,
    row: &RollupRow,
    lens: MetricLens,
    root: &RootView,
    current_path: &BrowsePath,
) -> String {
    match column {
        ColumnKey::Label => {
            format!(
                "{}{}",
                drillability_glyph(root, current_path, row),
                row.label
            )
        }
        ColumnKey::SelectedLens => format_metric(row.metrics.lens_value(lens)),
        ColumnKey::Optional(OptionalColumn::Kind) => row_kind_label(row.kind).to_string(),
        ColumnKey::Optional(OptionalColumn::GrossInput) => format_metric(row.metrics.gross_input),
        ColumnKey::Optional(OptionalColumn::Output) => format_metric(row.metrics.output),
        ColumnKey::Optional(OptionalColumn::Total) => format_metric(row.metrics.total),
        ColumnKey::Optional(OptionalColumn::Last5Hours) => {
            format_metric(row.indicators.selected_lens_last_5_hours)
        }
        ColumnKey::Optional(OptionalColumn::LastWeek) => {
            format_metric(row.indicators.selected_lens_last_week)
        }
        ColumnKey::Optional(OptionalColumn::UncachedReference) => {
            format_metric(row.indicators.uncached_input_reference)
        }
        ColumnKey::Optional(OptionalColumn::Items) => row.item_count.to_string(),
    }
}

fn drillability_glyph(root: &RootView, current_path: &BrowsePath, row: &RollupRow) -> &'static str {
    if next_browse_path(root, current_path, row).is_some() {
        "> "
    } else {
        "  "
    }
}

fn metric_lens_label(lens: MetricLens) -> &'static str {
    match lens {
        MetricLens::UncachedInput => "uncached",
        MetricLens::GrossInput => "gross",
        MetricLens::Output => "output",
        MetricLens::Total => "total",
    }
}

fn next_metric_lens(lens: MetricLens) -> MetricLens {
    match lens {
        MetricLens::UncachedInput => MetricLens::GrossInput,
        MetricLens::GrossInput => MetricLens::Output,
        MetricLens::Output => MetricLens::Total,
        MetricLens::Total => MetricLens::UncachedInput,
    }
}

fn row_kind_label(kind: RollupRowKind) -> &'static str {
    match kind {
        RollupRowKind::Project => "project",
        RollupRowKind::ActionCategory => "category",
        RollupRowKind::Action => "action",
        RollupRowKind::Directory => "dir",
        RollupRowKind::File => "file",
    }
}

fn build_radial_model(
    context: &RadialContext,
    visible_rows: &[RollupRow],
    selected_row: Option<&RollupRow>,
    root: &RootView,
    path: &BrowsePath,
    filter_options: &FilterOptions,
    lens: MetricLens,
) -> RadialModel {
    let mut layers = context.ancestor_layers.clone();
    if !visible_rows.is_empty() {
        layers.push(build_radial_layer(
            visible_rows,
            lens,
            selected_row.map(|row| row.key.as_str()),
            context.current_span,
        ));
    }

    let selection_label = selected_row
        .map(|row| {
            format!(
                "selected: {} ({}, {} {})",
                row.label,
                row_kind_label(row.kind),
                metric_lens_label(lens),
                format_metric(row.metrics.lens_value(lens))
            )
        })
        .unwrap_or_else(|| "selected: none".to_string());

    RadialModel {
        center: RadialCenter {
            scope_label: describe_browse_path(root, path, filter_options),
            lens_label: metric_lens_label(lens).to_string(),
            selection_label,
        },
        layers,
    }
}

fn build_radial_layer(
    rows: &[RollupRow],
    lens: MetricLens,
    selected_key: Option<&str>,
    span: RadialSpan,
) -> RadialLayer {
    let segments = rows
        .iter()
        .map(|row| RadialSegment {
            value: row.metrics.lens_value(lens),
            cached_ratio: cached_ratio(row),
            bucket: radial_bucket(row),
            is_selected: selected_key.is_some_and(|key| key == row.key),
        })
        .collect::<Vec<_>>();
    let total_value = segments.iter().map(|segment| segment.value.max(0.0)).sum();

    RadialLayer {
        span,
        segments,
        total_value,
    }
}

fn radial_ancestor_steps(
    root: &RootView,
    path: &BrowsePath,
    project_root: Option<&str>,
) -> Vec<RadialAncestorStep> {
    match (root, path) {
        (_, BrowsePath::Root) => Vec::new(),
        (RootView::ProjectHierarchy, BrowsePath::Project { project_id }) => {
            vec![RadialAncestorStep {
                query_path: BrowsePath::Root,
                selected_child: project_row_key(*project_id),
            }]
        }
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectCategory {
                project_id,
                category,
            },
        ) => vec![
            RadialAncestorStep {
                query_path: BrowsePath::Root,
                selected_child: project_row_key(*project_id),
            },
            RadialAncestorStep {
                query_path: BrowsePath::Project {
                    project_id: *project_id,
                },
                selected_child: category_row_key(category),
            },
        ],
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectAction {
                project_id,
                category,
                action,
                parent_path,
            },
        ) => {
            let mut steps = vec![
                RadialAncestorStep {
                    query_path: BrowsePath::Root,
                    selected_child: project_row_key(*project_id),
                },
                RadialAncestorStep {
                    query_path: BrowsePath::Project {
                        project_id: *project_id,
                    },
                    selected_child: category_row_key(category),
                },
                RadialAncestorStep {
                    query_path: BrowsePath::ProjectCategory {
                        project_id: *project_id,
                        category: category.clone(),
                    },
                    selected_child: action_row_key(action),
                },
            ];
            steps.extend(project_path_ancestor_steps(
                *project_id,
                category,
                action,
                parent_path.as_deref(),
                project_root,
            ));
            steps
        }
        (RootView::CategoryHierarchy, BrowsePath::Category { category }) => {
            vec![RadialAncestorStep {
                query_path: BrowsePath::Root,
                selected_child: category_row_key(category),
            }]
        }
        (RootView::CategoryHierarchy, BrowsePath::CategoryAction { category, action }) => vec![
            RadialAncestorStep {
                query_path: BrowsePath::Root,
                selected_child: category_row_key(category),
            },
            RadialAncestorStep {
                query_path: BrowsePath::Category {
                    category: category.clone(),
                },
                selected_child: action_row_key(action),
            },
        ],
        (
            RootView::CategoryHierarchy,
            BrowsePath::CategoryActionProject {
                category,
                action,
                project_id,
                parent_path,
            },
        ) => {
            let mut steps = vec![
                RadialAncestorStep {
                    query_path: BrowsePath::Root,
                    selected_child: category_row_key(category),
                },
                RadialAncestorStep {
                    query_path: BrowsePath::Category {
                        category: category.clone(),
                    },
                    selected_child: action_row_key(action),
                },
                RadialAncestorStep {
                    query_path: BrowsePath::CategoryAction {
                        category: category.clone(),
                        action: action.clone(),
                    },
                    selected_child: project_row_key(*project_id),
                },
            ];
            steps.extend(category_project_path_ancestor_steps(
                category,
                action,
                *project_id,
                parent_path.as_deref(),
                project_root,
            ));
            steps
        }
        _ => Vec::new(),
    }
}

fn project_path_ancestor_steps(
    project_id: i64,
    category: &str,
    action: &ActionKey,
    parent_path: Option<&str>,
    project_root: Option<&str>,
) -> Vec<RadialAncestorStep> {
    let Some(project_root) = project_root else {
        return Vec::new();
    };
    let Some(parent_path) = parent_path else {
        return Vec::new();
    };

    let mut query_parent = None;
    path_chain(project_root, parent_path)
        .into_iter()
        .map(|selected_path| {
            let step = RadialAncestorStep {
                query_path: BrowsePath::ProjectAction {
                    project_id,
                    category: category.to_string(),
                    action: action.clone(),
                    parent_path: query_parent.clone(),
                },
                selected_child: path_row_key(&selected_path),
            };
            query_parent = Some(selected_path);
            step
        })
        .collect()
}

fn category_project_path_ancestor_steps(
    category: &str,
    action: &ActionKey,
    project_id: i64,
    parent_path: Option<&str>,
    project_root: Option<&str>,
) -> Vec<RadialAncestorStep> {
    let Some(project_root) = project_root else {
        return Vec::new();
    };
    let Some(parent_path) = parent_path else {
        return Vec::new();
    };

    let mut query_parent = None;
    path_chain(project_root, parent_path)
        .into_iter()
        .map(|selected_path| {
            let step = RadialAncestorStep {
                query_path: BrowsePath::CategoryActionProject {
                    category: category.to_string(),
                    action: action.clone(),
                    project_id,
                    parent_path: query_parent.clone(),
                },
                selected_child: path_row_key(&selected_path),
            };
            query_parent = Some(selected_path);
            step
        })
        .collect()
}

fn path_chain(project_root: &str, parent_path: &str) -> Vec<String> {
    let project_root = Path::new(project_root);
    let parent_path = Path::new(parent_path);
    let Ok(relative_path) = parent_path.strip_prefix(project_root) else {
        return Vec::new();
    };

    let mut current = project_root.to_path_buf();
    let mut paths = Vec::new();
    for component in relative_path.components() {
        current.push(component.as_os_str());
        paths.push(current.to_string_lossy().to_string());
    }

    paths
}

fn radial_segment_at_angle(layer: &RadialLayer, angle: f64) -> Option<&RadialSegment> {
    let local_angle = layer.span.local_angle(angle)?;
    radial_segment_at_local_angle(layer, local_angle)
}

fn radial_segment_at_local_angle(layer: &RadialLayer, local_angle: f64) -> Option<&RadialSegment> {
    if layer.segments.is_empty() {
        return None;
    }

    let total_weight = if layer.total_value > 0.0 {
        layer.total_value
    } else {
        layer.segments.len() as f64
    };

    let mut cursor = 0.0;
    for (index, segment) in layer.segments.iter().enumerate() {
        let weight = if layer.total_value > 0.0 {
            segment.value.max(0.0)
        } else {
            1.0
        };
        let next = cursor + (weight / total_weight) * layer.span.sweep;
        if local_angle < next || index + 1 == layer.segments.len() {
            return Some(segment);
        }
        cursor = next;
    }

    None
}

fn radial_selected_child_span(layer: &RadialLayer) -> RadialSpan {
    if layer.segments.is_empty() {
        return layer.span;
    }

    let total_weight = if layer.total_value > 0.0 {
        layer.total_value
    } else {
        layer.segments.len() as f64
    };

    let mut cursor = 0.0;
    for (index, segment) in layer.segments.iter().enumerate() {
        let weight = if layer.total_value > 0.0 {
            segment.value.max(0.0)
        } else {
            1.0
        };
        let next = cursor + (weight / total_weight) * layer.span.sweep;
        if segment.is_selected {
            return layer.span.child_span(cursor, next - cursor);
        }
        cursor = next;
        if index + 1 == layer.segments.len() {
            break;
        }
    }

    layer.span
}

fn radial_segment_style(segment: &RadialSegment, focused: bool) -> Style {
    let mut style = Style::default()
        .bg(radial_bucket_color(segment.bucket))
        .fg(Color::Black);

    if segment.is_selected {
        style = style.fg(Color::White).add_modifier(Modifier::BOLD);
        if focused {
            style = style.add_modifier(Modifier::UNDERLINED);
        }
    }

    style
}

fn radial_texture(segment: &RadialSegment) -> char {
    if segment.is_selected {
        return '#';
    }

    match segment.cached_ratio {
        ratio if ratio >= 0.75 => '*',
        ratio if ratio >= 0.45 => ';',
        ratio if ratio >= 0.2 => ':',
        ratio if ratio > 0.0 => '.',
        _ => ' ',
    }
}

fn radial_bucket(row: &RollupRow) -> RadialBucket {
    match row
        .action
        .as_ref()
        .map(|action| action.classification_state)
    {
        Some(ClassificationState::Classified) => RadialBucket::Classified,
        Some(ClassificationState::Mixed) => RadialBucket::Mixed,
        Some(ClassificationState::Unclassified) => RadialBucket::Unclassified,
        None => match row.kind {
            RollupRowKind::Project => RadialBucket::Project,
            RollupRowKind::ActionCategory => RadialBucket::Category,
            RollupRowKind::Action => RadialBucket::Unclassified,
            RollupRowKind::Directory | RollupRowKind::File => RadialBucket::Project,
        },
    }
}

fn radial_bucket_color(bucket: RadialBucket) -> Color {
    match bucket {
        RadialBucket::Project => Color::LightBlue,
        RadialBucket::Category => Color::LightCyan,
        RadialBucket::Classified => Color::LightGreen,
        RadialBucket::Mixed => Color::LightYellow,
        RadialBucket::Unclassified => Color::Gray,
    }
}

fn cached_ratio(row: &RollupRow) -> f64 {
    if row.metrics.gross_input <= 0.0 {
        0.0
    } else {
        (row.metrics.cached_input / row.metrics.gross_input).clamp(0.0, 1.0)
    }
}

fn project_row_key(project_id: i64) -> String {
    format!("project:{project_id}")
}

fn category_row_key(category: &str) -> String {
    format!("category:{category}")
}

fn action_row_key(action: &ActionKey) -> String {
    format!("action:{}", stable_action_key(action))
}

fn path_row_key(path: &str) -> String {
    format!("path:{path}")
}

fn stable_action_key(action: &ActionKey) -> String {
    [
        classification_state_key(action.classification_state),
        action.normalized_action.as_deref().unwrap_or_default(),
        action.command_family.as_deref().unwrap_or_default(),
        action.base_command.as_deref().unwrap_or_default(),
    ]
    .join("|")
}

fn classification_state_key(state: ClassificationState) -> &'static str {
    match state {
        ClassificationState::Classified => "classified",
        ClassificationState::Mixed => "mixed",
        ClassificationState::Unclassified => "unclassified",
    }
}

fn filter_rows(rows: &[RollupRow], filter: &str) -> Vec<RollupRow> {
    let needle = filter.trim().to_ascii_lowercase();
    if needle.is_empty() {
        return rows.to_vec();
    }

    rows.iter()
        .filter(|row| row_search_text(row).contains(&needle))
        .cloned()
        .collect()
}

fn row_search_text(row: &RollupRow) -> String {
    let mut parts = vec![row.label.to_ascii_lowercase()];
    if let Some(category) = &row.category {
        parts.push(category.to_ascii_lowercase());
    }
    if let Some(action) = &row.action {
        parts.push(action_label(action).to_ascii_lowercase());
    }
    if let Some(path) = &row.full_path {
        parts.push(path.to_ascii_lowercase());
    }
    parts.join(" ")
}

fn build_jump_matches(query: &str, targets: Vec<JumpTarget>) -> Vec<JumpTarget> {
    if query.trim().is_empty() {
        return targets.into_iter().take(JUMP_MATCH_LIMIT).collect();
    }

    let mut matcher = Matcher::new(MatcherConfig::DEFAULT.match_paths());
    let pattern = Pattern::parse(query, CaseMatching::Ignore, Normalization::Smart);
    let indexed = targets
        .iter()
        .enumerate()
        .map(|(index, target)| JumpMatcherCandidate {
            index,
            haystack: format!("{} {}", target.label, target.detail),
        })
        .collect::<Vec<_>>();

    pattern
        .match_list(indexed, &mut matcher)
        .into_iter()
        .take(JUMP_MATCH_LIMIT)
        .filter_map(|(candidate, _)| targets.get(candidate.index).cloned())
        .collect()
}

#[derive(Debug, Clone)]
struct JumpMatcherCandidate {
    index: usize,
    haystack: String,
}

impl AsRef<str> for JumpMatcherCandidate {
    fn as_ref(&self) -> &str {
        &self.haystack
    }
}

fn cycle_option(current: Option<String>, options: &[String]) -> Option<String> {
    if options.is_empty() {
        return None;
    }

    match current {
        None => options.first().cloned(),
        Some(current) => {
            let Some(index) = options.iter().position(|option| *option == current) else {
                return options.first().cloned();
            };
            if index + 1 >= options.len() {
                None
            } else {
                options.get(index + 1).cloned()
            }
        }
    }
}

fn cycle_project(
    current: Option<i64>,
    options: &[gnomon_core::query::ProjectFilterOption],
) -> Option<i64> {
    if options.is_empty() {
        return None;
    }

    match current {
        None => options.first().map(|project| project.id),
        Some(current) => {
            let Some(index) = options.iter().position(|project| project.id == current) else {
                return options.first().map(|project| project.id);
            };
            if index + 1 >= options.len() {
                None
            } else {
                options.get(index + 1).map(|project| project.id)
            }
        }
    }
}

fn cycle_action(
    current: Option<ActionKey>,
    options: &[gnomon_core::query::ActionFilterOption],
    category: Option<&str>,
) -> Option<ActionKey> {
    let filtered = options
        .iter()
        .filter(|option| category.is_none_or(|category| option.category == category))
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        return None;
    }

    match current {
        None => filtered.first().map(|option| option.action.clone()),
        Some(current) => {
            let Some(index) = filtered.iter().position(|option| option.action == current) else {
                return filtered.first().map(|option| option.action.clone());
            };
            if index + 1 >= filtered.len() {
                None
            } else {
                filtered.get(index + 1).map(|option| option.action.clone())
            }
        }
    }
}

fn action_label(action: &ActionKey) -> String {
    match action.classification_state {
        gnomon_core::query::ClassificationState::Classified => action
            .normalized_action
            .clone()
            .or_else(|| action.base_command.clone())
            .or_else(|| action.command_family.clone())
            .unwrap_or_else(|| "classified".to_string()),
        gnomon_core::query::ClassificationState::Mixed => "mixed".to_string(),
        gnomon_core::query::ClassificationState::Unclassified => "unclassified".to_string(),
    }
}

fn action_key_from_row(row: &RollupRow) -> ActionKey {
    row.action.clone().unwrap_or_else(|| ActionKey {
        classification_state: gnomon_core::query::ClassificationState::Unclassified,
        normalized_action: Some(row.label.clone()),
        command_family: None,
        base_command: None,
    })
}

fn build_breadcrumb_targets(
    root: &RootView,
    path: &BrowsePath,
    filter_options: &FilterOptions,
    project_root: Option<&str>,
) -> Vec<BreadcrumbTarget> {
    let mut targets = Vec::new();
    let mut display_parts = Vec::new();
    let mut push_target = |label: String, root: RootView, path: BrowsePath| {
        display_parts.push(label.clone());
        targets.push(BreadcrumbTarget {
            label,
            display: display_parts.join(" > "),
            root,
            path,
        });
    };

    push_target(
        breadcrumb_root_label(root).to_string(),
        *root,
        BrowsePath::Root,
    );

    match (root, path) {
        (_, BrowsePath::Root) => {}
        (RootView::ProjectHierarchy, BrowsePath::Project { project_id }) => {
            push_target(
                project_name(*project_id, filter_options),
                *root,
                BrowsePath::Project {
                    project_id: *project_id,
                },
            );
        }
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectCategory {
                project_id,
                category,
            },
        ) => {
            push_target(
                project_name(*project_id, filter_options),
                *root,
                BrowsePath::Project {
                    project_id: *project_id,
                },
            );
            push_target(
                category.clone(),
                *root,
                BrowsePath::ProjectCategory {
                    project_id: *project_id,
                    category: category.clone(),
                },
            );
        }
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectAction {
                project_id,
                category,
                action,
                parent_path,
            },
        ) => {
            push_target(
                project_name(*project_id, filter_options),
                *root,
                BrowsePath::Project {
                    project_id: *project_id,
                },
            );
            push_target(
                category.clone(),
                *root,
                BrowsePath::ProjectCategory {
                    project_id: *project_id,
                    category: category.clone(),
                },
            );
            push_target(
                action_label(action),
                *root,
                BrowsePath::ProjectAction {
                    project_id: *project_id,
                    category: category.clone(),
                    action: action.clone(),
                    parent_path: None,
                },
            );
            if let Some(project_root) = project_root {
                for selected_path in path_chain(project_root, parent_path.as_deref().unwrap_or(""))
                {
                    push_target(
                        breadcrumb_path_label(&selected_path),
                        *root,
                        BrowsePath::ProjectAction {
                            project_id: *project_id,
                            category: category.clone(),
                            action: action.clone(),
                            parent_path: Some(selected_path),
                        },
                    );
                }
            }
        }
        (RootView::CategoryHierarchy, BrowsePath::Category { category }) => {
            push_target(
                category.clone(),
                *root,
                BrowsePath::Category {
                    category: category.clone(),
                },
            );
        }
        (RootView::CategoryHierarchy, BrowsePath::CategoryAction { category, action }) => {
            push_target(
                category.clone(),
                *root,
                BrowsePath::Category {
                    category: category.clone(),
                },
            );
            push_target(
                action_label(action),
                *root,
                BrowsePath::CategoryAction {
                    category: category.clone(),
                    action: action.clone(),
                },
            );
        }
        (
            RootView::CategoryHierarchy,
            BrowsePath::CategoryActionProject {
                category,
                action,
                project_id,
                parent_path,
            },
        ) => {
            push_target(
                category.clone(),
                *root,
                BrowsePath::Category {
                    category: category.clone(),
                },
            );
            push_target(
                action_label(action),
                *root,
                BrowsePath::CategoryAction {
                    category: category.clone(),
                    action: action.clone(),
                },
            );
            push_target(
                project_name(*project_id, filter_options),
                *root,
                BrowsePath::CategoryActionProject {
                    category: category.clone(),
                    action: action.clone(),
                    project_id: *project_id,
                    parent_path: None,
                },
            );
            if let Some(project_root) = project_root {
                for selected_path in path_chain(project_root, parent_path.as_deref().unwrap_or(""))
                {
                    push_target(
                        breadcrumb_path_label(&selected_path),
                        *root,
                        BrowsePath::CategoryActionProject {
                            category: category.clone(),
                            action: action.clone(),
                            project_id: *project_id,
                            parent_path: Some(selected_path),
                        },
                    );
                }
            }
        }
        _ => {}
    }

    targets
}

fn breadcrumb_root_label(root: &RootView) -> &'static str {
    match root {
        RootView::ProjectHierarchy => "all projects",
        RootView::CategoryHierarchy => "all categories",
    }
}

fn breadcrumb_path_label(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| path.to_string())
}

fn next_browse_path(root: &RootView, current: &BrowsePath, row: &RollupRow) -> Option<BrowsePath> {
    match (root, current, row.kind) {
        (RootView::ProjectHierarchy, BrowsePath::Root, RollupRowKind::Project) => row
            .project_id
            .map(|project_id| BrowsePath::Project { project_id }),
        (
            RootView::ProjectHierarchy,
            BrowsePath::Project { project_id },
            RollupRowKind::ActionCategory,
        ) => row
            .category
            .clone()
            .map(|category| BrowsePath::ProjectCategory {
                project_id: *project_id,
                category,
            }),
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectCategory {
                project_id,
                category,
            },
            RollupRowKind::Action,
        ) => row.action.clone().map(|action| BrowsePath::ProjectAction {
            project_id: *project_id,
            category: category.clone(),
            action,
            parent_path: None,
        }),
        (
            RootView::ProjectHierarchy,
            BrowsePath::ProjectAction {
                project_id,
                category,
                action,
                ..
            },
            RollupRowKind::Directory,
        ) => row
            .full_path
            .clone()
            .map(|parent_path| BrowsePath::ProjectAction {
                project_id: *project_id,
                category: category.clone(),
                action: action.clone(),
                parent_path: Some(parent_path),
            }),
        (RootView::CategoryHierarchy, BrowsePath::Root, RollupRowKind::ActionCategory) => row
            .category
            .clone()
            .map(|category| BrowsePath::Category { category }),
        (RootView::CategoryHierarchy, BrowsePath::Category { category }, RollupRowKind::Action) => {
            row.action.clone().map(|action| BrowsePath::CategoryAction {
                category: category.clone(),
                action,
            })
        }
        (
            RootView::CategoryHierarchy,
            BrowsePath::CategoryAction { category, action },
            RollupRowKind::Project,
        ) => row
            .project_id
            .map(|project_id| BrowsePath::CategoryActionProject {
                category: category.clone(),
                action: action.clone(),
                project_id,
                parent_path: None,
            }),
        (
            RootView::CategoryHierarchy,
            BrowsePath::CategoryActionProject {
                category,
                action,
                project_id,
                ..
            },
            RollupRowKind::Directory,
        ) => row
            .full_path
            .clone()
            .map(|parent_path| BrowsePath::CategoryActionProject {
                category: category.clone(),
                action: action.clone(),
                project_id: *project_id,
                parent_path: Some(parent_path),
            }),
        _ => None,
    }
}

fn parent_browse_path(path: &BrowsePath, project_root: Option<&str>) -> BrowsePath {
    match path {
        BrowsePath::Root => BrowsePath::Root,
        BrowsePath::Project { .. } | BrowsePath::Category { .. } => BrowsePath::Root,
        BrowsePath::ProjectCategory { project_id, .. } => BrowsePath::Project {
            project_id: *project_id,
        },
        BrowsePath::CategoryAction { category, .. } => BrowsePath::Category {
            category: category.clone(),
        },
        BrowsePath::ProjectAction {
            project_id,
            category,
            action,
            parent_path,
        } => match parent_path {
            Some(parent_path) => {
                let parent = Path::new(parent_path)
                    .parent()
                    .map(|parent| parent.to_string_lossy().to_string());
                let next_parent = match (parent, project_root) {
                    (Some(parent), Some(project_root)) if parent == project_root => None,
                    (Some(parent), Some(_)) => Some(parent),
                    _ => None,
                };
                BrowsePath::ProjectAction {
                    project_id: *project_id,
                    category: category.clone(),
                    action: action.clone(),
                    parent_path: next_parent,
                }
            }
            None => BrowsePath::ProjectCategory {
                project_id: *project_id,
                category: category.clone(),
            },
        },
        BrowsePath::CategoryActionProject {
            category,
            action,
            project_id,
            parent_path,
        } => match parent_path {
            Some(parent_path) => {
                let parent = Path::new(parent_path)
                    .parent()
                    .map(|parent| parent.to_string_lossy().to_string());
                let next_parent = match (parent, project_root) {
                    (Some(parent), Some(project_root)) if parent == project_root => None,
                    (Some(parent), Some(_)) => Some(parent),
                    _ => None,
                };
                BrowsePath::CategoryActionProject {
                    category: category.clone(),
                    action: action.clone(),
                    project_id: *project_id,
                    parent_path: next_parent,
                }
            }
            None => BrowsePath::CategoryAction {
                category: category.clone(),
                action: action.clone(),
            },
        },
    }
}

fn matches_path_root(root: &RootView, path: &BrowsePath) -> bool {
    matches!(
        (root, path),
        (_, BrowsePath::Root)
            | (RootView::ProjectHierarchy, BrowsePath::Project { .. })
            | (
                RootView::ProjectHierarchy,
                BrowsePath::ProjectCategory { .. }
            )
            | (RootView::ProjectHierarchy, BrowsePath::ProjectAction { .. })
            | (RootView::CategoryHierarchy, BrowsePath::Category { .. })
            | (
                RootView::CategoryHierarchy,
                BrowsePath::CategoryAction { .. }
            )
            | (
                RootView::CategoryHierarchy,
                BrowsePath::CategoryActionProject { .. }
            )
    )
}

fn project_id_from_path(path: &BrowsePath) -> Option<i64> {
    match path {
        BrowsePath::Project { project_id }
        | BrowsePath::ProjectCategory { project_id, .. }
        | BrowsePath::ProjectAction { project_id, .. }
        | BrowsePath::CategoryActionProject { project_id, .. } => Some(*project_id),
        BrowsePath::Root | BrowsePath::Category { .. } | BrowsePath::CategoryAction { .. } => None,
    }
}

fn describe_browse_path(
    root: &RootView,
    path: &BrowsePath,
    filter_options: &FilterOptions,
) -> String {
    match (root, path) {
        (RootView::ProjectHierarchy, BrowsePath::Root) => "all projects".to_string(),
        (RootView::CategoryHierarchy, BrowsePath::Root) => "all categories".to_string(),
        (_, BrowsePath::Project { project_id }) => project_name(*project_id, filter_options),
        (
            _,
            BrowsePath::ProjectCategory {
                project_id,
                category,
            },
        ) => {
            format!("{}/{}", project_name(*project_id, filter_options), category)
        }
        (
            _,
            BrowsePath::ProjectAction {
                project_id,
                category,
                action,
                parent_path,
            },
        ) => {
            let path_label = parent_path
                .as_ref()
                .and_then(|path| {
                    Path::new(path)
                        .file_name()
                        .map(|name| name.to_string_lossy().to_string())
                })
                .map(|path| format!("/{path}"))
                .unwrap_or_default();
            format!(
                "{}/{}/{}{}",
                project_name(*project_id, filter_options),
                category,
                action_label(action),
                path_label
            )
        }
        (_, BrowsePath::Category { category }) => category.clone(),
        (_, BrowsePath::CategoryAction { category, action }) => {
            format!("{}/{}", category, action_label(action))
        }
        (
            _,
            BrowsePath::CategoryActionProject {
                category,
                action,
                project_id,
                parent_path,
            },
        ) => {
            let path_label = parent_path
                .as_ref()
                .and_then(|path| {
                    Path::new(path)
                        .file_name()
                        .map(|name| name.to_string_lossy().to_string())
                })
                .map(|path| format!("/{path}"))
                .unwrap_or_default();
            format!(
                "{}/{}/{}{}",
                category,
                action_label(action),
                project_name(*project_id, filter_options),
                path_label
            )
        }
    }
}

fn project_name(project_id: i64, filter_options: &FilterOptions) -> String {
    filter_options
        .projects
        .iter()
        .find(|project| project.id == project_id)
        .map(|project| project.display_name.clone())
        .unwrap_or_else(|| format!("#{project_id}"))
}

fn format_metric(value: f64) -> String {
    if (value.fract()).abs() < f64::EPSILON {
        format!("{value:.0}")
    } else {
        format!("{value:.1}")
    }
}

fn centered_rect(area: Rect, width: u16, height: u16) -> Rect {
    let popup_width = width.min(area.width.saturating_sub(4)).max(10);
    let popup_height = height.min(area.height.saturating_sub(4)).max(6);
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(area.height.saturating_sub(popup_height) / 2),
            Constraint::Length(popup_height),
            Constraint::Min(0),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(area.width.saturating_sub(popup_width) / 2),
            Constraint::Length(popup_width),
            Constraint::Min(0),
        ])
        .split(vertical[1]);
    horizontal[1]
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use gnomon_core::config::RuntimeConfig;
    use gnomon_core::import::StartupOpenReason;
    use gnomon_core::query::{ClassificationState, SnapshotBounds};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn persisted_state_round_trips() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("tui-state.json");
        let state = PersistedUiState {
            root: RootView::CategoryHierarchy,
            path: BrowsePath::Category {
                category: "Editing".to_string(),
            },
            lens: MetricLens::Total,
            pane_mode: PaneMode::Radial,
            time_window: TimeWindowPreset::LastWeek,
            model: Some("claude-opus".to_string()),
            project_id: Some(7),
            action_category: Some("Editing".to_string()),
            action: Some(sample_action("read file")),
            row_filter: "src".to_string(),
            enabled_columns: vec![OptionalColumn::Kind, OptionalColumn::Items],
        };

        state.save(&path)?;
        let loaded = PersistedUiState::load(&path)?.context("missing persisted state")?;
        assert_eq!(loaded.row_filter, "src");
        assert_eq!(loaded.lens, MetricLens::Total);
        assert_eq!(loaded.pane_mode, PaneMode::Radial);
        assert_eq!(
            loaded.enabled_columns,
            vec![OptionalColumn::Kind, OptionalColumn::Items]
        );
        Ok(())
    }

    #[test]
    fn persisted_state_loads_legacy_details_pane_name() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("tui-state.json");
        fs::write(
            &path,
            r#"{
  "root": "ProjectHierarchy",
  "path": "Root",
  "lens": "UncachedInput",
  "pane_mode": "Details",
  "time_window": "All",
  "model": null,
  "project_id": null,
  "action_category": null,
  "action": null,
  "row_filter": "",
  "enabled_columns": ["Kind"]
}"#,
        )?;

        let loaded = PersistedUiState::load(&path)?.context("missing persisted state")?;
        assert_eq!(loaded.pane_mode, PaneMode::Radial);
        Ok(())
    }

    #[test]
    fn row_filter_matches_labels_and_paths_case_insensitively() {
        let rows = vec![
            sample_row("src", Some("/tmp/project/src".to_string())),
            sample_row("README.md", Some("/tmp/project/README.md".to_string())),
        ];

        let filtered = filter_rows(&rows, "README.md");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].label, "README.md");

        let filtered = filter_rows(&rows, "PROJECT/SRC");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].label, "src");
    }

    #[test]
    fn jump_matches_use_fuzzy_sorting() {
        let matches = build_jump_matches(
            "proj edit",
            vec![
                JumpTarget {
                    label: "project-a / Editing".to_string(),
                    detail: "project category".to_string(),
                    root: RootView::ProjectHierarchy,
                    path: BrowsePath::Root,
                },
                JumpTarget {
                    label: "documentation writing".to_string(),
                    detail: "category".to_string(),
                    root: RootView::CategoryHierarchy,
                    path: BrowsePath::Root,
                },
            ],
        );

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].label, "project-a / Editing");
    }

    #[test]
    fn parent_browse_path_unwinds_directory_state() {
        let parent = parent_browse_path(
            &BrowsePath::ProjectAction {
                project_id: 1,
                category: "Editing".to_string(),
                action: sample_action("read file"),
                parent_path: Some("/tmp/project-a/src".to_string()),
            },
            Some("/tmp/project-a"),
        );

        assert_eq!(
            parent,
            BrowsePath::ProjectAction {
                project_id: 1,
                category: "Editing".to_string(),
                action: sample_action("read file"),
                parent_path: None,
            }
        );
    }

    #[test]
    fn active_columns_hide_low_priority_entries_when_narrow() {
        let columns = active_columns(48, MetricLens::UncachedInput, &default_enabled_columns());
        assert!(columns.len() < default_enabled_columns().len() + 2);
        assert_eq!(columns[0].key, ColumnKey::Label);
        assert_eq!(columns[1].key, ColumnKey::SelectedLens);
    }

    #[test]
    fn label_column_marks_drillable_rows() {
        let row = RollupRow {
            kind: RollupRowKind::Project,
            key: "project:1".to_string(),
            label: "project-a".to_string(),
            metrics: gnomon_core::query::MetricTotals {
                uncached_input: 5.0,
                cached_input: 0.0,
                gross_input: 5.0,
                output: 0.0,
                total: 5.0,
            },
            indicators: gnomon_core::query::MetricIndicators {
                selected_lens_last_5_hours: 5.0,
                selected_lens_last_week: 5.0,
                uncached_input_reference: 5.0,
            },
            item_count: 1,
            project_id: Some(1),
            category: None,
            action: None,
            full_path: None,
        };

        let rendered = render_column_value(
            ColumnKey::Label,
            &row,
            MetricLens::UncachedInput,
            &RootView::ProjectHierarchy,
            &BrowsePath::Root,
        );

        assert_eq!(rendered, "> project-a");
    }

    #[test]
    fn label_column_leaves_leaf_rows_unmarked() {
        let row = RollupRow {
            kind: RollupRowKind::File,
            key: "path:/tmp/project-a/src/lib.rs".to_string(),
            label: "lib.rs".to_string(),
            metrics: gnomon_core::query::MetricTotals {
                uncached_input: 5.0,
                cached_input: 0.0,
                gross_input: 5.0,
                output: 0.0,
                total: 5.0,
            },
            indicators: gnomon_core::query::MetricIndicators {
                selected_lens_last_5_hours: 5.0,
                selected_lens_last_week: 5.0,
                uncached_input_reference: 5.0,
            },
            item_count: 1,
            project_id: Some(1),
            category: Some("Editing".to_string()),
            action: Some(sample_action("read file")),
            full_path: Some("/tmp/project-a/src/lib.rs".to_string()),
        };

        let rendered = render_column_value(
            ColumnKey::Label,
            &row,
            MetricLens::UncachedInput,
            &RootView::ProjectHierarchy,
            &BrowsePath::ProjectAction {
                project_id: 1,
                category: "Editing".to_string(),
                action: sample_action("read file"),
                parent_path: Some("/tmp/project-a/src".to_string()),
            },
        );

        assert_eq!(rendered, "  lib.rs");
    }

    #[test]
    fn pane_focus_round_trips_through_pane_mode() {
        assert_eq!(
            PaneFocus::from_pane_mode(PaneMode::from_focus(PaneFocus::Table)),
            PaneFocus::Table
        );
        assert_eq!(
            PaneFocus::from_pane_mode(PaneMode::from_focus(PaneFocus::Radial)),
            PaneFocus::Radial
        );
    }

    #[test]
    fn radial_ancestor_steps_include_directory_chain() {
        let action = sample_action("read file");
        let steps = radial_ancestor_steps(
            &RootView::ProjectHierarchy,
            &BrowsePath::ProjectAction {
                project_id: 1,
                category: "Editing".to_string(),
                action: action.clone(),
                parent_path: Some("/tmp/project-a/src/lib".to_string()),
            },
            Some("/tmp/project-a"),
        );

        assert_eq!(steps.len(), 5);
        assert_eq!(
            steps[0],
            RadialAncestorStep {
                query_path: BrowsePath::Root,
                selected_child: "project:1".to_string(),
            }
        );
        assert_eq!(
            steps[1],
            RadialAncestorStep {
                query_path: BrowsePath::Project { project_id: 1 },
                selected_child: "category:Editing".to_string(),
            }
        );
        assert_eq!(
            steps[2],
            RadialAncestorStep {
                query_path: BrowsePath::ProjectCategory {
                    project_id: 1,
                    category: "Editing".to_string(),
                },
                selected_child: action_row_key(&action),
            }
        );
        assert_eq!(
            steps[3],
            RadialAncestorStep {
                query_path: BrowsePath::ProjectAction {
                    project_id: 1,
                    category: "Editing".to_string(),
                    action: action.clone(),
                    parent_path: None,
                },
                selected_child: "path:/tmp/project-a/src".to_string(),
            }
        );
        assert_eq!(
            steps[4],
            RadialAncestorStep {
                query_path: BrowsePath::ProjectAction {
                    project_id: 1,
                    category: "Editing".to_string(),
                    action,
                    parent_path: Some("/tmp/project-a/src".to_string()),
                },
                selected_child: "path:/tmp/project-a/src/lib".to_string(),
            }
        );
    }

    #[test]
    fn radial_model_marks_selected_outer_segment() {
        let rows = vec![
            sample_row("src", Some("/tmp/project/src".to_string())),
            sample_row("tests", Some("/tmp/project/tests".to_string())),
        ];
        let filter_options = sample_filter_options();
        let model = build_radial_model(
            &RadialContext::default(),
            &rows,
            rows.get(1),
            &RootView::ProjectHierarchy,
            &BrowsePath::Project { project_id: 1 },
            &filter_options,
            MetricLens::UncachedInput,
        );

        assert_eq!(model.layers.len(), 1);
        assert_eq!(
            model.layers[0]
                .segments
                .iter()
                .filter(|segment| segment.is_selected)
                .count(),
            1
        );
        assert_eq!(
            model.center.selection_label,
            "selected: tests (dir, uncached 5)"
        );
        assert!(model.layers[0].segments[1].is_selected);
        assert_eq!(model.center.scope_label, "project-a");
    }

    #[test]
    fn breadcrumb_targets_include_project_and_directory_ancestors() {
        let targets = build_breadcrumb_targets(
            &RootView::ProjectHierarchy,
            &BrowsePath::ProjectAction {
                project_id: 1,
                category: "Editing".to_string(),
                action: sample_action("read file"),
                parent_path: Some("/tmp/project-a/src/lib".to_string()),
            },
            &sample_filter_options(),
            Some("/tmp/project-a"),
        );

        let labels = targets
            .iter()
            .map(|target| target.label.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            labels,
            vec![
                "all projects",
                "project-a",
                "Editing",
                "read file",
                "src",
                "lib",
            ]
        );
        assert_eq!(targets[0].path, BrowsePath::Root);
        assert_eq!(targets[1].path, BrowsePath::Project { project_id: 1 });
        assert_eq!(
            targets[3].path,
            BrowsePath::ProjectAction {
                project_id: 1,
                category: "Editing".to_string(),
                action: sample_action("read file"),
                parent_path: None,
            }
        );
        assert_eq!(
            targets[5].path,
            BrowsePath::ProjectAction {
                project_id: 1,
                category: "Editing".to_string(),
                action: sample_action("read file"),
                parent_path: Some("/tmp/project-a/src/lib".to_string()),
            }
        );
    }

    #[test]
    fn breadcrumb_targets_include_category_hierarchy_ancestors() {
        let targets = build_breadcrumb_targets(
            &RootView::CategoryHierarchy,
            &BrowsePath::CategoryActionProject {
                category: "Documentation".to_string(),
                action: sample_action("write file"),
                project_id: 2,
                parent_path: Some("/tmp/project-b/src".to_string()),
            },
            &FilterOptions {
                projects: vec![gnomon_core::query::ProjectFilterOption {
                    id: 2,
                    display_name: "project-b".to_string(),
                }],
                models: Vec::new(),
                categories: Vec::new(),
                actions: Vec::new(),
            },
            Some("/tmp/project-b"),
        );

        let labels = targets
            .iter()
            .map(|target| target.label.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            labels,
            vec![
                "all categories",
                "Documentation",
                "write file",
                "project-b",
                "src",
            ]
        );
        assert_eq!(targets[0].path, BrowsePath::Root);
        assert_eq!(
            targets[3].path,
            BrowsePath::CategoryActionProject {
                category: "Documentation".to_string(),
                action: sample_action("write file"),
                project_id: 2,
                parent_path: None,
            }
        );
        assert_eq!(
            targets[4].path,
            BrowsePath::CategoryActionProject {
                category: "Documentation".to_string(),
                action: sample_action("write file"),
                project_id: 2,
                parent_path: Some("/tmp/project-b/src".to_string()),
            }
        );
    }

    #[test]
    fn radial_selected_child_span_tracks_nested_drilldown_arcs() {
        let project_layer = build_radial_layer(
            &[
                radial_row(RadialRowSpec {
                    key: "project:1",
                    label: "project-a",
                    kind: RollupRowKind::Project,
                    value: 10.0,
                    project_id: Some(1),
                    category: None,
                    action: None,
                    full_path: None,
                }),
                radial_row(RadialRowSpec {
                    key: "project:2",
                    label: "project-b",
                    kind: RollupRowKind::Project,
                    value: 10.0,
                    project_id: Some(2),
                    category: None,
                    action: None,
                    full_path: None,
                }),
            ],
            MetricLens::UncachedInput,
            Some("project:1"),
            RadialSpan::full(),
        );
        assert_span_close(radial_selected_child_span(&project_layer), 0.0, TAU / 2.0);

        let category_layer = build_radial_layer(
            &[
                radial_row(RadialRowSpec {
                    key: "category:Editing",
                    label: "Editing",
                    kind: RollupRowKind::ActionCategory,
                    value: 8.0,
                    project_id: Some(1),
                    category: Some("Editing"),
                    action: None,
                    full_path: None,
                }),
                radial_row(RadialRowSpec {
                    key: "category:Documentation",
                    label: "Documentation",
                    kind: RollupRowKind::ActionCategory,
                    value: 8.0,
                    project_id: Some(1),
                    category: Some("Documentation"),
                    action: None,
                    full_path: None,
                }),
            ],
            MetricLens::UncachedInput,
            Some("category:Documentation"),
            radial_selected_child_span(&project_layer),
        );
        assert_span_close(
            radial_selected_child_span(&category_layer),
            TAU / 4.0,
            TAU / 4.0,
        );

        let action_layer = build_radial_layer(
            &[
                radial_row(RadialRowSpec {
                    key: "action:read",
                    label: "read",
                    kind: RollupRowKind::Action,
                    value: 4.0,
                    project_id: Some(1),
                    category: Some("Documentation"),
                    action: Some(sample_action("read file")),
                    full_path: None,
                }),
                radial_row(RadialRowSpec {
                    key: "action:write",
                    label: "write",
                    kind: RollupRowKind::Action,
                    value: 4.0,
                    project_id: Some(1),
                    category: Some("Documentation"),
                    action: Some(sample_action("write file")),
                    full_path: None,
                }),
            ],
            MetricLens::UncachedInput,
            Some("action:write"),
            radial_selected_child_span(&category_layer),
        );
        assert_span_close(
            radial_selected_child_span(&action_layer),
            3.0 * TAU / 8.0,
            TAU / 8.0,
        );

        let directory_layer = build_radial_layer(
            &[
                radial_row(RadialRowSpec {
                    key: "path:/tmp/project-a/src",
                    label: "src",
                    kind: RollupRowKind::Directory,
                    value: 2.0,
                    project_id: Some(1),
                    category: Some("Documentation"),
                    action: Some(sample_action("read file")),
                    full_path: Some("/tmp/project-a/src"),
                }),
                radial_row(RadialRowSpec {
                    key: "path:/tmp/project-a/src/lib",
                    label: "lib",
                    kind: RollupRowKind::Directory,
                    value: 2.0,
                    project_id: Some(1),
                    category: Some("Documentation"),
                    action: Some(sample_action("read file")),
                    full_path: Some("/tmp/project-a/src/lib"),
                }),
            ],
            MetricLens::UncachedInput,
            Some("path:/tmp/project-a/src/lib"),
            radial_selected_child_span(&action_layer),
        );
        let directory_span = radial_selected_child_span(&directory_layer);
        assert_span_close(directory_span, 7.0 * TAU / 16.0, TAU / 16.0);
        assert!(
            radial_segment_at_angle(&directory_layer, 0.1).is_none(),
            "child layer should not render outside the inherited arc"
        );
        assert!(
            radial_segment_at_angle(
                &directory_layer,
                directory_span.start + directory_span.sweep / 2.0
            )
            .is_some(),
            "child layer should render inside the inherited arc"
        );
    }

    #[test]
    fn radial_center_labels_stay_inside_fit_area_on_narrow_layout() -> Result<()> {
        let model = RadialModel {
            center: RadialCenter {
                scope_label: "QQQQQQ".to_string(),
                lens_label: "WWWWWW".to_string(),
                selection_label: "ZZZZZZZZZZ".to_string(),
            },
            layers: vec![RadialLayer {
                span: RadialSpan::full(),
                segments: vec![
                    RadialSegment {
                        value: 8.0,
                        cached_ratio: 0.0,
                        bucket: RadialBucket::Project,
                        is_selected: true,
                    },
                    RadialSegment {
                        value: 4.0,
                        cached_ratio: 0.0,
                        bucket: RadialBucket::Category,
                        is_selected: false,
                    },
                ],
                total_value: 12.0,
            }],
        };

        let width = 24;
        let height = 12;
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend)?;

        terminal.draw(|frame| {
            let pane = RadialPane {
                model: &model,
                focused: false,
            };
            frame.render_widget(&pane, frame.area());
        })?;

        let buffer = terminal.backend().buffer();
        let inner = pane_block("Radial", false).inner(Rect::new(0, 0, width, height));
        let center_area = radial_center_label_area(inner);

        let mut saw_center_text = false;
        for (index, cell) in buffer.content.iter().enumerate() {
            let x = (index as u16) % width;
            let y = (index as u16) / width;
            let symbol = cell.symbol();
            if matches!(symbol, "Q" | "W" | "Z") {
                saw_center_text = true;
                assert!(
                    x >= center_area.x
                        && x < center_area.x + center_area.width
                        && y >= center_area.y
                        && y < center_area.y + center_area.height,
                    "center label character escaped the fitted area at ({x}, {y}): {symbol:?}"
                );
            }
        }

        assert!(
            saw_center_text,
            "expected the fitted center labels to render"
        );
        Ok(())
    }

    #[test]
    fn render_header_shows_segmented_breadcrumbs() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
        )?;
        app.breadcrumb_targets = build_breadcrumb_targets(
            &RootView::ProjectHierarchy,
            &BrowsePath::ProjectAction {
                project_id: 1,
                category: "Editing".to_string(),
                action: sample_action("read file"),
                parent_path: Some("/tmp/project-a/src".to_string()),
            },
            &sample_filter_options(),
            Some("/tmp/project-a"),
        );

        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend)?;
        terminal.draw(|frame| app.render(frame))?;

        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol().to_string())
            .collect::<String>();
        assert!(content.contains("view:"));
        assert!(content.contains("all projects"));
        assert!(content.contains("project-a"));
        assert!(content.contains(" > "));

        Ok(())
    }

    fn sample_row(label: &str, full_path: Option<String>) -> RollupRow {
        RollupRow {
            kind: RollupRowKind::Directory,
            key: format!("path:{label}"),
            label: label.to_string(),
            metrics: gnomon_core::query::MetricTotals {
                uncached_input: 5.0,
                cached_input: 1.0,
                gross_input: 6.0,
                output: 2.0,
                total: 8.0,
            },
            indicators: gnomon_core::query::MetricIndicators {
                selected_lens_last_5_hours: 5.0,
                selected_lens_last_week: 6.0,
                uncached_input_reference: 5.0,
            },
            item_count: 1,
            project_id: Some(1),
            category: Some("Editing".to_string()),
            action: Some(sample_action("read file")),
            full_path,
        }
    }

    struct RadialRowSpec<'a> {
        key: &'a str,
        label: &'a str,
        kind: RollupRowKind,
        value: f64,
        project_id: Option<i64>,
        category: Option<&'a str>,
        action: Option<ActionKey>,
        full_path: Option<&'a str>,
    }

    fn radial_row(spec: RadialRowSpec<'_>) -> RollupRow {
        RollupRow {
            kind: spec.kind,
            key: spec.key.to_string(),
            label: spec.label.to_string(),
            metrics: gnomon_core::query::MetricTotals {
                uncached_input: spec.value,
                cached_input: 0.0,
                gross_input: spec.value,
                output: 0.0,
                total: spec.value,
            },
            indicators: gnomon_core::query::MetricIndicators {
                selected_lens_last_5_hours: spec.value,
                selected_lens_last_week: spec.value,
                uncached_input_reference: spec.value,
            },
            item_count: 1,
            project_id: spec.project_id,
            category: spec.category.map(str::to_string),
            action: spec.action,
            full_path: spec.full_path.map(str::to_string),
        }
    }

    fn sample_filter_options() -> FilterOptions {
        FilterOptions {
            projects: vec![gnomon_core::query::ProjectFilterOption {
                id: 1,
                display_name: "project-a".to_string(),
            }],
            models: Vec::new(),
            categories: Vec::new(),
            actions: Vec::new(),
        }
    }

    fn assert_span_close(span: RadialSpan, expected_start: f64, expected_sweep: f64) {
        assert!(
            (span.start - expected_start).abs() < 1e-9,
            "unexpected span start: {} != {}",
            span.start,
            expected_start
        );
        assert!(
            (span.sweep - expected_sweep).abs() < 1e-9,
            "unexpected span sweep: {} != {}",
            span.sweep,
            expected_sweep
        );
    }

    fn sample_action(label: &str) -> ActionKey {
        ActionKey {
            classification_state: ClassificationState::Classified,
            normalized_action: Some(label.to_string()),
            command_family: None,
            base_command: None,
        }
    }

    fn make_test_config(dir: &std::path::Path) -> RuntimeConfig {
        RuntimeConfig {
            app_name: "gnomon",
            state_dir: dir.to_path_buf(),
            db_path: dir.join("test.sqlite3"),
            source_root: dir.join("source"),
        }
    }

    fn render_to_string(
        config: RuntimeConfig,
        snapshot: SnapshotBounds,
        startup_open_reason: StartupOpenReason,
        latest_snapshot: Option<SnapshotBounds>,
    ) -> Result<String> {
        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend)?;
        let mut app = App::new(
            config,
            snapshot.clone(),
            startup_open_reason,
            None,
            None,
            None,
        )?;
        if let Some(latest_snapshot) = latest_snapshot {
            app.latest_snapshot = latest_snapshot;
            app.has_newer_snapshot = app.latest_snapshot.max_publish_seq > snapshot.max_publish_seq;
        }
        terminal.draw(|frame| app.render(frame))?;
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol().to_string())
            .collect::<String>();
        Ok(content)
    }

    #[test]
    fn startup_browse_state_defaults_fresh_sessions_to_root() {
        let mut state = PersistedUiState {
            root: RootView::CategoryHierarchy,
            path: BrowsePath::Category {
                category: "Editing".to_string(),
            },
            ..PersistedUiState::default()
        };

        state.apply_startup_browse_state(None);

        assert_eq!(state.root, RootView::CategoryHierarchy);
        assert_eq!(state.path, BrowsePath::Root);
    }

    #[test]
    fn startup_browse_state_applies_explicit_drill_down() {
        let mut state = PersistedUiState::default();
        let startup_browse_state = StartupBrowseState {
            root: RootView::ProjectHierarchy,
            path: BrowsePath::ProjectCategory {
                project_id: 7,
                category: "Editing".to_string(),
            },
        };

        state.apply_startup_browse_state(Some(startup_browse_state));

        assert_eq!(state.root, RootView::ProjectHierarchy);
        assert_eq!(
            state.path,
            BrowsePath::ProjectCategory {
                project_id: 7,
                category: "Editing".to_string(),
            }
        );
    }

    #[test]
    fn render_produces_three_pane_layout() -> Result<()> {
        let temp = tempdir()?;
        let content = render_to_string(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
        )?;
        assert!(
            content.contains("Status"),
            "Status pane header not rendered"
        );
        assert!(
            content.contains("snapshot: no imported data is visible yet"),
            "empty snapshot state not rendered in header"
        );
        assert!(content.contains("Keys"), "Keys pane header not rendered");
        Ok(())
    }

    #[test]
    fn render_shows_startup_context_in_header() -> Result<()> {
        let temp = tempdir()?;
        let content = render_to_string(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
        )?;
        assert!(
            content.contains("gnomon"),
            "app name not rendered in header"
        );
        assert!(
            content.contains("snapshot: no imported data is visible yet"),
            "empty snapshot state not rendered in header"
        );
        assert!(
            content.contains("Background import never changes the visible view"),
            "refresh policy not rendered in header"
        );
        Ok(())
    }

    #[test]
    fn render_keeps_layout_intact_with_long_import_status_message() -> Result<()> {
        let temp = tempdir()?;
        let mut app = App::new(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
            None,
            None,
        )?;
        app.status_message = Some(StatusMessage::error(
            "deferred import failed for 2 chunks; first error: unable to normalize source file source/path/to/session.jsonl: unexpected EOF while parsing malformed json",
        ));

        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend)?;
        terminal.draw(|frame| app.render(frame))?;
        let content = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol().to_string())
            .collect::<String>();

        assert!(
            content.contains("Status"),
            "status pane header not rendered"
        );
        assert!(content.contains("Keys"), "keys pane header not rendered");
        Ok(())
    }

    #[test]
    fn render_footer_contains_quit_hint() -> Result<()> {
        let temp = tempdir()?;
        let content = render_to_string(
            make_test_config(temp.path()),
            SnapshotBounds::bootstrap(),
            StartupOpenReason::Last24hReady,
            None,
        )?;
        assert!(
            content.contains("quit"),
            "footer should contain 'quit' key hint"
        );
        Ok(())
    }

    #[test]
    fn sqlite_snapshot_upper_bound_builds_time_window_filter() -> Result<()> {
        let filter = TimeWindowPreset::Last24Hours.to_filter(&SnapshotBounds {
            max_publish_seq: 1,
            published_chunk_count: 1,
            upper_bound_utc: Some("2026-03-27 18:28:38".to_string()),
        })?;

        assert_eq!(
            filter,
            Some(TimeWindowFilter {
                start_at_utc: Some("2026-03-26T18:28:38Z".to_string()),
                end_at_utc: None,
            })
        );

        Ok(())
    }

    #[test]
    fn render_shows_partial_coverage_when_startup_times_out() -> Result<()> {
        let temp = tempdir()?;
        let content = render_to_string(
            make_test_config(temp.path()),
            SnapshotBounds {
                max_publish_seq: 2,
                published_chunk_count: 2,
                upper_bound_utc: Some("2026-03-27 18:28:38".to_string()),
            },
            StartupOpenReason::TimedOut,
            None,
        )?;

        assert!(
            content.contains("refresh:"),
            "refresh line not rendered in header"
        );
        assert_eq!(
            snapshot_refresh_text(
                &SnapshotBounds {
                    max_publish_seq: 2,
                    published_chunk_count: 2,
                    upper_bound_utc: Some("2026-03-27 18:28:38".to_string()),
                },
                &SnapshotBounds {
                    max_publish_seq: 2,
                    published_chunk_count: 2,
                    upper_bound_utc: Some("2026-03-27 18:28:38".to_string()),
                },
                StartupOpenReason::TimedOut,
                false,
            ),
            "refresh: manual only. Startup opened before the last 24 hours finished importing, so this snapshot may still be partial."
        );

        Ok(())
    }

    #[test]
    fn render_shows_refresh_target_without_auto_applying_it() -> Result<()> {
        let temp = tempdir()?;
        let content = render_to_string(
            make_test_config(temp.path()),
            SnapshotBounds {
                max_publish_seq: 2,
                published_chunk_count: 2,
                upper_bound_utc: Some("2026-03-27T18:28:38Z".to_string()),
            },
            StartupOpenReason::Last24hReady,
            Some(SnapshotBounds {
                max_publish_seq: 3,
                published_chunk_count: 3,
                upper_bound_utc: Some("2026-03-27T19:45:00Z".to_string()),
            }),
        )?;

        assert!(
            content.contains(
                "snapshot: showing imported data through 2026-03-27T18:28:38Z (newer data is ready)"
            ),
            "current pinned snapshot summary not rendered"
        );
        assert!(
            content.contains("newer imported data is ready"),
            "newer snapshot availability not rendered"
        );
        assert!(
            content.contains("this view stays pinned until you refresh"),
            "manual refresh guarantee not rendered"
        );

        Ok(())
    }

    #[test]
    fn snapshot_coverage_footer_uses_meaningful_counts() {
        assert_eq!(
            snapshot_coverage_footer_text(&SnapshotCoverageSummary {
                project_count: 2,
                project_day_count: 5,
                day_count: 3,
                session_count: 7,
                turn_count: 11,
            }),
            "coverage: 2 projects, 5 project-days across 3 days, 7 sessions, 11 turns"
        );
    }

    #[test]
    fn snapshot_coverage_footer_handles_empty_snapshots() {
        assert_eq!(
            snapshot_coverage_footer_text(&SnapshotCoverageSummary::default()),
            "coverage: no imported data is visible yet"
        );
    }
}
