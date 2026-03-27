use std::fs;
use std::io::{self, Stdout};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use gnomon_core::config::RuntimeConfig;
use gnomon_core::db::Database;
use gnomon_core::import::StartupOpenReason;
use gnomon_core::query::{
    ActionKey, BrowseFilters, BrowsePath, BrowseRequest, FilterOptions, MetricLens, QueryEngine,
    RollupRow, RollupRowKind, RootView, SnapshotBounds, TimeWindowFilter,
};
use jiff::{Timestamp, ToSpan};
use nucleo_matcher::pattern::{CaseMatching, Normalization, Pattern};
use nucleo_matcher::{Config as MatcherConfig, Matcher};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Table, TableState, Wrap,
};
use ratatui::{Frame, Terminal};
use serde::{Deserialize, Serialize};

const UI_STATE_FILENAME: &str = "tui-state.json";
const REFRESH_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const WIDE_LAYOUT_WIDTH: u16 = 120;
const JUMP_MATCH_LIMIT: usize = 8;
pub struct App {
    database: Database,
    ui_state_path: PathBuf,
    ui_state: PersistedUiState,
    snapshot: SnapshotBounds,
    startup_open_reason: StartupOpenReason,
    has_newer_snapshot: bool,
    filter_options: FilterOptions,
    raw_rows: Vec<RollupRow>,
    visible_rows: Vec<RollupRow>,
    table_state: TableState,
    input_mode: InputMode,
    jump_state: JumpState,
    status_message: Option<StatusMessage>,
    last_refresh_check: Instant,
}

impl App {
    pub fn new(
        config: RuntimeConfig,
        snapshot: SnapshotBounds,
        startup_open_reason: StartupOpenReason,
    ) -> Result<Self> {
        let ui_state_path = config.state_dir.join(UI_STATE_FILENAME);
        let (ui_state, status_message) = match PersistedUiState::load(&ui_state_path) {
            Ok(Some(state)) => (state, None),
            Ok(None) => (PersistedUiState::default(), None),
            Err(error) => (
                PersistedUiState::default(),
                Some(StatusMessage::error(format!(
                    "Unable to load saved TUI state: {error:#}"
                ))),
            ),
        };

        let database = Database::open(&config.db_path)?;
        let mut app = Self {
            database,
            ui_state_path,
            ui_state,
            snapshot,
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
            jump_state: JumpState::default(),
            status_message,
            last_refresh_check: Instant::now(),
        };

        app.reload_view()?;
        app.refresh_newer_snapshot_flag()?;
        Ok(app)
    }

    pub fn run(mut self) -> Result<()> {
        let mut terminal = TerminalGuard::enter()?;

        loop {
            if self.last_refresh_check.elapsed() >= REFRESH_CHECK_INTERVAL {
                self.refresh_newer_snapshot_flag()?;
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

    fn render(&mut self, frame: &mut Frame<'_>) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(5),
                Constraint::Min(10),
                Constraint::Length(4),
            ])
            .split(frame.area());

        self.render_header(frame, layout[0]);
        self.render_body(frame, layout[1]);
        self.render_footer(frame, layout[2]);

        match self.input_mode {
            InputMode::JumpInput => self.render_jump_overlay(frame),
            InputMode::ColumnChooser => self.render_columns_overlay(frame),
            InputMode::Normal | InputMode::FilterInput => {}
        }
    }

    fn render_header(&self, frame: &mut Frame<'_>, area: Rect) {
        let status = if self.has_newer_snapshot {
            "new data available"
        } else {
            "pinned"
        };
        let snapshot_summary = if self.snapshot.is_bootstrap() {
            "snapshot: bootstrap".to_string()
        } else {
            format!(
                "snapshot: publish_seq <= {} ({})",
                self.snapshot.max_publish_seq, status
            )
        };

        let mut lines = vec![
            Line::from(vec![
                Span::styled("gnomon", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw("  table-first explorer"),
            ]),
            Line::from(format!(
                "{}  |  lens: {}  |  pane: {}",
                snapshot_summary,
                metric_lens_label(self.ui_state.lens),
                self.ui_state.pane_mode.label()
            )),
            Line::from(match self.startup_open_reason {
                StartupOpenReason::Last24hReady => {
                    "startup gate: last-24h import slice was ready before the TUI opened"
                }
                StartupOpenReason::TimedOut => {
                    "startup gate: opened on the 10s deadline while import continues in the background"
                }
            }),
            Line::from(format!(
                "path: {}",
                describe_browse_path(
                    &self.ui_state.root,
                    &self.ui_state.path,
                    &self.filter_options
                )
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
        let show_split =
            area.width >= WIDE_LAYOUT_WIDTH && matches!(self.ui_state.pane_mode, PaneMode::Details);

        if show_split {
            let panes = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(64), Constraint::Percentage(36)])
                .split(area);
            self.render_table(frame, panes[0]);
            self.render_details(frame, panes[1]);
            return;
        }

        if matches!(self.ui_state.pane_mode, PaneMode::Details) {
            self.render_details(frame, area);
        } else {
            self.render_table(frame, area);
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
                                Cell::from(render_column_value(column.key, row, self.ui_state.lens))
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
            .block(Block::default().borders(Borders::ALL).title("Rows"))
            .row_highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol(">> ");
        frame.render_stateful_widget(table, area, &mut self.table_state);
    }

    fn render_details(&self, frame: &mut Frame<'_>, area: Rect) {
        let block = Block::default().borders(Borders::ALL).title("Details");

        let lines = if let Some(row) = self.selected_row() {
            detail_lines(row, self.ui_state.lens, &self.filter_options)
        } else {
            vec![
                Line::from("No row selected."),
                Line::from("Use the arrow keys to move through the current view."),
                Line::from("Press Enter to drill down and Backspace to move up."),
            ]
        };

        let paragraph = Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: true });
        frame.render_widget(paragraph, area);
    }

    fn render_footer(&self, frame: &mut Frame<'_>, area: Rect) {
        let lines = match self.input_mode {
            InputMode::Normal => vec![
                Line::from(
                    "Enter/right drill  Backspace/left up  1/2 hierarchy  l lens  Tab pane  o columns  q quit",
                ),
                Line::from(
                    "t time  m model  p project  c category  a action  0 clear  / row filter  g jump  r refresh",
                ),
                Line::from(format!(
                    "row filter: {}",
                    if self.ui_state.row_filter.is_empty() {
                        "(none)".to_string()
                    } else {
                        self.ui_state.row_filter.clone()
                    }
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
            InputMode::ColumnChooser => self.handle_column_key(key),
        }
    }

    fn handle_normal_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => Ok(true),
            KeyCode::Up | KeyCode::Char('k') => {
                self.move_selection(-1);
                Ok(false)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.move_selection(1);
                Ok(false)
            }
            KeyCode::PageUp => {
                self.move_selection(-10);
                Ok(false)
            }
            KeyCode::PageDown => {
                self.move_selection(10);
                Ok(false)
            }
            KeyCode::Home => {
                self.select_first();
                Ok(false)
            }
            KeyCode::End => {
                self.select_last();
                Ok(false)
            }
            KeyCode::Enter | KeyCode::Right => {
                self.descend_into_selection()?;
                Ok(false)
            }
            KeyCode::Backspace | KeyCode::Left => {
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
                self.ui_state.pane_mode = self.ui_state.pane_mode.toggle();
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
            KeyCode::Char('r') => {
                self.refresh_snapshot()?;
                Ok(false)
            }
            KeyCode::Char('o') => {
                self.input_mode = InputMode::ColumnChooser;
                Ok(false)
            }
            _ => Ok(false),
        }
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
        self.refresh_newer_snapshot_flag()?;
        self.status_message = Some(StatusMessage::info(format!(
            "Adopted publish_seq <= {}.",
            self.snapshot.max_publish_seq
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

        self.apply_row_filter();
        self.restore_selection(selected_key);
        self.save_state();
        Ok(())
    }

    fn refresh_newer_snapshot_flag(&mut self) -> Result<()> {
        self.has_newer_snapshot = self.query_engine().has_newer_snapshot(&self.snapshot)?;
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
            return;
        }

        if let Some(preferred_key) = preferred_key {
            if let Some(index) = self
                .visible_rows
                .iter()
                .position(|row| row.key == preferred_key)
            {
                self.table_state.select(Some(index));
                return;
            }
        }

        let selected = self
            .table_state
            .selected()
            .unwrap_or(0)
            .min(self.visible_rows.len().saturating_sub(1));
        self.table_state.select(Some(selected));
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
    }

    fn select_last(&mut self) {
        if self.visible_rows.is_empty() {
            self.table_state.select(None);
        } else {
            self.table_state
                .select(Some(self.visible_rows.len().saturating_sub(1)));
        }
    }

    fn move_selection(&mut self, delta: isize) {
        if self.visible_rows.is_empty() {
            self.table_state.select(None);
            return;
        }

        let current = self.table_state.selected().unwrap_or(0) as isize;
        let max_index = self.visible_rows.len().saturating_sub(1) as isize;
        let next = (current + delta).clamp(0, max_index) as usize;
        self.table_state.select(Some(next));
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
    Details,
}

impl PaneMode {
    fn toggle(self) -> Self {
        match self {
            Self::Table => Self::Details,
            Self::Details => Self::Table,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Details => "inspect",
        }
    }
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
        let upper_bound = upper_bound
            .parse::<Timestamp>()
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputMode {
    Normal,
    FilterInput,
    JumpInput,
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
        .map(|column| optional_column_spec(column))
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

fn render_column_value(column: ColumnKey, row: &RollupRow, lens: MetricLens) -> String {
    match column {
        ColumnKey::Label => row.label.clone(),
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
    match (root, path) {
        (_, BrowsePath::Root) => true,
        (RootView::ProjectHierarchy, BrowsePath::Project { .. }) => true,
        (RootView::ProjectHierarchy, BrowsePath::ProjectCategory { .. }) => true,
        (RootView::ProjectHierarchy, BrowsePath::ProjectAction { .. }) => true,
        (RootView::CategoryHierarchy, BrowsePath::Category { .. }) => true,
        (RootView::CategoryHierarchy, BrowsePath::CategoryAction { .. }) => true,
        (RootView::CategoryHierarchy, BrowsePath::CategoryActionProject { .. }) => true,
        _ => false,
    }
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

fn detail_lines(
    row: &RollupRow,
    lens: MetricLens,
    filter_options: &FilterOptions,
) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::from(vec![
            Span::styled("label: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(row.label.clone()),
        ]),
        Line::from(vec![
            Span::styled("kind: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(row_kind_label(row.kind)),
        ]),
        Line::from(vec![
            Span::styled(
                "selected lens: ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                "{} = {}",
                metric_lens_label(lens),
                format_metric(row.metrics.lens_value(lens))
            )),
        ]),
        Line::from(format!(
            "uncached {}  |  gross {}  |  output {}  |  total {}",
            format_metric(row.metrics.uncached_input),
            format_metric(row.metrics.gross_input),
            format_metric(row.metrics.output),
            format_metric(row.metrics.total)
        )),
        Line::from(format!(
            "5h {}  |  1w {}  |  ref {}  |  items {}",
            format_metric(row.indicators.selected_lens_last_5_hours),
            format_metric(row.indicators.selected_lens_last_week),
            format_metric(row.indicators.uncached_input_reference),
            row.item_count
        )),
    ];

    if let Some(project_id) = row.project_id {
        lines.push(Line::from(format!(
            "project: {}",
            project_name(project_id, filter_options)
        )));
    }
    if let Some(category) = &row.category {
        lines.push(Line::from(format!("category: {category}")));
    }
    if let Some(action) = &row.action {
        lines.push(Line::from(format!("action: {}", action_label(action))));
    }
    if let Some(path) = &row.full_path {
        lines.push(Line::from(format!("path: {path}")));
    }

    lines
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
    use tempfile::tempdir;

    use super::*;
    use gnomon_core::query::ClassificationState;

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
            pane_mode: PaneMode::Details,
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
        assert_eq!(loaded.pane_mode, PaneMode::Details);
        assert_eq!(
            loaded.enabled_columns,
            vec![OptionalColumn::Kind, OptionalColumn::Items]
        );
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

    fn sample_action(label: &str) -> ActionKey {
        ActionKey {
            classification_state: ClassificationState::Classified,
            normalized_action: Some(label.to_string()),
            command_family: None,
            base_command: None,
        }
    }
}
