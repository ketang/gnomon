use std::io::{self, Stdout};
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use gnomon_core::config::RuntimeConfig;
use gnomon_core::import::{
    IMPORT_CHUNK_UNIT, STARTUP_IMPORT_WINDOW_HOURS, STARTUP_OPEN_DEADLINE_SECS, StartupOpenReason,
};
use gnomon_core::query::SnapshotBounds;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::{Frame, Terminal};

pub struct App {
    config: RuntimeConfig,
    snapshot: SnapshotBounds,
    startup_open_reason: StartupOpenReason,
}

impl App {
    pub fn new(
        config: RuntimeConfig,
        snapshot: SnapshotBounds,
        startup_open_reason: StartupOpenReason,
    ) -> Self {
        Self {
            config,
            snapshot,
            startup_open_reason,
        }
    }

    pub fn run(self) -> Result<()> {
        let mut terminal = TerminalGuard::enter()?;
        let app = self;

        loop {
            terminal.terminal.draw(|frame| app.render(frame))?;

            if event::poll(Duration::from_millis(250))? {
                let Event::Key(key) = event::read()? else {
                    continue;
                };

                if key.kind != KeyEventKind::Press {
                    continue;
                }

                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    _ => {}
                }
            }
        }

        Ok(())
    }

    fn render(&self, frame: &mut Frame<'_>) {
        let startup_status = match self.startup_open_reason {
            StartupOpenReason::Last24hReady => {
                "Startup gate: last-24h chunk slice finished before the UI opened."
            }
            StartupOpenReason::TimedOut => {
                "Startup gate: opened on the 10s deadline while the importer continues in the background."
            }
        };
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(10),
                Constraint::Length(3),
            ])
            .split(frame.area());

        let title = Paragraph::new(Line::from(vec![
            Span::styled("gnomon", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw("  bootstrap workspace"),
        ]))
        .block(Block::default().borders(Borders::ALL).title("Status"));
        frame.render_widget(title, chunks[0]);

        let body = Paragraph::new(vec![
            Line::from("The repository bootstrap is complete. This TUI is the first smoke-test shell."),
            Line::from(""),
            Line::from(format!("Source root: {}", self.config.source_root.display())),
            Line::from(format!("SQLite path: {}", self.config.db_path.display())),
            Line::from(format!("State dir: {}", self.config.state_dir.display())),
            Line::from(""),
            Line::from(format!(
                "Startup policy: prioritize the last {STARTUP_IMPORT_WINDOW_HOURS}h, open after ready or {STARTUP_OPEN_DEADLINE_SECS}s."
            )),
            Line::from(startup_status),
            Line::from(format!(
                "Import unit: {IMPORT_CHUNK_UNIT}; active snapshot publish_seq <= {}.",
                self.snapshot.max_publish_seq
            )),
            Line::from(""),
            Line::from("Current implementation targets after this milestone: aggregate queries, pinned refresh, and table-first navigation."),
        ])
        .block(Block::default().borders(Borders::ALL).title("Bootstrap"))
        .wrap(Wrap { trim: true });
        frame.render_widget(body, chunks[1]);

        let footer = Paragraph::new(Line::from(vec![
            Span::styled("q", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" quit"),
        ]))
        .block(Block::default().borders(Borders::ALL).title("Keys"));
        frame.render_widget(footer, chunks[2]);
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
