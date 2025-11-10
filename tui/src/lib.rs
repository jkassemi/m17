// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! TUI component status dashboard using ratatui.

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph},
    Frame, Terminal,
};
use std::io;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};
use tokio::sync::oneshot;

use metrics::Metrics;

pub struct Tui {
    metrics: Arc<Metrics>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl Tui {
    pub fn new(metrics: Arc<Metrics>, shutdown_tx: oneshot::Sender<()>) -> Self {
        Self { metrics, shutdown_tx: Some(shutdown_tx) }
    }

    /// Run the TUI dashboard.
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Setup terminal
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        // Run the app loop
        loop {
            terminal.draw(|f| self.ui(f))?;

            // Check for quit event
            if event::poll(Duration::from_millis(100))? {
                if let Event::Key(key) = event::read()? {
                    if key.code == KeyCode::Char('q') {
                        // Send shutdown signal
                        if let Some(tx) = self.shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                }
            }

            // Sleep to avoid busy loop
            sleep(Duration::from_millis(100)).await;
        }

        // Restore terminal
        disable_raw_mode()?;
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;
        terminal.show_cursor()?;

        Ok(())
    }

    fn ui(&self, f: &mut Frame) {
        let size = f.size();

        // Create a layout with a single block for the dashboard
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(100)].as_ref())
            .split(size);

        // Get last request timestamp
        let last_request = self.metrics.last_request_ts_ns();
        let last_request_str = match last_request {
            Some(ts) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64;
                let age_ms = (now - ts) / 1_000_000;
                format!("Last request: {} ms ago", age_ms)
            }
            None => "Last request: Never".to_string(),
        };

        // Get flatfile status
        let flatfile_status = self.metrics.flatfile_status();

        // Get last config reload timestamp
        let last_reload = self.metrics.last_config_reload_ts_ns();
        let last_reload_str = match last_reload {
            Some(ts) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64;
                let age_ms = (now - ts) / 1_000_000;
                format!("Last config reload: {} ms ago", age_ms)
            }
            None => "Last config reload: Never".to_string(),
        };

        // Format the strings with newline
        let last_request_formatted = format!("{}\n", last_request_str);
        let flatfile_formatted = format!("{}\n", flatfile_status);
        let last_reload_formatted = format!("{}\n", last_reload_str);

        // Display component statuses
        let status_spans = vec![
            Span::styled("Component Status Dashboard\n\n", Style::default().fg(Color::Cyan)),
            Span::styled("Metrics Server: Running on 127.0.0.1:9090\n", Style::default().fg(Color::Green)),
            Span::styled(&last_request_formatted, Style::default().fg(Color::Yellow)),
            Span::styled(&flatfile_formatted, Style::default().fg(Color::Blue)),
            Span::styled(&last_reload_formatted, Style::default().fg(Color::Magenta)),
            Span::styled("\nPress 'q' to quit.", Style::default().fg(Color::White)),
        ];
        let status_text = Text::from(vec![Line::from(status_spans)]);

        let paragraph = Paragraph::new(status_text)
            .block(Block::default().borders(Borders::ALL).title("Dashboard"))
            .alignment(Alignment::Left);

        f.render_widget(paragraph, chunks[0]);
    }
}
