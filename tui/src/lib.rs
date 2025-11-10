// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! TUI component status dashboard using ratatui.

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::widgets::Gauge;
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
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};

use metrics::Metrics;

pub struct Tui {
    metrics: Arc<Metrics>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    shutting_down: bool,
}

impl Tui {
    pub fn new(metrics: Arc<Metrics>, shutdown_tx: oneshot::Sender<()>) -> Self {
        Self {
            metrics,
            shutdown_tx: Some(shutdown_tx),
            shutting_down: false,
        }
    }

    /// Run the TUI dashboard.
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        loop {
            terminal.draw(|f| self.ui(f))?;

            if event::poll(Duration::from_millis(50))? {
                if let Event::Key(key) = event::read()? {
                    if key.code == KeyCode::Char('q') {
                        if let Some(tx) = self.shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        // show “Shutting down…” briefly
                        self.shutting_down = true;
                        for _ in 0..5 {
                            terminal.draw(|f| self.ui(f))?;
                            sleep(Duration::from_millis(50)).await;
                        }
                        break;
                    }
                }
            }

            sleep(Duration::from_millis(50)).await;
        }

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

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    Constraint::Length(3),       // progress gauge
                    Constraint::Percentage(100), // details
                ]
                .as_ref(),
            )
            .split(size);

        // Progress (days)
        let planned = self.metrics.planned_days();
        let completed = self.metrics.completed_days();
        let ratio = if planned > 0 {
            (completed as f64) / (planned as f64)
        } else {
            0.0
        };
        let gauge = Gauge::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Ingestion Progress"),
            )
            .ratio(ratio)
            .label(Span::styled(
                format!("Days {}/{}", completed, planned),
                Style::default().fg(Color::White),
            ));
        f.render_widget(gauge, chunks[0]);

        // Metrics
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        let last_request = self.metrics.last_request_ts_ns();
        let last_request_str = match last_request {
            Some(ts) => format!("Last request: {} ms ago", (now_ns - ts) / 1_000_000),
            None => "Last request: Never".to_string(),
        };

        let flatfile_status = self.metrics.flatfile_status();
        let last_reload = self.metrics.last_config_reload_ts_ns();
        let last_reload_str = match last_reload {
            Some(ts) => format!("Last config reload: {} ms ago", (now_ns - ts) / 1_000_000),
            None => "Last config reload: Never".to_string(),
        };

        let batches = self.metrics.ingested_batches();
        let rows = self.metrics.ingested_rows();

        let mut lines = Vec::new();
        lines.push(Line::from(Span::styled(
            "Component Status Dashboard",
            Style::default().fg(Color::Cyan),
        )));
        lines.push(Line::from("")); // blank line
        lines.push(Line::from(Span::styled(
            "Metrics Server: Running on 127.0.0.1:9090",
            Style::default().fg(Color::Green),
        )));
        lines.push(Line::from(Span::styled(
            last_request_str,
            Style::default().fg(Color::Yellow),
        )));
        lines.push(Line::from(Span::styled(
            flatfile_status,
            Style::default().fg(Color::Blue),
        )));
        lines.push(Line::from(Span::styled(
            last_reload_str,
            Style::default().fg(Color::Magenta),
        )));
        lines.push(Line::from(Span::raw(format!(
            "Batches processed: {}",
            batches
        ))));
        lines.push(Line::from(Span::raw(format!("Rows ingested: {}", rows))));
        lines.push(Line::from(""));
        if self.shutting_down {
            lines.push(Line::from(Span::styled(
                "Shutting down… please wait",
                Style::default().fg(Color::Red),
            )));
        } else {
            lines.push(Line::from(Span::styled(
                "Press 'q' to quit.",
                Style::default().fg(Color::White),
            )));
        }

        let status_text = Text::from(lines);
        let paragraph = Paragraph::new(status_text)
            .block(Block::default().borders(Borders::ALL).title("Dashboard"))
            .alignment(Alignment::Left);
        f.render_widget(paragraph, chunks[1]);
    }
}
