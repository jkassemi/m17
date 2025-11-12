// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! TUI component status dashboard using ratatui.

use core_types::status::{OverallStatus, ServiceStatusSnapshot};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Gauge, Paragraph},
    Frame, Terminal,
};
use std::io;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, watch};
use tokio::time::{sleep, Duration};

use metrics::{Metrics, OutdatedDataSnapshot};

pub struct Tui {
    metrics: Arc<Metrics>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    shutdown_rx: watch::Receiver<bool>,
    shutting_down: bool,
}

impl Tui {
    pub fn new(
        metrics: Arc<Metrics>,
        shutdown_tx: oneshot::Sender<()>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            metrics,
            shutdown_tx: Some(shutdown_tx),
            shutdown_rx,
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
            if *self.shutdown_rx.borrow() {
                self.shutting_down = true;
            }

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

            if self.shutting_down {
                break;
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
        let service_statuses = self.metrics.service_status_snapshots();
        let current_files = self.metrics.current_files();
        let outdated_entries = self.metrics.outdated_data();
        let status_block_height =
            std::cmp::max(4, (service_statuses.len() as u16).saturating_mul(3));
        let current_file_block_height =
            std::cmp::max(3, (current_files.len() as u16).saturating_mul(3));
        let outdated_block_height = if outdated_entries.is_empty() {
            3
        } else {
            std::cmp::max(3, (outdated_entries.len() as u16).saturating_mul(2))
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    Constraint::Length(3),                         // ingestion progress
                    Constraint::Length(current_file_block_height), // current file progress
                    Constraint::Length(status_block_height),
                    Constraint::Length(outdated_block_height),
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
        }
        .clamp(0.0, 1.0);
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

        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        // Current file progress
        if current_files.is_empty() {
            let placeholder = Paragraph::new("No active file downloads")
                .block(Block::default().borders(Borders::ALL).title("Current file"))
                .alignment(Alignment::Center);
            f.render_widget(placeholder, chunks[1]);
        } else {
            let cf_constraints = vec![Constraint::Length(3); current_files.len()];
            let cf_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints(cf_constraints)
                .split(chunks[1]);
            for (idx, file) in current_files.iter().enumerate() {
                let chunk = cf_chunks[idx];
                let elapsed_ns = (now_ns - file.started_ns).max(1);
                let read_mb = (file.read as f64) / (1024.0 * 1024.0);
                let total_mb = (file.total as f64) / (1024.0 * 1024.0);
                let throughput_mb_s = if elapsed_ns > 0 {
                    read_mb / ((elapsed_ns as f64) / 1_000_000_000.0)
                } else {
                    0.0
                };
                let ratio = if file.total > 0 {
                    (file.read as f64) / (file.total as f64)
                } else {
                    0.0
                }
                .clamp(0.0, 1.0);
                let max_label_len = chunk.width.saturating_sub(4) as usize;
                let label = Self::format_file_label(
                    &file.name,
                    read_mb,
                    if file.total > 0 { Some(total_mb) } else { None },
                    throughput_mb_s,
                    max_label_len,
                );
                let title = if current_files.len() == 1 {
                    "Current file".to_string()
                } else {
                    format!("Current file {} of {}", idx + 1, current_files.len())
                };
                let cf_gauge = Gauge::default()
                    .block(Block::default().borders(Borders::ALL).title(title))
                    .ratio(ratio)
                    .label(Span::styled(label, Style::default().fg(Color::White)));
                f.render_widget(cf_gauge, cf_chunks[idx]);
            }
        }

        // Service health
        let status_lines = Self::format_status_lines(&service_statuses);
        let status_block = Paragraph::new(Text::from(status_lines)).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Service health"),
        );
        f.render_widget(status_block, chunks[2]);

        // Outdated datasets
        let outdated_lines = Self::format_outdated_lines(&outdated_entries);
        let outdated_title = "Outdated data (see docs/runbooks/treasury-rebuild.md)";
        let outdated_block = Paragraph::new(Text::from(outdated_lines))
            .block(Block::default().borders(Borders::ALL).title(outdated_title));
        f.render_widget(outdated_block, chunks[3]);

        // Metrics
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
        let metrics_port = self.metrics.metrics_port();

        let mut lines = Vec::new();
        lines.push(Line::from(Span::styled(
            "Component Status Dashboard",
            Style::default().fg(Color::Cyan),
        )));
        lines.push(Line::from("")); // blank line
        lines.push(Line::from(Span::styled(
            format!("Metrics Server: Running on 127.0.0.1:{}", metrics_port),
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
        f.render_widget(paragraph, chunks[4]);
    }

    fn format_status_lines(statuses: &[ServiceStatusSnapshot]) -> Vec<Line<'static>> {
        if statuses.is_empty() {
            return vec![Line::from(Span::raw("No managed services registered"))];
        }
        let mut lines = Vec::new();
        for snapshot in statuses {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{:<18}", snapshot.name),
                    Style::default().fg(Color::White).bold(),
                ),
                Span::styled(
                    Self::status_label(snapshot.overall),
                    Style::default().fg(Self::status_color(snapshot.overall)),
                ),
            ]));
            for warn in &snapshot.warnings {
                lines.push(Line::from(vec![
                    Span::styled("  warn: ", Style::default().fg(Color::Yellow)),
                    Span::raw(warn.clone()),
                ]));
            }
            for err in &snapshot.errors {
                lines.push(Line::from(vec![
                    Span::styled("  error: ", Style::default().fg(Color::Red)),
                    Span::raw(err.clone()),
                ]));
            }
            for gauge in &snapshot.gauges {
                let detail = if let Some(max) = gauge.max {
                    format!(
                        "{}: {:.1}/{:.1} {}",
                        gauge.label,
                        gauge.value,
                        max,
                        gauge.unit.clone().unwrap_or_default()
                    )
                } else {
                    format!(
                        "{}: {:.1} {}",
                        gauge.label,
                        gauge.value,
                        gauge.unit.clone().unwrap_or_default()
                    )
                };
                lines.push(Line::from(vec![
                    Span::styled("  gauge: ", Style::default().fg(Color::Cyan)),
                    Span::raw(detail),
                ]));
            }
            lines.push(Line::from(""));
        }
        lines
    }

    fn format_outdated_lines(entries: &[OutdatedDataSnapshot]) -> Vec<Line<'static>> {
        if entries.is_empty() {
            return vec![Line::from(Span::raw(
                "No known outdated datasets; all runs match current dependencies.",
            ))];
        }
        let mut lines = Vec::new();
        for entry in entries {
            let dependency_label = entry
                .dependency_day
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            let lag_label = entry
                .lag_days
                .map(|lag| format!("{} day lag", lag))
                .unwrap_or_else(|| "lag unknown".to_string());
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{:<12}", entry.dataset),
                    Style::default().fg(Color::Yellow).bold(),
                ),
                Span::raw(format!(
                    "{} → {} ({}, {})",
                    entry.target_day, entry.dependency, dependency_label, lag_label
                )),
            ]));
            if let Some(note) = &entry.note {
                lines.push(Line::from(vec![
                    Span::styled("  note: ", Style::default().fg(Color::Cyan)),
                    Span::raw(note.clone()),
                ]));
            }
            lines.push(Line::from(""));
        }
        lines
    }

    fn status_label(status: OverallStatus) -> &'static str {
        match status {
            OverallStatus::Ok => "OK",
            OverallStatus::Warn => "WARN",
            OverallStatus::Crit => "CRIT",
        }
    }

    fn status_color(status: OverallStatus) -> Color {
        match status {
            OverallStatus::Ok => Color::Green,
            OverallStatus::Warn => Color::Yellow,
            OverallStatus::Crit => Color::Red,
        }
    }

    fn format_file_label(
        name: &str,
        read_mb: f64,
        total_mb: Option<f64>,
        throughput_mb_s: f64,
        max_len: usize,
    ) -> String {
        let safe_len = max_len.max(10);
        let shortened_name = Self::shorten_tail(name, safe_len.saturating_sub(25));
        let mut label = if let Some(total) = total_mb {
            format!(
                "{}  {:.1}/{:.1} MB  {:.1} MB/s",
                shortened_name, read_mb, total, throughput_mb_s
            )
        } else {
            format!(
                "{}  {:.1} MB read  (unknown total)",
                shortened_name, read_mb
            )
        };
        if label.len() > safe_len {
            label.truncate(safe_len);
        }
        label
    }

    fn shorten_tail(text: &str, max_len: usize) -> String {
        if text.len() <= max_len {
            return text.to_string();
        }
        if max_len <= 1 {
            return "…".to_string();
        }
        let tail = &text[text.len() - (max_len - 1)..];
        format!("…{}", tail)
    }
}
