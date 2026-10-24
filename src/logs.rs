use std::collections::VecDeque;

use bollard::Docker;
use bollard::query_parameters::LogsOptionsBuilder;
use crossterm::event::KeyCode;
use futures_util::StreamExt;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const MAX_LINES: usize = 10_000;

/// Full-screen log viewer for one container. `top` is the index of the first visible line.
/// Without a filter it shows the last 1000 lines and follows. With a filter it re-reads the
/// whole log history and keeps only the lines that contain the filter, like `logs | grep -i`.
pub struct LogView {
    docker: Docker,
    id: String,
    name: String,
    lines: VecDeque<String>,
    rx: mpsc::UnboundedReceiver<String>,
    task: JoinHandle<()>,
    top: usize,
    height: usize,
    width: usize,
    follow: bool,
    filter: String,
    input: String,
    typing: bool,
}

impl Drop for LogView {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn stream_logs(docker: Docker, id: String, filter: String) -> (mpsc::UnboundedReceiver<String>, JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let tail = if filter.is_empty() { "1000" } else { "all" };
        let opts = LogsOptionsBuilder::default()
            .follow(true)
            .stdout(true)
            .stderr(true)
            .tail(tail)
            .build();
        let filter = filter.to_ascii_lowercase();
        let mut stream = docker.logs(&id, Some(opts));
        let mut carry = String::new();
        while let Some(Ok(out)) = stream.next().await {
            carry.push_str(&String::from_utf8_lossy(&out.into_bytes()));
            while let Some(i) = carry.find('\n') {
                let line = clean(&carry[..=i]);
                carry.drain(..=i);
                if line.to_ascii_lowercase().contains(&filter) && tx.send(line).is_err() {
                    return;
                }
            }
        }
    });
    (rx, task)
}

impl LogView {
    pub fn new(docker: Docker, id: String, name: String) -> Self {
        let (rx, task) = stream_logs(docker.clone(), id.clone(), String::new());
        Self {
            docker,
            id,
            name,
            lines: VecDeque::new(),
            rx,
            task,
            top: 0,
            height: 1,
            width: 1,
            follow: true,
            filter: String::new(),
            input: String::new(),
            typing: false,
        }
    }

    /// Replaces the stream with one for the new filter. An empty filter restores the plain tail.
    fn set_filter(&mut self, filter: String) {
        self.task.abort();
        (self.rx, self.task) = stream_logs(self.docker.clone(), self.id.clone(), filter.clone());
        self.filter = filter;
        self.lines.clear();
        self.top = 0;
        self.follow = true;
    }

    pub fn drain(&mut self) {
        while let Ok(line) = self.rx.try_recv() {
            self.lines.push_back(line);
            if self.lines.len() > MAX_LINES {
                self.lines.pop_front();
                self.top = self.top.saturating_sub(1);
            }
        }
    }

    /// Returns true when the view should close.
    pub fn key(&mut self, k: KeyCode) -> bool {
        if self.typing {
            match k {
                KeyCode::Esc => self.typing = false,
                KeyCode::Enter => {
                    self.typing = false;
                    let filter = std::mem::take(&mut self.input);
                    self.set_filter(filter);
                }
                KeyCode::Backspace => {
                    self.input.pop();
                }
                KeyCode::Char(c) => self.input.push(c),
                _ => {}
            }
            return false;
        }
        let page = self.height.max(1) as isize;
        match k {
            KeyCode::Char('q') => return true,
            KeyCode::Esc if self.filter.is_empty() => return true,
            KeyCode::Esc => self.set_filter(String::new()),
            KeyCode::Char('/') => {
                self.typing = true;
                self.input.clear();
            }
            KeyCode::Char('f') => self.follow = !self.follow,
            KeyCode::Char('G') | KeyCode::End => self.follow = true,
            KeyCode::Char('g') | KeyCode::Home => self.scroll(isize::MIN / 2),
            KeyCode::Up | KeyCode::Char('k') => self.scroll(-1),
            KeyCode::Down | KeyCode::Char('j') => self.scroll(1),
            KeyCode::PageUp => self.scroll(-page),
            KeyCode::PageDown => self.scroll(page),
            _ => {}
        }
        false
    }

    fn rows(&self, line: &str) -> usize {
        line.chars().count().div_ceil(self.width).max(1)
    }

    /// First line index such that the lines from there to the end fit on screen once wrapped.
    fn max_top(&self) -> usize {
        let mut top = self.lines.len();
        let mut used = 0;
        while top > 0 {
            let r = self.rows(&self.lines[top - 1]);
            if used + r > self.height {
                break;
            }
            used += r;
            top -= 1;
        }
        if top == self.lines.len() { top.saturating_sub(1) } else { top }
    }

    fn scroll(&mut self, delta: isize) {
        let last = self.lines.len().saturating_sub(1);
        self.top = self.top.saturating_add_signed(delta).min(last);
        self.follow = self.top >= self.max_top();
    }

    pub fn draw(&mut self, f: &mut Frame) {
        let [body, foot] = Layout::vertical([Constraint::Min(3), Constraint::Length(1)]).areas(f.area());
        self.height = body.height.saturating_sub(2) as usize;
        self.width = (body.width.saturating_sub(2) as usize).max(1);
        if self.follow {
            self.top = self.max_top();
        }
        let q = self.filter.to_ascii_lowercase();
        let mut lines: Vec<Line> = Vec::new();
        for l in self.lines.iter().skip(self.top) {
            if lines.len() >= self.height {
                break;
            }
            lines.extend(wrap(spans(l, &q), self.width));
        }
        lines.truncate(self.height);
        let mode = if self.follow { " FOLLOW " } else { " PAUSED " };
        let mut title = vec![Span::raw(format!(
            " {} logs  line {}/{} ",
            self.name,
            (self.top + 1).min(self.lines.len()),
            self.lines.len()
        ))];
        if !self.filter.is_empty() {
            title.push(Span::styled(
                format!(" grep: {} ", self.filter),
                Style::new().bg(Color::Yellow).fg(Color::Black),
            ));
            title.push(Span::raw(" "));
        }
        title.push(Span::styled(mode, Style::new().bg(if self.follow { Color::Green } else { Color::Yellow }).fg(Color::Black)));
        f.render_widget(Paragraph::new(lines).block(Block::bordered().title(Line::from(title))), body);

        let footer = if self.typing {
            Line::from(vec![
                Span::styled(" grep /", Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
                Span::raw(format!("{}_", self.input)),
                Span::styled("   enter apply (empty clears)   esc cancel", Style::new().fg(Color::DarkGray)),
            ])
        } else {
            let esc = if self.filter.is_empty() { "esc close" } else { "esc clear grep" };
            Line::styled(
                format!(" q close   {esc}   / grep (whole history)   f follow   up/down pgup/pgdn g/G scroll"),
                Style::new().fg(Color::DarkGray),
            )
        };
        f.render_widget(footer, foot);
    }
}

/// Drops ANSI escape sequences and control characters, which would corrupt the terminal.
fn clean(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\x1b' => {
                if chars.peek() == Some(&'[') {
                    chars.next();
                    for c in chars.by_ref() {
                        if ('@'..='~').contains(&c) {
                            break;
                        }
                    }
                }
            }
            '\t' => out.push_str("    "),
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
    out
}

fn level_style(l: &str) -> Style {
    let l = l.to_ascii_lowercase();
    let has = |keys: &[&str]| keys.iter().any(|k| l.contains(k));
    if has(&["error", "fatal", "panic", "exception", "err]", "ftl]", "crit"]) {
        Style::new().fg(Color::Red)
    } else if has(&["warn", "wrn]"]) {
        Style::new().fg(Color::Yellow)
    } else if has(&["debug", "trace", "dbg]", "trc]", "vrb]"]) {
        Style::new().fg(Color::DarkGray)
    } else {
        Style::new()
    }
}

/// Colours the line by log level and highlights search matches.
/// `q` must already be ASCII-lowercased. ASCII lowercasing keeps byte offsets aligned with `l`.
fn spans(l: &str, q: &str) -> Vec<Span<'static>> {
    let base = level_style(l);
    let hit = Style::new().bg(Color::Yellow).fg(Color::Black);
    let mut out = Vec::new();
    let mut pos = 0;
    if !q.is_empty() {
        let lower = l.to_ascii_lowercase();
        while let Some(i) = lower[pos..].find(q) {
            let (s, e) = (pos + i, pos + i + q.len());
            out.push(Span::styled(l[pos..s].to_string(), base));
            out.push(Span::styled(l[s..e].to_string(), hit));
            pos = e;
        }
    }
    out.push(Span::styled(l[pos..].to_string(), base));
    out
}

/// Breaks a styled line into rows of `width` characters.
fn wrap(spans: Vec<Span<'static>>, width: usize) -> Vec<Line<'static>> {
    let mut rows = Vec::new();
    let mut cur: Vec<Span<'static>> = Vec::new();
    let mut used = 0;
    for span in spans {
        let mut buf = String::new();
        for c in span.content.chars() {
            if used == width {
                if !buf.is_empty() {
                    cur.push(Span::styled(std::mem::take(&mut buf), span.style));
                }
                rows.push(Line::from(std::mem::take(&mut cur)));
                used = 0;
            }
            buf.push(c);
            used += 1;
        }
        if !buf.is_empty() {
            cur.push(Span::styled(buf, span.style));
        }
    }
    rows.push(Line::from(cur));
    rows
}
