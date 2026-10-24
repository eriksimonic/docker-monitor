mod logs;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bollard::Docker;
use bollard::models::ContainerStatsResponse;
use bollard::query_parameters::{ListContainersOptionsBuilder, StatsOptionsBuilder};
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use futures_util::StreamExt;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols::Marker;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Axis, Block, Cell, Chart, Dataset, GraphType, Row, Table, TableState};
use ratatui::{DefaultTerminal, Frame};
use tokio::sync::mpsc;

use logs::LogView;

const WINDOWS: [f64; 3] = [1.0, 2.0, 5.0];
const WINDOW_LABELS: [&str; 4] = ["1s", "2s", "5s", "avg"];
const MAX_SAMPLES: usize = 300;
const MAX_POINTS: usize = 120;

#[derive(Clone, Copy)]
struct Sample {
    t: Instant,
    cpu_ns: u64,
    periods: u64,
    throttled: u64,
    mem: u64,
    mem_limit: u64,
    io_r: u64,
    io_w: u64,
    rx: u64,
    tx: u64,
}

enum Msg {
    Sample(String, String, Sample),
    Gone(String),
}

/// Rates over the 1s, 2s and 5s windows and since the first sample (index 3).
#[derive(Default, Clone, Copy)]
struct Rates {
    cpu: [f64; 4],
    io_r: [f64; 4],
    io_w: [f64; 4],
    rx: [f64; 4],
    tx: [f64; 4],
    throttle: f64,
}

#[derive(Clone, Copy)]
struct Point {
    cpu: f64,
    mem: f64,
    io_r: f64,
    io_w: f64,
    rx: f64,
    tx: f64,
}

struct Container {
    name: String,
    first: Sample,
    samples: VecDeque<Sample>,
    points: VecDeque<Point>,
    rates: Rates,
    mem_avg: f64,
}

impl Container {
    fn new(name: String, s: Sample) -> Self {
        Self {
            name,
            first: s,
            samples: VecDeque::from([s]),
            points: VecDeque::new(),
            rates: Rates::default(),
            mem_avg: s.mem as f64,
        }
    }

    fn push(&mut self, s: Sample) {
        self.samples.push_back(s);
        if self.samples.len() > MAX_SAMPLES {
            self.samples.pop_front();
        }
        self.rates = self.compute_rates();
        self.mem_avg = self.samples.iter().map(|s| s.mem as f64).sum::<f64>() / self.samples.len() as f64;
        self.points.push_back(Point {
            cpu: self.rates.cpu[0],
            mem: s.mem as f64,
            io_r: self.rates.io_r[0],
            io_w: self.rates.io_w[0],
            rx: self.rates.rx[0],
            tx: self.rates.tx[0],
        });
        if self.points.len() > MAX_POINTS {
            self.points.pop_front();
        }
    }

    fn compute_rates(&self) -> Rates {
        let last = *self.samples.back().unwrap();
        let mut r = Rates::default();
        for i in 0..4 {
            let base = if i == 3 {
                self.first
            } else {
                let w = WINDOWS[i] * 0.9;
                *self
                    .samples
                    .iter()
                    .rev()
                    .skip(1)
                    .find(|s| (last.t - s.t).as_secs_f64() >= w)
                    .unwrap_or_else(|| self.samples.front().unwrap())
            };
            let dt = (last.t - base.t).as_secs_f64();
            if dt <= 0.0 {
                continue;
            }
            let rate = |a: u64, b: u64| a.saturating_sub(b) as f64 / dt;
            r.cpu[i] = rate(last.cpu_ns, base.cpu_ns) / 1e7;
            r.io_r[i] = rate(last.io_r, base.io_r);
            r.io_w[i] = rate(last.io_w, base.io_w);
            r.rx[i] = rate(last.rx, base.rx);
            r.tx[i] = rate(last.tx, base.tx);
            if i == 2 {
                let periods = last.periods.saturating_sub(base.periods);
                if periods > 0 {
                    r.throttle = last.throttled.saturating_sub(base.throttled) as f64 * 100.0 / periods as f64;
                }
            }
        }
        r
    }
}

fn sample_from(s: &ContainerStatsResponse) -> Sample {
    let cpu = s.cpu_stats.as_ref();
    let throttling = cpu.and_then(|c| c.throttling_data.as_ref());
    let mem = s.memory_stats.as_ref();
    let cache = mem
        .and_then(|m| m.stats.as_ref())
        .and_then(|m| m.get("inactive_file").or_else(|| m.get("total_inactive_file")))
        .copied()
        .unwrap_or(0);
    let (mut io_r, mut io_w) = (0, 0);
    let entries = s
        .blkio_stats
        .as_ref()
        .and_then(|b| b.io_service_bytes_recursive.as_ref());
    for e in entries.into_iter().flatten() {
        match e.op.as_deref().map(str::to_ascii_lowercase).as_deref() {
            Some("read") => io_r += e.value.unwrap_or(0),
            Some("write") => io_w += e.value.unwrap_or(0),
            _ => {}
        }
    }
    let (mut rx, mut tx) = (0, 0);
    for n in s.networks.iter().flat_map(|n| n.values()) {
        rx += n.rx_bytes.unwrap_or(0);
        tx += n.tx_bytes.unwrap_or(0);
    }
    Sample {
        t: Instant::now(),
        cpu_ns: cpu.and_then(|c| c.cpu_usage.as_ref()).and_then(|u| u.total_usage).unwrap_or(0),
        periods: throttling.and_then(|t| t.periods).unwrap_or(0),
        throttled: throttling.and_then(|t| t.throttled_periods).unwrap_or(0),
        mem: mem.and_then(|m| m.usage).unwrap_or(0).saturating_sub(cache),
        mem_limit: mem.and_then(|m| m.limit).unwrap_or(0),
        io_r,
        io_w,
        rx,
        tx,
    }
}

async fn connect(args: &[String]) -> Result<(Docker, &'static str), String> {
    let mut candidates: Vec<(&'static str, Result<Docker, bollard::errors::Error>)> = Vec::new();
    if let Some(i) = args.iter().position(|a| a == "--host") {
        let host = args.get(i + 1).ok_or("--host needs a value")?;
        candidates.push(("custom", Docker::connect_with_host(host)));
    } else if args.iter().any(|a| a == "--podman") {
        candidates.push(("podman", Docker::connect_with_podman_defaults()));
    } else {
        candidates.push(("docker", Docker::connect_with_defaults()));
        candidates.push(("podman", Docker::connect_with_podman_defaults()));
    }
    let mut last_err = String::from("no runtime found");
    for (name, c) in candidates {
        match c {
            Ok(d) => match d.ping().await {
                Ok(_) => return Ok((d, name)),
                Err(e) => last_err = format!("{name}: {e}"),
            },
            Err(e) => last_err = format!("{name}: {e}"),
        }
    }
    Err(last_err)
}

/// Lists running containers every 2s and streams stats for each new one.
async fn watch(docker: Docker, tx: mpsc::UnboundedSender<Msg>) {
    let tracked: Arc<Mutex<HashSet<String>>> = Arc::default();
    loop {
        let opts = ListContainersOptionsBuilder::default().build();
        if let Ok(list) = docker.list_containers(Some(opts)).await {
            for c in list {
                let Some(id) = c.id else { continue };
                if !tracked.lock().unwrap().insert(id.clone()) {
                    continue;
                }
                let name = c
                    .names
                    .and_then(|n| n.into_iter().next())
                    .map(|n| n.trim_start_matches('/').to_string())
                    .unwrap_or_else(|| id.chars().take(12).collect());
                let (docker, tx, tracked) = (docker.clone(), tx.clone(), tracked.clone());
                tokio::spawn(async move {
                    let opts = StatsOptionsBuilder::default().stream(true).one_shot(false).build();
                    let mut stream = docker.stats(&id, Some(opts));
                    while let Some(Ok(s)) = stream.next().await {
                        if tx.send(Msg::Sample(id.clone(), name.clone(), sample_from(&s))).is_err() {
                            return;
                        }
                    }
                    tracked.lock().unwrap().remove(&id);
                    let _ = tx.send(Msg::Gone(id));
                });
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Sort {
    Name,
    Cpu,
    Mem,
    Io,
    Net,
}

impl Sort {
    fn next(self) -> Self {
        match self {
            Sort::Name => Sort::Cpu,
            Sort::Cpu => Sort::Mem,
            Sort::Mem => Sort::Io,
            Sort::Io => Sort::Net,
            Sort::Net => Sort::Name,
        }
    }
}

struct App {
    docker: Docker,
    log: Option<LogView>,
    runtime: &'static str,
    containers: HashMap<String, Container>,
    table: TableState,
    sort: Sort,
    /// Rate column shown: index into `WINDOW_LABELS` (1s, 2s, 5s, avg).
    win: usize,
}

impl App {
    fn order(&self) -> Vec<&String> {
        let mut ids: Vec<&String> = self.containers.keys().collect();
        let c = |id: &&String| &self.containers[*id];
        match self.sort {
            Sort::Name => ids.sort_by(|a, b| c(a).name.cmp(&c(b).name)),
            Sort::Cpu => ids.sort_by(|a, b| c(b).rates.cpu[0].total_cmp(&c(a).rates.cpu[0])),
            Sort::Mem => ids.sort_by(|a, b| c(b).samples.back().unwrap().mem.cmp(&c(a).samples.back().unwrap().mem)),
            Sort::Io => ids.sort_by(|a, b| io(c(b)).total_cmp(&io(c(a)))),
            Sort::Net => ids.sort_by(|a, b| net(c(b)).total_cmp(&net(c(a)))),
        }
        ids
    }

    fn apply(&mut self, m: Msg) {
        match m {
            Msg::Sample(id, name, s) => match self.containers.get_mut(&id) {
                Some(c) => c.push(s),
                None => {
                    self.containers.insert(id, Container::new(name, s));
                }
            },
            Msg::Gone(id) => {
                self.containers.remove(&id);
            }
        }
    }

    fn selected_id(&self) -> Option<&String> {
        let order = self.order();
        let i = self.table.selected()?.min(order.len().checked_sub(1)?);
        Some(order[i])
    }

    fn selected(&self) -> Option<&Container> {
        self.selected_id().map(|id| &self.containers[id])
    }

    fn step(&mut self, delta: isize) {
        let n = self.containers.len();
        if n == 0 {
            return;
        }
        let i = self.table.selected().unwrap_or(0) as isize + delta;
        self.table.select(Some(i.clamp(0, n as isize - 1) as usize));
    }
}

fn io(c: &Container) -> f64 {
    c.rates.io_r[0] + c.rates.io_w[0]
}

fn net(c: &Container) -> f64 {
    c.rates.rx[0] + c.rates.tx[0]
}

fn bytes(v: f64) -> String {
    const U: [&str; 5] = ["B", "K", "M", "G", "T"];
    let mut v = v;
    let mut i = 0;
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 { format!("{v:.0}{}", U[i]) } else { format!("{v:.1}{}", U[i]) }
}

fn rate(v: f64) -> String {
    format!("{}/s", bytes(v))
}

fn cpu_cell(v: f64) -> Cell<'static> {
    let color = match v {
        v if v >= 80.0 => Color::Red,
        v if v >= 40.0 => Color::Yellow,
        _ => Color::Green,
    };
    Cell::from(Line::from(format!("{v:.1}%")).right_aligned()).style(Style::new().fg(color))
}

fn num(s: String, color: Color) -> Cell<'static> {
    Cell::from(Line::from(s).right_aligned()).style(Style::new().fg(color))
}

fn group(title: &str, cols: &[&str]) -> Vec<Cell<'static>> {
    cols.iter()
        .enumerate()
        .map(|(i, c)| {
            let top = if i == 0 { title.to_string() } else { String::new() };
            Cell::from(Text::from(vec![Line::from(top).right_aligned(), Line::from(c.to_string()).right_aligned()]))
        })
        .collect()
}

fn draw(f: &mut Frame, app: &mut App) {
    let [head, main, detail, foot] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(6),
        Constraint::Length(14),
        Constraint::Length(1),
    ])
    .areas(f.area());

    let sort = match app.sort {
        Sort::Name => "name",
        Sort::Cpu => "cpu",
        Sort::Mem => "mem",
        Sort::Io => "io",
        Sort::Net => "net",
    };
    f.render_widget(
        Line::from(vec![
            Span::styled(" dmon ", Style::new().bg(Color::Cyan).fg(Color::Black).add_modifier(Modifier::BOLD)),
            Span::raw(format!("  {}  {} containers  sort: {sort}", app.runtime, app.containers.len())),
        ]),
        head,
    );
    f.render_widget(
        Line::styled(format!(" q quit  up/down select   l logs   s sort   w window: {} (1s 2s 5s = rate over window, avg = since start)", WINDOW_LABELS[app.win]), Style::new().fg(Color::DarkGray)),
        foot,
    );

    let order: Vec<String> = app.order().into_iter().cloned().collect();
    if app.table.selected().is_none() && !order.is_empty() {
        app.table.select(Some(0));
    }

    let mut header = vec![Cell::from(Text::from(vec![Line::from(""), Line::from("CONTAINER")]))];
    let w = app.win;
    let label = WINDOW_LABELS[w];
    header.extend(group("CPU", &[label]));
    header.extend(group("MEM", &["used", "limit%", "avg"]));
    header.extend(group("I/O r+w", &[label]));
    header.extend(group("NET rx+tx", &[label]));
    header.extend(group("CPU", &["throttle"]));
    let header = Row::new(header).height(2).style(Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = order
        .iter()
        .map(|id| {
            let c = &app.containers[id];
            let last = c.samples.back().unwrap();
            let r = &c.rates;
            let mut cells = vec![Cell::from(c.name.clone()).style(Style::new().add_modifier(Modifier::BOLD))];
            cells.push(cpu_cell(r.cpu[w]));
            let pct = if last.mem_limit > 0 { last.mem as f64 * 100.0 / last.mem_limit as f64 } else { 0.0 };
            cells.push(num(bytes(last.mem as f64), Color::White));
            cells.push(num(
                format!("{pct:.1}%"),
                if pct >= 90.0 { Color::Red } else if pct >= 70.0 { Color::Yellow } else { Color::Green },
            ));
            cells.push(num(bytes(c.mem_avg), Color::Gray));
            cells.push(num(rate(r.io_r[w] + r.io_w[w]), Color::Magenta));
            cells.push(num(rate(r.rx[w] + r.tx[w]), Color::Blue));
            let t = r.throttle;
            cells.push(num(format!("{t:.1}%"), if t > 0.0 { Color::Red } else { Color::DarkGray }));
            Row::new(cells)
        })
        .collect();

    let mut widths = vec![Constraint::Min(18)];
    widths.push(Constraint::Length(7));
    widths.extend([Constraint::Length(7), Constraint::Length(7), Constraint::Length(7)]);
    widths.push(Constraint::Length(10));
    widths.push(Constraint::Length(10));
    widths.push(Constraint::Length(9));
    let table = Table::new(rows, widths)
        .header(header)
        .column_spacing(1)
        .row_highlight_style(Style::new().bg(Color::Rgb(40, 44, 60)))
        .block(Block::bordered().title(" containers "));
    f.render_stateful_widget(table, main, &mut app.table);

    match app.selected() {
        Some(c) => draw_detail(f, detail, c),
        None => f.render_widget(
            Line::from("waiting for running containers...").alignment(Alignment::Center),
            detail,
        ),
    }
}

fn draw_detail(f: &mut Frame, area: Rect, c: &Container) {
    let cols = Layout::horizontal([Constraint::Ratio(1, 4); 4]).split(area);
    let series = |g: fn(&Point) -> f64| -> Vec<(f64, f64)> {
        c.points.iter().enumerate().map(|(i, p)| (i as f64, g(p))).collect()
    };
    let pct = |v: f64| format!("{v:.0}%");
    chart(f, cols[0], &format!(" {} cpu ", c.name), vec![("cpu", Color::Green, series(|p| p.cpu))], pct, 100.0);
    chart(f, cols[1], " memory ", vec![("used", Color::Yellow, series(|p| p.mem))], bytes, 1024.0 * 1024.0);
    chart(
        f,
        cols[2],
        " disk i/o ",
        vec![("read", Color::Magenta, series(|p| p.io_r)), ("write", Color::LightRed, series(|p| p.io_w))],
        rate,
        1024.0,
    );
    chart(
        f,
        cols[3],
        " network ",
        vec![("rx", Color::Cyan, series(|p| p.rx)), ("tx", Color::LightBlue, series(|p| p.tx))],
        rate,
        1024.0,
    );
}

fn chart(
    f: &mut Frame,
    area: Rect,
    title: &str,
    series: Vec<(&str, Color, Vec<(f64, f64)>)>,
    fmt: fn(f64) -> String,
    min_top: f64,
) {
    let max = series
        .iter()
        .flat_map(|(_, _, d)| d.iter().map(|p| p.1))
        .fold(min_top, f64::max)
        * 1.1;
    let datasets: Vec<Dataset> = series
        .iter()
        .map(|(name, color, data)| {
            Dataset::default()
                .name(*name)
                .marker(Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::new().fg(*color))
                .data(data)
        })
        .collect();
    let single = datasets.len() == 1;
    let mut chart = Chart::new(datasets)
        .block(Block::bordered().title(title.to_string()))
        .x_axis(Axis::default().bounds([0.0, MAX_POINTS as f64]))
        .y_axis(
            Axis::default()
                .bounds([0.0, max])
                .labels([fmt(0.0), fmt(max / 2.0), fmt(max)]),
        );
    if single {
        chart = chart.legend_position(None);
    }
    f.render_widget(chart, area);
}

fn run(term: &mut DefaultTerminal, app: &mut App, rx: &mut mpsc::UnboundedReceiver<Msg>) -> std::io::Result<()> {
    loop {
        while let Ok(m) = rx.try_recv() {
            app.apply(m);
        }
        if let Some(l) = app.log.as_mut() {
            l.drain();
        }
        term.draw(|f| match app.log.as_mut() {
            Some(l) => l.draw(f),
            None => draw(f, app),
        })?;
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(k) = event::read()? {
                if k.code == KeyCode::Char('c') && k.modifiers.contains(KeyModifiers::CONTROL) {
                    return Ok(());
                }
                if let Some(l) = app.log.as_mut() {
                    if l.key(k.code) {
                        app.log = None;
                    }
                    continue;
                }
                match k.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    KeyCode::Char('l') | KeyCode::Enter => {
                        if let Some(id) = app.selected_id().cloned() {
                            let name = app.containers[&id].name.clone();
                            app.log = Some(LogView::new(app.docker.clone(), id, name));
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') => app.step(1),
                    KeyCode::Up | KeyCode::Char('k') => app.step(-1),
                    KeyCode::Char('s') => app.sort = app.sort.next(),
                    KeyCode::Char('w') => app.win = (app.win + 1) % WINDOW_LABELS.len(),
                    _ => {}
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (docker, runtime) = match connect(&args).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cannot connect to a container runtime ({e}). Use --host unix:///path/to.sock or set DOCKER_HOST.");
            std::process::exit(1);
        }
    };
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(watch(docker.clone(), tx));
    let mut app = App { docker, log: None, runtime, containers: HashMap::new(), table: TableState::default(), sort: Sort::Name, win: 0 };
    let mut term = ratatui::init();
    let result = run(&mut term, &mut app, &mut rx);
    ratatui::restore();
    if let Err(e) = result {
        eprintln!("{e}");
    }
}
