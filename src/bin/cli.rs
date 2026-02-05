//! Interactive TUI for LSM Tree
//!
//! A beautiful terminal user interface to explore and interact with the LSM Tree.
//!
//! Run with: cargo run --bin lsm-cli

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use lsm_tree::LSMTree;
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Gauge, List, ListItem, Paragraph, Tabs},
};
use std::{
    io,
    path::PathBuf,
    time::{Duration, Instant},
};

/// Application state
struct App {
    /// The LSM tree instance
    lsm: LSMTree,
    /// Current active tab
    current_tab: usize,
    /// Input mode for key-value entry
    input_mode: InputMode,
    /// Current key input
    key_input: String,
    /// Current value input
    value_input: String,
    /// Search key input
    search_input: String,
    /// Search result
    search_result: Option<SearchResult>,
    /// Message log
    messages: Vec<(Instant, String, MessageType)>,
    /// Selected SSTable index for viewing
    selected_sstable: usize,
    /// Scroll offset for SSTable view
    sstable_scroll: usize,
    /// Scroll offset for memtable view
    memtable_scroll: usize,
    /// Operation history for visualization
    operation_history: Vec<Operation>,
    /// Should quit
    should_quit: bool,
    /// Show help popup
    show_help: bool,
    /// Auto-demo mode
    auto_demo: bool,
    /// Demo step counter
    demo_step: usize,
    /// Last demo time
    last_demo_time: Instant,
}

#[derive(Clone)]
enum Operation {
    Put(String, String),
    Get(String, bool), // key, found
    Flush,
}

enum SearchResult {
    Found(String),
    NotFound,
}

#[derive(PartialEq)]
enum InputMode {
    Normal,
    EnteringKey,
    EnteringValue,
    Searching,
}

#[derive(Clone)]
enum MessageType {
    Info,
    Success,
    Warning,
    Error,
}

impl App {
    fn new() -> io::Result<Self> {
        // Clean up for fresh start
        let _ = std::fs::remove_dir_all("./lsm_cli_data");

        let lsm = LSMTree::new(PathBuf::from("./lsm_cli_data"), 200)?;

        Ok(Self {
            lsm,
            current_tab: 0,
            input_mode: InputMode::Normal,
            key_input: String::new(),
            value_input: String::new(),
            search_input: String::new(),
            search_result: None,
            messages: Vec::new(),
            selected_sstable: 0,
            sstable_scroll: 0,
            memtable_scroll: 0,
            operation_history: Vec::new(),
            should_quit: false,
            show_help: false,
            auto_demo: false,
            demo_step: 0,
            last_demo_time: Instant::now(),
        })
    }

    fn add_message(&mut self, msg: String, msg_type: MessageType) {
        self.messages.push((Instant::now(), msg, msg_type));
        // Keep only last 100 messages
        if self.messages.len() > 100 {
            self.messages.remove(0);
        }
    }

    fn put(&mut self, key: String, value: String) {
        match self
            .lsm
            .put(key.as_bytes().to_vec(), value.as_bytes().to_vec())
        {
            Ok(_) => {
                self.add_message(format!("PUT {} = {}", key, value), MessageType::Success);
                self.operation_history.push(Operation::Put(key, value));
            }
            Err(e) => {
                self.add_message(format!("Error: {}", e), MessageType::Error);
            }
        }
    }

    fn get(&mut self, key: &str) -> Option<String> {
        let result = self.lsm.get(key.as_bytes());
        let found = result.is_some();
        self.operation_history
            .push(Operation::Get(key.to_string(), found));

        result.map(|v| String::from_utf8_lossy(&v).to_string())
    }

    fn run_demo_step(&mut self) {
        let demo_keys = vec![
            ("user:alice", "Alice Johnson"),
            ("user:bob", "Bob Smith"),
            ("user:charlie", "Charlie Brown"),
            ("product:1", "Widget A"),
            ("product:2", "Widget B"),
            ("product:3", "Gadget X"),
            ("order:100", "Order for Alice"),
            ("order:101", "Order for Bob"),
            ("config:theme", "dark"),
            ("config:lang", "en"),
        ];

        if self.demo_step < demo_keys.len() {
            let (key, value) = demo_keys[self.demo_step];
            self.put(key.to_string(), value.to_string());
            self.demo_step += 1;
        } else if self.demo_step < demo_keys.len() + 5 {
            // Search for some keys
            let search_keys = [
                "user:alice",
                "user:nonexistent",
                "product:1",
                "missing:key",
                "config:theme",
            ];
            let idx = self.demo_step - demo_keys.len();
            let key = search_keys[idx];
            let result = self.get(key);
            match result {
                Some(v) => self.add_message(format!("GET {} = {}", key, v), MessageType::Info),
                None => self.add_message(format!("GET {} = NOT FOUND", key), MessageType::Warning),
            }
            self.demo_step += 1;
        } else {
            self.auto_demo = false;
            self.add_message("Demo complete!".to_string(), MessageType::Success);
        }
    }
}

fn main() -> io::Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create app
    let mut app = App::new()?;

    // Initial welcome message
    app.add_message(
        "Welcome to LSM Tree Explorer! Press 'h' for help.".to_string(),
        MessageType::Info,
    );

    // Main loop
    let tick_rate = Duration::from_millis(100);
    let mut last_tick = Instant::now();

    loop {
        terminal.draw(|f| ui(f, &mut app))?;

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));

        if crossterm::event::poll(timeout)?
            && let Event::Key(key) = event::read()?
        {
            handle_input(&mut app, key.code, key.modifiers);
        }

        if last_tick.elapsed() >= tick_rate {
            // Auto-demo tick
            if app.auto_demo && app.last_demo_time.elapsed() >= Duration::from_millis(500) {
                app.run_demo_step();
                app.last_demo_time = Instant::now();
            }

            // Clean old messages (older than 10 seconds)
            let now = Instant::now();
            app.messages
                .retain(|(time, _, _)| now.duration_since(*time) < Duration::from_secs(30));

            last_tick = Instant::now();
        }

        if app.should_quit {
            break;
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    // Cleanup
    let _ = std::fs::remove_dir_all("./lsm_cli_data");

    Ok(())
}

fn handle_input(app: &mut App, key: KeyCode, modifiers: KeyModifiers) {
    // Handle help popup
    if app.show_help {
        if matches!(key, KeyCode::Esc | KeyCode::Char('h') | KeyCode::Char('q')) {
            app.show_help = false;
        }
        return;
    }

    match app.input_mode {
        InputMode::Normal => match key {
            KeyCode::Char('q') => app.should_quit = true,
            KeyCode::Char('h') => app.show_help = true,
            KeyCode::Char('1') => app.current_tab = 0,
            KeyCode::Char('2') => app.current_tab = 1,
            KeyCode::Char('3') => app.current_tab = 2,
            KeyCode::Char('4') => app.current_tab = 3,
            KeyCode::Tab => app.current_tab = (app.current_tab + 1) % 4,
            KeyCode::BackTab => app.current_tab = (app.current_tab + 3) % 4,
            KeyCode::Char('p') | KeyCode::Char('i') => {
                app.input_mode = InputMode::EnteringKey;
                app.key_input.clear();
                app.value_input.clear();
            }
            KeyCode::Char('g') | KeyCode::Char('/') => {
                app.input_mode = InputMode::Searching;
                app.search_input.clear();
                app.search_result = None;
            }
            KeyCode::Char('f') => {
                if let Err(e) = app.lsm.flush() {
                    app.add_message(format!("Flush error: {}", e), MessageType::Error);
                } else {
                    app.add_message(
                        "Flushed memtable to SSTable".to_string(),
                        MessageType::Success,
                    );
                    app.operation_history.push(Operation::Flush);
                }
            }
            KeyCode::Char('r') => {
                app.lsm.reset_bloom_filter_stats();
                app.add_message("Reset Bloom filter stats".to_string(), MessageType::Info);
            }
            KeyCode::Char('d') => {
                app.auto_demo = !app.auto_demo;
                if app.auto_demo {
                    app.demo_step = 0;
                    app.add_message("Starting auto-demo...".to_string(), MessageType::Info);
                } else {
                    app.add_message("Demo paused".to_string(), MessageType::Info);
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if app.current_tab == 1 && app.memtable_scroll > 0 {
                    app.memtable_scroll -= 1;
                } else if app.current_tab == 2 {
                    if modifiers.contains(KeyModifiers::SHIFT) {
                        if app.selected_sstable > 0 {
                            app.selected_sstable -= 1;
                            app.sstable_scroll = 0;
                        }
                    } else if app.sstable_scroll > 0 {
                        app.sstable_scroll -= 1;
                    }
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if app.current_tab == 1 {
                    app.memtable_scroll += 1;
                } else if app.current_tab == 2 {
                    if modifiers.contains(KeyModifiers::SHIFT) {
                        if app.selected_sstable < app.lsm.sstable_count().saturating_sub(1) {
                            app.selected_sstable += 1;
                            app.sstable_scroll = 0;
                        }
                    } else {
                        app.sstable_scroll += 1;
                    }
                }
            }
            KeyCode::Left => {
                if app.selected_sstable > 0 {
                    app.selected_sstable -= 1;
                    app.sstable_scroll = 0;
                }
            }
            KeyCode::Right => {
                if app.selected_sstable < app.lsm.sstable_count().saturating_sub(1) {
                    app.selected_sstable += 1;
                    app.sstable_scroll = 0;
                }
            }
            _ => {}
        },
        InputMode::EnteringKey => match key {
            KeyCode::Enter => {
                if !app.key_input.is_empty() {
                    app.input_mode = InputMode::EnteringValue;
                }
            }
            KeyCode::Char(c) => {
                app.key_input.push(c);
            }
            KeyCode::Backspace => {
                app.key_input.pop();
            }
            KeyCode::Esc => {
                app.input_mode = InputMode::Normal;
                app.key_input.clear();
            }
            _ => {}
        },
        InputMode::EnteringValue => match key {
            KeyCode::Enter => {
                if !app.value_input.is_empty() {
                    let key = app.key_input.clone();
                    let value = app.value_input.clone();
                    app.put(key, value);
                    app.input_mode = InputMode::Normal;
                    app.key_input.clear();
                    app.value_input.clear();
                }
            }
            KeyCode::Char(c) => {
                app.value_input.push(c);
            }
            KeyCode::Backspace => {
                app.value_input.pop();
            }
            KeyCode::Esc => {
                app.input_mode = InputMode::Normal;
                app.key_input.clear();
                app.value_input.clear();
            }
            _ => {}
        },
        InputMode::Searching => match key {
            KeyCode::Enter => {
                let key = app.search_input.clone();
                let result = app.get(&key);
                app.search_result = Some(match result {
                    Some(v) => {
                        app.add_message(format!("Found: {} = {}", key, v), MessageType::Success);
                        SearchResult::Found(v)
                    }
                    None => {
                        app.add_message(format!("Not found: {}", key), MessageType::Warning);
                        SearchResult::NotFound
                    }
                });
            }
            KeyCode::Char(c) => {
                app.search_input.push(c);
            }
            KeyCode::Backspace => {
                app.search_input.pop();
            }
            KeyCode::Esc => {
                app.input_mode = InputMode::Normal;
                app.search_input.clear();
                app.search_result = None;
            }
            _ => {}
        },
    }
}

fn ui(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3), // Title
            Constraint::Length(3), // Tabs
            Constraint::Min(10),   // Main content
            Constraint::Length(3), // Status bar
            Constraint::Length(5), // Messages
        ])
        .split(f.area());

    // Title
    let title = Paragraph::new(vec![Line::from(vec![
        Span::styled("  LSM Tree ", Style::default().fg(Color::Cyan).bold()),
        Span::styled("Explorer", Style::default().fg(Color::Yellow).bold()),
        Span::raw("  "),
        Span::styled("[Bloom Filters Enabled]", Style::default().fg(Color::Green)),
    ])])
    .alignment(Alignment::Center)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan))
            .title_alignment(Alignment::Center),
    );
    f.render_widget(title, chunks[0]);

    // Tabs
    let tab_titles = vec![
        "[1] Dashboard",
        "[2] MemTable",
        "[3] SSTables",
        "[4] Bloom Filters",
    ];
    let tabs = Tabs::new(tab_titles)
        .block(Block::default().borders(Borders::ALL).title(" Navigation "))
        .select(app.current_tab)
        .style(Style::default().fg(Color::White))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );
    f.render_widget(tabs, chunks[1]);

    // Main content based on tab
    match app.current_tab {
        0 => render_dashboard(f, app, chunks[2]),
        1 => render_memtable(f, app, chunks[2]),
        2 => render_sstables(f, app, chunks[2]),
        3 => render_bloom_filters(f, app, chunks[2]),
        _ => {}
    }

    // Status bar
    render_status_bar(f, app, chunks[3]);

    // Messages
    render_messages(f, app, chunks[4]);

    // Input popup
    if app.input_mode != InputMode::Normal {
        render_input_popup(f, app);
    }

    // Help popup
    if app.show_help {
        render_help_popup(f);
    }
}

fn render_dashboard(f: &mut Frame, app: &mut App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    let left_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[0]);

    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[1]);

    // Stats overview
    let stats = app.lsm.bloom_filter_stats();
    let memtable_pct = if app.lsm.memtable_threshold() > 0 {
        (app.lsm.memtable_size() as f64 / app.lsm.memtable_threshold() as f64 * 100.0) as u16
    } else {
        0
    };

    let overview_text = vec![
        Line::from(vec![
            Span::styled("  MemTable Entries: ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{}", app.lsm.len()),
                Style::default().fg(Color::Cyan).bold(),
            ),
        ]),
        Line::from(vec![
            Span::styled("  MemTable Size:    ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!(
                    "{} / {} bytes",
                    app.lsm.memtable_size(),
                    app.lsm.memtable_threshold()
                ),
                Style::default().fg(Color::Yellow),
            ),
        ]),
        Line::from(vec![
            Span::styled("  SSTable Count:    ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{}", app.lsm.sstable_count()),
                Style::default().fg(Color::Green).bold(),
            ),
        ]),
        Line::from(vec![
            Span::styled("  Bloom Filters:    ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{}", stats.num_filters),
                Style::default().fg(Color::Magenta).bold(),
            ),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("  Total Items:      ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{}", stats.total_items),
                Style::default().fg(Color::White),
            ),
        ]),
    ];

    let overview = Paragraph::new(overview_text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" System Overview ")
            .title_style(Style::default().fg(Color::Cyan).bold()),
    );
    f.render_widget(overview, left_chunks[0]);

    // Memtable gauge
    let gauge_block = Block::default()
        .borders(Borders::ALL)
        .title(" MemTable Fill Level ")
        .title_style(Style::default().fg(Color::Yellow).bold());

    let gauge_inner = gauge_block.inner(left_chunks[1]);
    f.render_widget(gauge_block, left_chunks[1]);

    let gauge_chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(gauge_inner);

    let gauge = Gauge::default()
        .gauge_style(
            Style::default()
                .fg(if memtable_pct > 80 {
                    Color::Red
                } else if memtable_pct > 50 {
                    Color::Yellow
                } else {
                    Color::Green
                })
                .bg(Color::DarkGray),
        )
        .percent(memtable_pct.min(100))
        .label(format!("{}%", memtable_pct));
    f.render_widget(gauge, gauge_chunks[0]);

    let gauge_info = Paragraph::new(vec![Line::from(if memtable_pct >= 100 {
        Span::styled(
            "  Will flush on next write!",
            Style::default().fg(Color::Red).bold(),
        )
    } else {
        Span::styled(
            format!("  {}% until flush", 100 - memtable_pct),
            Style::default().fg(Color::Gray),
        )
    })]);
    f.render_widget(gauge_info, gauge_chunks[1]);

    // Bloom filter effectiveness
    let skip_rate = stats.skip_rate() * 100.0;
    let bloom_text = vec![
        Line::from(vec![
            Span::styled("  Skip Rate: ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:.1}%", skip_rate),
                Style::default()
                    .fg(if skip_rate > 70.0 {
                        Color::Green
                    } else if skip_rate > 30.0 {
                        Color::Yellow
                    } else {
                        Color::Red
                    })
                    .bold(),
            ),
        ]),
        Line::from(vec![
            Span::styled("  Reads Skipped:   ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{}", stats.checks_negative),
                Style::default().fg(Color::Green),
            ),
        ]),
        Line::from(vec![
            Span::styled("  Reads Proceeded: ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{}", stats.checks_positive),
                Style::default().fg(Color::Yellow),
            ),
        ]),
        Line::from(vec![
            Span::styled("  Total Checks:    ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{}", stats.total_checks()),
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("  Memory Used:     ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{} bytes", stats.total_size_bytes),
                Style::default().fg(Color::Cyan),
            ),
        ]),
    ];

    let bloom_overview = Paragraph::new(bloom_text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Bloom Filter Stats ")
            .title_style(Style::default().fg(Color::Magenta).bold()),
    );
    f.render_widget(bloom_overview, right_chunks[0]);

    // Operation history display
    let history_items: Vec<ListItem> = app
        .operation_history
        .iter()
        .rev()
        .take(5)
        .map(|op| match op {
            Operation::Put(key, value) => ListItem::new(Line::from(vec![
                Span::styled(" PUT ", Style::default().fg(Color::Black).bg(Color::Green)),
                Span::styled(format!(" {} ", key), Style::default().fg(Color::Cyan)),
                Span::styled("= ", Style::default().fg(Color::Gray)),
                Span::styled(value.clone(), Style::default().fg(Color::White)),
            ])),
            Operation::Get(key, found) => ListItem::new(Line::from(vec![
                Span::styled(" GET ", Style::default().fg(Color::Black).bg(Color::Cyan)),
                Span::styled(format!(" {} ", key), Style::default().fg(Color::Cyan)),
                if *found {
                    Span::styled("[found]", Style::default().fg(Color::Green))
                } else {
                    Span::styled("[not found]", Style::default().fg(Color::Red))
                },
            ])),
            Operation::Flush => ListItem::new(Line::from(vec![
                Span::styled(
                    " FLUSH ",
                    Style::default().fg(Color::Black).bg(Color::Yellow),
                ),
                Span::styled(" MemTable -> SSTable", Style::default().fg(Color::Yellow)),
            ])),
        })
        .collect();

    let history_list = List::new(history_items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Recent Operations ")
            .title_style(Style::default().fg(Color::Green).bold()),
    );
    f.render_widget(history_list, right_chunks[1]);
}

fn render_memtable(f: &mut Frame, app: &mut App, area: Rect) {
    let entries = app.lsm.memtable_entries();

    let items: Vec<ListItem> = entries
        .iter()
        .enumerate()
        .map(|(i, (k, v))| {
            let key_str = String::from_utf8_lossy(k);
            let value_str = String::from_utf8_lossy(v);
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:4} ", i + 1),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("{}", key_str),
                    Style::default().fg(Color::Cyan).bold(),
                ),
                Span::styled(" = ", Style::default().fg(Color::Gray)),
                Span::styled(format!("{}", value_str), Style::default().fg(Color::White)),
            ]))
        })
        .collect();

    let title = format!(
        " MemTable ({} entries, {} bytes) ",
        entries.len(),
        app.lsm.memtable_size()
    );

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .title_style(Style::default().fg(Color::Yellow).bold()),
        )
        .highlight_style(Style::default().add_modifier(Modifier::BOLD));

    f.render_widget(list, area);

    if entries.is_empty() {
        let empty_msg = Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled(
                "MemTable is empty",
                Style::default().fg(Color::DarkGray),
            )),
            Line::from(""),
            Line::from(Span::styled(
                "Press 'p' to add a key-value pair",
                Style::default().fg(Color::Gray),
            )),
            Line::from(Span::styled(
                "Press 'd' to run auto-demo",
                Style::default().fg(Color::Gray),
            )),
        ])
        .alignment(Alignment::Center);
        f.render_widget(empty_msg, area);
    }
}

fn render_sstables(f: &mut Frame, app: &mut App, area: Rect) {
    let sstable_count = app.lsm.sstable_count();

    if sstable_count == 0 {
        let empty_msg = Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled(
                "No SSTables on disk",
                Style::default().fg(Color::DarkGray),
            )),
            Line::from(""),
            Line::from(Span::styled(
                "Add data and press 'f' to flush, or run auto-demo with 'd'",
                Style::default().fg(Color::Gray),
            )),
        ])
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" SSTables ")
                .title_style(Style::default().fg(Color::Green).bold()),
        );
        f.render_widget(empty_msg, area);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(25), Constraint::Min(30)])
        .split(area);

    // SSTable list
    let sstable_items: Vec<ListItem> = (0..sstable_count)
        .map(|i| {
            let marker = if i == app.selected_sstable { ">" } else { " " };
            let style = if i == app.selected_sstable {
                Style::default().fg(Color::Yellow).bold()
            } else {
                Style::default().fg(Color::White)
            };
            ListItem::new(Line::from(vec![
                Span::styled(format!("{} ", marker), Style::default().fg(Color::Yellow)),
                Span::styled(format!("SSTable {}", i), style),
            ]))
        })
        .collect();

    let sstable_list = List::new(sstable_items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" SSTables ({}) ", sstable_count))
            .title_style(Style::default().fg(Color::Green).bold()),
    );
    f.render_widget(sstable_list, chunks[0]);

    // SSTable content
    if let Some(entries) = app.lsm.read_sstable_entries(app.selected_sstable) {
        let items: Vec<ListItem> = entries
            .iter()
            .skip(app.sstable_scroll)
            .take(area.height.saturating_sub(4) as usize)
            .enumerate()
            .map(|(i, (k, v))| {
                let key_str = String::from_utf8_lossy(k);
                let value_str = String::from_utf8_lossy(v);
                ListItem::new(Line::from(vec![
                    Span::styled(
                        format!("{:4} ", i + 1 + app.sstable_scroll),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(format!("{}", key_str), Style::default().fg(Color::Cyan)),
                    Span::styled(" = ", Style::default().fg(Color::Gray)),
                    Span::styled(format!("{}", value_str), Style::default().fg(Color::White)),
                ]))
            })
            .collect();

        let bloom_stats = app.lsm.bloom_filter_stats();
        let bf_info = if app.selected_sstable < bloom_stats.individual_stats.len() {
            let stat = &bloom_stats.individual_stats[app.selected_sstable];
            format!(
                " [BF: {} items, {:.1}% FPP] ",
                stat.num_items,
                stat.estimated_fpp * 100.0
            )
        } else {
            String::new()
        };

        let content = List::new(items).block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(
                    " SSTable {} ({} entries){} ",
                    app.selected_sstable,
                    entries.len(),
                    bf_info
                ))
                .title_style(Style::default().fg(Color::Cyan).bold()),
        );
        f.render_widget(content, chunks[1]);
    }
}

fn render_bloom_filters(f: &mut Frame, app: &mut App, area: Rect) {
    let stats = app.lsm.bloom_filter_stats();

    if stats.num_filters == 0 {
        let empty_msg = Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled(
                "No Bloom Filters yet",
                Style::default().fg(Color::DarkGray),
            )),
            Line::from(""),
            Line::from(Span::styled(
                "Bloom filters are created when SSTables are flushed to disk",
                Style::default().fg(Color::Gray),
            )),
        ])
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Bloom Filters ")
                .title_style(Style::default().fg(Color::Magenta).bold()),
        );
        f.render_widget(empty_msg, area);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(8), Constraint::Min(5)])
        .split(area);

    // Summary
    let skip_rate = stats.skip_rate() * 100.0;
    let summary_text = vec![
        Line::from(vec![
            Span::styled("  Total Filters: ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{}", stats.num_filters),
                Style::default().fg(Color::Magenta).bold(),
            ),
            Span::raw("    "),
            Span::styled("Total Size: ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{} bytes", stats.total_size_bytes),
                Style::default().fg(Color::Cyan),
            ),
            Span::raw("    "),
            Span::styled("Total Items: ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{}", stats.total_items),
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("  Effectiveness: ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{:.1}% skip rate", skip_rate),
                Style::default()
                    .fg(if skip_rate > 70.0 {
                        Color::Green
                    } else if skip_rate > 30.0 {
                        Color::Yellow
                    } else {
                        Color::Red
                    })
                    .bold(),
            ),
            Span::raw("  ("),
            Span::styled(
                format!("{} skipped", stats.checks_negative),
                Style::default().fg(Color::Green),
            ),
            Span::raw(" / "),
            Span::styled(
                format!("{} proceeded", stats.checks_positive),
                Style::default().fg(Color::Yellow),
            ),
            Span::raw(")"),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "  Higher skip rate = more disk reads avoided = better performance!",
            Style::default().fg(Color::DarkGray).italic(),
        )),
    ];

    let summary = Paragraph::new(summary_text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Bloom Filter Summary ")
            .title_style(Style::default().fg(Color::Magenta).bold()),
    );
    f.render_widget(summary, chunks[0]);

    // Per-filter details
    let items: Vec<ListItem> = stats
        .individual_stats
        .iter()
        .enumerate()
        .map(|(i, stat)| {
            let fill_bar = create_fill_bar(stat.fill_ratio, 20);
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("  BF {} ", i),
                    Style::default().fg(Color::Magenta).bold(),
                ),
                Span::styled(
                    format!("items:{:4} ", stat.num_items),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format!("bits:{:5} ", stat.num_bits),
                    Style::default().fg(Color::Cyan),
                ),
                Span::styled(
                    format!("hashes:{:2} ", stat.num_hashes),
                    Style::default().fg(Color::Yellow),
                ),
                Span::styled("fill:", Style::default().fg(Color::Gray)),
                Span::styled(fill_bar, Style::default().fg(Color::Green)),
                Span::styled(
                    format!(" fpp:{:.2}%", stat.estimated_fpp * 100.0),
                    Style::default().fg(if stat.estimated_fpp < 0.02 {
                        Color::Green
                    } else if stat.estimated_fpp < 0.05 {
                        Color::Yellow
                    } else {
                        Color::Red
                    }),
                ),
            ]))
        })
        .collect();

    let details = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Per-SSTable Bloom Filters ")
            .title_style(Style::default().fg(Color::Cyan).bold()),
    );
    f.render_widget(details, chunks[1]);
}

fn create_fill_bar(ratio: f64, width: usize) -> String {
    let filled = (ratio * width as f64).round() as usize;
    let empty = width.saturating_sub(filled);
    format!("[{}{}]", "█".repeat(filled), "░".repeat(empty))
}

fn render_status_bar(f: &mut Frame, app: &App, area: Rect) {
    let mode_text = match app.input_mode {
        InputMode::Normal => "NORMAL",
        InputMode::EnteringKey => "INSERT KEY",
        InputMode::EnteringValue => "INSERT VALUE",
        InputMode::Searching => "SEARCH",
    };

    let mode_color = match app.input_mode {
        InputMode::Normal => Color::Green,
        InputMode::EnteringKey | InputMode::EnteringValue => Color::Yellow,
        InputMode::Searching => Color::Cyan,
    };

    let demo_status = if app.auto_demo {
        Span::styled(
            " [DEMO RUNNING] ",
            Style::default().fg(Color::Magenta).bold(),
        )
    } else {
        Span::raw("")
    };

    let status = Paragraph::new(Line::from(vec![
        Span::styled(
            format!(" {} ", mode_text),
            Style::default().bg(mode_color).fg(Color::Black).bold(),
        ),
        Span::raw(" "),
        demo_status,
        Span::raw(" "),
        Span::styled("p", Style::default().fg(Color::Yellow).bold()),
        Span::styled(":put ", Style::default().fg(Color::Gray)),
        Span::styled("g", Style::default().fg(Color::Yellow).bold()),
        Span::styled(":get ", Style::default().fg(Color::Gray)),
        Span::styled("f", Style::default().fg(Color::Yellow).bold()),
        Span::styled(":flush ", Style::default().fg(Color::Gray)),
        Span::styled("d", Style::default().fg(Color::Yellow).bold()),
        Span::styled(":demo ", Style::default().fg(Color::Gray)),
        Span::styled("h", Style::default().fg(Color::Yellow).bold()),
        Span::styled(":help ", Style::default().fg(Color::Gray)),
        Span::styled("q", Style::default().fg(Color::Yellow).bold()),
        Span::styled(":quit", Style::default().fg(Color::Gray)),
    ]))
    .block(Block::default().borders(Borders::ALL));
    f.render_widget(status, area);
}

fn render_messages(f: &mut Frame, app: &App, area: Rect) {
    let messages: Vec<ListItem> = app
        .messages
        .iter()
        .rev()
        .take(3)
        .rev()
        .map(|(_, msg, msg_type)| {
            let color = match msg_type {
                MessageType::Info => Color::Cyan,
                MessageType::Success => Color::Green,
                MessageType::Warning => Color::Yellow,
                MessageType::Error => Color::Red,
            };
            ListItem::new(Line::from(vec![
                Span::styled("  ", Style::default()),
                Span::styled(msg.clone(), Style::default().fg(color)),
            ]))
        })
        .collect();

    let messages_list = List::new(messages).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Messages ")
            .title_style(Style::default().fg(Color::Blue)),
    );
    f.render_widget(messages_list, area);
}

fn render_input_popup(f: &mut Frame, app: &App) {
    let area = centered_rect(60, 30, f.area());

    f.render_widget(Clear, area);

    let (title, content) = match app.input_mode {
        InputMode::EnteringKey => (
            " Enter Key ",
            vec![
                Line::from(""),
                Line::from(vec![
                    Span::styled("  Key: ", Style::default().fg(Color::Gray)),
                    Span::styled(&app.key_input, Style::default().fg(Color::Cyan).bold()),
                    Span::styled("_", Style::default().fg(Color::White).rapid_blink()),
                ]),
                Line::from(""),
                Line::from(Span::styled(
                    "  Press Enter to continue, Esc to cancel",
                    Style::default().fg(Color::DarkGray),
                )),
            ],
        ),
        InputMode::EnteringValue => (
            " Enter Value ",
            vec![
                Line::from(""),
                Line::from(vec![
                    Span::styled("  Key:   ", Style::default().fg(Color::Gray)),
                    Span::styled(&app.key_input, Style::default().fg(Color::Cyan)),
                ]),
                Line::from(vec![
                    Span::styled("  Value: ", Style::default().fg(Color::Gray)),
                    Span::styled(&app.value_input, Style::default().fg(Color::Yellow).bold()),
                    Span::styled("_", Style::default().fg(Color::White).rapid_blink()),
                ]),
                Line::from(""),
                Line::from(Span::styled(
                    "  Press Enter to save, Esc to cancel",
                    Style::default().fg(Color::DarkGray),
                )),
            ],
        ),
        InputMode::Searching => {
            let result_line = match &app.search_result {
                Some(SearchResult::Found(v)) => Line::from(vec![
                    Span::styled("  Result: ", Style::default().fg(Color::Gray)),
                    Span::styled(v, Style::default().fg(Color::Green).bold()),
                ]),
                Some(SearchResult::NotFound) => Line::from(Span::styled(
                    "  Result: NOT FOUND",
                    Style::default().fg(Color::Red),
                )),
                None => Line::from(""),
            };
            (
                " Search Key ",
                vec![
                    Line::from(""),
                    Line::from(vec![
                        Span::styled("  Key: ", Style::default().fg(Color::Gray)),
                        Span::styled(&app.search_input, Style::default().fg(Color::Cyan).bold()),
                        Span::styled("_", Style::default().fg(Color::White).rapid_blink()),
                    ]),
                    result_line,
                    Line::from(""),
                    Line::from(Span::styled(
                        "  Press Enter to search, Esc to close",
                        Style::default().fg(Color::DarkGray),
                    )),
                ],
            )
        }
        InputMode::Normal => return,
    };

    let popup = Paragraph::new(content).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow))
            .title(title)
            .title_style(Style::default().fg(Color::Yellow).bold()),
    );
    f.render_widget(popup, area);
}

fn render_help_popup(f: &mut Frame) {
    let area = centered_rect(70, 70, f.area());

    f.render_widget(Clear, area);

    let help_text = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  LSM Tree Interactive Explorer",
            Style::default().fg(Color::Cyan).bold(),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "  Navigation:",
            Style::default().fg(Color::Yellow).bold(),
        )),
        Line::from("    1-4, Tab    Switch between tabs"),
        Line::from("    j/k, ↑/↓    Scroll through entries"),
        Line::from("    ←/→         Switch SSTable (in SSTable view)"),
        Line::from(""),
        Line::from(Span::styled(
            "  Operations:",
            Style::default().fg(Color::Yellow).bold(),
        )),
        Line::from("    p, i        Put a new key-value pair"),
        Line::from("    g, /        Get/search for a key"),
        Line::from("    f           Flush memtable to SSTable"),
        Line::from("    r           Reset Bloom filter statistics"),
        Line::from(""),
        Line::from(Span::styled(
            "  Demo:",
            Style::default().fg(Color::Yellow).bold(),
        )),
        Line::from("    d           Toggle auto-demo mode"),
        Line::from(""),
        Line::from(Span::styled(
            "  General:",
            Style::default().fg(Color::Yellow).bold(),
        )),
        Line::from("    h           Show/hide this help"),
        Line::from("    q           Quit"),
        Line::from("    Esc         Cancel current operation"),
        Line::from(""),
        Line::from(Span::styled(
            "  Press any key to close this help",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let help = Paragraph::new(help_text).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan))
            .title(" Help ")
            .title_style(Style::default().fg(Color::Cyan).bold()),
    );
    f.render_widget(help, area);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
