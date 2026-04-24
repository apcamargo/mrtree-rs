//! Logging initialization for the CLI.

use std::fmt;
use std::io::{self, IsTerminal};

use nu_ansi_term::{Color, Style};
use time::{OffsetDateTime, UtcOffset};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::{
    FmtContext,
    format::{FormatEvent, FormatFields, Writer},
};
use tracing_subscriber::registry::LookupSpan;

/// Installs the tracing subscriber for the CLI process.
pub fn init(verbose: u8) -> Result<(), tracing::subscriber::SetGlobalDefaultError> {
    let ansi_enabled = io::stderr().is_terminal();
    let formatter = CliLogFormatter::new(TimestampClock::detect());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(level_filter(verbose))
        .with_writer(io::stderr)
        .with_ansi(ansi_enabled)
        .event_format(formatter)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

// Keep mrtree-rs's CLI verbosity contract while using a compact, styled CLI
// log format.
fn level_filter(verbose: u8) -> LevelFilter {
    match verbose {
        0 => LevelFilter::WARN,
        1 => LevelFilter::INFO,
        2 => LevelFilter::DEBUG,
        _ => LevelFilter::TRACE,
    }
}

/// A compact formatter tuned for CLI output rather than telemetry output.
#[derive(Debug, Clone)]
struct CliLogFormatter {
    clock: TimestampClock,
}

impl CliLogFormatter {
    fn new(clock: TimestampClock) -> Self {
        Self { clock }
    }

    fn timestamp(&self) -> String {
        self.clock.format_now()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimestampClock {
    offset: UtcOffset,
    show_utc_suffix: bool,
}

impl TimestampClock {
    fn detect() -> Self {
        match UtcOffset::current_local_offset() {
            Ok(offset) => Self {
                offset,
                show_utc_suffix: false,
            },
            Err(_) => Self {
                offset: UtcOffset::UTC,
                show_utc_suffix: true,
            },
        }
    }

    fn format_now(self) -> String {
        let local_time = OffsetDateTime::now_utc().to_offset(self.offset);
        let mut timestamp = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            local_time.year(),
            u8::from(local_time.month()),
            local_time.day(),
            local_time.hour(),
            local_time.minute(),
            local_time.second()
        );

        if self.show_utc_suffix {
            timestamp.push_str(" UTC");
        }

        timestamp
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct EventContent {
    message: Option<String>,
    fields: Vec<EventField>,
}

impl EventContent {
    fn from_event(event: &Event<'_>) -> Self {
        let mut visitor = EventContentVisitor::default();
        event.record(&mut visitor);
        visitor.content
    }

    fn fields_text(&self) -> String {
        let mut rendered = String::new();

        for (index, field) in self.fields.iter().enumerate() {
            if index > 0 {
                rendered.push(' ');
            }
            rendered.push_str(&field.name);
            rendered.push('=');
            rendered.push_str(&field.value);
        }

        rendered
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventField {
    name: String,
    value: String,
}

#[derive(Debug, Default)]
struct EventContentVisitor {
    content: EventContent,
}

impl EventContentVisitor {
    fn record_value(&mut self, field: &Field, value: &str) {
        let sanitized_value = value.replace('\u{1b}', "\\u{1b}");

        if field.name() == "message" {
            self.content.message = Some(sanitized_value);
            return;
        }

        self.content.fields.push(EventField {
            name: field.name().trim_start_matches("r#").to_string(),
            value: sanitized_value,
        });
    }
}

impl Visit for EventContentVisitor {
    fn record_f64(&mut self, field: &Field, value: f64) {
        let rendered = value.to_string();
        self.record_value(field, &rendered);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        let rendered = value.to_string();
        self.record_value(field, &rendered);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        let rendered = value.to_string();
        self.record_value(field, &rendered);
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        let rendered = value.to_string();
        self.record_value(field, &rendered);
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        let rendered = value.to_string();
        self.record_value(field, &rendered);
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        let rendered = value.to_string();
        self.record_value(field, &rendered);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_value(field, value);
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        let rendered = value.to_string();
        self.record_value(field, &rendered);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let rendered = format!("{value:?}");
        self.record_value(field, &rendered);
    }
}

impl<S, N> FormatEvent<S, N> for CliLogFormatter
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let ansi_enabled = writer.has_ansi_escapes();
        let content = EventContent::from_event(event);
        let timestamp = self.timestamp();
        let level = event.metadata().level().to_string();

        write_styled(
            &mut writer,
            level_style(*event.metadata().level()),
            &level,
            ansi_enabled,
        )?;
        writer.write_str(" ")?;
        write_styled(&mut writer, Style::new().dimmed(), "|", ansi_enabled)?;
        writer.write_str(" ")?;
        write_styled(&mut writer, Style::new().dimmed(), &timestamp, ansi_enabled)?;
        writer.write_str(" ")?;
        write_styled(&mut writer, Style::new().dimmed(), "|", ansi_enabled)?;
        writer.write_str(" ")?;

        if let Some(message) = &content.message {
            writer.write_str(message)?;
        }

        if !content.fields.is_empty() {
            if content.message.is_some() {
                writer.write_str("  ")?;
            }

            let fields = content.fields_text();
            write_styled(&mut writer, Style::new().dimmed(), &fields, ansi_enabled)?;
        }

        writeln!(writer)
    }
}

fn level_style(level: Level) -> Style {
    match level {
        Level::ERROR => Color::Red.bold(),
        Level::WARN => Color::Yellow.bold(),
        Level::INFO => Color::Cyan.bold(),
        Level::DEBUG => Color::Blue.bold(),
        Level::TRACE => Color::Purple.bold(),
    }
}

fn write_styled(
    writer: &mut Writer<'_>,
    style: Style,
    text: &str,
    ansi_enabled: bool,
) -> fmt::Result {
    if ansi_enabled {
        write!(writer, "{}", style.paint(text))
    } else {
        writer.write_str(text)
    }
}
