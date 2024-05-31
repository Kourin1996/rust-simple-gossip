use std::fmt;
use std::time::Instant;
use tracing::field::{Field, Visit};
use tracing::Level;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::FormatEvent;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt as tracing_fmt, Layer};

// CustomVisitor is a custom visitor to extract the message field
struct CustomVisitor {
    message: String,
}

impl CustomVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
        }
    }
}

impl Visit for CustomVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        }
    }
}

// CustomFormatter is a custom formatter for production
// to print the elapsed time and message
struct CustomFormatter {
    start_time: Instant,
}

impl CustomFormatter {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
        }
    }
}

impl<S, N> FormatEvent<S, N> for CustomFormatter
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> tracing_subscriber::fmt::format::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &tracing_fmt::FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> fmt::Result {
        let elapsed = self.start_time.elapsed();
        let total_secs = elapsed.as_secs();
        let hours = total_secs / 3600;
        let minutes = (total_secs / 60) % 60;
        let seconds = total_secs % 60;
        write!(
            &mut writer,
            "# {:02}:{:02}:{:02} - ",
            hours, minutes, seconds
        )?;

        let mut visitor = CustomVisitor::new();
        event.record(&mut visitor);
        let message = visitor.message;

        write!(&mut writer, "{}", message)?;

        writeln!(&mut writer)
    }
}

pub fn setup_logger(is_debug: bool) {
    let logger_level = if is_debug { Level::DEBUG } else { Level::INFO };

    let env_filter = tracing_subscriber::EnvFilter::new(logger_level.as_str());

    if is_debug {
        // use default compact format for debug
        let stdout_formatter = tracing_fmt::format().compact();
        let logger = tracing_subscriber::fmt::layer()
            .event_format(stdout_formatter)
            .with_filter(env_filter);
        tracing_subscriber::registry().with(logger).init();
    } else {
        // use custom format for production
        let custom_formatter = CustomFormatter::new();
        let logger = tracing_subscriber::fmt::layer()
            .event_format(custom_formatter)
            .with_filter(env_filter);
        tracing_subscriber::registry().with(logger).init();
    };
}
