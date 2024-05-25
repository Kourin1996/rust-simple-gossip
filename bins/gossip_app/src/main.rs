use clap::Parser;
use gossip::GossipApp;
use tracing::Level;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, Layer};

#[derive(Parser, Debug)]
struct Args {
    #[arg(
        long,
        value_parser = clap::value_parser!(u32).range(1..))
    ]
    period: u32,

    #[arg(
        long,
        value_parser = clap::value_parser!(u16).range(1..))
    ]
    port: u16,

    #[clap(short, long)]
    connect: Option<String>,

    #[clap(short, long)]
    debug: bool,
}

fn main() {
    let args = Args::parse();

    setup_logger(args.debug);

    if args.debug {
        tracing::debug!("Args: {:?}", args);
    }

    let initial_peers: Vec<String> = args
        .connect
        .map_or_else(Vec::new, |s| s.split(',').map(|s| s.to_string()).collect());

    let app = GossipApp::new(args.port, args.period, initial_peers);
    app.run();
}

fn setup_logger(is_debug: bool) {
    let logger_level = if is_debug { Level::DEBUG } else { Level::INFO };

    let env_filter = tracing_subscriber::EnvFilter::new(logger_level.as_str());
    let stdout_formatter = fmt::format().compact();

    let stdout_log = tracing_subscriber::fmt::layer()
        .event_format(stdout_formatter)
        .with_filter(env_filter);

    tracing_subscriber::registry().with(stdout_log).init();
}
