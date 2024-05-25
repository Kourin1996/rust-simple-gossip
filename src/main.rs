use clap::Parser;
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
    println!("Hello, world!");

    let args = Args::parse();
    println!("{:?}", args);

    let env_filter = tracing_subscriber::EnvFilter::new(if args.debug { "debug" } else { "info" });
    let stdout_formatter = fmt::format().compact();

    let stdout_log = tracing_subscriber::fmt::layer()
        .event_format(stdout_formatter)
        .with_filter(env_filter);

    tracing_subscriber::registry().with(stdout_log).init();

    print_log();
}

fn print_log() {
    tracing::debug!("Debugging enabled 1: {}", true);

    tracing::info!("Debugging enabled 2: {}", false);
}
