mod logger;

use crate::logger::setup_logger;
use clap::Parser;
use gossip::app::GossipApp;

#[derive(Parser, Debug)]
struct Args {
    /// The period in seconds for sending gossip messages
    #[arg(
        long,
        value_parser = clap::value_parser!(u32).range(1..))
    ]
    period: u32,

    /// The port to listen on
    #[arg(
        long,
        value_parser = clap::value_parser!(u16).range(1..))
    ]
    port: u16,

    /// The initial peers to connect to
    #[clap(short, long)]
    connect: Option<String>,

    /// Enable debug logging
    #[clap(short, long)]
    debug: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    setup_logger(args.debug);

    if args.debug {
        tracing::debug!("Args: {:?}", args);
    }

    let initial_peers: Vec<String> = args
        .connect
        .map_or_else(Vec::new, |s| s.split(',').map(|s| s.to_string()).collect());

    let app = GossipApp::new(args.port, args.period, initial_peers);

    app.run().await;
}
