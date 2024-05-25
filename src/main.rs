use clap::Parser;

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
}
